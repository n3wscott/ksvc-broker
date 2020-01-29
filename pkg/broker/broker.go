/*
 * Copyright 2019 The Knative Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package broker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kelseyhightower/envconfig"
	"github.com/n3wscott/ksvc-broker/pkg/reconciler/broker/resources"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/types"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	eventingv1alpha1 "knative.dev/eventing/pkg/apis/eventing/v1alpha1"
	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/eventing/pkg/utils"
	pkgtracing "knative.dev/pkg/tracing"
)

const (
	writeTimeout = 15 * time.Minute

	passFilter FilterResult = "pass"
	failFilter FilterResult = "fail"
	noFilter   FilterResult = "no_filter"

	readyz  = "/readyz"
	healthz = "/healthz"

	// TODO make these constants configurable (either as env variables, config map, or part of broker spec).
	//  Issue: https://github.com/knative/eventing/issues/1777
	// Constants for the underlying HTTP Client transport. These would enable better connection reuse.
	// Set them on a 10:1 ratio, but this would actually depend on the Triggers' subscribers and the workload itself.
	// These are magic numbers, partly set based on empirical evidence running performance workloads, and partly
	// based on what serving is doing. See https://github.com/knative/serving/blob/master/pkg/network/transports.go.
	defaultMaxIdleConnections        = 1000
	defaultMaxIdleConnectionsPerHost = 100
)

// Handler parses Cloud Events, determines if they pass a filter, and sends them to a subscriber.
type Handler struct {
	logger   *zap.SugaredLogger
	ceClient cloudevents.Client
	isReady  *atomic.Value

	TriggersJson string `envconfig:"TRIGGERS" required:"true"`
	triggers     []resources.Trigger
}

type sendError struct {
	Err    error
	Status int
}

func (e sendError) Error() string {
	return e.Err.Error()
}

func (e sendError) Unwrap() error {
	return e.Err
}

// FilterResult has the result of the filtering operation.
type FilterResult string

func NewBroker(logger *zap.SugaredLogger) (*Handler, error) {
	httpTransport, err := cloudevents.NewHTTPTransport(cloudevents.WithBinaryEncoding(), cloudevents.WithMiddleware(pkgtracing.HTTPSpanIgnoringPaths(readyz)))
	if err != nil {
		return nil, err
	}

	connectionArgs := kncloudevents.ConnectionArgs{
		MaxIdleConns:        defaultMaxIdleConnections,
		MaxIdleConnsPerHost: defaultMaxIdleConnectionsPerHost,
	}
	ceClient, err := kncloudevents.NewDefaultClientGivenHttpTransport(httpTransport, &connectionArgs)
	if err != nil {
		return nil, err
	}

	r := &Handler{
		logger:   logger,
		ceClient: ceClient,
		isReady:  &atomic.Value{},
	}
	r.isReady.Store(false)

	if err := envconfig.Process("", r); err != nil {
		return nil, err
	}

	r.triggers = make([]resources.Trigger, 0)
	if err := json.Unmarshal([]byte(r.TriggersJson), &r.triggers); err != nil {
		return nil, err
	}

	httpTransport.Handler = http.NewServeMux()
	httpTransport.Handler.HandleFunc(healthz, r.healthZ)
	httpTransport.Handler.HandleFunc(readyz, r.readyZ)

	return r, nil
}

func (r *Handler) healthZ(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(http.StatusOK)
}

func (r *Handler) readyZ(writer http.ResponseWriter, _ *http.Request) {
	if r.isReady == nil || !r.isReady.Load().(bool) {
		http.Error(writer, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	writer.WriteHeader(http.StatusOK)
}

// Start begins to receive messages for the handler.
//
// Only HTTP POST requests to the root path (/) are accepted. If other paths or
// methods are needed, use the HandleRequest method directly with another HTTP
// server.
//
// This method will block until a message is received on the stop channel.
func (r *Handler) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.ceClient.StartReceiver(ctx, r.receiver)
	}()

	// We are ready.
	r.isReady.Store(true)

	// Stop either if the receiver stops (sending to errCh) or if stopCh is closed.
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		break
	}

	// No longer ready.
	r.isReady.Store(false)

	// stopCh has been closed, we need to gracefully shutdown h.ceClient. cancel() will start its
	// shutdown, if it hasn't finished in a reasonable amount of time, just return an error.
	cancel()
	select {
	case err := <-errCh:
		return err
	case <-time.After(writeTimeout):
		return errors.New("timeout shutting down ceClient")
	}
}

func (r *Handler) receiver(ctx context.Context, event cloudevents.Event, resp *cloudevents.EventResponse) error {
	tctx := cloudevents.HTTPTransportContextFrom(ctx)
	if tctx.Method != http.MethodPost {
		resp.Status = http.StatusMethodNotAllowed
		return nil
	}

	//// tctx.URI is actually the path...
	//triggerRef, err := path.Parse(tctx.URI)
	//if err != nil {
	//	r.logger.Info("Unable to parse path as a trigger", zap.Error(err), zap.String("path", tctx.URI))
	//	return errors.New("unable to parse path as a Trigger")
	//}

	//// Remove the TTL attribute that is used by the Broker.
	//ttl, err := broker.GetTTL(event.Context)
	//if err != nil {
	//	// Only messages sent by the Broker should be here. If the attribute isn't here, then the
	//	// event wasn't sent by the Broker, so we can drop it.
	//	r.logger.Warn("No TTL seen, dropping", zap.Any("triggerRef", triggerRef), zap.Any("event", event))
	//	// Return a BadRequest error, so the upstream can decide how to handle it, e.g. sending
	//	// the message to a DLQ.
	//	resp.Status = http.StatusBadRequest
	//	return nil
	//}
	//if err := broker.DeleteTTL(event.Context); err != nil {
	//	r.logger.Warn("Failed to delete TTL.", zap.Error(err))
	//}

	//r.logger.Debug("Received message", zap.Any("triggerRef", triggerRef))

	//responseEvent, err :=
	r.sendEvent(ctx, tctx, &event)
	//if err != nil {
	//	// Propagate any error codes from the invoke back upstram.
	//	var httpError sendError
	//	if errors.As(err, &httpError) {
	//		resp.Status = httpError.Status
	//	}
	//	r.logger.Error("Error sending the event", zap.Error(err))
	//	return err
	//}
	//
	//resp.Status = http.StatusAccepted
	//if responseEvent == nil {
	//	return nil
	//}
	//
	//_ = responseEvent
	// TODO: use responseEvent

	//// Reattach the TTL (with the same value) to the response event before sending it to the Broker.
	//
	//if err := broker.SetTTL(responseEvent.Context, ttl); err != nil {
	//	return err
	//}
	//resp.Event = responseEvent
	//resp.Context = &cloudevents.HTTPTransportResponseContext{
	//	Header: utils.PassThroughHeaders(tctx.Header),
	//}

	return nil
}

// sendEvent sends an event to a subscriber if the trigger filter passes.
func (r *Handler) sendEvent(ctx context.Context, tctx cloudevents.HTTPTransportContext, event *cloudevents.Event) {
	r.logger.Infof("got cloudevent -> [%s] %s", event.ID(), event.Type())

	for _, trigger := range r.triggers {
		if eventMatchesFilter(ctx, event, trigger.AttributesFilter) {
			r.logger.Infof("matched! [%s] -> %s", event.ID(), trigger.Subscriber.URL().String())
			sendingCTX := utils.ContextFrom(tctx, trigger.Subscriber.URL())
			sendingCTX = trace.NewContext(sendingCTX, trace.FromContext(ctx))

			rctx, replyEvent, err := r.ceClient.Send(sendingCTX, *event)

			if err != nil {
				// OMG so much yolo...
				go func() {
					tctx := cloudevents.HTTPTransportContextFrom(rctx)
					r.sendEvent(rctx, tctx, replyEvent)
				}()
			}
		}
	}
}

func eventMatchesFilter(ctx context.Context, event *cloudevents.Event, attributesFilter eventingv1alpha1.TriggerFilterAttributes) bool {
	for a, v := range attributesFilter {
		a = strings.ToLower(a)
		// Find the value.
		var ev string
		switch a {
		case "specversion":
			ev = event.SpecVersion()
		case "type":
			ev = event.Type()
		case "source":
			ev = event.Source()
		case "subject":
			ev = event.Subject()
		case "id":
			ev = event.ID()
		case "time":
			ev = event.Time().String()
		case "schemaurl":
			ev = event.DataSchema()
		case "datacontenttype":
			ev = event.DataContentType()
		case "datamediatype":
			ev = event.DataMediaType()
		default:
			if exv, ok := event.Extensions()[a]; ok {
				ev, _ = types.ToString(exv)
			}
		}
		// Compare
		if v != ev {
			return false
		}
	}
	return true
}
