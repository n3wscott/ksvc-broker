/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package broker

import (
	"context"
	"github.com/kelseyhightower/envconfig"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing/pkg/client/injection/client"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1alpha1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1alpha1/trigger"
	servingclient "knative.dev/serving/pkg/client/injection/client"
	ksvcinformer "knative.dev/serving/pkg/client/injection/informers/serving/v1/service"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"knative.dev/eventing/pkg/apis/eventing/v1alpha1"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/tracker"
)

const (
	controllerAgentName = "ksvc-broker-controller"

	brokerClassAnnotation = "eventing.knative.dev/broker.class"
	thisBrokerClass       = "ksvc-broker"
)

type envConfig struct {
	Image string `envconfig:"BROKER_IMAGE" required:"true"`
}

// NewController returns a new HPA reconcile controller.
func NewController(
	ctx context.Context,
	cmw configmap.Watcher,
) *controller.Impl {
	logger := logging.FromContext(ctx)

	brokerInformer := brokerinformer.Get(ctx)
	triggerInformer := triggerinformer.Get(ctx)
	ksvcInformer := ksvcinformer.Get(ctx)

	c := &Reconciler{
		Client:           client.Get(ctx),
		ServingClientSet: servingclient.Get(ctx),
		BrokerLister:     brokerInformer.Lister(),
		TriggerLister:    triggerInformer.Lister(),
		ServiceLister:    ksvcInformer.Lister(),
		Recorder: record.NewBroadcaster().NewRecorder(
			scheme.Scheme, corev1.EventSource{Component: controllerAgentName}),
	}
	impl := controller.NewImpl(c, logger, "AddressableServices")

	env := &envConfig{}
	if err := envconfig.Process("", env); err != nil {
		logger.Panicf("unable to process Broker's required environment variables: %v", err)
	}

	c.BrokerImage = env.Image
	c.Tracker = tracker.New(impl.EnqueueKey, controller.GetTrackerLease(ctx))
	c.SubResolver = resolver.NewURIResolver(ctx, impl.EnqueueKey)

	logger.Info("Setting up event handlers")

	brokerInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	triggerInformer.Informer().AddEventHandler(controller.HandleAll(
		func(obj interface{}) {
			if trigger, ok := obj.(*v1alpha1.Trigger); ok {
				impl.EnqueueKey(types.NamespacedName{Namespace: trigger.Namespace, Name: trigger.Spec.Broker})
			}
		},
	))

	ksvcInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.Filter(v1alpha1.SchemeGroupVersion.WithKind("Broker")),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

	return impl
}
