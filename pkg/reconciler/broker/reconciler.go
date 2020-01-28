/*
Copyright 2020 The Knative Authors

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
	"k8s.io/apimachinery/pkg/labels"
	"knative.dev/pkg/resolver"
	"reflect"

	"knative.dev/pkg/reconciler"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"knative.dev/eventing/pkg/apis/eventing/v1alpha1"
	clientset "knative.dev/eventing/pkg/client/clientset/versioned"
	listers "knative.dev/eventing/pkg/client/listers/eventing/v1alpha1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/tracker"
	servingclientset "knative.dev/serving/pkg/client/clientset/versioned"
	servinglisters "knative.dev/serving/pkg/client/listers/serving/v1"
)

// Interface defines the strongly typed interfaces to be implemented by a
// controller reconciling v1alpha1.Broker.
type Interface interface {
	ReconcileBroker(context.Context, *v1alpha1.Broker) reconciler.Event
	ReconcileTrigger(context.Context, *v1alpha1.Broker, *v1alpha1.Trigger) reconciler.Event
}

var _ Interface = (*Reconciler)(nil)

var forRealz = false

// NewWarnInternal makes a new reconciler event with event type Warning, and
// reason InternalError.
func NewWarnInternal(msgf string, args ...interface{}) reconciler.Event {
	return reconciler.NewEvent(corev1.EventTypeWarning, "InternalError", msgf, args...)
}

// Reconciler implements controller.Reconciler for AddressableService resources.
type Reconciler struct {
	// Client is used to write back status updates.
	Client           clientset.Interface
	ServingClientSet servingclientset.Interface

	// Listers index properties about resources
	BrokerLister  listers.BrokerLister
	TriggerLister listers.TriggerLister

	ServiceLister servinglisters.ServiceLister

	// The tracker builds an index of what resources are watching other
	// resources so that we can immediately react to changes to changes in
	// tracked resources.
	Tracker tracker.Interface

	SubResolver *resolver.URIResolver
	BrokerImage string // TODO

	// Recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	Recorder record.EventRecorder
}

// Check that our Reconciler implements controller.Reconciler
var _ controller.Reconciler = (*Reconciler)(nil)

// Reconcile implements controller.Reconciler
func (r *Reconciler) Reconcile(ctx context.Context, key string) error {
	logger := logging.FromContext(ctx)

	logger.Infof("reconciler working on %q...", key)

	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		logger.Errorf("invalid resource key: %s", key)
		return nil
	}
	if err := r.reconcileBroker(ctx, key, namespace, name); err != nil {
		return err
	}
	return r.reconcileTrigger(ctx, key, namespace, name)
}

func isThisClass(labels map[string]string) bool {
	for k, v := range labels {
		if k == brokerClassLabel {
			if v == thisBrokerClass {
				return true
			}
		}
	}
	return false
}

func (r *Reconciler) reconcileBroker(ctx context.Context, key, namespace, name string) error {
	logger := logging.FromContext(ctx)

	// Get the resource with this namespace/name.
	original, err := r.BrokerLister.Brokers(namespace).Get(name)
	if apierrs.IsNotFound(err) {
		// The resource may no longer exist, in which case we stop processing.
		logger.Errorf("resource %q no longer exists", key)
		return nil
	} else if err != nil {
		return err
	}
	// Don't modify the informers copy.
	resource := original.DeepCopy()

	if match := isThisClass(resource.GetLabels()); !match {
		return nil
	}

	shouldReconcile := false
	for k, v := range resource.GetLabels() {
		if k == brokerClassLabel {
			if v == thisBrokerClass {
				shouldReconcile = true
			}
		}
	}
	if !shouldReconcile {
		return nil
	}

	// Reconcile this copy of the resource and then write back any status
	// updates regardless of whether the reconciliation errored out.
	reconcileEvent := r.ReconcileBroker(ctx, resource)

	if equality.Semantic.DeepEqual(original.Status, resource.Status) {
		// If we didn't change anything then don't call updateStatus.
		// This is important because the copy we loaded from the informer's
		// cache may be stale and we don't want to overwrite a prior update
		// to status with this stale state.
	} else if _, err = r.updateBrokerStatus(resource); err != nil {
		logger.Warnw("Failed to update resource status", zap.Error(err))
		r.Recorder.Eventf(resource, corev1.EventTypeWarning, "UpdateFailed",
			"Failed to update status for %q: %v", resource.Name, err)
		return err
	}
	if reconcileEvent != nil {
		logger.Error("ReconcileKind returned an error: %v", reconcileEvent)
		var event *reconciler.ReconcilerEvent
		if reconciler.EventAs(reconcileEvent, &event) {
			r.Recorder.Eventf(resource, event.EventType, event.Reason, event.Format, event.Args...)
		} else {
			r.Recorder.Event(resource, corev1.EventTypeWarning, "InternalError", reconcileEvent.Error())
		}
	}

	return reconcileEvent
}

func (r *Reconciler) reconcileTrigger(ctx context.Context, key, namespace, name string) error {
	logger := logging.FromContext(ctx)

	// Get the resource with this namespace/name.
	var broker *v1alpha1.Broker
	ogBroker, err := r.BrokerLister.Brokers(namespace).Get(name)
	if apierrs.IsNotFound(err) {
		// The resource may no longer exist, in which case we stop processing.
		logger.Errorf("broker resource %q no longer exists", key)
		broker = nil
	} else if err != nil {
		return err
	}
	// Don't modify the informers copy.
	broker = ogBroker.DeepCopy()

	// Get the resource with this namespace/name.
	originals, err := r.TriggerLister.Triggers(namespace).List(labels.Everything())
	if apierrs.IsNotFound(err) {
		// The resource may no longer exist, in which case we stop processing.
		logger.Errorf("resource %q no longer exists", key)
		return nil
	} else if err != nil {
		return err
	}

	for _, original := range originals {
		if original.Spec.Broker != name {
			continue
		}

		// Don't modify the informers copy.
		resource := original.DeepCopy()

		// Reconcile this copy of the resource and then write back any status
		// updates regardless of whether the reconciliation errored out.
		reconcileEvent := r.ReconcileTrigger(ctx, broker, resource)

		if equality.Semantic.DeepEqual(original.Status, resource.Status) {
			// If we didn't change anything then don't call updateStatus.
			// This is important because the copy we loaded from the informer's
			// cache may be stale and we don't want to overwrite a prior update
			// to status with this stale state.
		} else if _, err = r.updateTriggerStatus(resource); err != nil {
			logger.Warnw("Failed to update resource status", zap.Error(err))
			r.Recorder.Eventf(resource, corev1.EventTypeWarning, "UpdateFailed",
				"Failed to update status for %q: %v", resource.Name, err)
			return err
		}
		if reconcileEvent != nil {
			logger.Error("ReconcileKind returned an event: %v", reconcileEvent)
			var event *reconciler.ReconcilerEvent
			if reconciler.EventAs(reconcileEvent, &event) {
				r.Recorder.Eventf(resource, event.EventType, event.Reason, event.Format, event.Args...)
				if event.EventType == corev1.EventTypeNormal {
					continue
				}
			} else {
				r.Recorder.Event(resource, corev1.EventTypeWarning, "InternalError", reconcileEvent.Error())
			}
			return reconcileEvent
		}
	}
	return nil
}

// Update the Status of the resource.  Caller is responsible for checking
// for semantic differences before calling.
func (r *Reconciler) updateBrokerStatus(desired *v1alpha1.Broker) (*v1alpha1.Broker, error) {
	actual, err := r.BrokerLister.Brokers(desired.Namespace).Get(desired.Name)
	if err != nil {
		return nil, err
	}
	// If there's nothing to update, just return.
	if reflect.DeepEqual(actual.Status, desired.Status) {
		return actual, nil
	}
	// Don't modify the informers copy
	existing := actual.DeepCopy()
	existing.Status = desired.Status
	return r.Client.EventingV1alpha1().Brokers(desired.Namespace).UpdateStatus(existing)
}

// Update the Status of the resource.  Caller is responsible for checking
// for semantic differences before calling.
func (r *Reconciler) updateTriggerStatus(desired *v1alpha1.Trigger) (*v1alpha1.Trigger, error) {
	actual, err := r.TriggerLister.Triggers(desired.Namespace).Get(desired.Name)
	if err != nil {
		return nil, err
	}
	// If there's nothing to update, just return.
	if reflect.DeepEqual(actual.Status, desired.Status) {
		return actual, nil
	}
	// Don't modify the informers copy
	existing := actual.DeepCopy()
	existing.Status = desired.Status
	return r.Client.EventingV1alpha1().Triggers(desired.Namespace).UpdateStatus(existing)
}
