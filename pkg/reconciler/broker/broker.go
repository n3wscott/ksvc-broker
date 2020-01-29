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
	"encoding/json"
	"fmt"
	"github.com/n3wscott/ksvc-broker/pkg/reconciler/broker/resources"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"knative.dev/eventing/pkg/apis/eventing/v1alpha1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
)

// HACK HACK HACK
// this is going to manage custom a custom condition set for this broker.

var brokerCondSet = apis.NewLivingConditionSet(
	v1alpha1.BrokerConditionAddressable,
)

// InitializeConditions sets relevant unset conditions to Unknown state.
func brokerInitializeConditions(bs *v1alpha1.BrokerStatus) {
	brokerCondSet.Manage(bs).InitializeConditions()
}

// SetAddress makes this Broker addressable by setting the hostname. It also
// sets the BrokerConditionAddressable to true.
func brokerSetAddress(bs *v1alpha1.BrokerStatus, url *apis.URL) {
	if url != nil {
		bs.Address.Hostname = url.Host
		bs.Address.URL = url
		brokerCondSet.Manage(bs).MarkTrue(v1alpha1.BrokerConditionAddressable)
	} else {
		bs.Address.Hostname = ""
		bs.Address.URL = nil
		brokerCondSet.Manage(bs).MarkFalse(v1alpha1.BrokerConditionAddressable, "NotAddressable", "broker service has .status.addressable.url == nil")
	}
}

// -------------------

// ReconcileKind implements Interface
func (r *Reconciler) ReconcileBroker(ctx context.Context, o *v1alpha1.Broker) reconciler.Event {
	logger := logging.FromContext(ctx)
	if o.GetDeletionTimestamp() != nil {
		// Check for a DeletionTimestamp.  If present, elide the normal reconcile logic.
		// When a controller needs finalizer handling, it would go here.
		return nil
	}
	brokerInitializeConditions(&o.Status)
	o.Status.ObservedGeneration = o.Generation

	logger.Infof("Reconciling Broker: %s", o.Name)

	name := resources.GenerateServiceName(o)
	existing, err := r.ServiceLister.Services(o.Namespace).Get(name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			logger.Errorw("Unable to get an existing Service", zap.Error(err))
			return err
		}
		existing = nil
	} else if !metav1.IsControlledBy(existing, o) {
		s, _ := json.Marshal(existing)
		logger.Errorw("Broker does not own Service", zap.Any("service", s))
		return fmt.Errorf("Broker %q does not own Service: %q", o.Name, name) // TODO: should be an event.
	}

	args := &resources.Args{
		Image:  r.BrokerImage,
		Broker: o,
		Labels: resources.GetLabels(),
	}

	triggers, err := r.TriggerLister.List(labels.Everything())
	if err != nil {
		return err
	}
	for _, trigger := range triggers {
		if trigger.Spec.Broker == o.Name {
			if trigger.Status.SubscriberURI != nil {
				var af v1alpha1.TriggerFilterAttributes
				if trigger.Spec.Filter != nil && trigger.Spec.Filter.Attributes != nil {
					af = *trigger.Spec.Filter.Attributes
				}
				args.AddTrigger(resources.Trigger{
					AttributesFilter: af,
					Subscriber:       trigger.Status.SubscriberURI,
				})
			}
		}
	}

	desired := resources.MakeService(args)

	logger.Infof("Made a service, comparing it now..")

	ksvc := existing
	if existing == nil {
		ksvc, err = r.ServingClientSet.ServingV1().Services(o.Namespace).Create(desired)
		if err != nil {
			logger.Errorw("Failed to create broker service", zap.Error(err))
			return err
		}
	} else if !equality.Semantic.DeepEqual(&existing.Spec, &desired.Spec) {
		existing.Spec = desired.Spec
		ksvc, err = r.ServingClientSet.ServingV1().Services(o.Namespace).Update(existing)
		if err != nil {
			logger.Errorw("Failed to update broker service", zap.Any("service", existing), zap.Error(err))
			return err
		}
	}

	if ksvc.Status.Address != nil && ksvc.Status.Address.URL != nil {
		brokerSetAddress(&o.Status, ksvc.Status.Address.URL)
	} else {
		brokerSetAddress(&o.Status, nil)
	}

	return nil
}
