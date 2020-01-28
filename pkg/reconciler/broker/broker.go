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
	"knative.dev/pkg/logging"

	"knative.dev/eventing/pkg/apis/eventing/v1alpha1"
	"knative.dev/pkg/reconciler"
)

// ReconcileKind implements Interface
func (r *Reconciler) ReconcileBroker(ctx context.Context, o *v1alpha1.Broker) reconciler.Event {
	if o.GetDeletionTimestamp() != nil {
		// Check for a DeletionTimestamp.  If present, elide the normal reconcile logic.
		// When a controller needs finalizer handling, it would go here.
		return nil
	}
	o.Status.InitializeConditions()
	o.Status.ObservedGeneration = o.Generation

	name := resources.GenerateServiceName(o)
	existing, err := r.ServiceLister.Services(o.Namespace).Get(name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			logging.FromContext(ctx).Desugar().Error("Unable to get an existing Service", zap.Error(err))
			return err
		}
		existing = nil
	} else if !metav1.IsControlledBy(existing, o) {
		s, _ := json.Marshal(existing)
		logging.FromContext(ctx).Error("Broker does not own Service", zap.Any("service", s))
		return fmt.Errorf("Broker %q does not own Service: %q", o.Name, name) // TODO: should be an event.
	}

	desired := resources.MakeService(&resources.Args{
		Image:  r.BrokerImage,
		Broker: o,
		Labels: resources.GetLabels(),
	})

	svc := existing
	if existing == nil {
		svc, err = r.ServingClientSet.ServingV1().Services(o.Namespace).Create(desired)
		if err != nil {
			logging.FromContext(ctx).Desugar().Error("Failed to create broker service", zap.Error(err))
			return err
		}
	} else if !equality.Semantic.DeepEqual(&existing.Spec, &desired.Spec) {
		existing.Spec = desired.Spec
		svc, err = r.ServingClientSet.ServingV1().Services(o.Namespace).Update(existing)
		if err != nil {
			logging.FromContext(ctx).Desugar().Error("Failed to update broker service", zap.Any("service", existing), zap.Error(err))
			return err
		}
	}
	_ = svc
	return nil
}
