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
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/logging"

	"knative.dev/eventing/pkg/apis/eventing/v1alpha1"
	"knative.dev/pkg/reconciler"
)

// HACK HACK HACK

var triggerCondSet = apis.NewLivingConditionSet(
	v1alpha1.TriggerConditionBroker,
	v1alpha1.TriggerConditionSubscriberResolved)

func triggerInitializeConditions(ts *v1alpha1.TriggerStatus) {
	triggerCondSet.Manage(ts).InitializeConditions()
}

func triggerMarkSubscriberResolvedSucceeded(ts *v1alpha1.TriggerStatus) {
	triggerCondSet.Manage(ts).MarkTrue(v1alpha1.TriggerConditionSubscriberResolved)
}

func triggerMarkSubscriberResolvedFailed(ts *v1alpha1.TriggerStatus, reason, messageFormat string, messageA ...interface{}) {
	triggerCondSet.Manage(ts).MarkFalse(v1alpha1.TriggerConditionSubscriberResolved, reason, messageFormat, messageA...)
}

func triggerPropagateBrokerStatus(ts *v1alpha1.TriggerStatus, bs *v1alpha1.BrokerStatus) {
	bc := bs.GetTopLevelCondition()
	if bc == nil {
		triggerCondSet.Manage(ts).MarkUnknown(v1alpha1.TriggerConditionBroker,
			"BrokerNotConfigured", "Broker has not yet been reconciled.")
		return
	}

	switch {
	case bc.Status == corev1.ConditionUnknown:
		triggerCondSet.Manage(ts).MarkUnknown(v1alpha1.TriggerConditionBroker, bc.Reason, bc.Message)
	case bc.Status == corev1.ConditionTrue:
		triggerCondSet.Manage(ts).MarkTrue(v1alpha1.TriggerConditionBroker)
	case bc.Status == corev1.ConditionFalse:
		triggerCondSet.Manage(ts).MarkFalse(v1alpha1.TriggerConditionBroker, bc.Reason, bc.Message)
	default:
		triggerCondSet.Manage(ts).MarkUnknown(v1alpha1.TriggerConditionBroker, "BrokerUnknown", "The status of Broker is invalid: %v", bc.Status)
	}
}

// -----------------

// ReconcileKind implements Interface
func (r *Reconciler) ReconcileTrigger(ctx context.Context, b *v1alpha1.Broker, o *v1alpha1.Trigger) reconciler.Event {
	logger := logging.FromContext(ctx)
	if o.GetDeletionTimestamp() != nil {
		// Check for a DeletionTimestamp.  If present, elide the normal reconcile logic.
		// When a controller needs finalizer handling, it would go here.
		return nil
	}
	triggerInitializeConditions(&o.Status)

	logger.Infof("Reconciling Trigger: %s, for Broker %s", o.Name, b.Name)

	var err error
	if o.Spec.Subscriber.Ref != nil {
		o.Spec.Subscriber.Ref.Namespace = o.Namespace
	}
	o.Status.SubscriberURI, err = r.SubResolver.URIFromDestinationV1(o.Spec.Subscriber, b)
	if err == nil {
		triggerMarkSubscriberResolvedSucceeded(&o.Status)
	} else {
		logger.Infow("Failed to resolve subscriber.", zap.String("trigger", o.Name), zap.Error(err))
		triggerMarkSubscriberResolvedFailed(&o.Status, "ResolveSubscriberFailed", "Failed to resolve subscriber: %s", err.Error())
	}

	// Broker
	triggerPropagateBrokerStatus(&o.Status, &b.Status)

	o.Status.ObservedGeneration = o.Generation
	return err
}
