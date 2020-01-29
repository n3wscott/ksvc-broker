package resources

import (
	"encoding/json"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing/pkg/apis/eventing/v1alpha1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/kmeta"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"
	"strings"
)

func GenerateServiceName(b *v1alpha1.Broker) string {
	return strings.ToLower(fmt.Sprintf("%s-knbroker", b.Name))
}

func GetLabels() map[string]string {
	return map[string]string{
		"todo": "true",
	}
}

type Args struct {
	Broker   *v1alpha1.Broker
	Image    string
	Labels   map[string]string
	Triggers []Trigger
}

func (a *Args) AddTrigger(t Trigger) {
	if a.Triggers == nil {
		a.Triggers = make([]Trigger, 0)
	}
	a.Triggers = append(a.Triggers, t)
}

type Trigger struct {
	AttributesFilter v1alpha1.TriggerFilterAttributes `json:"af,omitempty"`
	Subscriber       *apis.URL                        `json:"s,omitempty"`
}

func makePodSpec(args *Args) corev1.PodSpec {
	triggerJson, _ := json.Marshal(args.Triggers)
	if triggerJson == nil || len(triggerJson) == 0 {
		triggerJson = []byte("{}")
	}

	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{{
			Image: args.Image,
			Env: []corev1.EnvVar{{
				Name:  "TRIGGERS",
				Value: string(triggerJson),
			}}},
		},
	}
	return podSpec
}

func MakeService(args *Args) *servingv1.Service {
	podSpec := makePodSpec(args)

	return &servingv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       args.Broker.Namespace,
			Name:            GenerateServiceName(args.Broker),
			Labels:          args.Labels,
			OwnerReferences: []metav1.OwnerReference{*kmeta.NewControllerRef(args.Broker)},
		},
		Spec: servingv1.ServiceSpec{
			ConfigurationSpec: servingv1.ConfigurationSpec{
				Template: servingv1.RevisionTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: args.Labels,
					},
					Spec: servingv1.RevisionSpec{
						PodSpec: podSpec,
					},
				},
			},
		},
	}
}
