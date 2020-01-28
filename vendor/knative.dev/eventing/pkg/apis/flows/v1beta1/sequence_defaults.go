/*
 * Copyright 2020 The Knative Authors
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

package v1beta1

import (
	"context"

	messagingv1beta1 "knative.dev/eventing/pkg/apis/messaging/v1beta1"
)

func (s *Sequence) SetDefaults(ctx context.Context) {
	if s != nil && s.Spec.ChannelTemplate == nil {
		// The singleton may not have been set, if so ignore it and validation will reject the
		// Channel.
		if cd := messagingv1beta1.ChannelDefaulterSingleton; cd != nil {
			channelTemplate := cd.GetDefault(s.Namespace)
			s.Spec.ChannelTemplate = channelTemplate
		}
	}
	s.Spec.SetDefaults(ctx)
}

func (ss *SequenceSpec) SetDefaults(ctx context.Context) {}
