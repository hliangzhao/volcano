/*
Copyright 2021 hliangzhao.

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

package state

import (
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
)

type openState struct {
	queue *schedulingv1alpha1.Queue
}

func (os *openState) Execute(action busv1alpha1.Action) error {
	switch action {
	case busv1alpha1.OpenQueueAction:
		return SyncQueue(os.queue, func(status *schedulingv1alpha1.QueueStatus, podGroupList []string) {
			status.State = schedulingv1alpha1.QueueStateOpen
		})
	case busv1alpha1.CloseQueueAction:
		return CloseQueue(os.queue, func(status *schedulingv1alpha1.QueueStatus, podGroupList []string) {
			if len(podGroupList) == 0 {
				status.State = schedulingv1alpha1.QueueStateClosed
				return
			}
			status.State = schedulingv1alpha1.QueueStateClosing
		})
	default:
		return SyncQueue(os.queue, func(status *schedulingv1alpha1.QueueStatus, podGroupList []string) {
			specState := os.queue.Status.State
			if len(specState) == 0 || specState == schedulingv1alpha1.QueueStateOpen {
				status.State = schedulingv1alpha1.QueueStateOpen
				return
			}
			if specState == schedulingv1alpha1.QueueStateClosed {
				if len(podGroupList) == 0 {
					status.State = schedulingv1alpha1.QueueStateClosed
					return
				}
				status.State = schedulingv1alpha1.QueueStateClosing

				return
			}
			status.State = schedulingv1alpha1.QueueStateUnknown
		})
	}
}
