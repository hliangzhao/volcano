/*
Copyright 2021-2022 The Volcano Authors.

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

// fully checked and understood

import (
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
)

// openState implements the State interface.
type openState struct {
	queue *schedulingv1alpha1.Queue
}

func (os *openState) Execute(action busv1alpha1.Action) error {
	switch action {
	case busv1alpha1.OpenQueueAction:
		var fn UpdateQueueStatusFn
		// fn updates `status` to `QueueStateOpen`
		fn = func(status *schedulingv1alpha1.QueueStatus, pgList []string) {
			status.State = schedulingv1alpha1.QueueStateOpen
		}
		// update os.queue's status with fn (os.queue.Status <- status)
		return SyncQueue(os.queue, fn) // TODO: why SyncQueue(), other than OpenQueue()?

	case busv1alpha1.CloseQueueAction:
		var fn UpdateQueueStatusFn
		// fn updates `status` to `QueueStateClosed` or `QueueStateClosing` according to pgList
		fn = func(status *schedulingv1alpha1.QueueStatus, pgList []string) {
			if len(pgList) == 0 {
				status.State = schedulingv1alpha1.QueueStateClosed
				return
			}
			status.State = schedulingv1alpha1.QueueStateClosing
		}
		// update os.queue's status with fn (os.queue.Status <- status)
		return CloseQueue(os.queue, fn)

	default:
		var fn UpdateQueueStatusFn
		// fn sync `status` with `os.queue.Status`
		fn = func(status *schedulingv1alpha1.QueueStatus, pgList []string) {
			if len(os.queue.Status.State) == 0 || os.queue.Status.State == schedulingv1alpha1.QueueStateOpen {
				status.State = schedulingv1alpha1.QueueStateOpen
				return
			}
			if os.queue.Status.State == schedulingv1alpha1.QueueStateClosed {
				if len(pgList) == 0 {
					status.State = schedulingv1alpha1.QueueStateClosed
					return
				}
				status.State = schedulingv1alpha1.QueueStateClosing

				return
			}
			status.State = schedulingv1alpha1.QueueStateUnknown
		}
		// update os.queue's status with fn (os.queue.Status <- status)
		return SyncQueue(os.queue, fn)
	}
}
