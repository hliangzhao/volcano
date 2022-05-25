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

// closingState implements the State interface.
type closingState struct {
	queue *schedulingv1alpha1.Queue
}

func (cs *closingState) Execute(action busv1alpha1.Action) error {
	switch action {
	case busv1alpha1.OpenQueueAction:
		var fn UpdateQueueStatusFn
		// fn updates `status` to `QueueStateOpen`
		fn = func(status *schedulingv1alpha1.QueueStatus, pgList []string) {
			status.State = schedulingv1alpha1.QueueStateOpen
		}
		// update cs.queue's status with fn (os.queue.Status <- status)
		return OpenQueue(cs.queue, fn)

	case busv1alpha1.CloseQueueAction:
		var fn UpdateQueueStatusFn
		// fn updates `status` to `QueueStateClosed` or `QueueStateClosing` according to pgList
		fn = func(status *schedulingv1alpha1.QueueStatus, pgList []string) {
			// queue closed until every podgroup which uses it finishes
			if len(pgList) == 0 {
				status.State = schedulingv1alpha1.QueueStateClosed
				return
			}
			status.State = schedulingv1alpha1.QueueStateClosing
		}
		// update cs.queue's status with fn (os.queue.Status <- status)
		return SyncQueue(cs.queue, fn) // TODO: why SyncQueue(), other than CloseQueue()?

	default:
		var fn UpdateQueueStatusFn
		// fn sync `status` with `cs.queue.Status`
		fn = func(status *schedulingv1alpha1.QueueStatus, pgList []string) {
			if cs.queue.Status.State == schedulingv1alpha1.QueueStateOpen {
				status.State = schedulingv1alpha1.QueueStateOpen
				return
			}
			if cs.queue.Status.State == schedulingv1alpha1.QueueStateClosing {
				if len(pgList) == 0 {
					status.State = schedulingv1alpha1.QueueStateClosed
					return
				}
				status.State = schedulingv1alpha1.QueueStateClosing
				return
			}
			status.State = schedulingv1alpha1.QueueStateUnknown
		}
		// update cs.queue's status with fn (os.queue.Status <- status)
		return SyncQueue(cs.queue, fn)
	}
}
