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

package queue

import (
	"context"
	"fmt"
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/controllers/queue/state"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"reflect"
)

func (qc *queueController) syncQueue(queue *schedulingv1alpha1.Queue, updateStateFn state.UpdateQueueStatusFn) error {
	klog.V(4).Infof("Begin to sync queue %s.", queue.Name)
	defer klog.V(4).Infof("End sync queue %s.", queue.Name)

	podgroups := qc.getPodgroups(queue.Name)
	queueStatus := schedulingv1alpha1.QueueStatus{}
	for _, pgKey := range podgroups {
		ns, name, _ := cache.SplitMetaNamespaceKey(pgKey)

		// TODO: check NotFound error and sync local cache.
		pg, err := qc.pgLister.PodGroups(ns).Get(name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// TODO: sync local cache
			}
			return err
		}

		// update according to current phase
		switch pg.Status.Phase {
		case schedulingv1alpha1.PodGroupPending:
			queueStatus.Pending++
		case schedulingv1alpha1.PodGroupRunning:
			queueStatus.Running++
		case schedulingv1alpha1.PodGroupUnknown:
			queueStatus.Unknown++
		case schedulingv1alpha1.PodGroupInqueue:
			queueStatus.Inqueue++
		}
	}
	if updateStateFn != nil {
		updateStateFn(&queueStatus, podgroups)
	} else {
		queueStatus.State = queue.Status.State
	}

	if reflect.DeepEqual(queueStatus, queue.Status) {
		return nil
	}

	// now update
	newQueue := queue.DeepCopy()
	newQueue.Status = queueStatus
	if _, err := qc.volcanoClient.SchedulingV1alpha1().Queues().UpdateStatus(context.TODO(),
		newQueue, metav1.UpdateOptions{}); err != nil {
		klog.Errorf("Failed to update status of Queue %s: %v.", newQueue.Name, err)
		return err
	}
	return nil
}

func (qc *queueController) openQueue(queue *schedulingv1alpha1.Queue, updateStateFn state.UpdateQueueStatusFn) error {
	klog.V(4).Infof("Begin to open queue %s.", queue.Name)

	newQueue := queue.DeepCopy()
	newQueue.Status.State = schedulingv1alpha1.QueueStateOpen

	if queue.Status.State != newQueue.Status.State {
		if _, err := qc.volcanoClient.SchedulingV1alpha1().Queues().Update(context.TODO(),
			newQueue, metav1.UpdateOptions{}); err != nil {
			// construct a warning event
			qc.recorder.Event(
				newQueue,
				corev1.EventTypeWarning,
				string(busv1alpha1.OpenQueueAction),
				fmt.Sprintf("Open queue failed for %v", err),
			)
			return err
		}
		// construct a normal event
		qc.recorder.Event(
			newQueue,
			corev1.EventTypeNormal,
			string(busv1alpha1.OpenQueueAction),
			"Open queue succeed",
		)
	} else {
		// nothing to do
		return nil
	}

	q, err := qc.volcanoClient.SchedulingV1alpha1().Queues().Get(context.TODO(), newQueue.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	newQueue = q.DeepCopy()
	if updateStateFn != nil {
		updateStateFn(&newQueue.Status, nil)
	} else {
		return fmt.Errorf("internal error, update state function should be provided")
	}

	if queue.Status.State != newQueue.Status.State {
		if _, err = qc.volcanoClient.SchedulingV1alpha1().Queues().UpdateStatus(context.TODO(),
			newQueue, metav1.UpdateOptions{}); err != nil {
			qc.recorder.Event(
				newQueue,
				corev1.EventTypeWarning,
				string(busv1alpha1.OpenQueueAction),
				fmt.Sprintf("Update queue status from %s to %s failed for %v",
					queue.Status.State, newQueue.Status.State, err),
			)
			return err
		}
	}
	return nil
}

func (qc *queueController) closeQueue(queue *schedulingv1alpha1.Queue, updateStateFn state.UpdateQueueStatusFn) error {
	klog.V(4).Infof("Begin to close queue %s.", queue.Name)

	newQueue := queue.DeepCopy()
	newQueue.Status.State = schedulingv1alpha1.QueueStateClosed

	if queue.Status.State != newQueue.Status.State {
		if _, err := qc.volcanoClient.SchedulingV1alpha1().Queues().Update(context.TODO(),
			newQueue, metav1.UpdateOptions{}); err != nil {
			qc.recorder.Event(
				newQueue,
				corev1.EventTypeWarning,
				string(busv1alpha1.CloseQueueAction),
				fmt.Sprintf("Close queue failed for %v", err),
			)
			return err
		}
		qc.recorder.Event(
			newQueue,
			corev1.EventTypeNormal,
			string(busv1alpha1.CloseQueueAction),
			"Close queue succeed",
		)
	} else {
		return nil
	}

	q, err := qc.volcanoClient.SchedulingV1alpha1().Queues().Get(context.TODO(), newQueue.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	newQueue = q.DeepCopy()
	podgroups := qc.getPodgroups(newQueue.Name)
	if updateStateFn != nil {
		updateStateFn(&newQueue.Status, podgroups)
	} else {
		return fmt.Errorf("internal error, update state function should be provided")
	}

	if queue.Status.State != newQueue.Status.State {
		if _, err = qc.volcanoClient.SchedulingV1alpha1().Queues().UpdateStatus(context.TODO(),
			newQueue, metav1.UpdateOptions{}); err != nil {
			qc.recorder.Event(
				newQueue,
				corev1.EventTypeWarning,
				string(busv1alpha1.CloseQueueAction),
				fmt.Sprintf("Update queue status from %s to %s failed for %v",
					queue.Status.State, newQueue.Status.State, err),
			)
			return err
		}
	}
	return nil
}
