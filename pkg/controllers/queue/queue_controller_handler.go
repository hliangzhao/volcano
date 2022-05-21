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

package queue

import (
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	controllerapis "github.com/hliangzhao/volcano/pkg/controllers/apis"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// enqueue pushes an request into the work queue of the queueController.
func (qc *queueController) enqueue(req *controllerapis.Request) {
	qc.queue.Add(req)
}

func (qc *queueController) addQueue(obj interface{}) {
	queue := obj.(*schedulingv1alpha1.Queue)
	req := &controllerapis.Request{
		QueueName: queue.Name,
		Event:     busv1alpha1.OutOfSyncEvent,
		Action:    busv1alpha1.SyncQueueAction,
	}
	qc.enqueue(req)
}

func (qc *queueController) deleteQueue(obj interface{}) {
	// obj is a queue
	queue, ok := obj.(*schedulingv1alpha1.Queue)
	if !ok {
		// obj is a tombstone, and queue is placed in it
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Could not get obj from tombstone %v.", obj)
			return
		}
		queue, ok = tombstone.Obj.(*schedulingv1alpha1.Queue)
		if !ok {
			klog.Errorf("Tombstone contained object that is not a Queue: %#v.", obj)
			return
		}
	}

	qc.pgMutex.Lock()
	defer qc.pgMutex.Unlock()
	delete(qc.podgroups, queue.Name)
}

func (qc *queueController) updateQueue(_, _ interface{}) {
	// TODO: maybe I can do sth.
	// currently do not care about queue update
}

// getPodgroups returns the podgroups for the given queue with name key.
func (qc *queueController) getPodgroups(key string) []string {
	qc.pgMutex.Lock()
	defer qc.pgMutex.Unlock()

	if qc.podgroups[key] == nil {
		return nil
	}
	podgroups := make([]string, 0, len(qc.podgroups[key]))
	for pgKey := range qc.podgroups[key] {
		podgroups = append(podgroups, pgKey)
	}
	return podgroups
}

func (qc *queueController) addPodgroup(obj interface{}) {
	pg := obj.(*schedulingv1alpha1.PodGroup)
	key, _ := cache.MetaNamespaceKeyFunc(obj)

	qc.pgMutex.Lock()
	defer qc.pgMutex.Unlock()

	// the corresponding queue non-exists, create it
	if qc.podgroups[pg.Spec.Queue] == nil {
		qc.podgroups[pg.Spec.Queue] = make(map[string]struct{})
	}
	// TODO: why not add obj into the following map directly?
	qc.podgroups[pg.Spec.Queue][key] = struct{}{}

	req := &controllerapis.Request{
		QueueName: pg.Spec.Queue,
		Event:     busv1alpha1.OutOfSyncEvent,
		Action:    busv1alpha1.SyncQueueAction,
	}
	qc.enqueue(req)
}

func (qc *queueController) updatePodgroup(old, new interface{}) {
	oldPg := old.(*schedulingv1alpha1.PodGroup)
	newPg := new.(*schedulingv1alpha1.PodGroup)

	// Note: we have no use case update PodGroup.Spec.Queue
	// So do not consider it here.
	if oldPg.Status.Phase != newPg.Status.Phase {
		qc.addPodgroup(newPg)
	}
}

func (qc *queueController) deletePodgroup(obj interface{}) {
	pg, ok := obj.(*schedulingv1alpha1.PodGroup)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %v.", obj)
			return
		}
		pg, ok = tombstone.Obj.(*schedulingv1alpha1.PodGroup)
		if !ok {
			klog.Errorf("Tombstone contained object that is not a PodGroup: %v.", obj)
			return
		}
	}

	key, _ := cache.MetaNamespaceKeyFunc(obj)
	qc.pgMutex.Lock()
	defer qc.pgMutex.Unlock()

	delete(qc.podgroups[pg.Spec.Queue], key)
	req := &controllerapis.Request{
		QueueName: pg.Spec.Queue,
		Event:     busv1alpha1.OutOfSyncEvent,
		Action:    busv1alpha1.SyncQueueAction,
	}
	qc.enqueue(req)
}

func (qc *queueController) addCmd(obj interface{}) {
	cmd, ok := obj.(*busv1alpha1.Command)
	if !ok {
		klog.Errorf("Obj %v is not command.", obj)
		return
	}
	qc.cmdQueue.Add(cmd)
}

func (qc *queueController) recordEventsForQueue(name, eventType, reason, msg string) {
	queue, err := qc.queueLister.Get(name)
	if err != nil {
		klog.Errorf("Get queue %s failed for %v.", name, err)
		return
	}
	qc.recorder.Event(queue, eventType, reason, msg)
}
