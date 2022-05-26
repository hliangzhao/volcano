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

package job

// fully checked and understood

import (
	"context"
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/apis/helpers"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	controllerapis "github.com/hliangzhao/volcano/pkg/controllers/apis"
	controllercache "github.com/hliangzhao/volcano/pkg/controllers/cache"
	jobhelpers "github.com/hliangzhao/volcano/pkg/controllers/job/helpers"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"reflect"
	"strconv"
)

// addCommand adds obj (a cmd) to the work-queue jc.cmdQueue.
func (jc *jobController) addCommand(obj interface{}) {
	cmd, ok := obj.(*busv1alpha1.Command)
	if !ok {
		klog.Errorf("obj is not Command")
		return
	}
	jc.cmdQueue.Add(cmd)
}

// addJob adds obj (a job) to the job work-queue. Use key to find the right work-queue.
// Also add the job to local store (cache).
func (jc *jobController) addJob(obj interface{}) {
	job, ok := obj.(*batchv1alpha1.Job)
	if !ok {
		klog.Errorf("obj is not Job")
		return
	}

	req := controllerapis.Request{
		Namespace: job.Namespace,
		JobName:   job.Name,
		Event:     busv1alpha1.OutOfSyncEvent,
	}

	// TODO: if failed to add job, the cache should be refresh
	if err := jc.cache.Add(job); err != nil {
		klog.Errorf("Failed to add job <%s/%s> to cache, error: %v", job.Namespace, job.Name, err)
	}
	key := jobhelpers.GetJobKeyByReq(&req)
	queue := jc.getWorkerQueue(key)
	queue.Add(req)
}

// updateJob updates job (from oldObj to newObj) to job controller's work queue.
// Also updated in cache.
func (jc *jobController) updateJob(oldObj, newObj interface{}) {
	oldJob, ok := oldObj.(*batchv1alpha1.Job)
	if !ok {
		klog.Errorf("oldObj is not Job")
		return
	}
	newJob, ok := newObj.(*batchv1alpha1.Job)
	if !ok {
		klog.Errorf("newObj is not Job")
		return
	}

	if newJob.ResourceVersion == oldJob.ResourceVersion {
		klog.V(6).Infof("No need to update because job is not modified.")
		return
	}

	if err := jc.cache.Update(newJob); err != nil {
		klog.Errorf("UpdateJob - Failed to update job <%s/%s>: %v in cache",
			newJob.Namespace, newJob.Name, err)
	}

	// NOTE: Since we only reconcile job based on Spec, we will ignore other attributes
	// For Job status, it's used internally and always been updated via our controller.
	if reflect.DeepEqual(newJob.Spec, oldJob.Spec) && newJob.Status.State.Phase == oldJob.Status.State.Phase {
		klog.V(6).Infof("Job update event is ignored since no update in 'Spec'.")
		return
	}

	req := controllerapis.Request{
		Namespace: newJob.Namespace,
		JobName:   newJob.Name,
		Event:     busv1alpha1.OutOfSyncEvent,
	}
	key := jobhelpers.GetJobKeyByReq(&req)
	queue := jc.getWorkerQueue(key)
	queue.Add(req)
}

// deleteJob deletes obj (a job) from job controller.
// Also deleted from cache.
func (jc *jobController) deleteJob(obj interface{}) {
	job, ok := obj.(*batchv1alpha1.Job)
	if !ok {
		// If we reached here it means the Job was deleted but its final state is unrecorded.
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %#v", obj)
			return
		}
		job, ok = tombstone.Obj.(*batchv1alpha1.Job)
		if !ok {
			klog.Errorf("Tombstone contained object that is not a volcano Job: %#v", obj)
			return
		}
	}

	if err := jc.cache.Delete(job); err != nil {
		klog.Errorf("Failed to delete job <%s/%s>: %v in cache",
			job.Namespace, job.Name, err)
	}
}

// addPod adds obj (a pod) to job controller's work-queue.
// Also added to cache.
func (jc *jobController) addPod(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		klog.Errorf("Failed to convert %v to v1.Pod", obj)
		return
	}
	// Filter out pods that are not created from volcano job
	if !isControlledBy(pod, helpers.JobKind) {
		return
	}

	jobName, found := pod.Annotations[batchv1alpha1.JobNameKey]
	if !found {
		klog.Infof("Failed to find jobName of Pod <%s/%s>, skipping",
			pod.Namespace, pod.Name)
		return
	}

	version, found := pod.Annotations[batchv1alpha1.JobVersion]
	if !found {
		klog.Infof("Failed to find jobVersion of Pod <%s/%s>, skipping",
			pod.Namespace, pod.Name)
		return
	}

	dVersion, err := strconv.Atoi(version)
	if err != nil {
		klog.Infof("Failed to convert jobVersion of Pod <%s/%s> into number, skipping",
			pod.Namespace, pod.Name)
		return
	}

	if pod.DeletionTimestamp != nil {
		jc.deletePod(pod)
		return
	}

	req := controllerapis.Request{
		Namespace: pod.Namespace,
		JobName:   jobName,

		Event:      busv1alpha1.OutOfSyncEvent,
		JobVersion: int32(dVersion),
	}

	if err := jc.cache.AddPod(pod); err != nil {
		klog.Errorf("Failed to add Pod <%s/%s>: %v to cache",
			pod.Namespace, pod.Name, err)
	}
	key := jobhelpers.GetJobKeyByReq(&req)
	queue := jc.getWorkerQueue(key)
	queue.Add(req)
}

// updatePod updates pod (from oldObj to newObj) to job controller's work queue.
// TODO: Why not call jc.cache.UpdatePod() in this func?
func (jc *jobController) updatePod(oldObj, newObj interface{}) {
	oldPod, ok := oldObj.(*corev1.Pod)
	if !ok {
		klog.Errorf("Failed to convert %v to v1.Pod", oldObj)
		return
	}
	newPod, ok := newObj.(*corev1.Pod)
	if !ok {
		klog.Errorf("Failed to convert %v to v1.Pod", newObj)
		return
	}

	// Filter out pods that are not created from volcano job
	if !isControlledBy(newPod, helpers.JobKind) {
		return
	}

	if newPod.ResourceVersion == oldPod.ResourceVersion {
		return
	}

	if newPod.DeletionTimestamp != nil {
		jc.deletePod(newObj)
		return
	}

	taskName, found := newPod.Annotations[batchv1alpha1.TaskSpecKey]
	if !found {
		klog.Infof("Failed to find taskName of Pod <%s/%s>, skipping",
			newPod.Namespace, newPod.Name)
		return
	}

	jobName, found := newPod.Annotations[batchv1alpha1.JobNameKey]
	if !found {
		klog.Infof("Failed to find jobName of Pod <%s/%s>, skipping",
			newPod.Namespace, newPod.Name)
		return
	}

	version, found := newPod.Annotations[batchv1alpha1.JobVersion]
	if !found {
		klog.Infof("Failed to find jobVersion of Pod <%s/%s>, skipping",
			newPod.Namespace, newPod.Name)
		return
	}

	dVersion, err := strconv.Atoi(version)
	if err != nil {
		klog.Infof("Failed to convert jobVersion of Pod into number <%s/%s>, skipping",
			newPod.Namespace, newPod.Name)
		return
	}

	if err := jc.cache.UpdatePod(newPod); err != nil {
		klog.Errorf("Failed to update Pod <%s/%s>: %v in cache",
			newPod.Namespace, newPod.Name, err)
	}

	event := busv1alpha1.OutOfSyncEvent
	var exitCode int32

	switch newPod.Status.Phase {
	case corev1.PodFailed:
		if oldPod.Status.Phase != corev1.PodFailed {
			event = busv1alpha1.PodFailedEvent
			// TODO: currently only one container pod is supported by volcano.
			//  Once multi containers pod is supported, update accordingly.
			if len(newPod.Status.ContainerStatuses) > 0 && newPod.Status.ContainerStatuses[0].State.Terminated != nil {
				exitCode = newPod.Status.ContainerStatuses[0].State.Terminated.ExitCode
			}
		}
	case corev1.PodSucceeded:
		if oldPod.Status.Phase != corev1.PodSucceeded &&
			jc.cache.TaskCompleted(controllercache.JobKeyByName(newPod.Namespace, jobName), taskName) {
			event = busv1alpha1.TaskCompletedEvent
		}
	case corev1.PodPending, corev1.PodRunning:
		if jc.cache.TaskFailed(controllercache.JobKeyByName(newPod.Namespace, jobName), taskName) {
			event = busv1alpha1.TaskFailedEvent
		}
	}

	req := controllerapis.Request{
		Namespace:  newPod.Namespace,
		JobName:    jobName,
		TaskName:   taskName,
		Event:      event,
		ExitCode:   exitCode,
		JobVersion: int32(dVersion),
	}
	key := jobhelpers.GetJobKeyByReq(&req)
	queue := jc.getWorkerQueue(key)
	queue.Add(req)
}

// deletePod deletes obj (a pod) from job controller.
func (jc *jobController) deletePod(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		// If we reached here it means the pod was deleted but its final state is unrecorded.
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %#v", obj)
			return
		}
		pod, ok = tombstone.Obj.(*corev1.Pod)
		if !ok {
			klog.Errorf("Tombstone contained object that is not a Pod: %#v", obj)
			return
		}
	}

	// Filter out pods that are not created from volcano job
	if !isControlledBy(pod, helpers.JobKind) {
		return
	}

	taskName, found := pod.Annotations[batchv1alpha1.TaskSpecKey]
	if !found {
		klog.Infof("Failed to find taskName of Pod <%s/%s>, skipping",
			pod.Namespace, pod.Name)
		return
	}

	jobName, found := pod.Annotations[batchv1alpha1.JobNameKey]
	if !found {
		klog.Infof("Failed to find jobName of Pod <%s/%s>, skipping",
			pod.Namespace, pod.Name)
		return
	}

	version, found := pod.Annotations[batchv1alpha1.JobVersion]
	if !found {
		klog.Infof("Failed to find jobVersion of Pod <%s/%s>, skipping",
			pod.Namespace, pod.Name)
		return
	}

	dVersion, err := strconv.Atoi(version)
	if err != nil {
		klog.Infof("Failed to convert jobVersion of Pod <%s/%s> into number, skipping",
			pod.Namespace, pod.Name)
		return
	}

	req := controllerapis.Request{
		Namespace: pod.Namespace,
		JobName:   jobName,
		TaskName:  taskName,

		Event:      busv1alpha1.PodEvictedEvent,
		JobVersion: int32(dVersion),
	}

	if err := jc.cache.DeletePod(pod); err != nil {
		klog.Errorf("Failed to delete Pod <%s/%s>: %v in cache",
			pod.Namespace, pod.Name, err)
	}

	key := jobhelpers.GetJobKeyByReq(&req)
	queue := jc.getWorkerQueue(key)
	queue.Add(req)
}

func (jc *jobController) recordJobEvent(namespace, name string, event batchv1alpha1.JobEvent, msg string) {
	job, err := jc.cache.Get(controllercache.JobKeyByName(namespace, name))
	if err != nil {
		klog.Warningf("Failed to find job in cache when reporting job event <%s/%s>: %v",
			namespace, name, err)
		return
	}
	jc.recorder.Event(job.Job, corev1.EventTypeNormal, string(event), msg)
}

func (jc *jobController) handleCommands() {
	for jc.processNextCommand() {
	}
}

func (jc *jobController) processNextCommand() bool {
	obj, shutdown := jc.cmdQueue.Get()
	if shutdown {
		return false
	}
	cmd := obj.(*busv1alpha1.Command)
	defer jc.cmdQueue.Done(cmd)

	// delete cmd from cluster and construct it as a request adding to the work-queue

	if err := jc.volcanoClient.BusV1alpha1().Commands(cmd.Namespace).Delete(context.TODO(), cmd.Name, metav1.DeleteOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to delete Command <%s/%s>.", cmd.Namespace, cmd.Name)
			jc.cmdQueue.AddRateLimited(cmd)
		}
		return true
	}
	jc.recordJobEvent(cmd.Namespace, cmd.TargetObject.Name, batchv1alpha1.CommandIssued,
		fmt.Sprintf("Start to execute command %s, and clean it up to make sure executed not more than once.", cmd.Action))

	req := controllerapis.Request{
		Namespace: cmd.Namespace,
		JobName:   cmd.TargetObject.Name,
		Event:     busv1alpha1.CommandIssuedEvent,
		Action:    busv1alpha1.Action(cmd.Action),
	}

	key := jobhelpers.GetJobKeyByReq(&req)
	queue := jc.getWorkerQueue(key)
	queue.Add(req)
	return true
}

// updatePodGroup updates podgroup (from oldObj to newObj) to job controller's work queue.
func (jc *jobController) updatePodGroup(oldObj, newObj interface{}) {
	oldPodGroup, ok := oldObj.(*schedulingv1alpha1.PodGroup)
	if !ok {
		klog.Errorf("Failed to convert %v to schedulingv1alpha1.PodGroup", oldObj)
		return
	}

	newPodGroup, ok := newObj.(*schedulingv1alpha1.PodGroup)
	if !ok {
		klog.Errorf("Failed to convert %v to schedulingv1alpha1.PodGroup", newObj)
		return
	}

	_, err := jc.cache.Get(controllercache.JobKeyByName(newPodGroup.Namespace, newPodGroup.Name))
	if err != nil && newPodGroup.Annotations != nil {
		klog.Warningf("Failed to find job in cache by PodGroup, this may not be a PodGroup for volcano job.")
	}

	if newPodGroup.Status.Phase != oldPodGroup.Status.Phase {
		req := controllerapis.Request{
			Namespace: newPodGroup.Namespace,
			JobName:   newPodGroup.Name,
		}
		switch newPodGroup.Status.Phase {
		case schedulingv1alpha1.PodGroupUnknown:
			req.Event = busv1alpha1.JobUnknownEvent
		}
		key := jobhelpers.GetJobKeyByReq(&req)
		queue := jc.getWorkerQueue(key)
		queue.Add(req)
	}
}

// TODO: add handler for PodGroup unschedulable event.
