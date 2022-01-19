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

package cache

import (
	"context"
	"fmt"
	`github.com/hliangzhao/volcano/pkg/apis/utils`
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	`k8s.io/client-go/tools/cache`
	"k8s.io/klog/v2"
)

func isTerminated(status apis.TaskStatus) bool {
	return status == apis.Succeeded || status == apis.Failed
}

// getOrCreateJob returns the apis.JobInfo that wraps ti if the job exists. Otherwise, create one and return it.
func (sc *SchedulerCache) getOrCreateJob(ti *apis.TaskInfo) *apis.JobInfo {
	if len(ti.Job) == 0 {
		if ti.Pod.Spec.SchedulerName != sc.schedulerName {
			klog.V(4).Infof("Pod %s/%s will not scheduled by %s, skip creating PodGroup and Job for it",
				ti.Pod.Namespace, ti.Pod.Name, sc.schedulerName)
		}
		return nil
	}

	if _, found := sc.Jobs[ti.Job]; !found {
		sc.Jobs[ti.Job] = apis.NewJobInfo(ti.Job)
	}

	return sc.Jobs[ti.Job]
}

// addTask adds ti to the right apis.JobInfo instance.
func (sc *SchedulerCache) addTask(ti *apis.TaskInfo) error {
	if len(ti.NodeName) != 0 {
		// create node for ti if not found
		if _, found := sc.Nodes[ti.NodeName]; !found {
			sc.Nodes[ti.NodeName] = apis.NewNodeInfo(nil)
			sc.Nodes[ti.NodeName].Name = ti.NodeName
		}

		// add ti to this node
		node := sc.Nodes[ti.NodeName]
		if !isTerminated(ti.Status) {
			if err := node.AddTask(ti); err != nil {
				if _, outOfSync := err.(*apis.AllocateFailError); outOfSync {
					node.State = apis.NodeState{
						Phase:  apis.NotReady,
						Reason: "OutOfSync",
					}
				}
				return err
			}
		} else {
			klog.V(4).Infof("Pod <%v/%v> is in status %s.", ti.Namespace, ti.Name, ti.Status.String())
		}
	}

	// get (or create) the job of ti
	job := sc.getOrCreateJob(ti)
	if job != nil {
		job.AddTaskInfo(ti)
	}

	return nil
}

func (sc *SchedulerCache) addPod(pod *corev1.Pod) error {
	// Assumes that lock is already acquired.
	ti := apis.NewTaskInfo(pod)
	return sc.addTask(ti)
}

func (sc *SchedulerCache) AddPod(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		klog.Errorf("Cannot convert to *corev1.Pod: %v", obj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	err := sc.addPod(pod)
	if err != nil {
		klog.Errorf("Failed to add pod <%s/%s> into cache: %v",
			pod.Namespace, pod.Name, err)
		return
	}

	klog.V(3).Infof("Added pod <%s/%v> into cache.", pod.Namespace, pod.Name)
}

func (sc *SchedulerCache) UpdatePod(oldObj, newObj interface{}) {
	oldPod, ok := oldObj.(*corev1.Pod)
	if !ok {
		klog.Errorf("Cannot convert oldObj to *corev1.Pod: %v", oldObj)
		return
	}
	newPod, ok := newObj.(*corev1.Pod)
	if !ok {
		klog.Errorf("Cannot convert newObj to *corev1.Pod: %v", newObj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	err := sc.updatePod(oldPod, newPod)
	if err != nil {
		klog.Errorf("Failed to update pod %v in cache: %v", oldPod.Name, err)
		return
	}

	klog.V(4).Infof("Updated pod <%s/%v> in cache.", oldPod.Namespace, oldPod.Name)
}

func (sc *SchedulerCache) DeletePod(obj interface{}) {
	var pod *corev1.Pod
	switch t := obj.(type) {
	case *corev1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*corev1.Pod)
		if !ok {
			klog.Errorf("Cannot convert to *corev1.Pod: %v", t.Obj)
			return
		}
	default:
		klog.Errorf("Cannot convert to *corev1.Pod: %v", t)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	err := sc.deletePod(pod)
	if err != nil {
		klog.Errorf("Failed to delete pod %v from cache: %v", pod.Name, err)
		return
	}

	klog.V(3).Infof("Deleted pod <%s/%v> from cache.", pod.Namespace, pod.Name)
}

func (sc *SchedulerCache) deleteTask(ti *apis.TaskInfo) error {
	var jobErr, nodeErr, numaErr error

	// remove ti from job
	if len(ti.Job) != 0 {
		if job, found := sc.Jobs[ti.Job]; found {
			jobErr = job.DeleteTaskInfo(ti)
		} else {
			jobErr = fmt.Errorf("failed to find Job <%v> for Task %v/%v",
				ti.Job, ti.Namespace, ti.Name)
		}
	}

	// remove ti from node
	if len(ti.NodeName) != 0 {
		node := sc.Nodes[ti.NodeName]
		if node != nil {
			nodeErr = node.RemoveTask(ti)
		}
	}

	// TODO: numaErr not set

	if jobErr != nil || nodeErr != nil {
		return apis.MergeErrors(jobErr, nodeErr, numaErr)
	}

	return nil
}

// updateTask deletes the old TaskInfo and add the new one.
func (sc *SchedulerCache) updateTask(oldTi, newTi *apis.TaskInfo) error {
	if err := sc.deleteTask(oldTi); err != nil {
		klog.Warningf("Failed to delete task: %v", err)
	}
	return sc.addTask(newTi)
}

// syncTask gets the latest pod from oldTi. If the pod changed, use it to update ti.
func (sc *SchedulerCache) syncTask(oldTi *apis.TaskInfo) error {
	latestPod, err := sc.kubeClient.CoreV1().Pods(oldTi.Namespace).Get(context.TODO(), oldTi.Name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			err := sc.deleteTask(oldTi)
			if err != nil {
				klog.Errorf("Failed to delete Pod <%v/%v> and remove from cache: %s", oldTi.Namespace, oldTi.Name, err.Error())
				return err
			}
			klog.V(3).Infof("Pod <%v/%v> was deleted, removed from cache.", oldTi.Namespace, oldTi.Name)
			return nil
		}
		return fmt.Errorf("failed to get Pod <%v/%v>: err %v", oldTi.Namespace, oldTi.Name, err)
	}

	newTi := apis.NewTaskInfo(latestPod)
	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()
	return sc.updateTask(oldTi, newTi)
}

// allocatedPodInCache checks the node allocation status of pod.
func (sc *SchedulerCache) allocatedPodInCache(pod *corev1.Pod) bool {
	ti := apis.NewTaskInfo(pod)
	// get the job cache of ti
	if job, found := sc.Jobs[ti.Job]; found {
		// if job found, get the ti cache in the job, update the cache
		if t, found := job.Tasks[ti.UID]; found {
			return apis.AllocatedStatus(t.Status)
		}
	}
	return false
}

func (sc *SchedulerCache) deletePod(pod *corev1.Pod) error {
	ti := apis.NewTaskInfo(pod)

	// Delete the Task (pod) in cache to handle Binding status.
	task := ti
	if job, found := sc.Jobs[ti.Job]; found {
		if t, found := job.Tasks[ti.UID]; found {
			task = t
		}
	}
	if err := sc.deleteTask(task); err != nil {
		klog.Warningf("Failed to delete task: %v", err)
	}

	// Further, if job was terminated, delete it.
	if job, found := sc.Jobs[ti.Job]; found && apis.JobTerminated(job) {
		sc.deleteJob(job)
	}

	return nil
}

// updatePod deletes oldPod and adds newPod to sc.
// Assumes that lock is already acquired.
func (sc *SchedulerCache) updatePod(oldPod, newPod *corev1.Pod) error {
	// ignore the update event if pod is allocated in cache but not present in NodeName
	if sc.allocatedPodInCache(newPod) && newPod.Spec.NodeName == "" {
		klog.V(4).Infof("Pod <%s/%v> already in cache with allocated status, ignore the update event",
			newPod.Namespace, newPod.Name)
		return nil
	}

	if err := sc.deletePod(oldPod); err != nil {
		return err
	}
	// when delete pod, the owner reference of pod will be set nil,just as orphan pod
	if len(utils.GetController(newPod)) == 0 {
		newPod.OwnerReferences = oldPod.OwnerReferences
	}
	return sc.addPod(newPod)
}

func (sc *SchedulerCache) addNode(node *corev1.Node) error {
	if sc.Nodes[node.Name] != nil {
		sc.Nodes[node.Name].SetNode(node)
	} else {
		sc.Nodes[node.Name] = apis.NewNodeInfo(node)
	}
	return nil
}

func (sc *SchedulerCache) updateNode(oldNode, newNode *corev1.Node) error {
	// TODO: oldNode not used
	if sc.Nodes[newNode.Name] != nil {
		sc.Nodes[newNode.Name].SetNode(newNode)
		return nil
	}
	return fmt.Errorf("node <%s> does not exist", newNode.Name)
}

func (sc *SchedulerCache) deleteNode(node *corev1.Node) error {
	if _, ok := sc.Nodes[node.Name]; !ok {
		return fmt.Errorf("node <%s> does not exist", node.Name)
	}

	numaInfo := sc.Nodes[node.Name].NumaInfo
	if numaInfo != nil {
		klog.V(3).Infof("delete numatopo <%s/%s>", numaInfo.Namespace, numaInfo.Name)
		err := sc.vcClient.NodeinfoV1alpha1().Numatopologies().Delete(context.TODO(), numaInfo.Name, metav1.DeleteOptions{})
		if err != nil {
			klog.Errorf("delete numatopo <%s/%s> failed.", numaInfo.Namespace, numaInfo.Name)
		}
	}

	delete(sc.Nodes, node.Name)

	return nil
}

func (sc *SchedulerCache) AddNode(obj interface{}) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		klog.Errorf("Cannot convert to *corev1.Node: %v", obj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	err := sc.addNode(node)
	if err != nil {
		klog.Errorf("Failed to add node %s into cache: %v", node.Name, err)
		return
	}
	sc.NodeList = append(sc.NodeList, node.Name)
}

func (sc *SchedulerCache) UpdateNode(oldObj, newObj interface{}) {
	oldNode, ok := oldObj.(*corev1.Node)
	if !ok {
		klog.Errorf("Cannot convert oldObj to *corev1.Node: %v", oldObj)
		return
	}
	newNode, ok := newObj.(*corev1.Node)
	if !ok {
		klog.Errorf("Cannot convert newObj to *corev1.Node: %v", newObj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	err := sc.updateNode(oldNode, newNode)
	if err != nil {
		klog.Errorf("Failed to update node %v in cache: %v", oldNode.Name, err)
		return
	}
}

func (sc *SchedulerCache) DeleteNode(obj interface{}) {
	var node *corev1.Node
	switch t := obj.(type) {
	case *corev1.Node:
		node = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		node, ok = t.Obj.(*corev1.Node)
		if !ok {
			klog.Errorf("Cannot convert to *corev1.Node: %v", t.Obj)
			return
		}
	default:
		klog.Errorf("Cannot convert to *corev1.Node: %v", t)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	err := sc.deleteNode(node)
	if err != nil {
		klog.Errorf("Failed to delete node %s from cache: %v", node.Name, err)
		return
	}

	for i, name := range sc.NodeList {
		if name == node.Name {
			sc.NodeList = append(sc.NodeList[:i], sc.NodeList[i+1:]...)
			break
		}
	}
}
