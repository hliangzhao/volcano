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

package utils

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	// Permit indicates that plugin callback function permits job to be inqueue, pipelined, or other status
	Permit = 1
	// Abstain indicates that plugin callback function abstains in voting job to be inqueue, pipelined, or other status
	Abstain = 0
	// Reject indicates that plugin callback function rejects job to be inqueue, pipelined, or other status
	Reject = -1
)

type PodFilter func(pod *corev1.Pod) bool

type PodsLister interface {
	List(set labels.Selector) ([]*corev1.Pod, error)
	FilteredList(podFilter PodFilter, selector labels.Selector) ([]*corev1.Pod, error)
}

type PodLister struct {
	Session *framework.Session

	CachedPods       map[apis.TaskID]*corev1.Pod
	Tasks            map[apis.TaskID]*apis.TaskInfo
	TaskWithAffinity map[apis.TaskID]*apis.TaskInfo
}

func NewPodLister(sess *framework.Session) *PodLister {
	pl := &PodLister{
		Session:          sess,
		CachedPods:       map[apis.TaskID]*corev1.Pod{},
		Tasks:            map[apis.TaskID]*apis.TaskInfo{},
		TaskWithAffinity: map[apis.TaskID]*apis.TaskInfo{},
	}

	for _, job := range pl.Session.Jobs {
		for status, tasks := range job.TaskStatusIndex {
			if !apis.AllocatedStatus(status) {
				continue
			}

			for _, task := range tasks {
				pl.Tasks[task.UID] = task

				pod := pl.copyTaskPod(task)
				pl.CachedPods[task.UID] = pod

				if HaveAffinity(task.Pod) {
					pl.TaskWithAffinity[task.UID] = task
				}
			}
		}
	}
	return pl
}

func NewPodListerFromNode(ssn *framework.Session) *PodLister {
	pl := &PodLister{
		Session:          ssn,
		CachedPods:       make(map[apis.TaskID]*corev1.Pod),
		Tasks:            make(map[apis.TaskID]*apis.TaskInfo),
		TaskWithAffinity: make(map[apis.TaskID]*apis.TaskInfo),
	}

	for _, node := range pl.Session.Nodes {
		for _, task := range node.Tasks {
			if !apis.AllocatedStatus(task.Status) && task.Status != apis.Releasing {
				continue
			}

			pl.Tasks[task.UID] = task
			pod := pl.copyTaskPod(task)
			pl.CachedPods[task.UID] = pod
			if HaveAffinity(task.Pod) {
				pl.TaskWithAffinity[task.UID] = task
			}
		}
	}

	return pl
}

func HaveAffinity(pod *corev1.Pod) bool {
	affinity := pod.Spec.Affinity
	return affinity != nil &&
		(affinity.NodeAffinity != nil ||
			affinity.PodAffinity != nil ||
			affinity.PodAntiAffinity != nil)
}

func (pl *PodLister) copyTaskPod(task *apis.TaskInfo) *corev1.Pod {
	pod := task.Pod.DeepCopy()
	pod.Spec.NodeName = task.NodeName
	return pod
}

// GetPod will get pod with proper nodeName, from cache or DeepCopy
// keeping this function read only to avoid concurrent panic of map.
func (pl *PodLister) GetPod(task *apis.TaskInfo) *corev1.Pod {
	if task.NodeName == task.Pod.Spec.NodeName {
		return task.Pod
	}

	pod, found := pl.CachedPods[task.UID]
	if !found {
		// we could not write the copied pod back into cache for read only
		pod = pl.copyTaskPod(task)
		klog.Warningf("DeepCopy for pod %s/%s at PodLister.GetPod is unexpected", pod.Namespace, pod.Name)
	}
	return pod
}

// UpdateTask will update the pod nodeName in cache using nodeName.
// NOT thread safe, please ensure UpdateTask is the only called function of PodLister at the same time.
func (pl *PodLister) UpdateTask(task *apis.TaskInfo, nodeName string) *corev1.Pod {
	pod, found := pl.CachedPods[task.UID]
	if !found {
		pod = pl.copyTaskPod(task)
		pl.CachedPods[task.UID] = pod
	}
	pod.Spec.NodeName = nodeName

	if !apis.AllocatedStatus(task.Status) {
		delete(pl.Tasks, task.UID)
		if HaveAffinity(task.Pod) {
			delete(pl.TaskWithAffinity, task.UID)
		}
	} else {
		pl.Tasks[task.UID] = task
		if HaveAffinity(task.Pod) {
			pl.TaskWithAffinity[task.UID] = task
		}
	}

	return pod
}

func (pl *PodLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	var pods []*corev1.Pod
	for _, task := range pl.Tasks {
		pod := pl.GetPod(task)
		if selector.Matches(labels.Set(pod.Labels)) {
			pods = append(pods, pod)
		}
	}
	return pods, nil
}

func (pl *PodLister) filteredListWithTaskSet(taskSet map[apis.TaskID]*apis.TaskInfo, podFilter PodFilter, selector labels.Selector) ([]*corev1.Pod, error) {
	var pods []*corev1.Pod
	for _, task := range taskSet {
		pod := pl.GetPod(task)
		if podFilter(pod) && selector.Matches(labels.Set(pod.Labels)) {
			pods = append(pods, pod)
		}
	}
	return pods, nil
}

func (pl *PodLister) FilteredList(podFilter PodFilter, selector labels.Selector) ([]*corev1.Pod, error) {
	return pl.filteredListWithTaskSet(pl.Tasks, podFilter, selector)
}

func (pl *PodLister) AffinityFilteredList(podFilter PodFilter, selector labels.Selector) ([]*corev1.Pod, error) {
	return pl.filteredListWithTaskSet(pl.TaskWithAffinity, podFilter, selector)
}

/* List Pods with Affinity Info */

type PodAffinityLister struct {
	pl *PodLister
}

func (pl *PodLister) AffinityLister() *PodAffinityLister {
	pal := &PodAffinityLister{
		pl: pl,
	}
	return pal
}

func (pal *PodAffinityLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	return pal.pl.List(selector)
}

func (pal *PodAffinityLister) FilteredList(podFilter PodFilter, selector labels.Selector) ([]*corev1.Pod, error) {
	return pal.pl.AffinityFilteredList(podFilter, selector)
}

func GenerateNodeMapAndSlice(nodes map[string]*apis.NodeInfo) map[string]*k8sframework.NodeInfo {
	nodeMap := make(map[string]*k8sframework.NodeInfo)
	for _, node := range nodes {
		ni := k8sframework.NewNodeInfo(node.Pods()...)
		ni.SetNode(node.Node)
		nodeMap[node.Name] = ni
	}
	return nodeMap
}

/* Get Node from Session */

type CachedNodeInfo struct {
	Session *framework.Session
}

func (cni *CachedNodeInfo) GetNodeInfo(name string) (*corev1.Node, error) {
	node, found := cni.Session.Nodes[name]
	if !found {
		return nil, errors.NewNotFound(corev1.Resource("node"), name)
	}
	return node.Node, nil
}

/* List nodes from Session */

type NodeLister struct {
	Session *framework.Session
}

func (nl *NodeLister) List() ([]*corev1.Node, error) {
	var nodes []*corev1.Node
	for _, node := range nl.Session.Nodes {
		nodes = append(nodes, node.Node)
	}
	return nodes, nil
}

func NormalizeScore(maxPriority int64, reverse bool, scores map[string]int64) {
	var maxCnt int64
	for _, score := range scores {
		if score > maxCnt {
			maxCnt = score
		}
	}
	if maxCnt == 0 {
		if reverse {
			for key := range scores {
				scores[key] = maxPriority
			}
		}
		return
	}
	for key, score := range scores {
		score = maxPriority * score / maxCnt
		if reverse {
			score = maxPriority - score
		}
		scores[key] = score
	}
}
