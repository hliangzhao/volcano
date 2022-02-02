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

package k8s

import (
	"fmt"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// Snapshot is a snapshot of cache NodeInfo and NodeTree order. The scheduler takes a
// snapshot at the beginning of each scheduling cycle and uses it for its operations in that cycle.
type Snapshot struct {
	// nodeInfoMap a map of node name to a snapshot of its NodeInfo.
	nodeInfoMap map[string]*k8sframework.NodeInfo
	// nodeInfoList is the list of nodes as ordered in the cache's nodeTree.
	nodeInfoList []*k8sframework.NodeInfo
	// havePodsWithAffinityNodeInfoList is the list of nodes with at least one pod declaring affinity terms.
	havePodsWithAffinityNodeInfoList []*k8sframework.NodeInfo
	// havePodsWithRequiredAntiAffinityNodeInfoList is the list of nodes with at least one pod declaring
	// required anti-affinity terms.
	havePodsWithRequiredAntiAffinityNodeInfoList []*k8sframework.NodeInfo
}

var _ k8sframework.SharedLister = &Snapshot{}

// NewEmptySnapshot initializes a Snapshot struct and returns it.
func NewEmptySnapshot() *Snapshot {
	return &Snapshot{
		nodeInfoMap: make(map[string]*k8sframework.NodeInfo),
	}
}

// NewSnapshot initializes a Snapshot struct and returns it.
func NewSnapshot(nodeInfoMap map[string]*k8sframework.NodeInfo) *Snapshot {
	nodeInfoList := make([]*k8sframework.NodeInfo, 0, len(nodeInfoMap))
	havePodsWithAffinityNodeInfoList := make([]*k8sframework.NodeInfo, 0, len(nodeInfoMap))
	havePodsWithRequiredAntiAffinityNodeInfoList := make([]*k8sframework.NodeInfo, 0, len(nodeInfoMap))
	for _, v := range nodeInfoMap {
		nodeInfoList = append(nodeInfoList, v)
		if len(v.PodsWithAffinity) > 0 {
			havePodsWithAffinityNodeInfoList = append(havePodsWithAffinityNodeInfoList, v)
		}
		if len(v.PodsWithRequiredAntiAffinity) > 0 {
			havePodsWithRequiredAntiAffinityNodeInfoList = append(havePodsWithRequiredAntiAffinityNodeInfoList, v)
		}
	}

	s := NewEmptySnapshot()
	s.nodeInfoMap = nodeInfoMap
	s.nodeInfoList = nodeInfoList
	s.havePodsWithAffinityNodeInfoList = havePodsWithAffinityNodeInfoList
	s.havePodsWithRequiredAntiAffinityNodeInfoList = havePodsWithRequiredAntiAffinityNodeInfoList

	return s
}

// Pods returns a PodLister
func (s *Snapshot) Pods() utils.PodsLister {
	return podLister(s.nodeInfoList)
}

// NodeInfos returns a NodeInfoLister.
func (s *Snapshot) NodeInfos() k8sframework.NodeInfoLister {
	return s
}

type podLister []*k8sframework.NodeInfo

// List returns the list of pods in the snapshot.
func (p podLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	alwaysTrue := func(*corev1.Pod) bool { return true }
	return p.FilteredList(alwaysTrue, selector)
}

// FilteredList returns a filtered list of pods in the snapshot.
func (p podLister) FilteredList(filter utils.PodFilter, selector labels.Selector) ([]*corev1.Pod, error) {
	// podFilter is expected to return true for most or all of the pods. We
	// can avoid expensive array growth without wasting too much memory by
	// pre-allocating capacity.
	maxSize := 0
	for _, n := range p {
		maxSize += len(n.Pods)
	}
	pods := make([]*corev1.Pod, 0, maxSize)
	for _, n := range p {
		for _, pod := range n.Pods {
			if filter(pod.Pod) && selector.Matches(labels.Set(pod.Pod.Labels)) {
				pods = append(pods, pod.Pod)
			}
		}
	}
	return pods, nil
}

// List returns the list of nodes in the snapshot.
func (s *Snapshot) List() ([]*k8sframework.NodeInfo, error) {
	return s.nodeInfoList, nil
}

// HavePodsWithAffinityList returns the list of nodes with at least one pods with inter-pod affinity
func (s *Snapshot) HavePodsWithAffinityList() ([]*k8sframework.NodeInfo, error) {
	return s.havePodsWithAffinityNodeInfoList, nil
}

// HavePodsWithRequiredAntiAffinityList returns the list of NodeInfos of nodes with pods with required anti-affinity terms.
func (s *Snapshot) HavePodsWithRequiredAntiAffinityList() ([]*k8sframework.NodeInfo, error) {
	return s.havePodsWithRequiredAntiAffinityNodeInfoList, nil
}

// Get returns the NodeInfo of the given node name.
func (s *Snapshot) Get(nodeName string) (*k8sframework.NodeInfo, error) {
	if v, ok := s.nodeInfoMap[nodeName]; ok && v.Node() != nil {
		return v, nil
	}
	return nil, fmt.Errorf("nodeinfo not found for node name %q", nodeName)
}
