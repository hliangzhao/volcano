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

package rescheduling

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"sort"
)

type NodeUtilization struct {
	node        *corev1.Node
	utilization map[corev1.ResourceName]*resource.Quantity
	pods        []*corev1.Pod
}

type thresholdFilter func(*corev1.Node, *NodeUtilization, interface{}) bool
type isContinueEviction func(*corev1.Node, *NodeUtilization, map[corev1.ResourceName]*resource.Quantity, interface{}) bool

// groupNodesByUtilization divides the nodes into two groups by resource utilization filters
func groupNodesByUtilization(nodeUtilizationList []*NodeUtilization,
	lowThresholdFilter, highThresholdFilter thresholdFilter, config interface{}) ([]*NodeUtilization, []*NodeUtilization) {

	lowNodes := make([]*NodeUtilization, 0)
	highNodes := make([]*NodeUtilization, 0)

	for _, nodeUtility := range nodeUtilizationList {
		if lowThresholdFilter(nodeUtility.node, nodeUtility, config) {
			lowNodes = append(lowNodes, nodeUtility)
		} else if highThresholdFilter(nodeUtility.node, nodeUtility, config) {
			highNodes = append(highNodes, nodeUtility)
		}
	}

	return lowNodes, highNodes
}

// getNodeUtilization returns all node resource utilization list
func getNodeUtilization() []*NodeUtilization {
	nodeUtilizationList := make([]*NodeUtilization, 0)
	for _, nodeInfo := range Session.Nodes {
		nodeUtility := &NodeUtilization{
			node:        nodeInfo.Node,
			utilization: map[corev1.ResourceName]*resource.Quantity{},
			pods:        nodeInfo.Pods(),
		}
		nodeUtility.utilization[corev1.ResourceCPU] = resource.NewMilliQuantity(
			int64(nodeInfo.ResourceUsage.CPUUsageAvg[Interval]),
			resource.DecimalSI,
		)
		nodeUtility.utilization[corev1.ResourceMemory] = resource.NewQuantity(
			int64(nodeInfo.ResourceUsage.MEMUsageAvg[Interval]),
			resource.BinarySI,
		)
		nodeUtilizationList = append(nodeUtilizationList, nodeUtility)
	}
	return nodeUtilizationList
}

// evictPodsFromSourceNodes evict pods from source nodes to target nodes according to priority and QoS
func evictPodsFromSourceNodes(sourceNodesUtilization, targetNodesUtilization []*NodeUtilization, tasks []*apis.TaskInfo,
	evictionCon isContinueEviction, config interface{}) []*apis.TaskInfo {

	resNames := []corev1.ResourceName{
		corev1.ResourceCPU,
		corev1.ResourceMemory,
	}
	utilizationConfig := parseArgToConfig(config)
	totalAllocatableResource := map[corev1.ResourceName]*resource.Quantity{
		corev1.ResourceCPU:    {},
		corev1.ResourceMemory: {},
	}
	for _, targetNodeUtility := range targetNodesUtilization {
		nodeCapacity := getNodeCapacity(targetNodeUtility.node)
		for _, rName := range resNames {
			totalAllocatableResource[rName].Add(
				*getThresholdForNode(rName, utilizationConfig.TargetThresholds[string(rName)], nodeCapacity),
			)
			totalAllocatableResource[rName].Sub(
				*targetNodeUtility.utilization[rName],
			)
		}
	}
	klog.V(4).Infof("totalAllocatableResource: %s", totalAllocatableResource)

	// sort the source nodes in descending order
	sortNodes(sourceNodesUtilization, Session.Nodes)

	// victims select algorithm:
	// 1. Evict pods from nodes with high utilization to low utilization
	// 2. As to one node, evict pods from low priority to high priority.
	//    If the priority is same, evict pods according to QoS from low to high
	victims := make([]*apis.TaskInfo, 0)
	for _, sourceNodeUtility := range sourceNodesUtilization {
		if len(sourceNodeUtility.pods) == 0 {
			klog.V(4).Infof("No pods can be removed on node: %s", sourceNodeUtility.node.Name)
			continue
		}
		sortPods(sourceNodeUtility.pods)
		victims = append(victims,
			evict(sourceNodeUtility.pods, sourceNodeUtility, totalAllocatableResource, evictionCon, tasks, config)...,
		)
	}
	return victims
}

// parseArgToConfig returns a nodeUtilizationConfig object from parameters
// TODO: It is just for lowNodeUtilization now, which should be abstracted as a common function.
func parseArgToConfig(config interface{}) *LowNodeUtilizationConf {
	var utilizationConfig *LowNodeUtilizationConf
	if arg, ok := config.(LowNodeUtilizationConf); ok {
		utilizationConfig = &arg
	}
	return utilizationConfig
}

// sortNodes sorts all the nodes according the usage of cpu and memory with weight score
func sortNodes(nodeUtilizationList []*NodeUtilization, nodes map[string]*apis.NodeInfo) {
	cmpFn := func(i, j int) bool {
		return getScoreForNode(i, nodeUtilizationList, nodes) > getScoreForNode(j, nodeUtilizationList, nodes)
	}
	sort.Slice(nodeUtilizationList, cmpFn)
}

// getScoreForNode returns the score for node which considers only for CPU and memory
func getScoreForNode(index int, nodeUtilizationList []*NodeUtilization, nodes map[string]*apis.NodeInfo) float64 {
	nodeName := nodeUtilizationList[index].node.Name
	cpuScore := float64(nodeUtilizationList[index].utilization[corev1.ResourceCPU].MilliValue()) / nodes[nodeName].Capability.MilliCPU
	memoryScore := float64(nodeUtilizationList[index].utilization[corev1.ResourceMemory].MilliValue()) / nodes[nodeName].Capability.Memory
	return cpuScore + memoryScore
}

// getThresholdForNode returns resource threshold on some dimension
func getThresholdForNode(rName corev1.ResourceName, thresholdPercent float64, nodeCapacity corev1.ResourceList) *resource.Quantity {
	var threshold *resource.Quantity
	if rName == corev1.ResourceCPU {
		threshold = resource.NewMilliQuantity(int64(thresholdPercent*float64(nodeCapacity.Cpu().MilliValue())*0.01), resource.DecimalSI)
	} else if rName == corev1.ResourceMemory {
		threshold = resource.NewQuantity(int64(thresholdPercent*float64(nodeCapacity.Memory().Value())*0.01), resource.BinarySI)
	}
	return threshold
}

// getNodeCapacity returns node's capacity
func getNodeCapacity(node *corev1.Node) corev1.ResourceList {
	nodeCapacity := node.Status.Capacity
	if len(node.Status.Allocatable) > 0 {
		nodeCapacity = node.Status.Allocatable
	}
	return nodeCapacity
}

// sortPods return the pods in order according the priority and QoS
func sortPods(pods []*corev1.Pod) {
	cmp := func(i, j int) bool {
		if pods[i].Spec.Priority == nil && pods[j].Spec.Priority != nil {
			return true
		}
		if pods[j].Spec.Priority == nil && pods[i].Spec.Priority != nil {
			return false
		}
		if (pods[j].Spec.Priority == nil && pods[i].Spec.Priority == nil) || (*pods[i].Spec.Priority == *pods[j].Spec.Priority) {
			if qos.GetPodQOS(pods[i]) == corev1.PodQOSBestEffort {
				return true
			}
			if qos.GetPodQOS(pods[i]) == corev1.PodQOSBurstable && qos.GetPodQOS(pods[j]) == corev1.PodQOSGuaranteed {
				return true
			}
			return false
		}
		return *pods[i].Spec.Priority < *pods[j].Spec.Priority
	}
	sort.Slice(pods, cmp)
}

// evict select victims and add to the eviction list
func evict(pods []*corev1.Pod, utilization *NodeUtilization, totalAllocatableResource map[corev1.ResourceName]*resource.Quantity,
	continueEviction isContinueEviction, tasks []*apis.TaskInfo, config interface{}) []*apis.TaskInfo {
	victims := make([]*apis.TaskInfo, 0)
	for _, pod := range pods {
		if !continueEviction(utilization.node, utilization, totalAllocatableResource, config) {
			return victims
		}
		for _, task := range tasks {
			if task.Pod.UID == pod.UID {
				totalAllocatableResource[corev1.ResourceCPU].Sub(*resource.NewMilliQuantity(int64(task.ResReq.MilliCPU), resource.DecimalSI))
				totalAllocatableResource[corev1.ResourceMemory].Sub(*resource.NewQuantity(int64(task.ResReq.Memory), resource.BinarySI))
				utilization.utilization[corev1.ResourceCPU].Sub(*resource.NewMilliQuantity(int64(task.ResReq.MilliCPU), resource.DecimalSI))
				utilization.utilization[corev1.ResourceMemory].Sub(*resource.NewQuantity(int64(task.ResReq.Memory), resource.BinarySI))
				victims = append(victims, task)
				break
			}
		}
	}
	return victims
}
