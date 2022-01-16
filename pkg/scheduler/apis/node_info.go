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

package apis

import (
	"fmt"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"strconv"
)

type AllocateFailError struct {
	Reason string
}

func (e *AllocateFailError) Error() string {
	return e.Reason
}

// NodeState defines the current state of node.
type NodeState struct {
	Phase  NodePhase
	Reason string
}

// NodeInfo is node level aggregated information.
type NodeInfo struct {
	Name string
	Node *corev1.Node

	State NodeState

	// The releasing resource on that node
	Releasing *Resource
	// The pipelined resource on that node
	Pipelined *Resource
	// The idle resource on that node
	Idle *Resource
	// The used resource on that node, including running and terminating pods
	Used *Resource

	Allocatable *Resource
	Capability  *Resource

	Tasks             map[TaskID]*TaskInfo
	NumaInfo          *NumaTopoInfo
	NumaChgFlag       NumaChangeFlag
	NumaSchedulerInfo *NumaTopoInfo
	RevocableZone     string

	// Used to store custom information
	Others     map[string]interface{}
	GPUDevices map[int]*GPUDevice

	// enable node resource oversubscription
	OversubscriptionNode bool
	// if OfflineJobEvicting is true, then when node resource usage too high,
	// the dispatched pod can not use oversubscription resource
	OfflineJobEvicting bool

	// the Oversubscription Resource reported in annotation
	OversubscriptionResource *Resource
}

func NewNodeInfo(node *corev1.Node) *NodeInfo {
	nodeInfo := &NodeInfo{
		Releasing: EmptyResource(),
		Pipelined: EmptyResource(),
		Idle:      EmptyResource(),
		Used:      EmptyResource(),

		Allocatable: EmptyResource(),
		Capability:  EmptyResource(),

		OversubscriptionResource: EmptyResource(),
		Tasks:                    map[TaskID]*TaskInfo{},

		GPUDevices: map[int]*GPUDevice{},
	}

	nodeInfo.setOversubscription(node)

	if node != nil {
		nodeInfo.Name = node.Name
		nodeInfo.Node = node
		nodeInfo.Idle = NewResource(node.Status.Allocatable).Add(nodeInfo.OversubscriptionResource)
		nodeInfo.Allocatable = NewResource(node.Status.Allocatable).Add(nodeInfo.OversubscriptionResource)
		nodeInfo.Capability = NewResource(node.Status.Capacity).Add(nodeInfo.OversubscriptionResource)
	}
	nodeInfo.setNodeGPUInfo(node)
	nodeInfo.setNodeState(node)
	nodeInfo.setRevocableZone(node)

	return nodeInfo
}

// Clone used to clone nodeInfo Object
func (ni *NodeInfo) Clone() *NodeInfo {
	res := NewNodeInfo(ni.Node)

	for _, p := range ni.Tasks {
		_ = res.AddTask(p)
	}
	if ni.NumaInfo != nil {
		res.NumaInfo = ni.NumaInfo.DeepCopy()
	}

	if ni.NumaSchedulerInfo != nil {
		res.NumaSchedulerInfo = ni.NumaSchedulerInfo.DeepCopy()
		klog.V(5).Infof("node[%s]", ni.Name)
		for resName, resInfo := range res.NumaSchedulerInfo.NumaResMap {
			klog.V(5).Infof("current resource %s : %v", resName, resInfo)
		}

		klog.V(5).Infof("current Policies : %v", res.NumaSchedulerInfo.Policies)
	}

	res.Others = ni.Others
	return res
}

// SetNode sets kubernetes node object to nodeInfo object
func (ni *NodeInfo) SetNode(node *corev1.Node) {
	ni.setNodeState(node)
	if !ni.Ready() {
		klog.Warningf("Failed to set node info for %s, phase: %s, reason: %s",
			ni.Name, ni.State.Phase, ni.State.Reason)
		return
	}

	// Dry run, make sure all fields other than `State` are in the original state.
	copied := ni.Clone()
	copied.setNode(node)
	copied.setNodeState(node)
	if !copied.Ready() {
		klog.Warningf("SetNode makes node %s not ready, phase: %s, reason: %s",
			copied.Name, copied.State.Phase, copied.State.Reason)
		// Set state of node to !Ready, left other fields untouched
		ni.State = copied.State
		return
	}

	ni.setNode(node)
}

// AddTask is used to add a task in nodeInfo object.
// If error occurs both task and node are guaranteed to be in the original state.
func (ni *NodeInfo) AddTask(task *TaskInfo) error {
	if len(task.NodeName) > 0 && len(ni.Name) > 0 && task.NodeName != ni.Name {
		return fmt.Errorf("task <%v/%v> already on different node <%v>",
			task.Namespace, task.Name, task.NodeName)
	}

	key := PodKey(task.Pod)
	if _, found := ni.Tasks[key]; found {
		return fmt.Errorf("task <%v/%v> already on node <%v>",
			task.Namespace, task.Name, ni.Name)
	}

	// Node will hold a copy of task to make sure the status
	// change will not impact resource in node.
	ti := task.Clone()

	if ni.Node != nil {
		switch ti.Status {
		case Releasing:
			if err := ni.allocateIdleResource(ti); err != nil {
				return err
			}
			ni.Releasing.Add(ti.ResReq)
			ni.Used.Add(ti.ResReq)
			ni.AddGPUResource(ti.Pod)
		case Pipelined:
			ni.Pipelined.Add(ti.ResReq)
		default:
			if err := ni.allocateIdleResource(ti); err != nil {
				return err
			}
			ni.Used.Add(ti.ResReq)
			ni.AddGPUResource(ti.Pod)
		}
	}

	if ni.NumaInfo != nil {
		ni.NumaInfo.AddTask(ti)
	}

	// Update task node name upon successful task addition.
	task.NodeName = ni.Name
	ti.NodeName = ni.Name
	ni.Tasks[key] = ti

	return nil
}

func (ni *NodeInfo) allocateIdleResource(ti *TaskInfo) error {
	if ti.ResReq.LessEqual(ni.Idle, Zero) {
		ni.Idle.Sub(ti.ResReq)
		return nil
	}

	return &AllocateFailError{Reason: fmt.Sprintf(
		"cannot allocate resource, <%s> idle: %s <%s/%s> req: %s",
		ni.Name, ni.Idle.String(), ti.Namespace, ti.Name, ti.ResReq.String(),
	)}
}

// RemoveTask used to remove a task from nodeInfo object.
// If error occurs both task and node are guaranteed to be in the original state.
func (ni *NodeInfo) RemoveTask(ti *TaskInfo) error {
	key := PodKey(ti.Pod)

	task, found := ni.Tasks[key]
	if !found {
		klog.Warningf("failed to find task <%v/%v> on host <%v>",
			ti.Namespace, ti.Name, ni.Name)
		return nil
	}

	if ni.Node != nil {
		switch task.Status {
		case Releasing:
			ni.Releasing.Sub(task.ResReq)
			ni.Idle.Add(task.ResReq)
			ni.Used.Sub(task.ResReq)
			ni.SubGPUResource(ti.Pod)
		case Pipelined:
			ni.Pipelined.Sub(task.ResReq)
		default:
			ni.Idle.Add(task.ResReq)
			ni.Used.Sub(task.ResReq)
			ni.SubGPUResource(ti.Pod)
		}
	}

	if ni.NumaInfo != nil {
		ni.NumaInfo.RemoveTask(ti)
	}

	delete(ni.Tasks, key)

	return nil
}

// UpdateTask is used to update a task in nodeInfo object.
// If error occurs both task and node are guaranteed to be in the original state.
func (ni *NodeInfo) UpdateTask(ti *TaskInfo) error {
	if err := ni.RemoveTask(ti); err != nil {
		return err
	}

	if err := ni.AddTask(ti); err != nil {
		// This should never happen if task removal was successful,
		// because only possible error during task addition is when task is still on a node.
		klog.Fatalf("Failed to add Task <%s,%s> to Node <%s> during task update",
			ti.Namespace, ti.Name, ni.Name)
	}
	return nil
}

// String returns nodeInfo details in string format
func (ni NodeInfo) String() string {
	tasks := ""

	i := 0
	for _, task := range ni.Tasks {
		tasks += fmt.Sprintf("\n\t %d: %v", i, task)
		i++
	}

	return fmt.Sprintf("Node (%s): allocatable<%v> idle <%v>, used <%v>, releasing <%v>, oversubscribution <%v>, "+
		"state <phase %s, reason %s>, oversubscributionNode <%v>, offlineJobEvicting <%v>,taints <%v>%s",
		ni.Name, ni.Allocatable, ni.Idle, ni.Used, ni.Releasing, ni.OversubscriptionResource,
		ni.State.Phase, ni.State.Reason, ni.OversubscriptionNode, ni.OfflineJobEvicting, ni.Node.Spec.Taints, tasks)
}

// Check node if enable Oversubscription and set Oversubscription resources
// Only support oversubscription cpu and memory resource for this version
func (ni *NodeInfo) setOversubscription(node *corev1.Node) {
	if node == nil {
		return
	}

	// default values
	ni.OversubscriptionNode = false
	ni.OfflineJobEvicting = false

	// check labels
	if len(node.Labels) > 0 {
		if value, found := node.Labels[OverSubscriptionNode]; found {
			b, err := strconv.ParseBool(value)
			if err == nil {
				ni.OversubscriptionNode = b
			} else {
				ni.OversubscriptionNode = false
			}
			klog.V(5).Infof("Set node %s Oversubscription to %v", node.Name, ni.OversubscriptionNode)
		}
	}

	// check annotations
	if len(node.Annotations) > 0 {
		if value, found := node.Labels[OfflineJobEvicting]; found {
			b, err := strconv.ParseBool(value)
			if err == nil {
				ni.OfflineJobEvicting = b
			} else {
				ni.OfflineJobEvicting = false
			}
			klog.V(5).Infof("Set node %s OfflineJobEvicting to %v",
				node.Name, ni.OfflineJobEvicting)
		}

		if value, found := node.Annotations[OverSubscriptionCPU]; found {
			ni.OversubscriptionResource.MilliCPU, _ = strconv.ParseFloat(value, 64)
			klog.V(5).Infof("Set node %s Oversubscription CPU to %v",
				node.Name, ni.OversubscriptionResource.MilliCPU)
		}
		if value, found := node.Annotations[OverSubscriptionMemory]; found {
			ni.OversubscriptionResource.Memory, _ = strconv.ParseFloat(value, 64)
			klog.V(5).Infof("Set node %s Oversubscription Memory to %v",
				node.Name, ni.OversubscriptionResource.Memory)
		}
	}
}

func (ni *NodeInfo) setNodeGPUInfo(node *corev1.Node) {
	if node == nil {
		return
	}
	mem, ok := node.Status.Capacity[VolcanoGPUResource]
	if !ok {
		return
	}
	totalMem := mem.Value()

	res, ok := node.Status.Capacity[VolcanoGPUNumber]
	if !ok {
		return
	}
	gpuNum := res.Value()
	if gpuNum == 0 {
		klog.Warningf("invalid %s=%s", VolcanoGPUNumber, res.String())
		return
	}

	memPerCard := uint(totalMem / gpuNum)
	for i := 0; i < int(gpuNum); i++ {
		ni.GPUDevices[i] = NewGPUDevice(i, memPerCard)
	}
}

func (ni *NodeInfo) setNodeState(node *corev1.Node) {
	// If node is nil, the node is un-initialized in cache
	if node == nil {
		ni.State = NodeState{
			Phase:  NotReady,
			Reason: "UnInitialized",
		}
		return
	}

	if !ni.Used.LessEqual(ni.Allocatable, Zero) {
		ni.State = NodeState{
			Phase:  NotReady,
			Reason: "OutOfSync",
		}
		return
	}

	// If node not ready, e.g. power off
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady && cond.Status != corev1.ConditionTrue {
			ni.State = NodeState{
				Phase:  NotReady,
				Reason: "NotReady",
			}
			klog.Warningf("set the node %s status to %s.", node.Name, NotReady.String())
			return
		}
	}

	// Node is ready (ignore node conditions because of taint/toleration)
	ni.State = NodeState{
		Phase:  Ready,
		Reason: "",
	}

	klog.V(4).Infof("set the node %s status to %s.", node.Name, Ready.String())
}

func (ni *NodeInfo) setRevocableZone(node *corev1.Node) {
	if node == nil {
		klog.Warningf("the argument node is null.")
		return
	}

	revocableZone := ""
	if len(node.Labels) > 0 {
		if value, found := node.Labels[schedulingv1alpha1.RevocableZone]; found {
			revocableZone = value
		}
	}
	ni.RevocableZone = revocableZone
}

// FutureIdle returns resources that will be idle in the future:
// That is current idle resources plus released resources minus pipelined resources.
func (ni *NodeInfo) FutureIdle() *Resource {
	return ni.Idle.Clone().Add(ni.Releasing).Sub(ni.Pipelined)
}

// GetNodeAllocatable return node Allocatable without OversubscriptionResource resource
func (ni *NodeInfo) GetNodeAllocatable() *Resource {
	return NewResource(ni.Node.Status.Allocatable)
}

// Ready returns whether node is ready for scheduling
func (ni *NodeInfo) Ready() bool {
	return ni.State.Phase == Ready
}

// setNode sets kubernetes node object to nodeInfo object without assertion
func (ni *NodeInfo) setNode(node *corev1.Node) {
	ni.setOversubscription(node)
	ni.setNodeGPUInfo(node)
	ni.setRevocableZone(node)

	ni.Name = node.Name
	ni.Node = node

	ni.Allocatable = NewResource(node.Status.Allocatable).Add(ni.OversubscriptionResource)
	ni.Capability = NewResource(node.Status.Capacity).Add(ni.OversubscriptionResource)
	ni.Releasing = EmptyResource()
	ni.Pipelined = EmptyResource()
	ni.Idle = NewResource(node.Status.Allocatable).Add(ni.OversubscriptionResource)
	ni.Used = EmptyResource()

	for _, ti := range ni.Tasks {
		switch ti.Status {
		case Releasing:
			ni.Idle.sub(ti.ResReq) // sub without assertion
			ni.Releasing.Add(ti.ResReq)
			ni.Used.Add(ti.ResReq)
			ni.AddGPUResource(ti.Pod)
		case Pipelined:
			ni.Pipelined.Add(ti.ResReq)
		default:
			ni.Idle.sub(ti.ResReq) // sub without assertion
			ni.Used.Add(ti.ResReq)
			ni.AddGPUResource(ti.Pod)
		}
	}
}

// GetDevicesIdleGPUMemory returns all the idle GPU memory by gpu card.
func (ni *NodeInfo) GetDevicesIdleGPUMemory() map[int]uint {
	devicesAllGPUMemory := ni.getDevicesAllGPUMemory()
	devicesUsedGPUMemory := ni.getDevicesUsedGPUMemory()
	res := map[int]uint{}
	for id, allMemory := range devicesAllGPUMemory {
		if usedMemory, found := devicesUsedGPUMemory[id]; found {
			res[id] = allMemory - usedMemory
		} else {
			res[id] = allMemory
		}
	}
	return res
}

// AddGPUResource adds the pod to GPU pool if it is assigned
func (ni *NodeInfo) AddGPUResource(pod *corev1.Pod) {
	gpuRes := GetGPUResourceOfPod(pod)
	if gpuRes > 0 {
		id := GetGPUIndex(pod)
		if dev := ni.GPUDevices[id]; dev != nil {
			dev.PodMap[string(pod.UID)] = pod
		}
	}
}

// SubGPUResource frees the gpu hold by the pod
func (ni *NodeInfo) SubGPUResource(pod *corev1.Pod) {
	gpuRes := GetGPUResourceOfPod(pod)
	if gpuRes > 0 {
		id := GetGPUIndex(pod)
		if dev := ni.GPUDevices[id]; dev != nil {
			delete(dev.PodMap, string(pod.UID))
		}
	}
}

func (ni *NodeInfo) getDevicesUsedGPUMemory() map[int]uint {
	res := map[int]uint{}
	for _, device := range ni.GPUDevices {
		res[device.ID] = device.getUsedGPUMemory()
	}
	return res
}

func (ni *NodeInfo) getDevicesAllGPUMemory() map[int]uint {
	res := map[int]uint{}
	for _, device := range ni.GPUDevices {
		res[device.ID] = device.Memory
	}
	return res
}

// Pods returns all pods running in that node
func (ni *NodeInfo) Pods() (pods []*corev1.Pod) {
	for _, t := range ni.Tasks {
		pods = append(pods, t.Pod)
	}
	return
}

// RefreshNumaSchedulerInfoByCrd used to update scheduler numa information based the CRD numatopo
func (ni *NodeInfo) RefreshNumaSchedulerInfoByCrd() {
	if ni.NumaInfo == nil {
		ni.NumaSchedulerInfo = nil
		return
	}

	tmp := ni.NumaInfo.DeepCopy()
	if ni.NumaChgFlag == NumaInfoMoreFlag {
		ni.NumaSchedulerInfo = tmp
	} else if ni.NumaChgFlag == NumaInfoLessFlag {
		numaResMap := ni.NumaSchedulerInfo.NumaResMap
		for resName, resInfo := range tmp.NumaResMap {
			klog.V(5).Infof("resource %s Allocatable : current %v new %v on node %s",
				resName, numaResMap[resName], resInfo, ni.Name)
			if numaResMap[resName].Allocatable.Size() >= resInfo.Allocatable.Size() {
				numaResMap[resName].Allocatable = resInfo.Allocatable.Clone()
				numaResMap[resName].Capacity = resInfo.Capacity
			}
		}
	}

	ni.NumaChgFlag = NumaInfoResetFlag
}
