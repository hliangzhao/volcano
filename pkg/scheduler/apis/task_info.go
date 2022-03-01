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
	"encoding/json"
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
	"strconv"
)

type TaskID types.UID

// getTaskID returns the task that this pod belongs to.
func getTaskID(pod *corev1.Pod) TaskID {
	if taskSpec, found := pod.Annotations[batchv1alpha1.TaskSpecKey]; found && len(taskSpec) != 0 {
		return TaskID(taskSpec)
	}
	return ""
}

// DisruptionBudget defines job min pod available and max pod unavailable value.
type DisruptionBudget struct {
	MinAvailable   string
	MaxUnavailable string
}

func NewDisruptionBudget(minAvail, maxUnAvail string) *DisruptionBudget {
	return &DisruptionBudget{
		MinAvailable:   minAvail,
		MaxUnavailable: maxUnAvail,
	}
}

func (db *DisruptionBudget) Clone() *DisruptionBudget {
	return &DisruptionBudget{
		MinAvailable:   db.MinAvailable,
		MaxUnavailable: db.MaxUnavailable,
	}
}

// TaskStatus defines the status of a task (a pod).
type TaskStatus int

const (
	// Pending means the task is pending in the api-server.
	Pending TaskStatus = 1 << iota

	// Allocated means the scheduler assigns a host to it.
	Allocated

	// Pipelined means the scheduler assigns a host to wait for releasing resource.
	Pipelined

	// Binding means the scheduler send Bind request to apiserver.
	Binding

	// Bound means the task/Pod bounds to a host.
	Bound

	// Running means a task is running on the host.
	Running

	// Releasing means a task/pod is deleted.
	Releasing

	// Succeeded means that all containers in the pod have voluntarily terminated
	// with a container exit code of 0, and the system is not going to restart any of these containers.
	Succeeded

	// Failed means that all containers in the pod have terminated, and at least one container has
	// terminated in a failure (exited with a non-zero exit code or was stopped by the system).
	Failed

	// Unknown means the status of task/pod is unknown to the scheduler.
	Unknown
)

func (ts TaskStatus) String() string {
	switch ts {
	case Pending:
		return "Pending"
	case Allocated:
		return "Allocated"
	case Pipelined:
		return "Pipelined"
	case Binding:
		return "Binding"
	case Bound:
		return "Bound"
	case Running:
		return "Running"
	case Releasing:
		return "Releasing"
	case Succeeded:
		return "Succeeded"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// getTaskStatus returns current status of the input pod.
func getTaskStatus(pod *corev1.Pod) TaskStatus {
	switch pod.Status.Phase {
	case corev1.PodRunning:
		if pod.DeletionTimestamp != nil {
			return Releasing
		}
		return Running
	case corev1.PodPending:
		if pod.DeletionTimestamp != nil {
			return Releasing
		}
		if len(pod.Spec.NodeName) == 0 {
			return Pending
		}
		return Bound
	case corev1.PodUnknown: // TODO: this case could be removed
		return Unknown
	case corev1.PodSucceeded:
		return Succeeded
	case corev1.PodFailed:
		return Failed
	}
	return Unknown
}

// AllocatedStatus checks whether the task is scheduled to a node.
// If a task has been scheduled, returns true; otherwise, returns false.
func AllocatedStatus(status TaskStatus) bool {
	switch status {
	case Bound, Binding, Running, Allocated:
		// scheduled
		return true
	default:
		// not scheduled
		return false
	}
}

// validateStatusUpdate validates whether the status transfer is valid.
func validateStatusUpdate(oldStatus, newStatus TaskStatus) error {
	// TODO: this func seems not implemented?
	return nil
}

// TransactionContext holds all the fields that needed by the scheduling transaction.
type TransactionContext struct {
	NodeName string
	Status   TaskStatus
}

func (tx *TransactionContext) Clone() *TransactionContext {
	if tx == nil {
		return nil
	}
	clone := *tx // this will allocate new mem space for variable clone
	return &clone
}

// TaskInfo has all info of a task.
type TaskInfo struct {
	UID TaskID
	Job JobID // the job that this task belongs to

	Name      string
	Namespace string

	InitResReq *Resource // resource requests of init containers
	ResReq     *Resource // resource requests of containers

	TransactionContext
	LastTx *TransactionContext

	Priority    int32
	VolumeReady bool
	Preemptable bool
	BestEffort  bool

	// RevocableZone supports setting volcano.sh/revocable-zone annotation or label for pod/podgroup.
	// We only support empty value or * value for this version, and we will support specify revocable zone name for future release.
	// Empty value means workload can not use revocable node.
	// "*" value means workload can use all the revocable nodes during node active revocable time.
	RevocableZone string

	NumaInfo   *TopologyInfo
	PodVolumes *volumebinding.PodVolumes
	Pod        *corev1.Pod // TODO: a task may consist of multiple pods, why not use slice here?
}

func NewTaskInfo(pod *corev1.Pod) *TaskInfo {
	initResReq := GetPodResourceRequest(pod)
	resReq := initResReq

	bestEffort := initResReq.IsEmpty()
	preemptable := GetPodPreemptable(pod)
	revocableZone := GetPodRevocableZone(pod)
	numaInfo := GetPodTopologyInfo(pod)

	jobId := getJobID(pod)

	ti := &TaskInfo{
		UID:        TaskID(pod.UID),
		Job:        jobId,
		Name:       pod.Name,
		Namespace:  pod.Namespace,
		InitResReq: initResReq,
		ResReq:     resReq,
		TransactionContext: TransactionContext{
			NodeName: pod.Spec.NodeName,
			Status:   getTaskStatus(pod),
		},
		Priority:      1,
		Preemptable:   preemptable,
		BestEffort:    bestEffort,
		RevocableZone: revocableZone,
		NumaInfo:      numaInfo,
		Pod:           pod,
	}

	// update priority if exist
	if pod.Spec.Priority != nil {
		ti.Priority = *pod.Spec.Priority
	}
	if taskPriority, ok := pod.Annotations[TaskPriorityAnnotation]; ok {
		if priority, err := strconv.ParseInt(taskPriority, 10, 32); err == nil {
			ti.Priority = int32(priority)
		}
	}

	return ti
}

func (ti *TaskInfo) Clone() *TaskInfo {
	return &TaskInfo{
		UID:           ti.UID,
		Job:           ti.Job,
		Name:          ti.Name,
		Namespace:     ti.Namespace,
		Priority:      ti.Priority,
		PodVolumes:    ti.PodVolumes,
		Pod:           ti.Pod,
		ResReq:        ti.ResReq.Clone(),
		InitResReq:    ti.InitResReq.Clone(),
		VolumeReady:   ti.VolumeReady,
		Preemptable:   ti.Preemptable,
		BestEffort:    ti.BestEffort,
		RevocableZone: ti.RevocableZone,
		NumaInfo:      ti.NumaInfo.Clone(),
		TransactionContext: TransactionContext{
			NodeName: ti.NodeName,
			Status:   ti.Status,
		},
		LastTx: ti.LastTx.Clone(),
	}
}

const TaskPriorityAnnotation = "volcano.sh/task-priority"

// GetTransactionContext gets transaction context of a task。
func (ti *TaskInfo) GetTransactionContext() TransactionContext {
	return ti.TransactionContext
}

// GenerateLastTxContext generates and sets context of last transaction for a task.
func (ti *TaskInfo) GenerateLastTxContext() {
	ctx := ti.GetTransactionContext()
	ti.LastTx = &ctx
}

// ClearLastTxContext clears context of last transaction for a task.
func (ti *TaskInfo) ClearLastTxContext() {
	ti.LastTx = nil
}

// SetPodResourceDecision sets the TopologyDecisionAnnotation annotation for the pod that wrapped by this TaskInfo.
func (ti *TaskInfo) SetPodResourceDecision() error {
	if ti.NumaInfo == nil || len(ti.NumaInfo.ResMap) == 0 {
		return nil
	}

	klog.V(4).Infof("%v/%v resource decision: %v", ti.Namespace, ti.Name, ti.NumaInfo.ResMap)
	decision := PodResourceDecision{
		NumaResources: ti.NumaInfo.ResMap,
	}
	layout, err := json.Marshal(&decision)
	if err != nil {
		return err
	}

	metav1.SetMetaDataAnnotation(&ti.Pod.ObjectMeta, TopologyDecisionAnnotation, string(layout[:]))
	return nil
}

// UnsetPodResourceDecision unsets the TopologyDecisionAnnotation annotation for the pod that wrapped by this TaskInfo.
func (ti *TaskInfo) UnsetPodResourceDecision() {
	delete(ti.Pod.Annotations, TopologyDecisionAnnotation)
}

// GetTaskSpecKey gets the task key (<namespace>/<name>) of ti.
func (ti *TaskInfo) GetTaskSpecKey() TaskID {
	if ti.Pod == nil {
		return ""
	}
	return getTaskID(ti.Pod)
}

// String returns the taskInfo details in a string
func (ti TaskInfo) String() string {
	if ti.NumaInfo == nil {
		return fmt.Sprintf("Task (%v:%v/%v): job %v, status %v, pri %v"+
			"resreq %v, preemptable %v, revocableZone %v",
			ti.UID, ti.Namespace, ti.Name, ti.Job, ti.Status, ti.Priority,
			ti.ResReq, ti.Preemptable, ti.RevocableZone)
	}

	return fmt.Sprintf("Task (%v:%v/%v): job %v, status %v, pri %v"+
		"resreq %v, preemptable %v, revocableZone %v, numaInfo %v",
		ti.UID, ti.Namespace, ti.Name, ti.Job, ti.Status, ti.Priority,
		ti.ResReq, ti.Preemptable, ti.RevocableZone, *ti.NumaInfo)
}
