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
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"gopkg.in/square/go-jose.v2/json"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
	"strconv"
)

/* The file provides Task info and Job info. */

// DisruptionBudget define job min pod available and max pod unavailable value
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

// JobWaitingTime is maximum waiting time that a job could stay Pending in service level agreement
// when job waits longer than waiting time, it should be inqueue at once, and cluster should reserve resources for it
const JobWaitingTime = "sla-waiting-time"

type TaskID types.UID

func getTaskID(pod *corev1.Pod) TaskID {
	if taskSpec, found := pod.Annotations[batchv1alpha1.TaskSpecKey]; found && len(taskSpec) != 0 {
		return TaskID(taskSpec)
	}
	return ""
}

type TaskInfo struct {
	UID TaskID
	Job JobID

	Name      string
	Namespace string

	InitResReq *Resource
	ResReq     *Resource

	TransactionContext
	LastTx *TransactionContext

	Priority    int32
	VolumeReady bool
	Preemptable bool
	BestEffort  bool

	// RevocableZone support set volcano.sh/revocable-zone annotation or label for pod/podgroup
	// we only support empty value or * value for this version, and we will support specify revocable zone name for future release
	// empty value means workload can not use revocable node
	// * value means workload can use all the revocable node for during node active revocable time.
	RevocableZone string

	NumaInfo   *TopologyInfo
	PodVolumes *volumebinding.PodVolumes
	Pod        *corev1.Pod
}

func NewTaskInfo(pod *corev1.Pod) *TaskInfo {
	initResReq := GetPodResourceRequest(pod)
	reqReq := initResReq

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
		ResReq:     reqReq,
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

// TransactionContext holds all the fields that needed by scheduling transaction
type TransactionContext struct {
	NodeName string
	Status   TaskStatus
}

func (tx *TransactionContext) Clone() *TransactionContext {
	if tx == nil {
		return nil
	}
	clone := *tx
	return &clone
}

type TopologyInfo struct {
	Policy string
	ResMap map[int]corev1.ResourceList // key: NUMA ID
}

func (info *TopologyInfo) Clone() *TopologyInfo {
	copyInfo := &TopologyInfo{
		Policy: info.Policy,
		ResMap: map[int]corev1.ResourceList{},
	}

	for numaId, resList := range info.ResMap {
		copyInfo.ResMap[numaId] = resList.DeepCopy()
	}

	return copyInfo
}

type JobID types.UID

func getJobID(pod *corev1.Pod) JobID {
	if groupName, found := pod.Annotations[schedulingv1alpha1.KubeGroupNameAnnotationKey]; found && len(groupName) != 0 {
		// Make sure Pod and PodGroup belong to the same namespace.
		jobId := fmt.Sprintf("%s/%s", pod.Namespace, groupName)
		return JobID(jobId)
	}
	return ""
}

const TaskPriorityAnnotation = "volcano.sh/task-priority"

// GetTransactionContext get transaction context of a task
func (ti *TaskInfo) GetTransactionContext() TransactionContext {
	return ti.TransactionContext
}

// GenerateLastTxContext generate and set context of last transaction for a task
func (ti *TaskInfo) GenerateLastTxContext() {
	ctx := ti.GetTransactionContext()
	ti.LastTx = &ctx
}

// ClearLastTxContext clear context of last transaction for a task
func (ti *TaskInfo) ClearLastTxContext() {
	ti.LastTx = nil
}

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

func (ti *TaskInfo) UnsetPodResourceDecision() {
	delete(ti.Pod.Annotations, TopologyDecisionAnnotation)
}

// Clone is used for cloning a task
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

type tasksMap map[TaskID]*TaskInfo

// NodeResourceMap stores resource in a node
type NodeResourceMap map[string]*Resource

// TODO: JobInfo
