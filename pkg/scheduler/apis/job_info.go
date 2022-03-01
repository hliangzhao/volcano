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
	"errors"
	"fmt"
	"github.com/hliangzhao/volcano/pkg/apis/scheduling"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sort"
	"strconv"
	"strings"
	"time"
)

// JobWaitingTime is the maximum waiting time that a job could stay Pending in service level agreement.
// When job waits longer than waiting time, it should be inqueue at once, and cluster should reserve resources for it.
const JobWaitingTime = "sla-waiting-time"

type JobID types.UID

// getJobID returns the job that this pod belongs to. NOTE that a job is wrapped as a podgroup.
func getJobID(pod *corev1.Pod) JobID {
	if groupName, found := pod.Annotations[schedulingv1alpha1.KubeGroupNameAnnotationKey]; found && len(groupName) != 0 {
		// Make sure Pod and PodGroup belong to the same namespace.
		jobId := fmt.Sprintf("%s/%s", pod.Namespace, groupName)
		return JobID(jobId)
	}
	return ""
}

type tasksMap map[TaskID]*TaskInfo

// NodeResourceMap stores resource in a node
type NodeResourceMap map[string]*Resource

// JobInfo has all info of a job
type JobInfo struct {
	UID JobID

	Name      string
	Namespace string

	Queue        QueueID // the queue that this job (this podgroup) is put in
	Priority     int32
	MinAvailable int32 // min num of ready tasks to run this job
	WaitingTime  *time.Duration

	JobFitErrors   string
	NodesFitErrors map[TaskID]*FitErrors

	TaskStatusIndex       map[TaskStatus]tasksMap // task scheduling status map
	Tasks                 tasksMap                // all tasks in this job
	TaskMinAvailable      map[TaskID]int32        // min available number of pods of each task
	TaskMinAvailableTotal int32                   // required total min available pods num
	Budget                *DisruptionBudget

	Allocated    *Resource
	TotalRequest *Resource

	CreationTimestamp metav1.Time
	PodGroup          *PodGroup // the podgroup that this job wraps

	ScheduleStartTimestamp metav1.Time

	Preemptable bool

	// RevocableZone support set volcano.sh/revocable-zone annotation or label for pod/podgroup.
	// We only support empty value or * value for this version, and we will support specify revocable zone name for future release.
	// Empty value means workload can not use revocable node;
	// "*" value means workload can use all the revocable node for during node active revocable time.
	RevocableZone string
}

// NewJobInfo creates a new jobInfo for a set of tasks.
func NewJobInfo(uid JobID, tasks ...*TaskInfo) *JobInfo {
	job := &JobInfo{
		UID:            uid,
		MinAvailable:   0,
		NodesFitErrors: map[TaskID]*FitErrors{},

		Allocated:    EmptyResource(),
		TotalRequest: EmptyResource(),

		TaskStatusIndex:  map[TaskStatus]tasksMap{},
		Tasks:            tasksMap{},
		TaskMinAvailable: map[TaskID]int32{},
	}

	for _, task := range tasks {
		job.AddTaskInfo(task)
	}

	return job
}

func (ji *JobInfo) Clone() *JobInfo {
	info := &JobInfo{
		UID:       ji.UID,
		Name:      ji.Name,
		Namespace: ji.Namespace,
		Queue:     ji.Queue,
		Priority:  ji.Priority,

		MinAvailable:   ji.MinAvailable,
		WaitingTime:    ji.WaitingTime,
		JobFitErrors:   ji.JobFitErrors,
		NodesFitErrors: map[TaskID]*FitErrors{},
		Allocated:      EmptyResource(),
		TotalRequest:   EmptyResource(),

		PodGroup: ji.PodGroup.Clone(),

		TaskStatusIndex:       map[TaskStatus]tasksMap{},
		TaskMinAvailable:      ji.TaskMinAvailable,
		TaskMinAvailableTotal: ji.TaskMinAvailableTotal,
		Tasks:                 tasksMap{},
		Preemptable:           ji.Preemptable,
		RevocableZone:         ji.RevocableZone,
		Budget:                ji.Budget.Clone(),
	}

	ji.CreationTimestamp.DeepCopyInto(&info.CreationTimestamp)

	for _, task := range ji.Tasks {
		info.AddTaskInfo(task.Clone())
	}

	return info
}

func (ji JobInfo) String() string {
	res := ""

	i := 0
	for _, task := range ji.Tasks {
		res += fmt.Sprintf("\n\t %d: %v", i, task)
		i++
	}

	return fmt.Sprintf("Job (%v): namespace %v (%v), name %v, "+
		"minAvailable %d, podGroup %+v, preemptable %+v, revocableZone %+v, "+
		"minAvailable %+v, maxAvailable %+v",
		ji.UID, ji.Namespace, ji.Queue, ji.Name,
		ji.MinAvailable, ji.PodGroup, ji.Preemptable, ji.RevocableZone,
		ji.Budget.MinAvailable, ji.Budget.MaxUnavailable) + res
}

// SetPodGroup sets the podgroup details to ji.
func (ji *JobInfo) SetPodGroup(pg *PodGroup) {
	ji.Name = pg.Name
	ji.Namespace = pg.Namespace
	ji.MinAvailable = pg.Spec.MinMember
	ji.Queue = QueueID(pg.Spec.Queue)
	ji.CreationTimestamp = pg.GetCreationTimestamp()

	var err error
	ji.WaitingTime, err = ji.extractWaitingTime(pg)
	if err != nil {
		klog.Warningf("Error occurs in parsing waiting time for job <%s/%s>, err: %s.",
			pg.Namespace, pg.Name, err.Error())
		ji.WaitingTime = nil
	}

	ji.Preemptable = ji.extractPreemptable(pg)
	ji.RevocableZone = ji.extractRevocableZone(pg)
	ji.Budget = ji.extractBudget(pg)

	taskMinAvalTotal := int32(0)
	for task, member := range pg.Spec.MinTaskMember {
		ji.TaskMinAvailable[TaskID(task)] = member
		taskMinAvalTotal += member
	}
	ji.TaskMinAvailableTotal = taskMinAvalTotal
	ji.PodGroup = pg
}

// UnsetPodGroup removes podgroup details from ji.
func (ji *JobInfo) UnsetPodGroup() {
	ji.PodGroup = nil
}

/*
The following four extract functions read from the annotations and labels
of the input podgroup and returns the parsed value.
*/

// extractWaitingTime reads sla waiting time for job from podgroup annotations.
func (ji *JobInfo) extractWaitingTime(pg *PodGroup) (*time.Duration, error) {
	// TODO: should also read from given field in volcano job spec
	if _, exist := pg.Annotations[JobWaitingTime]; !exist {
		return nil, nil
	}

	waitingTime, err := time.ParseDuration(pg.Annotations[JobWaitingTime])
	if err != nil {
		return nil, err
	}
	if waitingTime <= 0 {
		return nil, errors.New("invalid sla waiting time")
	}

	return &waitingTime, nil
}

// extractPreemptable return volcano.sh/preemptable value for job.
func (ji *JobInfo) extractPreemptable(pg *PodGroup) bool {
	// check annotation and label in turn
	if len(pg.Annotations) > 0 {
		if value, found := pg.Annotations[schedulingv1alpha1.PodPreemptable]; found {
			b, err := strconv.ParseBool(value)
			if err != nil {
				klog.Warningf("invalid %s=%s", schedulingv1alpha1.PodPreemptable, value)
				return false
			}
			return b
		}
	}
	if len(pg.Labels) > 0 {
		if value, found := pg.Labels[schedulingv1alpha1.PodPreemptable]; found {
			b, err := strconv.ParseBool(value)
			if err != nil {
				klog.Warningf("invalid %s=%s", schedulingv1alpha1.PodPreemptable, value)
				return false
			}
			return b
		}
	}

	return false
}

// extractRevocableZone return volcano.sh/revocable-zone value for pod/podgroup.
func (ji *JobInfo) extractRevocableZone(pg *PodGroup) string {
	if len(pg.Annotations) > 0 {
		if value, found := pg.Annotations[schedulingv1alpha1.RevocableZone]; found {
			if value != "*" {
				return ""
			}
			return value
		}

		if value, found := pg.Annotations[schedulingv1alpha1.PodPreemptable]; found {
			if b, err := strconv.ParseBool(value); err == nil && b {
				return "*"
			}
		}
	}

	return ""
}

// extractBudget return budget value for job
func (ji *JobInfo) extractBudget(pg *PodGroup) *DisruptionBudget {
	if len(pg.Annotations) > 0 {
		if value, found := pg.Annotations[schedulingv1alpha1.JDBMinAvailable]; found {
			return NewDisruptionBudget(value, "")
		} else if value, found = pg.Annotations[schedulingv1alpha1.JDBMaxUnavailable]; found {
			return NewDisruptionBudget("", value)
		}
	}
	return NewDisruptionBudget("", "")
}

// GetMinResources return the min resources of ji.PodGroup.
func (ji *JobInfo) GetMinResources() *Resource {
	if ji.PodGroup.Spec.MinResources == nil {
		return EmptyResource()
	}
	return NewResource(*ji.PodGroup.Spec.MinResources)
}

/* The following four functions add or delete a specific task to and from ji. */

// addTaskIndex adds ti to ji.TaskStatusIndex.
func (ji *JobInfo) addTaskIndex(ti *TaskInfo) {
	if _, found := ji.TaskStatusIndex[ti.Status]; !found {
		ji.TaskStatusIndex[ti.Status] = tasksMap{}
	}
	ji.TaskStatusIndex[ti.Status][ti.UID] = ti
}

// AddTaskInfo adds a task to ji.
func (ji *JobInfo) AddTaskInfo(ti *TaskInfo) {
	ji.Tasks[ti.UID] = ti
	ji.addTaskIndex(ti)
	ji.TotalRequest.Add(ti.ResReq)
	if AllocatedStatus(ti.Status) {
		ji.Allocated.Add(ti.ResReq)
	}
}

// deleteTaskIndex removes ti from ji.TaskStatusIndex
func (ji *JobInfo) deleteTaskIndex(ti *TaskInfo) {
	if tasks, found := ji.TaskStatusIndex[ti.Status]; found {
		delete(tasks, ti.UID)
		if len(tasks) == 0 {
			delete(ji.TaskStatusIndex, ti.Status)
		}
	}
}

// DeleteTaskInfo is used to delete a task from a job
func (ji *JobInfo) DeleteTaskInfo(ti *TaskInfo) error {
	if task, found := ji.Tasks[ti.UID]; found {
		ji.TotalRequest.Sub(task.ResReq)
		if AllocatedStatus(task.Status) {
			ji.Allocated.Sub(task.ResReq)
		}
		delete(ji.Tasks, task.UID)
		ji.deleteTaskIndex(task)
		return nil
	}

	return fmt.Errorf("failed to find task <%v/%v> in job <%v/%v>",
		ti.Namespace, ti.Name, ji.Namespace, ji.Name)
}

// UpdateTaskStatus updates task's status in a job (Delete-then-Add).
// If error occurs both task and job are guaranteed to be in the original state.
func (ji *JobInfo) UpdateTaskStatus(task *TaskInfo, status TaskStatus) error {
	if err := validateStatusUpdate(task.Status, status); err != nil {
		return err
	}

	// First remove the task (if exist) from the task list.
	if _, found := ji.Tasks[task.UID]; found {
		if err := ji.DeleteTaskInfo(task); err != nil {
			return err
		}
	}

	// Update task's status to the target status once task addition is guaranteed to succeed.
	task.Status = status
	ji.AddTaskInfo(task)

	return nil
}

// IsPending returns whether job is pending.
func (ji *JobInfo) IsPending() bool {
	if ji.PodGroup == nil ||
		ji.PodGroup.Status.Phase == scheduling.PodGroupPending ||
		ji.PodGroup.Status.Phase == "" {
		return true
	}
	return false
}

// ReadyTaskNum returns the number of tasks that are ready or that is best-effort.
func (ji *JobInfo) ReadyTaskNum() int32 {
	ready := 0
	ready += len(ji.TaskStatusIndex[Bound])
	ready += len(ji.TaskStatusIndex[Binding])
	ready += len(ji.TaskStatusIndex[Running])
	ready += len(ji.TaskStatusIndex[Allocated])
	ready += len(ji.TaskStatusIndex[Succeeded])

	if tasks, found := ji.TaskStatusIndex[Pending]; found {
		for _, task := range tasks {
			if task.BestEffort {
				ready++
			}
		}
	}

	return int32(ready)
}

// WaitingTaskNum returns the number of tasks that are pipelined.
func (ji *JobInfo) WaitingTaskNum() int32 {
	return int32(len(ji.TaskStatusIndex[Pipelined]))
}

// ValidTaskNum returns the number of tasks that are valid.
func (ji *JobInfo) ValidTaskNum() int32 {
	valid := 0
	for status, tasks := range ji.TaskStatusIndex {
		if AllocatedStatus(status) || status == Succeeded || status == Pipelined || status == Pending {
			valid += len(tasks)
		}
	}
	return int32(valid)
}

// Ready returns whether job is ready for running.
func (ji *JobInfo) Ready() bool {
	return ji.ReadyTaskNum() >= ji.MinAvailable
}

// TaskSchedulingReason get detailed reason and message of the given task's scheduling result.
// It returns detailed reason and message for tasks based on last scheduling transaction.
func (ji *JobInfo) TaskSchedulingReason(tid TaskID) (reason, msg string) {
	taskInfo, exists := ji.Tasks[tid]
	if !exists {
		return "", ""
	}

	// Get detailed scheduling reason based on LastTransaction
	ctx := taskInfo.GetTransactionContext()
	if taskInfo.LastTx != nil {
		ctx = *taskInfo.LastTx
	}

	msg = ji.JobFitErrors
	switch status := ctx.Status; status {
	case Allocated, Pipelined:
		// Pod is schedulable
		msg = fmt.Sprintf("Pod %s/%s can possibly be assigned to %s", taskInfo.Namespace, taskInfo.Name, ctx.NodeName)
		if status == Pipelined {
			msg += " once resource is released"
		}
		return PodReasonSchedulable, msg
	case Pending:
		if fe := ji.NodesFitErrors[tid]; fe != nil {
			return PodReasonUnschedulable, fe.Error()
		}
		return PodReasonUndetermined, msg
	default:
		return status.String(), msg
	}
}

// FitError returns detailed information on why a job's tasks failed to fit on each available node.
func (ji *JobInfo) FitError() string {
	// sort tasks by status num
	sortReasonsHistogram := func(reasons map[string]int) []string {
		var reasonStrings []string
		for k, v := range reasons {
			reasonStrings = append(reasonStrings, fmt.Sprintf("%v %v", v, k))
		}
		sort.Strings(reasonStrings)
		return reasonStrings
	}

	// stat histogram for all tasks of the job
	reasons := make(map[string]int)
	for status, taskMap := range ji.TaskStatusIndex {
		reasons[status.String()] += len(taskMap)
	}
	reasons["minAvailable"] = int(ji.MinAvailable)
	reasonMsg := fmt.Sprintf("%v, %v", scheduling.PodGroupNotReady, strings.Join(sortReasonsHistogram(reasons), ", "))

	// stat histogram for pending tasks only
	reasons = map[string]int{}
	for uid := range ji.TaskStatusIndex[Pending] {
		reason, _ := ji.TaskSchedulingReason(uid)
		reasons[reason]++
	}
	if len(reasons) > 0 {
		reasonMsg += "; " + fmt.Sprintf("%s: %s", Pending.String(), strings.Join(sortReasonsHistogram(reasons), ", "))
	}

	return reasonMsg
}

// CheckTaskMinAvailable returns whether each task of job is valid.
// This func checks min tasks num.
func (ji *JobInfo) CheckTaskMinAvailable() bool {
	// pg.Spec.MinMember < sum(pg.Spec.MinTaskMember), then this job is valid to run
	if ji.MinAvailable < ji.TaskMinAvailableTotal {
		return true
	}

	actual := map[TaskID]int32{}
	for status, tasks := range ji.TaskStatusIndex {
		if AllocatedStatus(status) || status == Succeeded || status == Pipelined || status == Pending {
			for _, task := range tasks {
				actual[getTaskID(task.Pod)]++
			}
		}
	}

	klog.V(4).Infof("job %s/%s actual: %+v, ji.TaskMinAvailable: %+v", ji.Name, ji.Namespace, actual, ji.TaskMinAvailable)
	for taskID, minAvail := range ji.TaskMinAvailable {
		if act, ok := actual[taskID]; !ok || act < minAvail {
			return false
		}
	}

	return true
}

// CheckTaskMinAvailableReady returns ready pods meet task min available.
// This func checks min pods num.
func (ji *JobInfo) CheckTaskMinAvailableReady() bool {
	// pg.Spec.MinMember < sum(pg.Spec.MinTaskMember), then this job is valid to run
	if ji.MinAvailable < ji.TaskMinAvailableTotal {
		return true
	}

	occupiedMap := map[TaskID]int32{}
	for status, tasks := range ji.TaskStatusIndex {
		if AllocatedStatus(status) || status == Succeeded {
			for _, task := range tasks {
				occupiedMap[getTaskID(task.Pod)]++
			}
			continue
		}
		if status == Pending {
			for _, task := range tasks {
				if task.InitResReq.IsEmpty() {
					occupiedMap[getTaskID(task.Pod)]++
				}
			}
		}
	}
	for taskId, minNum := range ji.TaskMinAvailable {
		if occupiedMap[taskId] < minNum {
			klog.V(4).Infof("Job %s/%s Task %s occupied %v less than task min available",
				ji.Namespace, ji.Name, taskId, occupiedMap[taskId])
			return false
		}
	}
	return true
}

// CheckTaskMinAvailablePipelined returns ready pods meet task min available.
// Different from the above function, Pipelined tasks are viewed as valid.
func (ji *JobInfo) CheckTaskMinAvailablePipelined() bool {
	// pg.Spec.MinMember < sum(pg.Spec.MinTaskMember), then this job is valid to run
	if ji.MinAvailable < ji.TaskMinAvailableTotal {
		return true
	}

	occupiedMap := map[TaskID]int32{}
	for status, tasks := range ji.TaskStatusIndex {
		if AllocatedStatus(status) || status == Succeeded || status == Pipelined {
			for _, task := range tasks {
				occupiedMap[getTaskID(task.Pod)]++
			}
			continue
		}

		if status == Pending {
			for _, task := range tasks {
				if task.InitResReq.IsEmpty() {
					occupiedMap[getTaskID(task.Pod)]++
				}
			}
		}
	}
	for taskId, minNum := range ji.TaskMinAvailable {
		if occupiedMap[taskId] < minNum {
			klog.V(4).Infof("Job %s/%s Task %s occupied %v less than task min available",
				ji.Namespace, ji.Name, taskId, occupiedMap[taskId])
			return false
		}
	}
	return true
}
