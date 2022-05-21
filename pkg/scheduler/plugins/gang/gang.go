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

package gang

import (
	"fmt"
	"github.com/hliangzhao/volcano/pkg/apis/scheduling"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/metrics"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// PluginName indicates name of volcano scheduler plugin.
const PluginName = "gang"

type gangPlugin struct {
	pluginArguments framework.Arguments
}

func New(arguments framework.Arguments) framework.Plugin {
	return &gangPlugin{pluginArguments: arguments}
}

func (gp *gangPlugin) Name() string {
	return PluginName
}

func (gp *gangPlugin) OnSessionOpen(sess *framework.Session) {
	validJobFn := func(obj interface{}) *apis.ValidateResult {
		job, ok := obj.(*apis.JobInfo)
		if !ok {
			return &apis.ValidateResult{
				Pass:    false,
				Message: fmt.Sprintf("Failed to convert <%v> to *JobInfo", obj),
			}
		}

		// the job is valid only if min-available is satisfied
		if valid := job.CheckTaskMinAvailable(); !valid {
			return &apis.ValidateResult{
				Pass:    false,
				Reason:  string(schedulingv1alpha1.NotEnoughPodsOfTaskReason),
				Message: "Not enough valid pods of each task for gang-scheduling",
			}
		}

		vtn := job.ValidTaskNum()
		if vtn < job.MinAvailable {
			return &apis.ValidateResult{
				Pass:   false,
				Reason: string(schedulingv1alpha1.NotEnoughPodsReason),
				Message: fmt.Sprintf("Not enough valid tasks for gang-scheduling, valid: %d, min: %d",
					vtn, job.MinAvailable),
			}
		}
		return nil
	}
	sess.AddJobValidFn(gp.Name(), validJobFn)

	preemptableFn := func(preemptor *apis.TaskInfo, preemptees []*apis.TaskInfo) ([]*apis.TaskInfo, int) {
		var victims []*apis.TaskInfo
		jobOccupiedMap := map[apis.JobID]int32{}

		for _, preemptee := range preemptees {
			job := sess.Jobs[preemptee.Job]
			if _, found := jobOccupiedMap[job.UID]; !found {
				jobOccupiedMap[job.UID] = job.ReadyTaskNum()
			}
			if jobOccupiedMap[job.UID] > job.MinAvailable {
				jobOccupiedMap[job.UID]--
				victims = append(victims, preemptee)
			} else {
				klog.V(4).Infof("Can not preempt task <%v/%v> because job %s ready num(%d) <= MinAvailable(%d) for gang-scheduling",
					preemptee.Namespace, preemptee.Name, job.Name, jobOccupiedMap[job.UID], job.MinAvailable)
			}
		}
		klog.V(4).Infof("Victims from Gang plugins are %+v", victims)

		return victims, utils.Permit
	}
	// TODO: Support preempt/reclaim batch job.
	sess.AddReclaimableFn(gp.Name(), preemptableFn)
	sess.AddPreemptableFn(gp.Name(), preemptableFn)

	jobOrderFn := func(l, r interface{}) int {
		lv := l.(*apis.JobInfo)
		rv := r.(*apis.JobInfo)
		lReady := lv.Ready()
		rReady := rv.Ready()

		klog.V(4).Infof("Gang JobOrderFn: <%v/%v> is ready: %t, <%v/%v> is ready: %t",
			lv.Namespace, lv.Name, lReady, rv.Namespace, rv.Name, rReady)

		if lReady && rReady {
			return 0
		}
		if lReady {
			return 1
		}
		if rReady {
			return -1
		}
		return 0
	}
	sess.AddJobOrderFn(gp.Name(), jobOrderFn)
	sess.AddJobReadyFn(gp.Name(), func(obj interface{}) bool {
		ji := obj.(*apis.JobInfo)
		if ji.CheckTaskMinAvailableReady() && ji.Ready() {
			return true
		}
		return false
	})

	pipelinedFn := func(obj interface{}) int {
		ji := obj.(*apis.JobInfo)
		occupied := ji.WaitingTaskNum() + ji.ReadyTaskNum()
		if ji.CheckTaskMinAvailablePipelined() && occupied >= ji.MinAvailable {
			return utils.Permit
		}
		return utils.Reject
	}
	sess.AddJobPipelinedFn(gp.Name(), pipelinedFn)

	jobStarvingFn := func(obj interface{}) bool {
		ji := obj.(*apis.JobInfo)
		occupied := ji.WaitingTaskNum() + ji.ReadyTaskNum()
		if ji.CheckTaskMinAvailablePipelined() && occupied < ji.MinAvailable {
			return true
		}
		return false
	}
	sess.AddJobStarvingFns(gp.Name(), jobStarvingFn)
}

func (gp *gangPlugin) OnSessionClose(sess *framework.Session) {
	var unReadyTaskCount int32
	var unScheduleJobCount int

	for _, job := range sess.Jobs {
		if !job.Ready() {
			schedulableTaskNum := func() (num int32) {
				for _, task := range job.TaskStatusIndex[apis.Pending] {
					txCtx := task.GetTransactionContext()
					if task.LastTx != nil {
						txCtx = *task.LastTx
					}
					if apis.AllocatedStatus(txCtx.Status) {
						num++
					}
				}
				return num + job.ReadyTaskNum()
			}
			unReadyTaskCount = job.MinAvailable - schedulableTaskNum()
			msg := fmt.Sprintf("%v/%v tasks in gang unschedulable: %v",
				unReadyTaskCount, len(job.Tasks), job.FitError())
			job.JobFitErrors = msg

			unScheduleJobCount++
			metrics.RegisterJobRetries(job.Name)

			jc := &scheduling.PodGroupCondition{
				Type:               scheduling.PodGroupUnschedulableType,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				TransitionID:       string(sess.UID),
				Reason:             string(schedulingv1alpha1.NotEnoughResourcesReason),
				Message:            msg,
			}
			if err := sess.UpdatePodGroupCondition(job, jc); err != nil {
				klog.Errorf("Failed to update job <%s/%s> condition: %v",
					job.Namespace, job.Name, err)
			}

			// allocated task should follow the job fit error
			for _, taskInfo := range job.TaskStatusIndex[apis.Allocated] {
				fitError := job.NodesFitErrors[taskInfo.UID]
				if fitError != nil {
					continue
				}

				fitError = apis.NewFitErrors()
				job.NodesFitErrors[taskInfo.UID] = fitError
				fitError.SetError(msg)
			}
		} else {
			jc := &scheduling.PodGroupCondition{
				Type:               scheduling.PodGroupScheduled,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				TransitionID:       string(sess.UID),
				Reason:             "tasks in gang are ready to be scheduled",
				Message:            "",
			}

			if err := sess.UpdatePodGroupCondition(job, jc); err != nil {
				klog.Errorf("Failed to update job <%s/%s> condition: %v",
					job.Namespace, job.Name, err)
			}
		}
		metrics.UpdateUnscheduledTaskCount(job.Name, int(unReadyTaskCount))
		unReadyTaskCount = 0
	}
	metrics.UpdateUnscheduledJobCount(unScheduleJobCount)
}
