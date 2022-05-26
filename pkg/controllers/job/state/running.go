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

package state

// fully checked and understood

import (
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/controllers/apis"
	corev1 "k8s.io/api/core/v1"
)

// runningState implements the State interface.
type runningState struct {
	job *apis.JobInfo
}

func (state *runningState) Execute(action busv1alpha1.Action) error {
	switch action {
	case busv1alpha1.RestartJobAction:
		var fn UpdateJobStatusFn
		// fn updates `status` to `Restarting`
		fn = func(status *batchv1alpha1.JobStatus) (jobPhaseChanged bool) {
			status.State.Phase = batchv1alpha1.Restarting
			status.RetryCount++
			return true
		}
		return KillJob(state.job, PodRetainPhaseNone, fn)

	case busv1alpha1.AbortJobAction:
		var fn UpdateJobStatusFn
		// fn updates `status` to `Aborting`
		fn = func(status *batchv1alpha1.JobStatus) (jobPhaseChanged bool) {
			status.State.Phase = batchv1alpha1.Aborting
			return true
		}
		return KillJob(state.job, PodRetainPhaseSoft, fn)

	case busv1alpha1.TerminateJobAction:
		var fn UpdateJobStatusFn
		// fn updates `status` to `Terminating`
		fn = func(status *batchv1alpha1.JobStatus) (jobPhaseChanged bool) {
			status.State.Phase = batchv1alpha1.Terminating
			return true
		}
		return KillJob(state.job, PodRetainPhaseSoft, fn)

	case busv1alpha1.CompleteJobAction:
		var fn UpdateJobStatusFn
		// fn updates `status` to `Completing`
		fn = func(status *batchv1alpha1.JobStatus) (jobPhaseChanged bool) {
			status.State.Phase = batchv1alpha1.Completing
			return true
		}
		return KillJob(state.job, PodRetainPhaseSoft, fn)

	default:
		var fn UpdateJobStatusFn
		// fn updates `status` to `Completed` or `Failed` according to the actual status of `state`
		fn = func(status *batchv1alpha1.JobStatus) (jobPhaseChanged bool) {
			totalTaskNum := TotalTasks(state.job.Job)
			if totalTaskNum == 0 {
				return false
			}

			// if completed, set `status` to completed
			minSuccess := state.job.Job.Spec.MinSuccess
			if minSuccess != nil && status.Succeeded >= *minSuccess {
				status.State.Phase = batchv1alpha1.Completed
				return true
			}

			totalTaskMinAvailable := TotalTaskMinAvailable(state.job.Job)
			if status.Succeeded+status.Failed == totalTaskNum {
				// if all task pods either in Succeeded or Failed status, we need a classified discussion...

				// as I have questioned before, `Job.Spec.MinAvailable` and `totalTaskMinAvailable`, both of them should be satisfied
				// to satisfy both of them, we only need to judge the larger one is satisfied or not
				if state.job.Job.Spec.MinAvailable >= totalTaskMinAvailable {
					// judge each task in the job: if the task's MinAvailable is not satisfied, set `status` to `Failed`
					for _, task := range state.job.Job.Spec.Tasks {
						if task.MinAvailable == nil {
							continue
						}
						if taskStatus, ok := status.TaskStatusCount[task.Name]; ok {
							if taskStatus.Phase[corev1.PodSucceeded] < *task.MinAvailable {
								status.State.Phase = batchv1alpha1.Failed
								return true
							}
						}
					}
				}

				if minSuccess != nil && status.Succeeded < *minSuccess {
					status.State.Phase = batchv1alpha1.Failed
				} else if status.Succeeded >= state.job.Job.Spec.MinAvailable {
					status.State.Phase = batchv1alpha1.Completed
				} else {
					status.State.Phase = batchv1alpha1.Failed
				}
				return true
			}
			return false
		}
		return SyncJob(state.job, fn)
	}
}
