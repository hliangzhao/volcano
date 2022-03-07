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

package state

import (
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/controllers/apis"
	corev1 "k8s.io/api/core/v1"
)

type runningState struct {
	job *apis.JobInfo
}

func (state *runningState) Execute(act busv1alpha1.Action) error {
	switch act {
	case busv1alpha1.RestartJobAction:
		return KillJob(state.job, PodRetainPhaseNone, func(status *batchv1alpha1.JobStatus) bool {
			status.State.Phase = batchv1alpha1.Restarting
			status.RetryCount++
			return true
		})
	case busv1alpha1.AbortJobAction:
		return KillJob(state.job, PodRetainPhaseSoft, func(status *batchv1alpha1.JobStatus) bool {
			status.State.Phase = batchv1alpha1.Aborting
			return true
		})
	case busv1alpha1.TerminateJobAction:
		return KillJob(state.job, PodRetainPhaseSoft, func(status *batchv1alpha1.JobStatus) bool {
			status.State.Phase = batchv1alpha1.Terminating
			return true
		})
	case busv1alpha1.CompleteJobAction:
		return KillJob(state.job, PodRetainPhaseSoft, func(status *batchv1alpha1.JobStatus) bool {
			status.State.Phase = batchv1alpha1.Completing
			return true
		})
	default:
		return SyncJob(state.job, func(status *batchv1alpha1.JobStatus) bool {
			jobReplicas := TotalTasks(state.job.Job)
			if jobReplicas == 0 {
				return false
			}

			minSuccess := state.job.Job.Spec.MinSuccess
			if minSuccess != nil && status.Succeeded >= *minSuccess {
				status.State.Phase = batchv1alpha1.Completed
				return true
			}

			totalTaskMinAvailable := TotalTaskMinAvailable(state.job.Job)
			if status.Succeeded+status.Failed == jobReplicas {
				if state.job.Job.Spec.MinAvailable >= totalTaskMinAvailable {
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
		})
	}
}
