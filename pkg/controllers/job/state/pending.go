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
)

// pendingState implements the State interface.
type pendingState struct {
	job *apis.JobInfo
}

func (state *pendingState) Execute(action busv1alpha1.Action) error {
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

	case busv1alpha1.CompleteJobAction:
		var fn UpdateJobStatusFn
		// fn updates `status` to `Completing`
		fn = func(status *batchv1alpha1.JobStatus) (jobPhaseChanged bool) {
			status.State.Phase = batchv1alpha1.Completing
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

	default:
		var fn UpdateJobStatusFn
		// fn updates `status` to `Running` if min-available is satisfied (make the job still runnable)
		// otherwise does not update it
		fn = func(status *batchv1alpha1.JobStatus) (jobPhaseChanged bool) {
			if state.job.Job.Spec.MinAvailable <= status.Running+status.Succeeded+status.Failed {
				status.State.Phase = batchv1alpha1.Running
				return true
			}
			return false
		}
		return SyncJob(state.job, fn)
	}
}
