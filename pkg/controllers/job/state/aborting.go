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

// abortingState implements the State interface.
type abortingState struct {
	job *apis.JobInfo
}

func (state *abortingState) Execute(action busv1alpha1.Action) error {
	switch action {
	case busv1alpha1.ResumeJobAction:
		var fn UpdateJobStatusFn
		// fn updates `status` to `Restarting`
		fn = func(status *batchv1alpha1.JobStatus) (jobPhaseChanged bool) {
			status.State.Phase = batchv1alpha1.Restarting
			status.RetryCount++
			return true
		}
		// kill the non-retained pods of the job and update the job status to restarting
		// wait for next schedule
		return KillJob(state.job, PodRetainPhaseSoft, fn)
	default:
		var fn UpdateJobStatusFn
		// fn updates `status` to `Aborted` only when the job can indeed be aborted
		// (i.e., no terminating, pending, and running pods)
		fn = func(status *batchv1alpha1.JobStatus) (jobPhaseChanged bool) {
			if status.Terminating != 0 || status.Pending != 0 || status.Running != 0 {
				return false
			}
			status.State.Phase = batchv1alpha1.Aborted
			return true
		}
		// kill the non-retained pods of the job and set the job status to aborted
		return KillJob(state.job, PodRetainPhaseSoft, fn)
	}
}
