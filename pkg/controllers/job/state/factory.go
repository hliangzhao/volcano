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

// UpdateJobStatusFn is a function that updates the variable `status`.
// If `status` been changed, return true
type UpdateJobStatusFn func(status *batchv1alpha1.JobStatus) (jobPhaseChanged bool)

// JobActionFn is a function that updates `job` by executing the UpdateJobStatusFn fn.
type JobActionFn func(job *apis.JobInfo, fn UpdateJobStatusFn) error

// JobActionKillFn is a function that kill the non-retained pods of the job, and then executing the UpdateJobStatusFn fn to update the job's status.
type JobActionKillFn func(job *apis.JobInfo, podRetainPhase PhaseMap, fn UpdateJobStatusFn) error

var (
	SyncJob JobActionFn     // SyncJob sync job resource in cluster
	KillJob JobActionKillFn // KillJob kill the non-retained pods of the job in cluster
)

type PhaseMap map[corev1.PodPhase]struct{}

// PodRetainPhaseNone stores no phase
var PodRetainPhaseNone = PhaseMap{}

// PodRetainPhaseSoft stores PodSucceeded and PodFailed phases
var PodRetainPhaseSoft = PhaseMap{
	corev1.PodSucceeded: {},
	corev1.PodFailed:    {},
}

type State interface {
	// Execute uses an internal status variable to update the status of the job we care about
	Execute(action busv1alpha1.Action) error
}

// NewState transforms the input job into new state.
func NewState(jobInfo *apis.JobInfo) State {
	switch jobInfo.Job.Status.State.Phase {
	case batchv1alpha1.Pending:
		return &pendingState{job: jobInfo}
	case batchv1alpha1.Running:
		return &runningState{job: jobInfo}
	case batchv1alpha1.Restarting:
		return &restartingState{job: jobInfo}
	case batchv1alpha1.Terminated, batchv1alpha1.Completed, batchv1alpha1.Failed:
		return &finishedState{job: jobInfo}
	case batchv1alpha1.Terminating:
		return &terminatingState{job: jobInfo}
	case batchv1alpha1.Aborting:
		return &abortingState{job: jobInfo}
	case batchv1alpha1.Aborted:
		return &abortedState{job: jobInfo}
	case batchv1alpha1.Completing:
		return &completingState{job: jobInfo}
	}
	// if not specified, transform it to pending state
	return &pendingState{job: jobInfo}
}
