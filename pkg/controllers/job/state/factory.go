/*
Copyright 2021-2022 hliangzhao.

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

type PhaseMap map[corev1.PodPhase]struct{}

// UpdateJobStatusFn updates job status
type UpdateJobStatusFn func(status *batchv1alpha1.JobStatus) (jobPhaseChanged bool)

// ActionFn creates or deletes Pods according to job's Spec
type ActionFn func(job *apis.JobInfo, fn UpdateJobStatusFn) error

// KillActionFn kills all the Pods of job whose phase is not in podRetainPhase
type KillActionFn func(job *apis.JobInfo, podRetainPhase PhaseMap, fn UpdateJobStatusFn) error

var (
	SyncJob ActionFn
	KillJob KillActionFn
)

// PodRetainPhaseNone stores no phase
var PodRetainPhaseNone = PhaseMap{}

// PodRetainPhaseSoft stores PodSucceeded and PodFailed phases
var PodRetainPhaseSoft = PhaseMap{
	corev1.PodSucceeded: {},
	corev1.PodFailed:    {},
}

type State interface {
	// Execute executes act based on current state
	Execute(act busv1alpha1.Action) error
}

// NewState returns the state of given job
func NewState(jobInfo *apis.JobInfo) State {
	job := jobInfo.Job
	switch job.Status.State.Phase {
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
	// pending state by default
	return &pendingState{job: jobInfo}
}
