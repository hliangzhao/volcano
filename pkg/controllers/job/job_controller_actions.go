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

package job

import (
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	controllerapis "github.com/hliangzhao/volcano/pkg/controllers/apis"
	"github.com/hliangzhao/volcano/pkg/controllers/job/state"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sync"
)

var calMutex sync.Mutex

func (jc *jobController) initJob(job *batchv1alpha1.Job) (*batchv1alpha1.Job, error) {
	return nil, nil
}

func (jc *jobController) initOnJobUpdate(job *batchv1alpha1.Job) error {
	return nil
}

func (jc *jobController) GetQueueInfo(queue string) (*schedulingv1alpha1.Queue, error) {
	return nil, nil
}

func (jc *jobController) syncJob(jobInfo *controllerapis.JobInfo, updateStatus state.UpdateJobStatusFn) error {
	return nil
}

func (jc *jobController) killJob(jobInfo *controllerapis.JobInfo, podRetainPhase state.PhaseMap, updateStatus state.UpdateJobStatusFn) error {
	return nil
}

func (jc *jobController) waitDependsOnTaskMeetCondition(taskName string, taskIdx int, podToCreateEachTask []*corev1.Pod, job *batchv1alpha1.Job) {
}

func (jc *jobController) isDependsOnPodsReady(task string, job *batchv1alpha1.Job) bool {
	return false
}

func (jc *jobController) createJobIOIfNotExist(job *batchv1alpha1.Job) (*batchv1alpha1.Job, error) {
	return nil, nil
}

func (jc *jobController) checkPVCExist(job *batchv1alpha1.Job, pvc string) (bool, error) {
	return false, nil
}

func (jc *jobController) createPVC(job *batchv1alpha1.Job, vcName string, pvc *corev1.PersistentVolumeClaimSpec) error {
	return nil
}

func (jc *jobController) createOrUpdatePodGroup(job *batchv1alpha1.Job) error {
	return nil
}

func (jc *jobController) deleteJobPod(jobName string, poid *corev1.Pod) error {
	return nil
}

func (jc *jobController) calcPGMinResources(job *batchv1alpha1.Job) *corev1.ResourceList {
	return nil
}

func (jc *jobController) initJobStatus(job *batchv1alpha1.Job) (*batchv1alpha1.Job, error) {
	return nil, nil
}

func classifyAndAddUpPodBaseOnPhase(pod *corev1.Pod, pending, running, succeeded, failed, unknown *int32) {
}

func calcPodStatus(pod *corev1.Pod, taskStatusCount map[string]batchv1alpha1.TaskState) {}

func isInitiated(job *batchv1alpha1.Job) bool {
	return false
}

func newCondition(status batchv1alpha1.JobPhase, lastTransitionTime *metav1.Time) batchv1alpha1.JobCondition {
	return batchv1alpha1.JobCondition{}
}
