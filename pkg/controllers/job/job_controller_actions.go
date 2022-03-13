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
	"context"
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/apis/helpers"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	controllerapis "github.com/hliangzhao/volcano/pkg/controllers/apis"
	jobhelpers "github.com/hliangzhao/volcano/pkg/controllers/job/helpers"
	"github.com/hliangzhao/volcano/pkg/controllers/job/state"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"sort"
	"sync"
	"sync/atomic"
)

var calMutex sync.Mutex

func (jc *jobController) initJob(job *batchv1alpha1.Job) (*batchv1alpha1.Job, error) {
	klog.V(3).Infof("Starting to initiate Job <%s/%s>", job.Namespace, job.Name)

	jobInstance, err := jc.initJobStatus(job)
	if err != nil {
		jc.recorder.Event(job, corev1.EventTypeWarning, string(batchv1alpha1.JobStatusError),
			fmt.Sprintf("Failed to initialize job status, err: %v", err))
		return nil, err
	}

	if err := jc.pluginOnJobAdd(jobInstance); err != nil {
		jc.recorder.Event(job, corev1.EventTypeWarning, string(batchv1alpha1.PluginError),
			fmt.Sprintf("Execute plugin when job add failed, err: %v", err))
		return nil, err
	}

	newJob, err := jc.createJobIOIfNotExist(jobInstance)
	if err != nil {
		jc.recorder.Event(job, corev1.EventTypeWarning, string(batchv1alpha1.PVCError),
			fmt.Sprintf("Failed to create PVC, err : %v", err))
		return nil, err
	}

	if err := jc.createOrUpdatePodGroup(newJob); err != nil {
		jc.recorder.Event(job, corev1.EventTypeWarning, string(batchv1alpha1.PodGroupError),
			fmt.Sprintf("Failed to create PodGroup, err : %v", err))
		return nil, err
	}

	return newJob, nil
}

func (jc *jobController) initOnJobUpdate(job *batchv1alpha1.Job) error {
	klog.V(3).Infof("Starting to initiate Job <%s/%s> on update", job.Namespace, job.Name)

	if err := jc.pluginOnJobUpdate(job); err != nil {
		jc.recorder.Event(job, corev1.EventTypeWarning, string(batchv1alpha1.PluginError),
			fmt.Sprintf("Failed to execute plugin on job update, err: %v", err))
		return err
	}

	if err := jc.createOrUpdatePodGroup(job); err != nil {
		jc.recorder.Event(job, corev1.EventTypeWarning, string(batchv1alpha1.PluginError),
			fmt.Sprintf("Failed to create (or update) PodGroup, err: %v", err))
		return err
	}

	return nil
}

func (jc *jobController) GetQueueInfo(queue string) (*schedulingv1alpha1.Queue, error) {
	queueInfo, err := jc.queueLister.Get(queue)
	if err != nil {
		klog.Errorf("Failed to get queue from queueLister, error: %s", err.Error())
	}
	return queueInfo, nil
}

func (jc *jobController) syncJob(jobInfo *controllerapis.JobInfo, updateStatus state.UpdateJobStatusFn) error {
	return nil
}

func (jc *jobController) killJob(jobInfo *controllerapis.JobInfo, podRetainPhase state.PhaseMap, updateStatus state.UpdateJobStatusFn) error {
	return nil
}

func (jc *jobController) waitDependsOnTaskMeetCondition(taskName string, taskIdx int, podToCreateEachTask []*corev1.Pod, job *batchv1alpha1.Job) {
	if job.Spec.Tasks[taskIdx].DependsOn != nil {
		dependsOn := *job.Spec.Tasks[taskIdx].DependsOn
		if len(dependsOn.Name) > 1 && dependsOn.Iteration == batchv1alpha1.IterationAny {
			// TODO: return this error
			_ = wait.PollInfinite(detectionPeriodOfDependsOnTask, func() (bool, error) {
				for _, task := range dependsOn.Name {
					if jc.isDependsOnPodsReady(task, job) {
						return true, nil
					}
				}
				return false, nil
			})
		} else {
			for _, dependsOnTask := range dependsOn.Name {
				_ = wait.PollInfinite(detectionPeriodOfDependsOnTask, func() (bool, error) {
					if jc.isDependsOnPodsReady(dependsOnTask, job) {
						return true, nil
					}
					return false, nil
				})
			}
		}
	}
}

// isDependsOnPodsReady checks the dependent task in job is ready or not.
func (jc *jobController) isDependsOnPodsReady(task string, job *batchv1alpha1.Job) bool {
	dependsOnPods := jobhelpers.GetPodsNameUnderTask(task, job)
	dependsOnTaskIndex := jobhelpers.GetTaskIndexUnderJob(task, job)

	runningPodCount := 0
	for _, podName := range dependsOnPods {
		pod, err := jc.podLister.Pods(job.Namespace).Get(podName)
		if err != nil {
			klog.Errorf("Failed to get pod %v/%v %v", job.Namespace, podName, err)
			continue
		}

		if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodSucceeded {
			klog.V(5).Infof("Sequential state, pod %v/%v of depends on tasks is not running", pod.Namespace, pod.Name)
			continue
		}

		allContainerReady := true
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if !containerStatus.Ready {
				allContainerReady = false
				break
			}
		}
		if allContainerReady {
			runningPodCount++
		}
	}

	dependsOnTaskMinReplicas := job.Spec.Tasks[dependsOnTaskIndex].MinAvailable
	if dependsOnTaskMinReplicas != nil {
		if runningPodCount < int(*dependsOnTaskMinReplicas) {
			klog.V(5).Infof("In a depends on startup state, there are already %d pods running, which is less than the minimum number of runs", runningPodCount)
			return false
		}
	}
	return true
}

func (jc *jobController) createJobIOIfNotExist(job *batchv1alpha1.Job) (*batchv1alpha1.Job, error) {
	// If PVC does not exist, create them for Job.
	var needUpdate bool
	if job.Status.ControlledResources == nil {
		job.Status.ControlledResources = map[string]string{}
	}

	for idx, volume := range job.Spec.Volumes {
		vcName := volume.VolumeClaimName
		if len(vcName) == 0 {
			// TODO: Ensure never have duplicated generated names.
			for {
				vcName = jobhelpers.GenPVCName(job.Name)
				exist, err := jc.checkPVCExist(job, vcName)
				if err != nil {
					return job, err
				}
				if exist {
					continue
				}
				job.Spec.Volumes[idx].VolumeClaimName = vcName
				needUpdate = true
				break
			}
			// TODO: check VolumeClaim must be set if VolumeClaimName is empty
			if volume.VolumeClaim != nil {
				if err := jc.createPVC(job, vcName, volume.VolumeClaim); err != nil {
					return job, err
				}
			}
		} else {
			exist, err := jc.checkPVCExist(job, vcName)
			if err != nil {
				return job, err
			}
			if !exist {
				return job, fmt.Errorf("pvc %s is not found, the job will be in the Pending state until the PVC is created", vcName)
			}
		}
		job.Status.ControlledResources["volume-pvc-"+vcName] = vcName
	}

	if needUpdate {
		newJob, err := jc.volcanoClient.BatchV1alpha1().Jobs(job.Namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to update Job %v/%v for volume claim name: %v ",
				job.Namespace, job.Name, err)
			return job, err
		}
		newJob.Status = job.Status
		return newJob, err
	}

	return job, nil
}

func (jc *jobController) checkPVCExist(job *batchv1alpha1.Job, pvc string) (bool, error) {
	if _, err := jc.pvcLister.PersistentVolumeClaims(job.Namespace).Get(pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		klog.V(3).Infof("Failed to get PVC %s for job <%s/%s>: %v",
			pvc, job.Namespace, job.Name, err)
		return false, err
	}
	return true, nil
}

// createPVC creates PVC in cluster for job.
func (jc *jobController) createPVC(job *batchv1alpha1.Job, vcName string, pvcSpec *corev1.PersistentVolumeClaimSpec) error {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: job.Namespace,
			Name:      vcName,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, helpers.JobKind),
			},
		},
		Spec: *pvcSpec,
	}

	klog.V(3).Infof("Try to create PVC: %v", pvc)

	if _, err := jc.kubeClient.CoreV1().PersistentVolumeClaims(job.Namespace).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
		klog.V(3).Infof("Failed to create PVC for Job <%s/%s>: %v",
			job.Namespace, job.Name, err)
		return err
	}
	return nil
}

func (jc *jobController) createOrUpdatePodGroup(job *batchv1alpha1.Job) error {
	return nil
}

// deleteJobPod deletes pod from the cluster for job.
func (jc *jobController) deleteJobPod(jobName string, pod *corev1.Pod) error {
	err := jc.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to delete pod %s/%s for Job %s, err %#v",
			pod.Namespace, pod.Name, jobName, err)

		return fmt.Errorf("failed to delete pod %s, err %#v", pod.Name, err)
	}
	return nil
}

func (jc *jobController) calcPGMinResources(job *batchv1alpha1.Job) *corev1.ResourceList {
	// sort task by priorityClasses
	var tasksPriority TasksPriority
	for _, task := range job.Spec.Tasks {
		tp := TaskPriority{
			priority: 0,
			TaskSpec: task,
		}
		pcName := task.Template.Spec.PriorityClassName
		pc, err := jc.priorityclassLister.Get(pcName)
		if err != nil || pc == nil {
			klog.Warningf("Ignore task %s priority class %s: %v", task.Name, pcName, err)
		} else {
			tp.priority = pc.Value
		}
		tasksPriority = append(tasksPriority, tp)
	}

	sort.Sort(tasksPriority)

	minAvailableTasksRes := corev1.ResourceList{}
	podCount := int32(0)
	for _, tp := range tasksPriority {
		for i := int32(0); i < tp.Replicas; i++ {
			if podCount >= job.Spec.MinAvailable {
				break
			}
			podCount++
			for _, c := range tp.Template.Spec.Containers {
				addResourceList(minAvailableTasksRes, c.Resources.Requests, c.Resources.Limits)
			}
		}
	}

	return &minAvailableTasksRes
}

func (jc *jobController) initJobStatus(job *batchv1alpha1.Job) (*batchv1alpha1.Job, error) {
	// init job status
	if job.Status.State.Phase != "" {
		return job, nil
	}
	job.Status.State.Phase = batchv1alpha1.Pending
	job.Status.State.LastTransitionTime = metav1.Now()
	job.Status.MinAvailable = job.Spec.MinAvailable
	jobCondition := newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
	job.Status.Conditions = append(job.Status.Conditions, jobCondition)

	// create new job in cluster
	newJob, err := jc.volcanoClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update status of Job %v/%v: %v",
			job.Namespace, job.Name, err)
		return nil, err
	}

	// update local cache
	if err := jc.cache.Update(newJob); err != nil {
		klog.Errorf("CreateJob - Failed to update Job %v/%v in cache:  %v",
			newJob.Namespace, newJob.Name, err)
		return nil, err
	}

	return newJob, nil
}

func classifyAndAddUpPodBaseOnPhase(pod *corev1.Pod, pending, running, succeeded, failed, unknown *int32) {
	switch pod.Status.Phase {
	case corev1.PodPending:
		atomic.AddInt32(pending, 1)
	case corev1.PodRunning:
		atomic.AddInt32(running, 1)
	case corev1.PodSucceeded:
		atomic.AddInt32(succeeded, 1)
	case corev1.PodFailed:
		atomic.AddInt32(failed, 1)
	default:
		atomic.AddInt32(unknown, 1)
	}
}

// calcPodStatus updates taskStatusCount with the given pod.
func calcPodStatus(pod *corev1.Pod, taskStatusCount map[string]batchv1alpha1.TaskState) {
	taskName, found := pod.Annotations[batchv1alpha1.TaskSpecKey]
	if !found {
		return
	}

	calMutex.Lock()
	defer calMutex.Unlock()

	if _, ok := taskStatusCount[taskName]; !ok {
		taskStatusCount[taskName] = batchv1alpha1.TaskState{
			Phase: map[corev1.PodPhase]int32{},
		}
	}

	switch pod.Status.Phase {
	case corev1.PodPending:
		taskStatusCount[taskName].Phase[corev1.PodPending]++
	case corev1.PodRunning:
		taskStatusCount[taskName].Phase[corev1.PodRunning]++
	case corev1.PodSucceeded:
		taskStatusCount[taskName].Phase[corev1.PodSucceeded]++
	case corev1.PodFailed:
		taskStatusCount[taskName].Phase[corev1.PodFailed]++
	default:
		taskStatusCount[taskName].Phase[corev1.PodUnknown]++
	}
}

func isInitiated(job *batchv1alpha1.Job) bool {
	if job.Status.State.Phase == "" || job.Status.State.Phase == batchv1alpha1.Pending {
		return false
	}
	return true
}

func newCondition(status batchv1alpha1.JobPhase, lastTransitionTime *metav1.Time) batchv1alpha1.JobCondition {
	return batchv1alpha1.JobCondition{
		Status:             status,
		LastTransitionTime: lastTransitionTime,
	}
}
