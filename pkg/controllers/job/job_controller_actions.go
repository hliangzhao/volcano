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
	v1 "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/quota/v1/evaluator/core"
	"k8s.io/utils/clock"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var calMutex sync.Mutex

// initJob initializes the job.
// Specifically, the func will do the following things:
// (1) Create a job instance with the given job struct pointer;
// (2) Execute the configurated plugins of the given job;
// (3) Create PVC instances for the given job;
// (4) Create or update the corresponding podgroup instance for the given job.
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

// initOnJobUpdate initializes the job when updating it.
// Specifically, the func will do the following things:
// (1) Execute the configurated plugins according to the given job pointer;
// (2) Create or update the corresponding podgroup instance for the given job.
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

// GetQueueInfo returns the queueInfo instance with the given queue name.
func (jc *jobController) GetQueueInfo(queue string) (*schedulingv1alpha1.Queue, error) {
	queueInfo, err := jc.queueLister.Get(queue)
	if err != nil {
		klog.Errorf("Failed to get queue from queueLister, error: %s", err.Error())
	}
	return queueInfo, nil
}

// syncJob syncs the job instance in the cluster.
func (jc *jobController) syncJob(jobInfo *controllerapis.JobInfo, updateStatus state.UpdateJobStatusFn) error {
	job := jobInfo.Job
	klog.V(3).Infof("Starting to sync up Job <%s/%s>, current version %d", job.Namespace, job.Name, job.Status.Version)
	defer klog.V(3).Infof("Finished Job <%s/%s> sync up, current version %d", job.Namespace, job.Name, job.Status.Version)

	if jobInfo.Job.DeletionTimestamp != nil {
		klog.Infof("Job <%s/%s> is terminating, skip management process.",
			jobInfo.Job.Namespace, jobInfo.Job.Name)
		return nil
	}

	// deep copy job to prevent mutation
	job = job.DeepCopy()

	// Find the queue that the job belongs to, and check if the queue has forwarding metadata
	// If yes, update the queue instance in cluster
	queueInfo, err := jc.GetQueueInfo(job.Spec.Queue)
	if err != nil {
		return err
	}

	var jobForwarding bool
	if len(queueInfo.Spec.ExtendClusters) != 0 {
		jobForwarding = true
		if len(job.Annotations) == 0 {
			job.Annotations = make(map[string]string)
		}
		job.Annotations[batchv1alpha1.JobForwardingKey] = "true"
		job, err = jc.volcanoClient.BatchV1alpha1().Jobs(job.Namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("failed to update job: %s/%s, error: %s", job.Namespace, job.Name, err.Error())
			return err
		}
	}

	// Skip job initiation if job is already initiated
	if !isInitiated(job) {
		if job, err = jc.initJob(job); err != nil {
			return err
		}
	} else {
		// TODO: optimize this --- call it only when scale up/down
		if err = jc.initOnJobUpdate(job); err != nil {
			return err
		}
	}

	// TODO: why call this again? (looks like a bug)
	if len(queueInfo.Spec.ExtendClusters) != 0 {
		jobForwarding = true
		job.Annotations[batchv1alpha1.JobForwardingKey] = "true"
		_, err := jc.volcanoClient.BatchV1alpha1().Jobs(job.Namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("failed to update job: %s/%s, error: %s", job.Namespace, job.Name, err.Error())
			return err
		}
	}

	var syncTask bool
	// Sync the corresponding podgroup's status (update Unscheduable event if it happens)
	if pg, _ := jc.pgLister.PodGroups(job.Namespace).Get(job.Name); pg != nil {
		if pg.Status.Phase != "" && pg.Status.Phase != schedulingv1alpha1.PodGroupPending {
			syncTask = true
		}

		for _, condition := range pg.Status.Conditions {
			if condition.Type == schedulingv1alpha1.PodGroupUnschedulableType {
				jc.recorder.Eventf(job, corev1.EventTypeWarning, string(batchv1alpha1.PodGroupPending),
					fmt.Sprintf("PodGroup %s:%s unschedule, reason: %s", job.Namespace, job.Name, condition.Message))
			}
		}
	}

	// Sync the job instance's status in cluster and update the job in cache
	// (1) Update job conditions
	var jobCondition batchv1alpha1.JobCondition
	if !syncTask {
		if updateStatus != nil {
			if updateStatus(&job.Status) {
				job.Status.State.LastTransitionTime = metav1.Now()
				jobCondition = newJobCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
				job.Status.Conditions = append(job.Status.Conditions, jobCondition)
			}
		}
		newJob, err := jc.volcanoClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to update status of Job %v/%v: %v",
				job.Namespace, job.Name, err)
			return err
		}
		if err := jc.cache.Update(newJob); err != nil {
			klog.Errorf("SyncJob - Failed to update Job %v/%v in cache:  %v",
				newJob.Namespace, newJob.Name, err)
			return err
		}
		return nil
	}

	// Sync the job's status and update the job in cache
	// (2) Create task pods that need to be created, and delete task pods that need to be deleted
	//     And then update the count of tasks in different status
	var running, pending, terminating, succeeded, failed, unknown int32
	taskStatusCount := make(map[string]batchv1alpha1.TaskState)

	podToCreate := make(map[string][]*corev1.Pod)
	var podToDelete []*corev1.Pod
	var creationErrs []error
	var deletionErrs []error
	appendMutex := sync.Mutex{}

	appendError := func(container *[]error, err error) {
		appendMutex.Lock()
		defer appendMutex.Unlock()
		*container = append(*container, err)
	}

	waitCreationGroup := sync.WaitGroup{}

	// (2.1) Create task pod struct instances
	for _, task := range job.Spec.Tasks {
		task.Template.Name = task.Name
		tc := task.Template.DeepCopy()
		name := task.Template.Name

		pods, found := jobInfo.Pods[name]
		if !found {
			pods = map[string]*corev1.Pod{}
		}

		var podToCreateEachTask []*corev1.Pod
		for i := 0; i < int(task.Replicas); i++ {
			podName := fmt.Sprintf(jobhelpers.PodNameFmt, job.Name, name, i)
			if pod, found := pods[podName]; !found {
				newPod := createJobPod(job, tc, task.TopologyPolicy, i, jobForwarding)
				if err := jc.pluginOnPodCreate(job, newPod); err != nil {
					return err
				}
				podToCreateEachTask = append(podToCreateEachTask, newPod)
				waitCreationGroup.Add(1)
			} else {
				delete(pods, podName)
				if pod.DeletionTimestamp != nil {
					klog.Infof("Pod <%s/%s> is terminating", pod.Namespace, pod.Name)
					atomic.AddInt32(&terminating, 1)
					continue
				}

				classifyAndAddUpPodBaseOnPhase(pod, &pending, &running, &succeeded, &failed, &unknown)
				calcPodStatus(pod, taskStatusCount)
			}
		}
		podToCreate[task.Name] = podToCreateEachTask
		for _, pod := range pods {
			podToDelete = append(podToDelete, pod)
		}
	}

	// (2.2) Create the task pod instances in the cluster
	for taskName, podToCreateEachTask := range podToCreate {
		if len(podToCreateEachTask) == 0 {
			continue
		}
		go func(taskName string, podToCreateEachTask []*corev1.Pod) {
			taskIndex := jobhelpers.GetTaskIndexUnderJob(taskName, job)
			if job.Spec.Tasks[taskIndex].DependsOn != nil {
				jc.waitDependsOnTaskMeetCondition(taskName, taskIndex, podToCreateEachTask, job)
			}

			for _, pod := range podToCreateEachTask {
				go func(pod *corev1.Pod) {
					defer waitCreationGroup.Done()
					newPod, err := jc.kubeClient.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
					if err != nil && !apierrors.IsAlreadyExists(err) {
						// Failed to create Pod, wait a moment and then create it again
						// This is to ensure all podsMap under the same Job created
						// So gang-scheduling could schedule the Job successfully
						klog.Errorf("Failed to create pod %s for Job %s, err %#v",
							pod.Name, job.Name, err)
						appendError(&creationErrs, fmt.Errorf("failed to create pod %s, err: %#v", pod.Name, err))
					} else {
						classifyAndAddUpPodBaseOnPhase(newPod, &pending, &running, &succeeded, &failed, &unknown)
						calcPodStatus(pod, taskStatusCount)
						klog.V(5).Infof("Created Task <%s> of Job <%s/%s>",
							pod.Name, job.Namespace, job.Name)
					}
				}(pod)
			}
		}(taskName, podToCreateEachTask)
	}
	waitCreationGroup.Wait()

	if len(creationErrs) != 0 {
		jc.recorder.Event(job, corev1.EventTypeWarning, FailedCreatePodReason,
			fmt.Sprintf("Error creating pods: %+v", creationErrs))
		return fmt.Errorf("failed to create %d pods of %d", len(creationErrs), len(podToCreate))
	}

	// (2.3) Delete task pod instances when scale down from the cluster
	waitDeletionGroup := sync.WaitGroup{}
	waitDeletionGroup.Add(len(podToDelete))
	for _, pod := range podToDelete {
		go func(pod *corev1.Pod) {
			defer waitDeletionGroup.Done()
			err := jc.deleteJobPod(job.Name, pod)
			if err != nil {
				// Failed to delete Pod, waitCreationGroup a moment and then create it again
				// This is to ensure all podsMap under the same Job created
				// So gang-scheduling could schedule the Job successfully
				klog.Errorf("Failed to delete pod %s for Job %s, err %#v",
					pod.Name, job.Name, err)
				appendError(&deletionErrs, err)
				jc.reSyncTask(pod)
			} else {
				klog.V(3).Infof("Deleted Task <%s> of Job <%s/%s>",
					pod.Name, job.Namespace, job.Name)
				atomic.AddInt32(&terminating, 1)
			}
		}(pod)
	}
	waitDeletionGroup.Wait()

	if len(deletionErrs) != 0 {
		jc.recorder.Event(job, corev1.EventTypeWarning, FailedDeletePodReason,
			fmt.Sprintf("Error deleting pods: %+v", deletionErrs))
		return fmt.Errorf("failed to delete %d pods of %d", len(deletionErrs), len(podToDelete))
	}

	// Sync the job's status again with the above changes
	job.Status = batchv1alpha1.JobStatus{
		State: job.Status.State,

		Pending:             pending,
		Running:             running,
		Succeeded:           succeeded,
		Failed:              failed,
		Terminating:         terminating,
		Unknown:             unknown,
		Version:             job.Status.Version,
		MinAvailable:        job.Spec.MinAvailable,
		TaskStatusCount:     taskStatusCount,
		ControlledResources: job.Status.ControlledResources,
		Conditions:          job.Status.Conditions,
		RetryCount:          job.Status.RetryCount,
	}

	if updateStatus != nil {
		if updateStatus(&job.Status) {
			job.Status.State.LastTransitionTime = metav1.Now()
			jobCondition = newJobCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
			job.Status.Conditions = append(job.Status.Conditions, jobCondition)
		}
	}
	newJob, err := jc.volcanoClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update status of Job %v/%v: %v",
			job.Namespace, job.Name, err)
		return err
	}
	if e := jc.cache.Update(newJob); e != nil {
		klog.Errorf("SyncJob - Failed to update Job %v/%v in cache:  %v",
			newJob.Namespace, newJob.Name, e)
		return e
	}

	return nil
}

func (jc *jobController) killJob(jobInfo *controllerapis.JobInfo, podRetainPhase state.PhaseMap, updateStatus state.UpdateJobStatusFn) error {
	job := jobInfo.Job
	klog.V(3).Infof("Killing Job <%s/%s>, current version %d", job.Namespace, job.Name, job.Status.Version)
	defer klog.V(3).Infof("Finished Job <%s/%s> killing, current version %d", job.Namespace, job.Name, job.Status.Version)

	if job.DeletionTimestamp != nil {
		klog.Infof("Job <%s/%s> is terminating, skip management process.",
			job.Namespace, job.Name)
		return nil
	}

	var pending, running, terminating, succeed, failed, unknown int32
	taskStateCount := map[string]batchv1alpha1.TaskState{}
	var errs []error
	var total int

	for _, pods := range jobInfo.Pods {
		for _, pod := range pods {
			total++

			if pod.DeletionTimestamp != nil {
				klog.Infof("Pod <%s/%s> is terminating", pod.Namespace, pod.Name)
				terminating++
				continue
			}

			maxRetry := job.Spec.MaxRetry
			lastRetry := false
			if job.Status.RetryCount >= maxRetry-1 {
				lastRetry = true
			}

			// Only retain the Failed and Succeeded pods at the last retry.
			// If it is not the last retry, kill pod as defined in `podRetainPhase`.
			retainPhase := podRetainPhase
			if lastRetry {
				retainPhase = state.PodRetainPhaseSoft
			}
			_, retain := retainPhase[pod.Status.Phase]
			if !retain {
				err := jc.deleteJobPod(job.Name, pod)
				if err == nil {
					terminating++
					continue
				}
				// record the error, and then collect the pod info like retained pod
				errs = append(errs, err)
				jc.reSyncTask(pod)
			}
			classifyAndAddUpPodBaseOnPhase(pod, &pending, &running, &succeed, &failed, &unknown)
			calcPodStatus(pod, taskStateCount)
		}
	}

	if len(errs) != 0 {
		klog.Errorf("failed to kill pods for job %s/%s, with err %+v", job.Namespace, job.Name, errs)
		jc.recorder.Event(job, corev1.EventTypeWarning, FailedDeletePodReason,
			fmt.Sprintf("Error deleting pods: %+v", errs))
		return fmt.Errorf("failed to kill %d pods of %d", len(errs), total)
	}

	job = job.DeepCopy()
	// Job version is bumped only when job is killed
	job.Status.Version++
	job.Status.Pending = pending
	job.Status.Running = running
	job.Status.Succeeded = succeed
	job.Status.Failed = failed
	job.Status.Terminating = terminating
	job.Status.Unknown = unknown
	job.Status.TaskStatusCount = taskStateCount

	// Update running duration
	klog.V(3).Infof("Running duration is %s", metav1.Duration{Duration: time.Since(jobInfo.Job.CreationTimestamp.Time)}.ToUnstructured())
	job.Status.RunningDuration = &metav1.Duration{Duration: time.Since(jobInfo.Job.CreationTimestamp.Time)}

	if updateStatus != nil {
		if updateStatus(&job.Status) {
			job.Status.State.LastTransitionTime = metav1.Now()
			jobCondition := newJobCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
			job.Status.Conditions = append(job.Status.Conditions, jobCondition)
		}
	}

	// must be called before update job status
	if err := jc.pluginOnJobDelete(job); err != nil {
		return err
	}

	// Update Job status
	newJob, err := jc.volcanoClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update status of Job %v/%v: %v",
			job.Namespace, job.Name, err)
		return err
	}
	if e := jc.cache.Update(newJob); e != nil {
		klog.Errorf("KillJob - Failed to update Job %v/%v in cache:  %v",
			newJob.Namespace, newJob.Name, e)
		return e
	}

	// Delete PodGroup
	if err := jc.volcanoClient.SchedulingV1alpha1().PodGroups(job.Namespace).Delete(context.TODO(), job.Name, metav1.DeleteOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to delete PodGroup of Job %v/%v: %v",
				job.Namespace, job.Name, err)
			return err
		}
	}

	// TODO: DO NOT delete input/output until job is deleted.

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

// createJobIOIfNotExist creates pvc resources for job if needed.
func (jc *jobController) createJobIOIfNotExist(job *batchv1alpha1.Job) (*batchv1alpha1.Job, error) {
	// If PVCs does not exist, create them for Job.
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

// createOrUpdatePodGroup creates or updates podgroup for job.
// Note that each job must be wrapped as a podgroup for scheduling.
func (jc *jobController) createOrUpdatePodGroup(job *batchv1alpha1.Job) error {
	// If the corresponding podgroup does not exist, create a podgroup with the same name for Job.
	pg, err := jc.pgLister.PodGroups(job.Namespace).Get(job.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to get PodGroup for Job <%s/%s>: %v",
				job.Namespace, job.Name, err)
			return err
		}

		// create one
		minTaskMember := map[string]int32{}
		for _, task := range job.Spec.Tasks {
			if task.MinAvailable != nil {
				minTaskMember[task.Name] = *task.MinAvailable
			} else {
				minTaskMember[task.Name] = task.Replicas
			}
		}

		pg = &schedulingv1alpha1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       job.Namespace,
				Name:            job.Name,
				Annotations:     job.Annotations,
				Labels:          job.Labels,
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(job, helpers.JobKind)},
			},
			Spec: schedulingv1alpha1.PodGroupSpec{
				MinMember:         job.Spec.MinAvailable,
				MinTaskMember:     minTaskMember,
				Queue:             job.Spec.Queue,
				MinResources:      jc.calcPGMinResources(job),
				PriorityClassName: job.Spec.PriorityClassName,
			},
		}

		if _, err = jc.volcanoClient.SchedulingV1alpha1().PodGroups(job.Namespace).Create(context.TODO(), pg, metav1.CreateOptions{}); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				klog.Errorf("Failed to create PodGroup for Job <%s/%s>: %v",
					job.Namespace, job.Name, err)
				return err
			}
		}
		return nil
	}

	// Exists. Update it
	pgShouldUpdate := false

	if pg.Spec.PriorityClassName != job.Spec.PriorityClassName {
		pg.Spec.PriorityClassName = job.Spec.PriorityClassName
		pgShouldUpdate = true
	}

	minRes := jc.calcPGMinResources(job)
	if pg.Spec.MinMember != job.Spec.MinAvailable || !reflect.DeepEqual(pg.Spec.MinResources, minRes) {
		pg.Spec.MinMember = job.Spec.MinAvailable
		pg.Spec.MinResources = minRes
		pgShouldUpdate = true
	}

	if pg.Spec.MinTaskMember == nil {
		pg.Spec.MinTaskMember = map[string]int32{}
		pgShouldUpdate = true
	}

	for _, task := range job.Spec.Tasks {
		if task.MinAvailable == nil {
			continue
		}
		if taskMember, ok := pg.Spec.MinTaskMember[task.Name]; !ok {
			pg.Spec.MinTaskMember[task.Name] = *task.MinAvailable
			pgShouldUpdate = true
		} else {
			if taskMember == *task.MinAvailable {
				continue
			}
			pgShouldUpdate = true
			pg.Spec.MinTaskMember[task.Name] = *task.MinAvailable
		}
	}
	if !pgShouldUpdate {
		return nil
	}
	if _, err = jc.volcanoClient.SchedulingV1alpha1().PodGroups(job.Namespace).Update(context.TODO(), pg, metav1.UpdateOptions{}); err != nil {
		if err != nil {
			klog.V(3).Infof("Failed to update PodGroup for Job <%s/%s>: %v",
				job.Namespace, job.Name, err)
		}
		return err
	}
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
			// for _, c := range tp.Template.Spec.Containers {
			// 	addResourceList(minAvailableTasksRes, c.Resources.Requests, c.Resources.Limits)
			// }
			pod := &corev1.Pod{
				Spec: tp.Template.Spec,
			}
			res, _ := core.PodUsageFunc(pod, clock.RealClock{})
			minAvailableTasksRes = v1.Add(minAvailableTasksRes, res)
		}
	}

	return &minAvailableTasksRes
}

// initJobStatus updates a job instance's status in the cluster and updates it in cache.
func (jc *jobController) initJobStatus(job *batchv1alpha1.Job) (*batchv1alpha1.Job, error) {
	// init job status
	if job.Status.State.Phase != "" {
		return job, nil
	}
	job.Status.State.Phase = batchv1alpha1.Pending
	job.Status.State.LastTransitionTime = metav1.Now()
	job.Status.MinAvailable = job.Spec.MinAvailable
	jobCondition := newJobCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
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

// calcPodStatus updates the corresponding task's taskStatusCount with the given pod.
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

func newJobCondition(status batchv1alpha1.JobPhase, lastTransitionTime *metav1.Time) batchv1alpha1.JobCondition {
	return batchv1alpha1.JobCondition{
		Status:             status,
		LastTransitionTime: lastTransitionTime,
	}
}
