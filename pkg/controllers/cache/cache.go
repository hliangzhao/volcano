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

package cache

import (
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	controllerapis "github.com/hliangzhao/volcano/pkg/controllers/apis"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"sync"
	"time"
)

type jobCache struct {
	sync.Mutex
	jobInfos    map[string]*controllerapis.JobInfo
	deletedJobs workqueue.RateLimitingInterface
}

func keyFn(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func JobKeyByName(namespace string, name string) string {
	return keyFn(namespace, name)
}

func JobKeyByRequest(req *controllerapis.Request) string {
	return keyFn(req.Namespace, req.JobName)
}

func JobKey(job *batchv1alpha1.Job) string {
	return keyFn(job.Namespace, job.Name)
}

func jobTerminated(job *controllerapis.JobInfo) bool {
	return job.Job == nil || len(job.Pods) == 0
}

func jobKeyOfPod(pod *corev1.Pod) (string, error) {
	jobName, found := pod.Annotations[batchv1alpha1.JobNameKey]
	if !found {
		return "", fmt.Errorf("failed to find job name of pod <%s/%s>", pod.Namespace, pod.Name)
	}
	return keyFn(pod.Namespace, jobName), nil
}

func NewCache() Cache {
	queue := workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Microsecond, 180*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
	return &jobCache{
		jobInfos:    map[string]*controllerapis.JobInfo{},
		deletedJobs: workqueue.NewRateLimitingQueue(queue),
	}
}

func (jc *jobCache) Get(key string) (*controllerapis.JobInfo, error) {
	jc.Lock()
	defer jc.Unlock()

	ji, found := jc.jobInfos[key]
	if !found {
		return nil, fmt.Errorf("failed to find job <%s>", key)
	}
	if ji.Job == nil {
		return nil, fmt.Errorf("job <%s> is not ready", key)
	}
	return ji.Clone(), nil
}

func (jc *jobCache) GetStatus(key string) (*batchv1alpha1.JobStatus, error) {
	jc.Lock()
	defer jc.Unlock()

	ji, found := jc.jobInfos[key]
	if !found {
		return nil, fmt.Errorf("failed to find job <%s>", key)
	}
	if ji.Job == nil {
		return nil, fmt.Errorf("job <%s> is not ready", key)
	}

	status := ji.Job.Status
	return &status, nil
}

func (jc *jobCache) Add(job *batchv1alpha1.Job) error {
	jc.Lock()
	defer jc.Unlock()

	key := JobKey(job)
	if ji, found := jc.jobInfos[key]; found {
		if ji.Job == nil {
			ji.SetJob(job)
			return nil
		}
		return fmt.Errorf("duplicated jobInfo <%v>", key)
	}

	jc.jobInfos[key] = &controllerapis.JobInfo{
		Name:      job.Name,
		Namespace: job.Namespace,
		Job:       job,
		Pods:      make(map[string]map[string]*corev1.Pod),
	}
	return nil
}

func (jc *jobCache) Update(job *batchv1alpha1.Job) error {
	jc.Lock()
	defer jc.Unlock()

	key := JobKey(job)
	ji, found := jc.jobInfos[key]
	if !found {
		return fmt.Errorf("failed to find job <%v>", key)
	}

	ji.Job = job
	return nil
}

func (jc *jobCache) deleteJob(ji *controllerapis.JobInfo) {
	klog.V(3).Infof("Try to delete Job <%v/%v>", ji.Namespace, ji.Name)
	jc.deletedJobs.AddRateLimited(ji)
}

func (jc *jobCache) Delete(job *batchv1alpha1.Job) error {
	jc.Lock()
	defer jc.Unlock()

	key := JobKey(job)
	ji, found := jc.jobInfos[key]
	if !found {
		return fmt.Errorf("failed to find job <%v>", key)
	}

	ji.Job = nil
	jc.deleteJob(ji)
	return nil
}

func (jc *jobCache) AddPod(pod *corev1.Pod) error {
	jc.Lock()
	defer jc.Unlock()

	key, err := jobKeyOfPod(pod)
	if err != nil {
		return err
	}
	ji, found := jc.jobInfos[key]
	if !found {
		jc.jobInfos[key] = &controllerapis.JobInfo{
			Pods: make(map[string]map[string]*corev1.Pod),
		}
	}

	return ji.AddPod(pod)
}

func (jc *jobCache) UpdatePod(pod *corev1.Pod) error {
	jc.Lock()
	defer jc.Unlock()

	key, err := jobKeyOfPod(pod)
	if err != nil {
		return err
	}
	ji, found := jc.jobInfos[key]
	if !found {
		jc.jobInfos[key] = &controllerapis.JobInfo{
			Pods: make(map[string]map[string]*corev1.Pod),
		}
	}

	return ji.UpdatePod(pod)
}

func (jc *jobCache) DeletePod(pod *corev1.Pod) error {
	jc.Lock()
	defer jc.Unlock()

	key, err := jobKeyOfPod(pod)
	if err != nil {
		return err
	}
	ji, found := jc.jobInfos[key]
	if !found {
		jc.jobInfos[key] = &controllerapis.JobInfo{
			Pods: make(map[string]map[string]*corev1.Pod),
		}
	}

	if err = ji.DeletePod(pod); err != nil {
		return err
	}
	if jc.jobInfos[key].Job == nil {
		jc.deleteJob(ji)
	}
	return nil
}

// TaskCompleted judges whether all pods of a task are succeeded.
func (jc *jobCache) TaskCompleted(jobKey, taskName string) bool {
	jc.Lock()
	defer jc.Unlock()

	var taskReplicas, completed int32

	ji, found := jc.jobInfos[jobKey]
	if !found {
		return false
	}
	taskPods, found := ji.Pods[taskName]
	if !found || ji.Job == nil {
		return false
	}

	for _, task := range ji.Job.Spec.Tasks {
		if task.Name == taskName {
			taskReplicas = task.Replicas
			break
		}
	}
	if taskReplicas <= 0 {
		return false
	}

	for _, pod := range taskPods {
		if pod.Status.Phase == corev1.PodSucceeded {
			completed++
		}
	}
	return completed >= taskReplicas
}

func (jc *jobCache) TaskFailed(jobKey, taskName string) bool {
	jc.Lock()
	defer jc.Unlock()

	var taskReplicas, retried, maxRetry int32

	ji, found := jc.jobInfos[jobKey]
	if !found {
		return false
	}
	taskPods, found := ji.Pods[taskName]
	if !found || ji.Job == nil {
		return false
	}

	for _, task := range ji.Job.Spec.Tasks {
		if task.Name == taskName {
			maxRetry = task.MaxRetry
			taskReplicas = task.Replicas
			break
		}
	}
	// maxRetry == -1 means no limit
	if taskReplicas == 0 || maxRetry == -1 {
		return false
	}

	// Compatible with existing job
	if maxRetry == 0 {
		maxRetry = 3
	}

	for _, pod := range taskPods {
		if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
			for j := range pod.Status.InitContainerStatuses {
				stat := pod.Status.InitContainerStatuses[j]
				retried += stat.RestartCount
			}
			for j := range pod.Status.ContainerStatuses {
				stat := pod.Status.ContainerStatuses[j]
				retried += stat.RestartCount
			}
		}
	}
	return retried > maxRetry
}

func (jc *jobCache) processCleanupJob() bool {
	obj, shutdown := jc.deletedJobs.Get()
	if shutdown {
		return false
	}
	defer jc.deletedJobs.Done(obj)

	ji, ok := obj.(*controllerapis.JobInfo)
	if !ok {
		klog.Errorf("failed to convert %v to *controllerapis.JobInfo", obj)
		return true
	}

	jc.Lock()
	defer jc.Unlock()

	if jobTerminated(ji) {
		jc.deletedJobs.Forget(obj)
		key := keyFn(ji.Namespace, ji.Name)
		delete(jc.jobInfos, key)
		klog.V(3).Infof("Job <%s> was deleted.", key)
	} else {
		jc.deleteJob(ji)
	}

	return true
}

func (jc *jobCache) worker() {
	// TODO: why write in this way?
	for jc.processCleanupJob() {
	}
}

func (jc *jobCache) Run(stopCh <-chan struct{}) {
	wait.Until(jc.worker, 0, stopCh)
}
