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

package apis

import (
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// JobInfo is a wrapper of job, which contains more structure information of job.
// JobInfo is used to construct job cache stored in the volcano-scheduler.
type JobInfo struct {
	Namespace string
	Name      string
	Job       *batchv1alpha1.Job
	Pods      map[string]map[string]*corev1.Pod // a job consists of multiple tasks, which has multiple pods
}

// Clone deeply copies ji and returns the result.
func (ji *JobInfo) Clone() *JobInfo {
	clonedJi := &JobInfo{
		Namespace: ji.Namespace,
		Name:      ji.Name,
		Job:       ji.Job,
		Pods:      make(map[string]map[string]*corev1.Pod),
	}

	for taskName, task := range ji.Pods {
		clonedJi.Pods[taskName] = make(map[string]*corev1.Pod)
		for podName, pod := range task {
			clonedJi.Pods[taskName][podName] = pod
		}
	}
	return clonedJi
}

// SetJob updates ji's information with given job.
func (ji *JobInfo) SetJob(job *batchv1alpha1.Job) {
	ji.Name = job.Name
	ji.Namespace = job.Namespace
	ji.Job = job
}

// AddPod adds the input pod to the right place.
// Specifically, we find the task of the given job according to the annotations,
// and add pod to the right place of the map ji.Pods.
func (ji *JobInfo) AddPod(pod *corev1.Pod) error {
	taskName, err := getTaskOfPod(pod)
	if err != nil {
		return err
	}

	// create task if required
	if _, found := ji.Pods[taskName]; !found {
		ji.Pods[taskName] = make(map[string]*corev1.Pod)
	}
	// exist pod cannot be added
	if _, found := ji.Pods[taskName][pod.Name]; found {
		return fmt.Errorf("duplicated pod")
	}

	// add
	ji.Pods[taskName][pod.Name] = pod
	return nil
}

// UpdatePod updates ji with the input pod.
func (ji *JobInfo) UpdatePod(pod *corev1.Pod) error {
	taskName, err := getTaskOfPod(pod)
	if err != nil {
		return err
	}

	// task and pod should be found
	if _, found := ji.Pods[taskName]; !found {
		return fmt.Errorf("cannot find task %s in cache", taskName)
	}
	if _, found := ji.Pods[taskName][pod.Name]; !found {
		return fmt.Errorf("cannot find pod <%s/%s> in cache", pod.Namespace, pod.Name)
	}

	// update
	ji.Pods[taskName][pod.Name] = pod
	return nil
}

func (ji *JobInfo) DeletePod(pod *corev1.Pod) error {
	taskName, err := getTaskOfPod(pod)
	if err != nil {
		return err
	}

	// delete
	if task, found := ji.Pods[taskName]; found {
		delete(task, pod.Name)
		if len(task) == 0 {
			delete(ji.Pods, taskName)
		}
	}
	return nil
}

func getTaskOfPod(pod *corev1.Pod) (string, error) {
	taskName, found := pod.Annotations[batchv1alpha1.TaskSpecKey]
	if !found {
		return "", fmt.Errorf("failed to find taskName of Pod <%s/%s>",
			pod.Namespace, pod.Name)
	}
	_, found = pod.Annotations[batchv1alpha1.JobVersion]
	if !found {
		return "", fmt.Errorf("failed to find jobVersion of Pod <%s/%s>",
			pod.Namespace, pod.Name)
	}
	return taskName, nil
}
