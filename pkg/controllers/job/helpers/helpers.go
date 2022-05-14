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

package helpers

import (
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	controllerapis "github.com/hliangzhao/volcano/pkg/controllers/apis"
	schedulerapis "github.com/hliangzhao/volcano/pkg/scheduler/apis"
	corev1 "k8s.io/api/core/v1"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const (
	// PodNameFmt pod name format (jobName-taskName-index)
	PodNameFmt = "%s-%s-%d"

	// persistentVolumeClaimFmt represents persistent volume claim name format
	persistentVolumeClaimFmt = "%s-pvc-%s"
)

// GetPodIndexUnderTask returns the pod index.
func GetPodIndexUnderTask(pod *corev1.Pod) string {
	num := strings.Split(pod.Name, "-")
	if len(num) >= 3 {
		return num[len(num)-1]
	}
	return ""
}

// CompareTask judges whether lv is before rv.
func CompareTask(lv, rv *schedulerapis.TaskInfo) bool {
	lStr := GetPodIndexUnderTask(lv.Pod)
	rStr := GetPodIndexUnderTask(rv.Pod)
	lIndex, lErr := strconv.Atoi(lStr)
	rIndex, rErr := strconv.Atoi(rStr)
	if lErr != nil || rErr != nil || lIndex == rIndex {
		return lv.Pod.CreationTimestamp.Before(&rv.Pod.CreationTimestamp)
	}
	if lIndex > rIndex {
		return false
	}
	return true
}

// GetTaskKey returns pod.Annotations[batchv1alpha1.TaskSpecKey].
func GetTaskKey(pod *corev1.Pod) string {
	if pod.Annotations == nil || pod.Annotations[batchv1alpha1.TaskSpecKey] == "" {
		return batchv1alpha1.DefaultTaskSpec
	}
	return pod.Annotations[batchv1alpha1.TaskSpecKey]
}

// GetTaskSpec returns the task in job with the given taskName.
func GetTaskSpec(job *batchv1alpha1.Job, taskName string) (batchv1alpha1.TaskSpec, bool) {
	for _, task := range job.Spec.Tasks {
		if task.Name == taskName {
			return task, true
		}
	}
	return batchv1alpha1.TaskSpec{}, false
}

// MakePodName creates pod name.
func MakePodName(jobName string, taskName string, index int) string {
	return fmt.Sprintf(PodNameFmt, jobName, taskName, index)
}

// MakeDomainName creates task domain name.
func MakeDomainName(task batchv1alpha1.TaskSpec, job *batchv1alpha1.Job, index int) string {
	hostName := task.Template.Spec.Hostname
	subdomain := task.Template.Spec.Subdomain
	if len(hostName) == 0 {
		hostName = MakePodName(job.Name, task.Name, index)
	}
	if len(subdomain) == 0 {
		subdomain = job.Name
	}
	return hostName + "." + subdomain
}

// GenRandomStr generate random str with specified length.
func GenRandomStr(length int) string {
	bytes := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	var res []byte
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < length; i++ {
		res = append(res, bytes[r.Intn(len(bytes))])
	}
	return string(res)
}

// GenPVCName generates pvc name with job name.
func GenPVCName(jobName string) string {
	return fmt.Sprintf(persistentVolumeClaimFmt, jobName, GenRandomStr(12))
}

// GetJobKeyByReq gets the key for the job request.
func GetJobKeyByReq(req *controllerapis.Request) string {
	return fmt.Sprintf("%s/%s", req.Namespace, req.JobName)
}

// GetTaskIndexUnderJob returns the index of the task in the given job.
func GetTaskIndexUnderJob(taskName string, job *batchv1alpha1.Job) int {
	for idx, task := range job.Spec.Tasks {
		if task.Name == taskName {
			return idx
		}
	}
	return -1
}

// GetPodsNameUnderTask returns names of all pods in the task.
func GetPodsNameUnderTask(taskName string, job *batchv1alpha1.Job) []string {
	var res []string
	for _, task := range job.Spec.Tasks {
		if task.Name == taskName {
			for idx := 0; idx < int(task.Replicas); idx++ {
				res = append(res, MakePodName(job.Name, taskName, idx))
			}
			break
		}
	}
	return res
}
