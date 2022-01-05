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
	batchv1alpha1 `github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1`
	controllerapis `github.com/hliangzhao/volcano/pkg/controllers/apis`
	corev1 `k8s.io/api/core/v1`
)

type Cache interface {
	Run(stopCh <-chan struct{})

	Get(key string) (*controllerapis.JobInfo, error)
	GetStatus(key string) (*batchv1alpha1.JobStatus, error)
	Add(obj *batchv1alpha1.Job) error
	Update(obj *batchv1alpha1.Job) error
	Delete(obj *batchv1alpha1.Job) error

	AddPod(pod *corev1.Pod) error
	UpdatePod(pod *corev1.Pod) error
	DeletePod(pod *corev1.Pod) error

	TaskCompleted(jobKey, taskName string) bool
	TaskFailed(jobKey, taskName string) bool
}
