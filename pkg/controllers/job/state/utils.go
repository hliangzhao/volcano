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

import batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"

// TotalTasks returns the total number of task pods of the given job.
func TotalTasks(job *batchv1alpha1.Job) int32 {
	var num int32
	for _, task := range job.Spec.Tasks {
		num += task.Replicas
	}
	return num
}

// TotalTaskMinAvailable returns the minimum number of available task pods required by job.
func TotalTaskMinAvailable(job *batchv1alpha1.Job) int32 {
	var num int32
	for _, task := range job.Spec.Tasks {
		// if minAvailable not specific, all replicas should be available
		if task.MinAvailable != nil {
			num += *task.MinAvailable
		} else {
			num += task.Replicas
		}
	}
	return num
}
