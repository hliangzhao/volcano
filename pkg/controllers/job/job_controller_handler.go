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
)

func (jc *jobController) addCommand(obj interface{}) {}

func (jc *jobController) addJob(obj interface{}) {}

func (jc *jobController) updateJob(oldObj, newObj interface{}) {}

func (jc *jobController) deleteJob(obj interface{}) {}

func (jc *jobController) addPod(obj interface{}) {}

func (jc *jobController) updatePod(oldObj, newObj interface{}) {}

func (jc *jobController) deletePod(obj interface{}) {}

func (jc *jobController) recordJobEvent(ns, name string, event batchv1alpha1.JobEvent, msg string) {}

func (jc *jobController) handleCommands() {}

func (jc *jobController) processNextCommand() bool {
	return false
}

func (jc *jobController) updatePodGroup(oldObj, newObj interface{}) {}

// TODO: add handler for PodGroup unschedulable event.
