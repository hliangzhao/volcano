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

package env

// fully checked and understood

import (
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/controllers/job/helpers"
	plugininterface "github.com/hliangzhao/volcano/pkg/controllers/job/plugins/interface"
	corev1 "k8s.io/api/core/v1"
)

/* This plugin will set environment variables to the given pod containers when creating the job pods. */

// envPlugin implements the PluginInterface interface.
type envPlugin struct {
	arguments []string
	client    plugininterface.PluginClient
}

// New creates env plugin.
func New(client plugininterface.PluginClient, arguments []string) plugininterface.PluginInterface {
	return &envPlugin{arguments: arguments, client: client}
}

func (ep *envPlugin) Name() string {
	return "env"
}

func (ep *envPlugin) OnPodCreate(pod *corev1.Pod, job *batchv1alpha1.Job) error {
	index := helpers.GetPodIndexUnderTask(pod)

	// add VK_TASK_INDEX and VC_TASK_INDEX env to each container
	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, corev1.EnvVar{
			Name:  TaskVkIndex,
			Value: index,
		})
		pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, corev1.EnvVar{
			Name:  TaskIndex,
			Value: index,
		})
	}

	// add VK_TASK_INDEX and VC_TASK_INDEX env to each init container
	for i := range pod.Spec.InitContainers {
		pod.Spec.InitContainers[i].Env = append(pod.Spec.InitContainers[i].Env, corev1.EnvVar{
			Name:  TaskVkIndex,
			Value: index,
		})
		pod.Spec.InitContainers[i].Env = append(pod.Spec.InitContainers[i].Env, corev1.EnvVar{
			Name:  TaskIndex,
			Value: index,
		})
	}

	return nil
}

// OnJobAdd sets the envPlugin as the controlled resource of job.
func (ep *envPlugin) OnJobAdd(job *batchv1alpha1.Job) error {
	if job.Status.ControlledResources["plugin-"+ep.Name()] == ep.Name() {
		return nil
	}
	job.Status.ControlledResources["plugin-"+ep.Name()] = ep.Name()
	return nil
}

// OnJobDelete deletes the envPlugin from the controlled resource of job.
func (ep *envPlugin) OnJobDelete(job *batchv1alpha1.Job) error {
	if job.Status.ControlledResources["plugin-"+ep.Name()] != ep.Name() {
		return nil
	}
	delete(job.Status.ControlledResources, "plugin-"+ep.Name())
	return nil
}

func (ep *envPlugin) OnJobUpdate(job *batchv1alpha1.Job) error {
	return nil
}
