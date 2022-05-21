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

package job

import (
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/controllers/job/plugins"
	pluginsinterface "github.com/hliangzhao/volcano/pkg/controllers/job/plugins/interface"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// pluginOnPodCreate executes plugins defined in job when creating the pod instance.
func (jc *jobController) pluginOnPodCreate(job *batchv1alpha1.Job, pod *corev1.Pod) error {
	client := pluginsinterface.PluginClient{KubeClient: jc.kubeClient}
	for name, args := range job.Spec.Plugins {
		pb, found := plugins.GetPluginBuilder(name)
		if !found {
			err := fmt.Errorf("failed to get plugin %s", name)
			klog.Error(err)
			return err
		}
		klog.Infof("Starting to execute plugin at <pluginOnPodCreate>: %s on job: <%s/%s>", name, job.Namespace, job.Name)
		if err := pb(client, args).OnPodCreate(pod, job); err != nil {
			klog.Errorf("Failed to process plugin %s on pod create, err %v.", name, err)
			return err
		}
	}
	return nil
}

// pluginOnJobAdd executes plugins defined in job when creating the job instance.
func (jc *jobController) pluginOnJobAdd(job *batchv1alpha1.Job) error {
	client := pluginsinterface.PluginClient{KubeClient: jc.kubeClient}
	if job.Status.ControlledResources == nil {
		job.Status.ControlledResources = map[string]string{}
	}
	for name, args := range job.Spec.Plugins {
		pb, found := plugins.GetPluginBuilder(name)
		if !found {
			err := fmt.Errorf("failed to get plugin %s", name)
			klog.Error(err)
			return err
		}
		klog.Infof("Starting to execute plugin at <pluginOnJobAdd>: %s on job: <%s/%s>", name, job.Namespace, job.Name)
		if err := pb(client, args).OnJobAdd(job); err != nil {
			klog.Errorf("Failed to process plugin %s on job add, err %v.", name, err)
			return err
		}
	}
	return nil
}

// pluginOnJobDelete executes plugins defined in job when deleting the job instance.
func (jc *jobController) pluginOnJobDelete(job *batchv1alpha1.Job) error {
	client := pluginsinterface.PluginClient{KubeClient: jc.kubeClient}
	if job.Status.ControlledResources == nil {
		job.Status.ControlledResources = map[string]string{}
	}
	for name, args := range job.Spec.Plugins {
		pb, found := plugins.GetPluginBuilder(name)
		if !found {
			err := fmt.Errorf("failed to get plugin %s", name)
			klog.Error(err)
			return err
		}
		klog.Infof("Starting to execute plugin at <pluginOnJobDelete>: %s on job: <%s/%s>", name, job.Namespace, job.Name)
		if err := pb(client, args).OnJobDelete(job); err != nil {
			klog.Errorf("failed to process plugin %s on job delete, err %v.", name, err)
			return err
		}
	}
	return nil
}

// pluginOnJobUpdate executes plugins defined in job when updating the job instance.
func (jc *jobController) pluginOnJobUpdate(job *batchv1alpha1.Job) error {
	client := pluginsinterface.PluginClient{KubeClient: jc.kubeClient}
	if job.Status.ControlledResources == nil {
		job.Status.ControlledResources = map[string]string{}
	}
	for name, args := range job.Spec.Plugins {
		pb, found := plugins.GetPluginBuilder(name)
		if !found {
			err := fmt.Errorf("failed to get plugin %s", name)
			klog.Error(err)
			return err
		}
		klog.Infof("Starting to execute plugin at <pluginOnJobUpdate>: %s on job: <%s/%s>", name, job.Namespace, job.Name)
		if err := pb(client, args).OnJobUpdate(job); err != nil {
			klog.Errorf("failed to process plugin %s on job update, err %v.", name, err)
			return err
		}
	}
	return nil
}
