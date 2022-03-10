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

package svc

import (
	"flag"
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	plugininterface "github.com/hliangzhao/volcano/pkg/controllers/job/plugins/interface"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

type svcPlugin struct {
	arguments            []string
	client               plugininterface.PluginClient
	publishNotReadyAddrs bool
	disableNetworkPolicy bool
}

func New(client plugininterface.PluginClient, arguments []string) plugininterface.PluginInterface {
	sp := svcPlugin{arguments: arguments, client: client}
	sp.addFlags()
	return &sp
}

func (sp *svcPlugin) addFlags() {
	flagSet := flag.NewFlagSet(sp.Name(), flag.ContinueOnError)
	flagSet.BoolVar(&sp.publishNotReadyAddrs, "publish-not-ready-addresses", sp.publishNotReadyAddrs,
		"set publishNotReadyAddresses of svc to true")
	flagSet.BoolVar(&sp.disableNetworkPolicy, "disable-network-policy", sp.disableNetworkPolicy,
		"set disableNetworkPolicy of svc to true")

	if err := flagSet.Parse(sp.arguments); err != nil {
		klog.Errorf("plugin %s flagset parse failed, err: %v", sp.Name(), err)
	}
}

func (sp *svcPlugin) Name() string {
	return "svc"
}

func (sp *svcPlugin) OnPodCreate(pod *corev1.Pod, job *batchv1alpha1.Job) error {
	return nil
}

func (sp *svcPlugin) OnJobAdd(job *batchv1alpha1.Job) error {
	return nil
}

func (sp *svcPlugin) OnJobDelete(job *batchv1alpha1.Job) error {
	return nil
}

func (sp *svcPlugin) OnJobUpdate(job *batchv1alpha1.Job) error {
	return nil
}

func (sp *svcPlugin) mountConfigMap(pod *corev1.Pod, job *batchv1alpha1.Job) {}

func (sp *svcPlugin) createServiceIfNotExist(job *batchv1alpha1.Job) error {
	return nil
}

func (sp *svcPlugin) createNetworkPolicyIfNotExist(job *batchv1alpha1.Job) error {
	return nil
}

func (sp *svcPlugin) cmName(job *batchv1alpha1.Job) string {
	return fmt.Sprintf("%s-%s", job.Name, sp.Name())
}

func GenerateHosts(job *batchv1alpha1.Job) map[string]string {
	return nil
}
