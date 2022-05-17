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

package numaaware

import (
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	`github.com/hliangzhao/volcano/pkg/scheduler/framework`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/numaaware/policy`
	`k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology`
	`k8s.io/kubernetes/pkg/kubelet/cm/cpuset`
	`sync`
)

const (
	PluginName     = "numa-aware"
	NumaTopoWeight = "weight"
)

type numaPlugin struct {
	sync.Mutex
	args            framework.Arguments
	hintProviders   []policy.HintProvider
	assignRes       map[apis.TaskID]map[string]apis.ResNumaSets // map[taskUID]map[nodeName][resourceName]cpuset.CPUSet
	nodeResSets     map[string]apis.ResNumaSets                 // map[nodeName][resourceName]cpuset.CPUSet
	taskBindNodeMap map[apis.TaskID]string
}

func New(args framework.Arguments) framework.Plugin {
	// TODO
	return nil
}

func (np *numaPlugin) Name() string {
	return PluginName
}

func (np *numaPlugin) OnSessionOpen(sess *framework.Session) {
	// TODO
}

func (np *numaPlugin) OnSessionClose(sess *framework.Session) {
	// TODO
}

func calculateWeight(args framework.Arguments) int {
	// TODO
	return 0
}

func filterNodeByPolicy(task *apis.TaskInfo, node *apis.NodeInfo, nodeResSets map[string]apis.ResNumaSets) (fit bool, err error) {
	// TODO
	return false, nil
}

func getNodeNumaNumForTask(nodeInfo []*apis.NodeInfo, resAssignMap map[string]apis.ResNumaSets) []apis.ScoredNode {
	// TODO
	return nil
}

func getNumaNodeCntForCPUID(cpus cpuset.CPUSet, cpuDetails topology.CPUDetails) int {
	// TODO
	return 0
}
