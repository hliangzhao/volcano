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

package cpumanager

import (
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/numaaware/policy`
	corev1 `k8s.io/api/core/v1`
	`k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology`
	`k8s.io/kubernetes/pkg/kubelet/cm/cpuset`
)

type cpuManage struct {
}

func NewProvider() policy.HintProvider {
	return &cpuManage{}
}

func (mng *cpuManage) Name() string {
	return "cpuManage"
}

func guaranteedCPUs(container *corev1.Container) int {
	// TODO
	return 0
}

// generateCPUTopologyHints return the numa topology hints based on
// - availableCPUs
func generateCPUTopologyHints(availableCPUs cpuset.CPUSet, CPUDetails topology.CPUDetails, request int) []policy.TopologyHint {
	// TODO
	return nil
}

func (mng *cpuManage) GetTopologyHints(container *corev1.Container,
	topoInfo *apis.NumaTopoInfo, resNumaSets apis.ResNumaSets) map[string][]policy.TopologyHint {
	// TODO
	return nil
}

func (mng *cpuManage) Allocate(container *corev1.Container, bestHit *policy.TopologyHint,
	topoInfo *apis.NumaTopoInfo, resNumaSets apis.ResNumaSets) map[string]cpuset.CPUSet {
	// TODO
	return nil
}
