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

package predicates

import (
	"fmt"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	corev1 "k8s.io/api/core/v1"
)

// checkNodeGPUSharingPredicate checks if a gpu sharing pod can be scheduled on a node.
func checkNodeGPUSharingPredicate(pod *corev1.Pod, nodeInfo *apis.NodeInfo) (bool, error) {
	if apis.GetGPUResourceOfPod(pod) <= 0 {
		return true, nil
	}
	gpuId := predicateGPU(pod, nodeInfo)
	if gpuId < 0 {
		return false, fmt.Errorf("no enough gpu memory on single device of node %s", nodeInfo.Name)
	}
	return true, nil
}

// predicateGPU returns one available GPU ID.
func predicateGPU(pod *corev1.Pod, node *apis.NodeInfo) int {
	gpuRequest := apis.GetGPUResourceOfPod(pod)
	allocatableGPUs := node.GetDevicesIdleGPUMemory()

	for devId := 0; devId < len(allocatableGPUs); devId++ {
		availableGPU, ok := allocatableGPUs[devId]
		if ok {
			if availableGPU >= gpuRequest {
				return devId
			}
		}
	}
	return -1
}
