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

package predicates

import (
	"fmt"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	corev1 "k8s.io/api/core/v1"
)

// checkNodeResourceIsProportional checks if a gpu:cpu:memory is Proportional
func checkNodeResourceIsProportional(task *apis.TaskInfo, node *apis.NodeInfo,
	proportional map[corev1.ResourceName]baseResource) (bool, error) {

	for resourceName := range proportional {
		if value, found := task.ResReq.ScalarResources[resourceName]; found && value > 0 {
			return true, nil
		}
	}
	for resourceName, resourceRate := range proportional {
		if value, found := node.Idle.ScalarResources[resourceName]; found {
			cpuReserved := value * resourceRate.CPU
			memoryReserved := value * resourceRate.Memory * 1000 * 1000
			r := node.Idle.Clone()
			r = r.Sub(task.ResReq)
			if r.MilliCPU < cpuReserved || r.Memory < memoryReserved {
				return false, fmt.Errorf("proportional of resource %s check failed", resourceName)
			}
		}
	}
	return true, nil
}
