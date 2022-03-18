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

package predicates

import (
	"fmt"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	corev1 "k8s.io/api/core/v1"
)

func checkNodeResourceIsProportional(task *apis.TaskInfo, node *apis.NodeInfo, proportional map[corev1.ResourceName]baseResource) (bool, error) {
	for resName := range proportional {
		if val, found := task.ResReq.ScalarResources[resName]; found && val > 0 {
			return true, nil
		}
	}
	for resName, resRate := range proportional {
		if val, found := node.Idle.ScalarResources[resName]; found {
			cpuReserved := val * resRate.CPU
			memReserved := val * resRate.Memory * 1000 * 1000
			r := node.Idle.Clone()
			r = r.Sub(task.ResReq)
			if r.MilliCPU < cpuReserved || r.Memory < memReserved {
				return false, fmt.Errorf("proportional of resource %s check failed", resName)
			}
		}
	}
	return true, nil
}
