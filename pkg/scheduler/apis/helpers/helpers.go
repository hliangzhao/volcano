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

package helpers

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	corev1 "k8s.io/api/core/v1"
	"math"
)

// Min returns a new Resource instance where each resource quantity is the min one of l and r.
func Min(l, r *apis.Resource) *apis.Resource {
	res := &apis.Resource{}
	res.MilliCPU = math.Min(l.MilliCPU, r.MilliCPU)
	res.Memory = math.Min(l.Memory, r.Memory)

	if l.ScalarResources == nil || r.ScalarResources == nil {
		return res
	}

	res.ScalarResources = map[corev1.ResourceName]float64{}
	for lName, lQuantity := range l.ScalarResources {
		res.ScalarResources[lName] = math.Min(lQuantity, r.ScalarResources[lName])
	}
	return res
}

// Max returns a new Resource instance where each resource quantity is the max one of l and r.
func Max(l, r *apis.Resource) *apis.Resource {
	res := &apis.Resource{}
	res.MilliCPU = math.Max(l.MilliCPU, r.MilliCPU)
	res.Memory = math.Max(l.Memory, r.Memory)

	if l.ScalarResources == nil && r.ScalarResources == nil {
		return res
	}

	res.ScalarResources = map[corev1.ResourceName]float64{}
	if l.ScalarResources != nil {
		for lName, lQuantity := range l.ScalarResources {
			if lQuantity > 0 {
				res.ScalarResources[lName] = math.Max(lQuantity, r.ScalarResources[lName])
			}
		}
	}
	if r.ScalarResources != nil {
		for rName, rQuantity := range r.ScalarResources {
			if rQuantity > 0 {
				res.ScalarResources[rName] = math.Max(rQuantity, res.ScalarResources[rName])
			}
		}
	}
	return res
}

func Share(l, r float64) float64 {
	var share float64
	if r == 0 {
		if l == 0 {
			share = 0
		} else {
			share = 1
		}
	} else {
		share = l / r
	}
	return share
}
