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

package apis

// fully checked and understood

import corev1 "k8s.io/api/core/v1"

// GPUDevice include gpu id, memory and the pods who are sharing it.
type GPUDevice struct {
	ID int
	// The pods who are sharing this GPU
	PodMap map[string]*corev1.Pod
	// memory per card
	Memory uint
}

// NewGPUDevice creates a GPU device.
func NewGPUDevice(id int, mem uint) *GPUDevice {
	return &GPUDevice{
		ID:     id,
		Memory: mem,
		PodMap: map[string]*corev1.Pod{},
	}
}

// getGPUResourceOfContainer returns the GPU resource (GPU mem) size required by the given container c.
func getGPUResourceOfContainer(c *corev1.Container) uint {
	var gpuMem uint
	if val, ok := c.Resources.Limits[VolcanoGPUResource]; ok {
		gpuMem = uint(val.Value())
	}
	return gpuMem
}

// GetGPUResourceOfPod returns the GPU resource size required by the input pod.
func GetGPUResourceOfPod(pod *corev1.Pod) uint {
	var gpuMem uint
	for _, c := range pod.Spec.Containers {
		gpuMem += getGPUResourceOfContainer(&c)
	}
	return gpuMem
}

// getUsedGPUMemory calculates the used memory of the device.
func (g *GPUDevice) getUsedGPUMemory() uint {
	res := uint(0)
	for _, pod := range g.PodMap {
		// in PodMap, the pod could only in three cases: succeed, failed, and running
		// apparently we only count on the running pods
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		} else {
			res += GetGPUResourceOfPod(pod)
		}
	}
	return res
}
