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

package apis

import (
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// buildNode creates a node.
func buildNode(name string, alloc corev1.ResourceList) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Status: corev1.NodeStatus{
			Capacity:    alloc,
			Allocatable: alloc,
		},
	}
}

// buildPod creates a pod.
func buildPod(namespace, name, nodeName string, phase corev1.PodPhase, req corev1.ResourceList,
	owner []metav1.OwnerReference, labels map[string]string) *corev1.Pod {

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:             types.UID(fmt.Sprintf("%v-%v", namespace, name)),
			Name:            name,
			Namespace:       namespace,
			OwnerReferences: owner,
			Labels:          labels,
		},
		Status: corev1.PodStatus{
			Phase: phase,
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Requests: req,
					},
				},
			},
		},
	}
}

func buildResourceList(cpu string, memory string) corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpu),
		corev1.ResourceMemory: resource.MustParse(memory),
	}
}

func buildResource(cpu string, memory string) *Resource {
	return NewResource(corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpu),
		corev1.ResourceMemory: resource.MustParse(memory),
	})
}

func buildOwnerReference(owner string) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		Controller: &controller,
		UID:        types.UID(owner),
	}
}
