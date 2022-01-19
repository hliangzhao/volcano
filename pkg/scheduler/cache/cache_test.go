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

package cache

import (
	`fmt`
	corev11 `k8s.io/api/core/v1`
	`k8s.io/apimachinery/pkg/api/resource`
	metav1 `k8s.io/apimachinery/pkg/apis/meta/v1`
	`k8s.io/apimachinery/pkg/types`
)

func buildNode(name string, alloc corev11.ResourceList) *corev11.Node {
	return &corev11.Node{
		ObjectMeta: metav1.ObjectMeta{
			UID:  types.UID(name),
			Name: name,
		},
		Status: corev11.NodeStatus{
			Capacity:    alloc,
			Allocatable: alloc,
		},
	}
}

func buildPod(ns, n, nn string,
	p corev11.PodPhase, req corev11.ResourceList,
	owner []metav1.OwnerReference, labels map[string]string) *corev11.Pod {

	return &corev11.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:             types.UID(fmt.Sprintf("%v-%v", ns, n)),
			Name:            n,
			Namespace:       ns,
			OwnerReferences: owner,
			Labels:          labels,
		},
		Status: corev11.PodStatus{
			Phase: p,
		},
		Spec: corev11.PodSpec{
			NodeName: nn,
			Containers: []corev11.Container{
				{
					Resources: corev11.ResourceRequirements{
						Requests: req,
					},
				},
			},
		},
	}
}

func buildResourceList(cpu string, memory string) corev11.ResourceList {
	return corev11.ResourceList{
		corev11.ResourceCPU:    resource.MustParse(cpu),
		corev11.ResourceMemory: resource.MustParse(memory),
	}
}

func buildOwnerReference(owner string) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		Controller: &controller,
		UID:        types.UID(owner),
	}
}
