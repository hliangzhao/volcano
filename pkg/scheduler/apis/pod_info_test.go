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

package apis

// TODO: just copied. Not checked.
// Passed.

import (
	corev1 "k8s.io/api/core/v1"
	"reflect"
	"testing"
)

func TestGetPodResourceRequest(t *testing.T) {
	tests := []struct {
		name             string
		pod              *corev1.Pod
		expectedResource *Resource
	}{
		{
			name: "get resource for pod without init containers",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: buildResourceList("1000m", "1G"),
							},
						},
						{
							Resources: corev1.ResourceRequirements{
								Requests: buildResourceList("2000m", "1G"),
							},
						},
					},
				},
			},
			expectedResource: NewResource(buildResourceList("3000m", "2G")),
		},
		{
			name: "get resource for pod with init containers",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: buildResourceList("2000m", "5G"),
							},
						},
						{
							Resources: corev1.ResourceRequirements{
								Requests: buildResourceList("2000m", "1G"),
							},
						},
					},
					Containers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: buildResourceList("1000m", "1G"),
							},
						},
						{
							Resources: corev1.ResourceRequirements{
								Requests: buildResourceList("2000m", "1G"),
							},
						},
					},
				},
			},
			expectedResource: NewResource(buildResourceList("3000m", "5G")),
		},
	}

	for i, test := range tests {
		req := GetPodResourceRequest(test.pod)
		if !reflect.DeepEqual(req, test.expectedResource) {
			t.Errorf("case %d(%s) failed: \n expected %v, \n got: %v \n",
				i, test.name, test.expectedResource, req)
		}
	}
}

func TestGetPodResourceWithoutInitContainers(t *testing.T) {
	tests := []struct {
		name             string
		pod              *corev1.Pod
		expectedResource *Resource
	}{
		{
			name: "get resource for pod without init containers",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: buildResourceList("1000m", "1G"),
							},
						},
						{
							Resources: corev1.ResourceRequirements{
								Requests: buildResourceList("2000m", "1G"),
							},
						},
					},
				},
			},
			expectedResource: NewResource(buildResourceList("3000m", "2G")),
		},
		{
			name: "get resource for pod with init containers",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: buildResourceList("2000m", "5G"),
							},
						},
						{
							Resources: corev1.ResourceRequirements{
								Requests: buildResourceList("2000m", "1G"),
							},
						},
					},
					Containers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: buildResourceList("1000m", "1G"),
							},
						},
						{
							Resources: corev1.ResourceRequirements{
								Requests: buildResourceList("2000m", "1G"),
							},
						},
					},
				},
			},
			expectedResource: NewResource(buildResourceList("3000m", "2G")),
		},
	}

	for i, test := range tests {
		req := GetPodResourceWithoutInitContainers(test.pod)
		if !reflect.DeepEqual(req, test.expectedResource) {
			t.Errorf("case %d(%s) failed: \n expected %v, \n got: %v \n",
				i, test.name, test.expectedResource, req)
		}
	}
}
