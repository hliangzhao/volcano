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

package utils

import (
	"fmt"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
	"sync"
)

func BuildResourceList(cpu string, memory string) corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpu),
		corev1.ResourceMemory: resource.MustParse(memory),
		apis.GPUResourceName:  resource.MustParse("0"),
	}
}

func BuildResourceListWithGPU(cpu string, memory string, GPU string) corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpu),
		corev1.ResourceMemory: resource.MustParse(memory),
		apis.GPUResourceName:  resource.MustParse(GPU),
	}
}

func BuildNode(name string, alloc corev1.ResourceList, labels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      labels,
			Annotations: map[string]string{},
		},
		Status: corev1.NodeStatus{
			Capacity:    alloc,
			Allocatable: alloc,
		},
	}
}

func BuildPod(namespace, name, nodeName string, p corev1.PodPhase, req corev1.ResourceList,
	groupName string, labels map[string]string, selector map[string]string) *corev1.Pod {

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(fmt.Sprintf("%v-%v", namespace, name)),
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
			Annotations: map[string]string{
				schedulingv1alpha1.KubeGroupNameAnnotationKey: groupName,
			},
		},
		Status: corev1.PodStatus{
			Phase: p,
		},
		Spec: corev1.PodSpec{
			NodeName:     nodeName,
			NodeSelector: selector,
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

// FakeBinder is used as fake binder
type FakeBinder struct {
	Binds   map[string]string
	Channel chan string
}

// Bind binds each task to its allocated node.
func (fb *FakeBinder) Bind(kubeClient *kubernetes.Clientset, tasks []*apis.TaskInfo) (error, []*apis.TaskInfo) {
	for _, p := range tasks {
		key := fmt.Sprintf("%v/%v", p.Namespace, p.Name)
		fb.Binds[key] = p.NodeName
	}
	return nil, nil
}

// FakeEvictor is used as fake evictor
type FakeEvictor struct {
	sync.Mutex
	evicts  []string
	Channel chan string
}

func (fe *FakeEvictor) Evicts() []string {
	fe.Lock()
	defer fe.Unlock()
	return append([]string{}, fe.evicts...)
}

// Evict adds p to FakeEvictor.evicts.
func (fe *FakeEvictor) Evict(pod *corev1.Pod, reason string) error {
	fe.Lock()
	defer fe.Unlock()

	fmt.Println("PodName: ", pod.Name)
	key := fmt.Sprintf("%v/%v", pod.Namespace, pod.Name)
	fe.evicts = append(fe.evicts, key)

	fe.Channel <- key

	return nil
}

// FakeStatusUpdater is used for fake status update
type FakeStatusUpdater struct {
}

func (fsu *FakeStatusUpdater) UpdatePodCondition(pod *corev1.Pod, podCondition *corev1.PodCondition) (*corev1.Pod, error) {
	// do nothing here
	return nil, nil
}

func (fsu *FakeStatusUpdater) UpdatePodGroup(pg *apis.PodGroup) (*apis.PodGroup, error) {
	// do nothing here
	return nil, nil
}

// FakeVolumeBinder is used as fake volume binder
type FakeVolumeBinder struct {
}

func (fvb *FakeVolumeBinder) AllocateVolumes(task *apis.TaskInfo, hostname string, podVolumes *volumebinding.PodVolumes) error {
	return nil
}

func (fvb *FakeVolumeBinder) BindVolumes(task *apis.TaskInfo, podVolumes *volumebinding.PodVolumes) error {
	return nil
}

func (fvb *FakeVolumeBinder) GetPodVolumes(task *apis.TaskInfo, node *corev1.Node) (*volumebinding.PodVolumes, error) {
	return nil, nil
}
