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

package utils

// fully checked and understood

import (
	"context"
	"fmt"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
	"sync"
	"time"
)

/* The fake functions defined in this file are used for code tests. */

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

func BuildPodWithPVC(namespace, name, nodeName string, p corev1.PodPhase, req corev1.ResourceList, pvc *corev1.PersistentVolumeClaim,
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
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      pvc.Name,
							MountPath: "/data",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: pvc.Name,
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvc.Name,
						},
					},
				},
			},
		},
	}
}

func BuildDynamicPVC(namespace, name string, req corev1.ResourceList) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, *storagev1.StorageClass) {
	tmp := corev1.PersistentVolumeReclaimDelete
	tmp2 := storagev1.VolumeBindingWaitForFirstConsumer
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			UID:             types.UID(fmt.Sprintf("%v-%v", namespace, name)),
			ResourceVersion: "1",
			Name:            name,
		},
		Provisioner:       name,
		ReclaimPolicy:     &tmp,
		VolumeBindingMode: &tmp2,
	}
	tmp3 := corev1.PersistentVolumeFilesystem
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			UID:             types.UID(fmt.Sprintf("%v-%v", namespace, name)),
			ResourceVersion: "1",
			Namespace:       namespace,
			Name:            name,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.ResourceRequirements{
				Requests: req,
			},
			StorageClassName: &sc.Name,
			VolumeMode:       &tmp3,
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			UID:             types.UID(fmt.Sprintf("%v-%v", namespace, name)),
			ResourceVersion: "1",
			Name:            name,
		},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: sc.Name,
			Capacity:         req,
			VolumeMode:       &tmp3,
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
		},
		Status: corev1.PersistentVolumeStatus{
			Phase: corev1.VolumeAvailable,
		},
	}
	return pvc, pv, sc
}

func BuildBestEffortPod(namespace, name, nodeName string, p corev1.PodPhase, groupName string, labels map[string]string, selector map[string]string) *corev1.Pod {
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
						Requests: corev1.ResourceList{},
					},
				},
			},
		},
	}
}

func BuildPodWithPriority(namespace, name, nodeName string, p corev1.PodPhase, req corev1.ResourceList, groupName string, labels map[string]string, selector map[string]string, priority *int32) *corev1.Pod {
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
			Priority:     priority,
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
func (fb *FakeBinder) Bind(kubeClient *kubernetes.Clientset, tasks []*apis.TaskInfo) ([]*apis.TaskInfo, error) {
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
	volumeBinder volumebinding.SchedulerVolumeBinder
	Actions      map[string][]string
}

func NewFakeVolumeBinder(kubeClient kubernetes.Interface) *FakeVolumeBinder {
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	podInformer := informerFactory.Core().V1().Pods()
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	pvInformer := informerFactory.Core().V1().PersistentVolumes()
	scInformer := informerFactory.Storage().V1().StorageClasses()
	nodeInformer := informerFactory.Core().V1().Nodes()
	csiNodeInformer := informerFactory.Storage().V1().CSINodes()

	go podInformer.Informer().Run(context.TODO().Done())
	go pvcInformer.Informer().Run(context.TODO().Done())
	go pvInformer.Informer().Run(context.TODO().Done())
	go scInformer.Informer().Run(context.TODO().Done())
	go nodeInformer.Informer().Run(context.TODO().Done())
	go csiNodeInformer.Informer().Run(context.TODO().Done())

	cache.WaitForCacheSync(context.TODO().Done(), podInformer.Informer().HasSynced,
		pvcInformer.Informer().HasSynced,
		pvInformer.Informer().HasSynced,
		scInformer.Informer().HasSynced,
		nodeInformer.Informer().HasSynced,
		csiNodeInformer.Informer().HasSynced)
	return &FakeVolumeBinder{
		volumeBinder: volumebinding.NewVolumeBinder(
			kubeClient,
			podInformer,
			nodeInformer,
			csiNodeInformer,
			pvcInformer,
			pvInformer,
			scInformer,
			nil,
			30*time.Second,
		),
		Actions: make(map[string][]string),
	}
}

func (fvb *FakeVolumeBinder) AllocateVolumes(task *apis.TaskInfo, hostname string, podVolumes *volumebinding.PodVolumes) error {
	if fvb.volumeBinder == nil {
		return nil
	}
	_, err := fvb.volumeBinder.AssumePodVolumes(task.Pod, hostname, podVolumes)

	key := fmt.Sprintf("%s/%s", task.Namespace, task.Name)
	fvb.Actions[key] = append(fvb.Actions[key], "AllocateVolumes")
	return err
}

func (fvb *FakeVolumeBinder) BindVolumes(task *apis.TaskInfo, podVolumes *volumebinding.PodVolumes) error {
	if fvb.volumeBinder == nil {
		return nil
	}

	key := fmt.Sprintf("%s/%s", task.Namespace, task.Name)
	if len(podVolumes.DynamicProvisions) > 0 {
		fvb.Actions[key] = append(fvb.Actions[key], "DynamicProvisions")
	}
	if len(podVolumes.StaticBindings) > 0 {
		fvb.Actions[key] = append(fvb.Actions[key], "StaticBindings")
	}
	return nil
}

func (fvb *FakeVolumeBinder) GetPodVolumes(task *apis.TaskInfo, node *corev1.Node) (*volumebinding.PodVolumes, error) {
	if fvb.volumeBinder == nil {
		return nil, nil
	}
	key := fmt.Sprintf("%s/%s", task.Namespace, task.Name)
	fvb.Actions[key] = []string{"GetPodVolumes"}
	boundClaims, claimsToBind, unboundClaimsImmediate, err := fvb.volumeBinder.GetPodVolumes(task.Pod)
	if err != nil {
		return nil, err
	}
	if len(unboundClaimsImmediate) > 0 {
		return nil, fmt.Errorf("pod has unbound immediate PersistentVolumeClaims")
	}

	podVolumes, reasons, err := fvb.volumeBinder.FindPodVolumes(task.Pod, boundClaims, claimsToBind, node)
	if err != nil {
		return nil, err
	} else if len(reasons) > 0 {
		return nil, fmt.Errorf("%v", reasons[0])
	}
	return podVolumes, err
}

func (fvb *FakeVolumeBinder) RevertVolumes(task *apis.TaskInfo, podVolumes *volumebinding.PodVolumes) {
	if fvb.volumeBinder == nil {
		return
	}
	key := fmt.Sprintf("%s/%s", task.Namespace, task.Name)
	fvb.Actions[key] = append(fvb.Actions[key], "RevertVolumes")
	if podVolumes != nil {
		fvb.volumeBinder.RevertAssumedPodVolumes(podVolumes)
	}
}
