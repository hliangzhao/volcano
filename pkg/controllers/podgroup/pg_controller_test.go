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

package podgroup

// TODO: re-test is required

// TODO: just copied. Not checked.
// Passed.

import (
	"context"
	"github.com/hliangzhao/volcano/pkg/apis/scheduling"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	fakevolcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned/fake"
	"github.com/hliangzhao/volcano/pkg/controllers/framework"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	fakekubeclient "k8s.io/client-go/kubernetes/fake"
	"reflect"
	"testing"
)

func newFakeController() *pgController {
	kubeClient := fakekubeclient.NewSimpleClientset()
	vcClient := fakevolcanoclient.NewSimpleClientset()
	sharedInformers := informers.NewSharedInformerFactory(kubeClient, 0)

	controller := &pgController{}
	opt := &framework.ControllerOption{
		KubeClient:            kubeClient,
		VolcanoClient:         vcClient,
		SharedInformerFactory: sharedInformers,
		SchedulerNames:        []string{"volcano"},
	}

	_ = controller.Initialize(opt)

	return controller
}

func TestAddPodGroup(t *testing.T) {
	namespace := "test"
	isController := true
	blockOwnerDeletion := true

	testCases := []struct {
		name             string
		pod              *corev1.Pod
		expectedPodGroup *scheduling.PodGroup
	}{
		{
			name: "AddPodGroup: pod has ownerReferences and priorityClassName",
			pod: &corev1.Pod{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Pod",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod1",
					Namespace: namespace,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "app/v1",
							Kind:       "ReplicaSet",
							Name:       "rs1",
							UID:        "7a09885b-b753-4924-9fba-77c0836bac20",
							Controller: &isController,
						},
					},
				},
				Spec: corev1.PodSpec{
					PriorityClassName: "test-pc",
				},
			},
			expectedPodGroup: &scheduling.PodGroup{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "scheduling.volcano.sh/v1alpha1",
					Kind:       "PodGroup",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "podgroup-7a09885b-b753-4924-9fba-77c0836bac20",
					Namespace: namespace,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "app/v1",
							Kind:       "ReplicaSet",
							Name:       "rs1",
							UID:        "7a09885b-b753-4924-9fba-77c0836bac20",
							Controller: &isController,
						},
					},
				},
				Spec: scheduling.PodGroupSpec{
					MinMember:         1,
					PriorityClassName: "test-pc",
				},
			},
		},
		{
			name: "AddPodGroup: pod has no ownerReferences or priorityClassName",
			pod: &corev1.Pod{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Pod",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod1",
					Namespace: namespace,
					UID:       types.UID("7a09885b-b753-4924-9fba-77c0836bac20"),
				},
			},
			expectedPodGroup: &scheduling.PodGroup{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "scheduling.volcano.sh/v1alpha1",
					Kind:       "PodGroup",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "podgroup-7a09885b-b753-4924-9fba-77c0836bac20",
					Namespace: namespace,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "v1",
							Kind:               "Pod",
							Name:               "pod1",
							UID:                "7a09885b-b753-4924-9fba-77c0836bac20",
							Controller:         &isController,
							BlockOwnerDeletion: &blockOwnerDeletion,
						},
					},
				},
				Spec: scheduling.PodGroupSpec{
					MinMember: 1,
				},
			},
		},
	}

	for _, testCase := range testCases {
		c := newFakeController()

		pod, err := c.kubeClient.CoreV1().Pods(testCase.pod.Namespace).Create(context.TODO(), testCase.pod, metav1.CreateOptions{})
		if err != nil {
			t.Errorf("Case %s failed when creating pod for %v", testCase.name, err)
		}

		c.addPod(pod)
		_ = c.createNormalPodPGIfNotExist(pod)

		pg, err := c.volcanoClient.SchedulingV1alpha1().PodGroups(pod.Namespace).Get(context.TODO(),
			testCase.expectedPodGroup.Name,
			metav1.GetOptions{},
		)
		if err != nil {
			t.Errorf("Case %s failed when getting podGroup for %v", testCase.name, err)
		}

		if false == reflect.DeepEqual(pg.OwnerReferences, testCase.expectedPodGroup.OwnerReferences) {
			t.Errorf("Case %s failed, expect %v, got %v", testCase.name, testCase.expectedPodGroup, pg)
		}

		podAnnotation := pod.Annotations[schedulingv1alpha1.KubeGroupNameAnnotationKey]
		if testCase.expectedPodGroup.Name != podAnnotation {
			t.Errorf("Case %s failed, expect %v, got %v", testCase.name,
				testCase.expectedPodGroup.Name, podAnnotation)
		}

		if testCase.expectedPodGroup.Spec.PriorityClassName != pod.Spec.PriorityClassName {
			t.Errorf("Case %s failed, expect %v, got %v", testCase.name,
				testCase.expectedPodGroup.Spec.PriorityClassName, pod.Spec.PriorityClassName)
		}
	}
}
