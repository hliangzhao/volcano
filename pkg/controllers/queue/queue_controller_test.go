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

package queue

// TODO: just copied.
//  Passed.

import (
	"context"
	"fmt"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	fakevolcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned/fake"
	"github.com/hliangzhao/volcano/pkg/controllers/framework"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakekubeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"testing"
)

func newFakeController() *queueController {
	KubeBatchClientSet := fakevolcanoclient.NewSimpleClientset()
	KubeClientSet := fakekubeclient.NewSimpleClientset()

	controller := &queueController{}
	opt := framework.ControllerOption{
		VolcanoClient: KubeBatchClientSet,
		KubeClient:    KubeClientSet,
	}

	_ = controller.Initialize(&opt)

	return controller
}

func TestAddQueue(t *testing.T) {
	testCases := []struct {
		Name        string
		queue       *schedulingv1alpha1.Queue
		ExpectValue int
	}{
		{
			Name: "AddQueue",
			queue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c1",
				},
				Spec: schedulingv1alpha1.QueueSpec{
					Weight: 1,
				},
			},
			ExpectValue: 1,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()

		c.addQueue(testcase.queue)

		if testcase.ExpectValue != c.queueQueue.Len() {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queueQueue.Len())
		}
	}
}

func TestDeleteQueue(t *testing.T) {
	testCases := []struct {
		Name        string
		queue       *schedulingv1alpha1.Queue
		ExpectValue bool
	}{
		{
			Name: "DeleteQueue",
			queue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c1",
				},
				Spec: schedulingv1alpha1.QueueSpec{
					Weight: 1,
				},
			},
			ExpectValue: false,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()
		c.podgroups[testcase.queue.Name] = make(map[string]struct{})

		c.deleteQueue(testcase.queue)

		if _, ok := c.podgroups[testcase.queue.Name]; ok != testcase.ExpectValue {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, ok)
		}
	}

}

func TestAddPodGroup(t *testing.T) {
	namespace := "c1"

	testCases := []struct {
		Name        string
		podGroup    *schedulingv1alpha1.PodGroup
		ExpectValue int
	}{
		{
			Name: "addpodgroup",
			podGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1alpha1.PodGroupSpec{
					Queue: "c1",
				},
			},
			ExpectValue: 1,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()

		c.addPodgroup(testcase.podGroup)

		if testcase.ExpectValue != c.queueQueue.Len() {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queueQueue.Len())
		}
		if testcase.ExpectValue != len(c.podgroups[testcase.podGroup.Spec.Queue]) {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, len(c.podgroups[testcase.podGroup.Spec.Queue]))
		}
	}

}

func TestDeletePodGroup(t *testing.T) {
	namespace := "c1"

	testCases := []struct {
		Name        string
		podGroup    *schedulingv1alpha1.PodGroup
		ExpectValue bool
	}{
		{
			Name: "deletepodgroup",
			podGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1alpha1.PodGroupSpec{
					Queue: "c1",
				},
			},
			ExpectValue: false,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()

		key, _ := cache.MetaNamespaceKeyFunc(testcase.podGroup)
		c.podgroups[testcase.podGroup.Spec.Queue] = make(map[string]struct{})
		c.podgroups[testcase.podGroup.Spec.Queue][key] = struct{}{}

		c.deletePodgroup(testcase.podGroup)
		if _, ok := c.podgroups[testcase.podGroup.Spec.Queue][key]; ok != testcase.ExpectValue {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, ok)
		}
	}
}

func TestUpdatePodGroup(t *testing.T) {
	namespace := "c1"

	testCases := []struct {
		Name        string
		podGroupOld *schedulingv1alpha1.PodGroup
		podGroupNew *schedulingv1alpha1.PodGroup
		ExpectValue int
	}{
		{
			Name: "updatepodgroup",
			podGroupOld: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1alpha1.PodGroupSpec{
					Queue: "c1",
				},
				Status: schedulingv1alpha1.PodGroupStatus{
					Phase: schedulingv1alpha1.PodGroupPending,
				},
			},
			podGroupNew: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1alpha1.PodGroupSpec{
					Queue: "c1",
				},
				Status: schedulingv1alpha1.PodGroupStatus{
					Phase: schedulingv1alpha1.PodGroupRunning,
				},
			},
			ExpectValue: 1,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()

		c.updatePodgroup(testcase.podGroupOld, testcase.podGroupNew)

		if testcase.ExpectValue != c.queueQueue.Len() {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queueQueue.Len())
		}
	}
}

func TestSyncQueue(t *testing.T) {
	namespace := "c1"

	testCases := []struct {
		Name        string
		podGroup    *schedulingv1alpha1.PodGroup
		queue       *schedulingv1alpha1.Queue
		ExpectValue int32
	}{
		{
			Name: "syncQueue",
			podGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1alpha1.PodGroupSpec{
					Queue: "c1",
				},
				Status: schedulingv1alpha1.PodGroupStatus{
					Phase: schedulingv1alpha1.PodGroupPending,
				},
			},
			queue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c1",
				},
				Spec: schedulingv1alpha1.QueueSpec{
					Weight: 1,
				},
			},
			ExpectValue: 1,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()

		key, _ := cache.MetaNamespaceKeyFunc(testcase.podGroup)
		c.podgroups[testcase.podGroup.Spec.Queue] = make(map[string]struct{})
		c.podgroups[testcase.podGroup.Spec.Queue][key] = struct{}{}

		_ = c.pgInformer.Informer().GetIndexer().Add(testcase.podGroup)
		_ = c.queueInformer.Informer().GetIndexer().Add(testcase.queue)
		_, _ = c.volcanoClient.SchedulingV1alpha1().Queues().Create(context.TODO(), testcase.queue, metav1.CreateOptions{})

		err := c.syncQueue(testcase.queue, nil)
		item, _ := c.volcanoClient.SchedulingV1alpha1().Queues().Get(context.TODO(), testcase.queue.Name, metav1.GetOptions{})
		if err != nil && testcase.ExpectValue != item.Status.Pending {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queueQueue.Len())
		}
	}

}

func TestProcessNextWorkItem(t *testing.T) {
	testCases := []struct {
		Name        string
		ExpectValue int32
	}{
		{
			Name:        "processNextWorkItem",
			ExpectValue: 0,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()
		c.queueQueue.Add("test")
		bVal := c.processNextReq()
		fmt.Println("The value of boolean is ", bVal)
		if c.queueQueue.Len() != 0 {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queueQueue.Len())
		}
	}
}
