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

package allocate

import (
	"context"
	"fmt"
	"github.com/agiledragon/gomonkey/v2"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	api "github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/cache"
	"github.com/hliangzhao/volcano/pkg/scheduler/conf"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/drf"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/gang"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/priority"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/proportion"
	"github.com/hliangzhao/volcano/pkg/scheduler/utils"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"reflect"
	"testing"
)

// TODO: test not passed

func TestAllocate(t *testing.T) {
	var tmp *cache.SchedulerCache
	patches := gomonkey.ApplyMethod(reflect.TypeOf(tmp), "AddBindTask", func(scCache *cache.SchedulerCache, task *api.TaskInfo) error {
		_, _ = scCache.Binder.Bind(nil, []*api.TaskInfo{task})
		return nil
	})
	defer patches.Reset()

	framework.RegisterPluginBuilder("drf", drf.New)
	framework.RegisterPluginBuilder("proportion", proportion.New)

	// TODO: cmd/scheduler/app not implemented
	// options.ServerOpts = &options.ServerOption{
	// 	MinNodesToFind:             100,
	// 	MinPercentageOfNodesToFind: 5,
	// 	PercentageOfNodesToFind:    100,
	// }

	defer framework.CleanupPluginBuilders()

	tests := []struct {
		name      string
		podGroups []*schedulingv1alpha1.PodGroup
		pods      []*v1.Pod
		nodes     []*v1.Node
		queues    []*schedulingv1alpha1.Queue
		expected  map[string]string
	}{
		{
			name: "one Job with two Pods on one node",
			podGroups: []*schedulingv1alpha1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: schedulingv1alpha1.PodGroupSpec{
						Queue: "c1",
					},
					Status: schedulingv1alpha1.PodGroupStatus{
						Phase: schedulingv1alpha1.PodGroupInqueue,
					},
				},
			},
			pods: []*v1.Pod{
				utils.BuildPod("c1", "p1", "", v1.PodPending, utils.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				utils.BuildPod("c1", "p2", "", v1.PodPending, utils.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				utils.BuildNode("n1", utils.BuildResourceList("2", "4Gi"), make(map[string]string)),
			},
			queues: []*schedulingv1alpha1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "c1",
					},
					Spec: schedulingv1alpha1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: map[string]string{
				"c1/p1": "n1",
				"c1/p2": "n1",
			},
		},
		{
			name: "two Jobs on one node",
			podGroups: []*schedulingv1alpha1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: schedulingv1alpha1.PodGroupSpec{
						Queue: "c1",
					},
					Status: schedulingv1alpha1.PodGroupStatus{
						Phase: schedulingv1alpha1.PodGroupInqueue,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "c2",
					},
					Spec: schedulingv1alpha1.PodGroupSpec{
						Queue: "c2",
					},
					Status: schedulingv1alpha1.PodGroupStatus{
						Phase: schedulingv1alpha1.PodGroupInqueue,
					},
				},
			},

			// pod name should be like "*-*-{index}",
			// due to change of TaskOrderFn
			pods: []*v1.Pod{
				// pending pod with owner1, under c1
				utils.BuildPod("c1", "pg1-p-1", "", v1.PodPending, utils.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				// pending pod with owner1, under c1
				utils.BuildPod("c1", "pg1-p-2", "", v1.PodPending, utils.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				// pending pod with owner2, under c2
				utils.BuildPod("c2", "pg2-p-1", "", v1.PodPending, utils.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
				// pending pod with owner2, under c2
				utils.BuildPod("c2", "pg2-p-2", "", v1.PodPending, utils.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				utils.BuildNode("n1", utils.BuildResourceList("2", "4G"), make(map[string]string)),
			},
			queues: []*schedulingv1alpha1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "c1",
					},
					Spec: schedulingv1alpha1.QueueSpec{
						Weight: 1,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "c2",
					},
					Spec: schedulingv1alpha1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: map[string]string{
				"c2/pg2-p-1": "n1",
				"c1/pg1-p-1": "n1",
			},
		},
		{
			name: "high priority queue should not block others",
			podGroups: []*schedulingv1alpha1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: schedulingv1alpha1.PodGroupSpec{
						Queue: "c1",
					},
					Status: schedulingv1alpha1.PodGroupStatus{
						Phase: schedulingv1alpha1.PodGroupInqueue,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "c1",
					},
					Spec: schedulingv1alpha1.PodGroupSpec{
						Queue: "c2",
					},
					Status: schedulingv1alpha1.PodGroupStatus{
						Phase: schedulingv1alpha1.PodGroupInqueue,
					},
				},
			},

			pods: []*v1.Pod{
				// pending pod with owner1, under ns:c1/q:c1
				utils.BuildPod("c1", "p1", "", v1.PodPending, utils.BuildResourceList("3", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				// pending pod with owner2, under ns:c1/q:c2
				utils.BuildPod("c1", "p2", "", v1.PodPending, utils.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				utils.BuildNode("n1", utils.BuildResourceList("2", "4G"), make(map[string]string)),
			},
			queues: []*schedulingv1alpha1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "c1",
					},
					Spec: schedulingv1alpha1.QueueSpec{
						Weight: 1,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "c2",
					},
					Spec: schedulingv1alpha1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: map[string]string{
				"c1/p2": "n1",
			},
		},
	}

	allocate := New()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binder := &utils.FakeBinder{
				Binds:   map[string]string{},
				Channel: make(chan string),
			}
			schedulerCache := &cache.SchedulerCache{
				Nodes:         make(map[string]*api.NodeInfo),
				Jobs:          make(map[api.JobID]*api.JobInfo),
				Queues:        make(map[api.QueueID]*api.QueueInfo),
				Binder:        binder,
				StatusUpdater: &utils.FakeStatusUpdater{},
				VolumeBinder:  &utils.FakeVolumeBinder{},

				Recorder: record.NewFakeRecorder(100),
			}

			for _, node := range test.nodes {
				schedulerCache.AddNode(node)
			}
			for _, pod := range test.pods {
				schedulerCache.AddPod(pod)
			}

			for _, ss := range test.podGroups {
				schedulerCache.AddPodGroupV1alpha1(ss)
			}

			for _, q := range test.queues {
				schedulerCache.AddQueueV1alpha1(q)
			}

			trueValue := true
			ssn := framework.OpenSession(schedulerCache, []conf.Tier{
				{
					Plugins: []conf.PluginOption{
						{
							Name:                  "drf",
							EnabledPreemptable:    &trueValue,
							EnabledJobOrder:       &trueValue,
							EnabledNamespaceOrder: &trueValue,
						},
						{
							Name:               "proportion",
							EnabledQueueOrder:  &trueValue,
							EnabledReclaimable: &trueValue,
						},
					},
				},
			}, nil)
			defer framework.CloseSession(ssn)

			allocate.Execute(ssn)

			if !reflect.DeepEqual(test.expected, binder.Binds) {
				t.Errorf("expected: %v, got %v ", test.expected, binder.Binds)
			}
		})
	}
}

func TestAllocateWithDynamicPVC(t *testing.T) {
	var tmp *cache.SchedulerCache
	patches := gomonkey.ApplyMethod(reflect.TypeOf(tmp), "AddBindTask", func(scCache *cache.SchedulerCache, task *api.TaskInfo) error {
		_ = scCache.VolumeBinder.BindVolumes(task, task.PodVolumes)
		_, _ = scCache.Binder.Bind(nil, []*api.TaskInfo{task})
		return nil
	})
	defer patches.Reset()

	framework.RegisterPluginBuilder("gang", gang.New)
	framework.RegisterPluginBuilder("priority", priority.New)

	// TODO: cmd/scheduler/app not implemented
	// options.ServerOpts = &options.ServerOption{
	// 	MinNodesToFind:             100,
	// 	MinPercentageOfNodesToFind: 5,
	// 	PercentageOfNodesToFind:    100,
	// }

	defer framework.CleanupPluginBuilders()

	queue := &schedulingv1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "c1",
		},
		Spec: schedulingv1alpha1.QueueSpec{
			Weight: 1,
		},
	}
	pg := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg1",
			Namespace: "c1",
		},
		Spec: schedulingv1alpha1.PodGroupSpec{
			Queue:     "c1",
			MinMember: 2,
		},
		Status: schedulingv1alpha1.PodGroupStatus{
			Phase: schedulingv1alpha1.PodGroupInqueue,
		},
	}

	pvc, _, sc := utils.BuildDynamicPVC("c1", "pvc", v1.ResourceList{
		v1.ResourceStorage: resource.MustParse("1Gi"),
	})
	pvc1 := pvc.DeepCopy()
	pvc1.Name = fmt.Sprintf("pvc%d", 1)

	allocate := New()

	tests := []struct {
		name            string
		pods            []*v1.Pod
		nodes           []*v1.Node
		pvs             []*v1.PersistentVolume
		pvcs            []*v1.PersistentVolumeClaim
		sc              *storagev1.StorageClass
		expectedBind    map[string]string
		expectedActions map[string][]string
	}{
		{
			name: "resource not match",
			pods: []*v1.Pod{
				utils.BuildPodWithPVC("c1", "p1", "", v1.PodPending, utils.BuildResourceList("1", "1G"), pvc, "pg1", make(map[string]string), make(map[string]string)),
				utils.BuildPodWithPVC("c1", "p2", "", v1.PodPending, utils.BuildResourceList("1", "1G"), pvc1, "pg1", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				utils.BuildNode("n1", utils.BuildResourceList("1", "4Gi"), make(map[string]string)),
			},
			sc:           sc,
			pvcs:         []*v1.PersistentVolumeClaim{pvc, pvc1},
			expectedBind: map[string]string{},
			expectedActions: map[string][]string{
				"c1/p1": {"GetPodVolumes", "AllocateVolumes", "RevertVolumes"},
			},
		},
		{
			name: "node changed with enough resource",
			pods: []*v1.Pod{
				utils.BuildPodWithPVC("c1", "p1", "", v1.PodPending, utils.BuildResourceList("1", "1G"), pvc, "pg1", make(map[string]string), make(map[string]string)),
				utils.BuildPodWithPVC("c1", "p2", "", v1.PodPending, utils.BuildResourceList("1", "1G"), pvc1, "pg1", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				utils.BuildNode("n2", utils.BuildResourceList("2", "4Gi"), make(map[string]string)),
			},
			sc:   sc,
			pvcs: []*v1.PersistentVolumeClaim{pvc, pvc1},
			expectedBind: map[string]string{
				"c1/p1": "n2",
				"c1/p2": "n2",
			},
			expectedActions: map[string][]string{
				"c1/p1": {"GetPodVolumes", "AllocateVolumes", "DynamicProvisions"},
				"c1/p2": {"GetPodVolumes", "AllocateVolumes", "DynamicProvisions"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kubeClient := fake.NewSimpleClientset()
			_, _ = kubeClient.StorageV1().StorageClasses().Create(context.TODO(), test.sc, metav1.CreateOptions{})
			for _, pv := range test.pvs {
				_, _ = kubeClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
			}
			for _, pvc := range test.pvcs {
				_, _ = kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
			}

			fakeVolumeBinder := utils.NewFakeVolumeBinder(kubeClient)
			binder := &utils.FakeBinder{
				Binds:   map[string]string{},
				Channel: make(chan string),
			}
			schedulerCache := &cache.SchedulerCache{
				Nodes:         make(map[string]*api.NodeInfo),
				Jobs:          make(map[api.JobID]*api.JobInfo),
				Queues:        make(map[api.QueueID]*api.QueueInfo),
				Binder:        binder,
				StatusUpdater: &utils.FakeStatusUpdater{},
				VolumeBinder:  fakeVolumeBinder,
				Recorder:      record.NewFakeRecorder(100),
			}
			schedulerCache.AddQueueV1alpha1(queue)
			schedulerCache.AddPodGroupV1alpha1(pg)
			for i, pod := range test.pods {
				pri := int32(-i)
				pod.Spec.Priority = &pri
				schedulerCache.AddPod(pod)
			}
			for _, node := range test.nodes {
				schedulerCache.AddNode(node)
			}

			trueValue := true
			ssn := framework.OpenSession(schedulerCache, []conf.Tier{
				{
					Plugins: []conf.PluginOption{
						{
							Name:                "priority",
							EnabledJobReady:     &trueValue,
							EnabledPredicate:    &trueValue,
							EnabledJobPipelined: &trueValue,
							EnabledTaskOrder:    &trueValue,
						},
						{
							Name:                "gang",
							EnabledJobReady:     &trueValue,
							EnabledPredicate:    &trueValue,
							EnabledJobPipelined: &trueValue,
							EnabledTaskOrder:    &trueValue,
						},
					},
				},
			}, nil)
			defer framework.CloseSession(ssn)

			allocate.Execute(ssn)
			if !reflect.DeepEqual(test.expectedBind, binder.Binds) {
				t.Errorf("expected: %v, got %v ", test.expectedBind, binder.Binds)
			}
			if !reflect.DeepEqual(test.expectedActions, fakeVolumeBinder.Actions) {
				t.Errorf("expected: %v, got %v ", test.expectedActions, fakeVolumeBinder.Actions)
			}
			fakeVolumeBinder.Actions = make(map[string][]string)
		})
	}
}
