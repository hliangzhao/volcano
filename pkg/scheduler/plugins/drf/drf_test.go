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

package drf

import (
	"flag"
	"fmt"
	"github.com/agiledragon/gomonkey/v2"
	`github.com/hliangzhao/volcano/cmd/scheduler/app/options`
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/scheduler/actions/allocate"
	api "github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/cache"
	"github.com/hliangzhao/volcano/pkg/scheduler/conf"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/proportion"
	"github.com/hliangzhao/volcano/pkg/scheduler/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"reflect"
	"testing"
)

// TODO: test not passed

func makePods(num int, cpu, mem, podGroupName string) []*corev1.Pod {
	var pods []*corev1.Pod
	for i := 0; i < num; i++ {
		pods = append(pods, utils.BuildPod("default",
			fmt.Sprintf("%s-p%d", podGroupName, i), "",
			corev1.PodPending, utils.BuildResourceList(cpu, mem),
			podGroupName, make(map[string]string), make(map[string]string)))
	}
	return pods
}

type queueSpec struct {
	name      string
	hierarchy string
	weights   string
}

type pgSpec struct {
	taskNum int
	cpu     string
	mem     string
	pg      string
	queue   string
}

func TestHDRF(t *testing.T) {
	klog.InitFlags(nil)
	flag.Set("v", "4")
	flag.Set("alsologtostderr", "true")
	var tmp *cache.SchedulerCache
	patches := gomonkey.ApplyMethod(reflect.TypeOf(tmp), "AddBindTask", func(scCache *cache.SchedulerCache, task *api.TaskInfo) error {
		scCache.Binder.Bind(nil, []*api.TaskInfo{task})
		return nil
	})
	defer patches.Reset()

	s := options.NewServerOption()
	s.MinNodesToFind = 100
	s.PercentageOfNodesToFind = 100
	s.RegisterOptions()

	framework.RegisterPluginBuilder(PluginName, New)
	framework.RegisterPluginBuilder("proportion", proportion.New)
	defer framework.CleanupPluginBuilders()

	tests := []struct {
		name       string
		pgSpecs    []pgSpec
		nodes      []*corev1.Node
		queues     []*schedulingv1alpha1.Queue
		queueSpecs []queueSpec
		expected   map[string]string
	}{
		{
			name: "rescaling test",
			pgSpecs: []pgSpec{
				{
					taskNum: 10,
					cpu:     "1",
					mem:     "1G",
					pg:      "pg1",
					queue:   "root-sci",
				},
				{
					taskNum: 10,
					cpu:     "1",
					mem:     "0G",
					pg:      "pg21",
					queue:   "root-eng-dev",
				},
				{
					taskNum: 10,
					cpu:     "0",
					mem:     "1G",
					pg:      "pg22",
					queue:   "root-eng-prod",
				},
			},
			nodes: []*corev1.Node{utils.BuildNode("n",
				utils.BuildResourceList("10", "10G"),
				make(map[string]string))},
			queueSpecs: []queueSpec{
				{
					name:      "root-sci",
					hierarchy: "root/sci",
					weights:   "100/50",
				},
				{
					name:      "root-eng-dev",
					hierarchy: "root/eng/dev",
					weights:   "100/50/50",
				},
				{
					name:      "root-eng-prod",
					hierarchy: "root/eng/prod",
					weights:   "100/50/50",
				},
			},
			expected: map[string]string{
				"pg1":  "cpu 5000.00, memory 5000000000.00, nvidia.com/gpu 0.00",
				"pg21": "cpu 5000.00, memory 0.00, nvidia.com/gpu 0.00",
				"pg22": "cpu 0.00, memory 5000000000.00, nvidia.com/gpu 0.00",
			},
		},
		{
			name: "blocking nodes test",
			pgSpecs: []pgSpec{
				{
					taskNum: 30,
					cpu:     "1",
					mem:     "0G",
					pg:      "pg1",
					queue:   "root-pg1",
				},
				{
					taskNum: 30,
					cpu:     "1",
					mem:     "0G",
					pg:      "pg2",
					queue:   "root-pg2",
				},
				{
					taskNum: 30,
					cpu:     "1",
					mem:     "0G",
					pg:      "pg31",
					queue:   "root-pg3-pg31",
				},
				{
					taskNum: 30,
					cpu:     "0",
					mem:     "1G",
					pg:      "pg32",
					queue:   "root-pg3-pg32",
				},
				{
					taskNum: 30,
					cpu:     "0",
					mem:     "1G",
					pg:      "pg4",
					queue:   "root-pg4",
				},
			},
			nodes: []*corev1.Node{utils.BuildNode("n",
				utils.BuildResourceList("30", "30G"),
				make(map[string]string))},
			queueSpecs: []queueSpec{
				{
					name:      "root-pg1",
					hierarchy: "root/pg1",
					weights:   "100/25",
				},
				{
					name:      "root-pg2",
					hierarchy: "root/pg2",
					weights:   "100/25",
				},
				{
					name:      "root-pg3-pg31",
					hierarchy: "root/pg3/pg31",
					weights:   "100/25/50",
				},
				{
					name:      "root-pg3-pg32",
					hierarchy: "root/pg3/pg32",
					weights:   "100/25/50",
				},
				{
					name:      "root-pg4",
					hierarchy: "root/pg4",
					weights:   "100/25",
				},
			},
			expected: map[string]string{
				"pg1":  "cpu 10000.00, memory 0.00, nvidia.com/gpu 0.00",
				"pg2":  "cpu 10000.00, memory 0.00, nvidia.com/gpu 0.00",
				"pg31": "cpu 10000.00, memory 0.00, nvidia.com/gpu 0.00",
				"pg32": "cpu 0.00, memory 15000000000.00, nvidia.com/gpu 0.00",
				"pg4":  "cpu 0.00, memory 15000000000.00, nvidia.com/gpu 0.00",
			},
		},
	}
	for _, test := range tests {
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
			Recorder:      record.NewFakeRecorder(100),
		}
		for _, node := range test.nodes {
			schedulerCache.AddNode(node)
		}
		for _, q := range test.queueSpecs {
			schedulerCache.AddQueueV1alpha1(
				&schedulingv1alpha1.Queue{
					ObjectMeta: metav1.ObjectMeta{
						Name: q.name,
						Annotations: map[string]string{
							schedulingv1alpha1.KubeHierarchyAnnotationKey:       q.hierarchy,
							schedulingv1alpha1.KubeHierarchyWeightAnnotationKey: q.weights,
						},
					},
					Spec: schedulingv1alpha1.QueueSpec{
						Weight: 1,
					},
				})
		}
		for _, pgSpec := range test.pgSpecs {
			pods := makePods(pgSpec.taskNum, pgSpec.cpu, pgSpec.mem, pgSpec.pg)
			for _, pod := range pods {
				schedulerCache.AddPod(pod)
			}
			schedulerCache.AddPodGroupV1alpha1(&schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pgSpec.pg,
					Namespace: "default",
				},
				Spec: schedulingv1alpha1.PodGroupSpec{
					Queue: pgSpec.queue,
				},
				Status: schedulingv1alpha1.PodGroupStatus{
					Phase: schedulingv1alpha1.PodGroupInqueue,
				},
			})
		}
		trueValue := true
		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:              PluginName,
						EnabledHierarchy:  &trueValue,
						EnabledQueueOrder: &trueValue,
						EnabledJobOrder:   &trueValue,
					},
					{
						Name:               "proportion",
						EnabledJobEnqueued: &trueValue,
						EnabledQueueOrder:  &trueValue,
						EnabledReclaimable: &trueValue,
					},
				},
			},
		}, nil)
		defer framework.CloseSession(ssn)
		allocateAction := allocate.New()

		allocateAction.Execute(ssn)

		for _, job := range ssn.Jobs {
			if test.expected[job.Name] != job.Allocated.String() {
				t.Fatalf("%s: job %s expected resource %s, but got %s", test.name, job.Name, test.expected[job.Name], job.Allocated)
			}
		}

	}
}
