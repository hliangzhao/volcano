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

package predicates

// TODO: test not passed

import (
	"github.com/agiledragon/gomonkey/v2"
	`github.com/hliangzhao/volcano/cmd/scheduler/app/options`
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/scheduler/actions/allocate"
	api "github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/cache"
	"github.com/hliangzhao/volcano/pkg/scheduler/conf"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/gang"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/priority"
	"github.com/hliangzhao/volcano/pkg/scheduler/utils"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"reflect"
	"testing"
)

func getWorkerAffinity() *corev1.Affinity {
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "role",
								Operator: "In",
								Values:   []string{"worker"},
							},
						},
					},
					TopologyKey: "kubernetes.io/hostname",
				},
			},
		},
	}
}

func TestEventHandler(t *testing.T) {
	var tmp *cache.SchedulerCache
	patches := gomonkey.ApplyMethod(reflect.TypeOf(tmp), "AddBindTask", func(scCache *cache.SchedulerCache, task *api.TaskInfo) error {
		scCache.Binder.Bind(nil, []*api.TaskInfo{task})
		return nil
	})
	defer patches.Reset()

	framework.RegisterPluginBuilder(PluginName, New)
	framework.RegisterPluginBuilder(gang.PluginName, gang.New)
	framework.RegisterPluginBuilder(priority.PluginName, priority.New)

	options.ServerOpts = options.NewServerOption()
	defer framework.CleanupPluginBuilders()

	// pending pods
	w1 := utils.BuildPod("ns1", "worker-1", "", corev1.PodPending, utils.BuildResourceList("3", "3k"), "pg1", map[string]string{"role": "worker"}, map[string]string{"selector": "worker"})
	w2 := utils.BuildPod("ns1", "worker-2", "", corev1.PodPending, utils.BuildResourceList("5", "5k"), "pg1", map[string]string{"role": "worker"}, map[string]string{})
	w3 := utils.BuildPod("ns1", "worker-3", "", corev1.PodPending, utils.BuildResourceList("4", "4k"), "pg2", map[string]string{"role": "worker"}, map[string]string{})
	w1.Spec.Affinity = getWorkerAffinity()
	w2.Spec.Affinity = getWorkerAffinity()
	w3.Spec.Affinity = getWorkerAffinity()

	// nodes
	n1 := utils.BuildNode("node1", utils.BuildResourceList("4", "4k"), map[string]string{"selector": "worker"})
	n2 := utils.BuildNode("node2", utils.BuildResourceList("3", "3k"), map[string]string{})
	n1.Status.Allocatable["pods"] = resource.MustParse("15")
	n2.Status.Allocatable["pods"] = resource.MustParse("15")
	n1.Labels["kubernetes.io/hostname"] = "node1"
	n2.Labels["kubernetes.io/hostname"] = "node2"

	// priority
	p1 := &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "p1"}, Value: 1}
	p2 := &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "p2"}, Value: 2}
	// podgroup
	pg1 := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			Name:      "pg1",
		},
		Spec: schedulingv1alpha1.PodGroupSpec{
			Queue:             "q1",
			MinMember:         int32(2),
			PriorityClassName: p2.Name,
		},
	}
	pg2 := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			Name:      "pg2",
		},
		Spec: schedulingv1alpha1.PodGroupSpec{
			Queue:             "q1",
			MinMember:         int32(1),
			PriorityClassName: p1.Name,
		},
	}
	// queue
	queue1 := &schedulingv1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "q1",
		},
	}

	// tests
	tests := []struct {
		name     string
		pods     []*corev1.Pod
		nodes    []*corev1.Node
		pcs      []*schedulingv1.PriorityClass
		pgs      []*schedulingv1alpha1.PodGroup
		expected map[string]string
	}{
		{
			name:  "pod-deallocate",
			pods:  []*corev1.Pod{w1, w2, w3},
			nodes: []*corev1.Node{n1, n2},
			pcs:   []*schedulingv1.PriorityClass{p1, p2},
			pgs:   []*schedulingv1alpha1.PodGroup{pg1, pg2},
			expected: map[string]string{ // podKey -> node
				"ns1/worker-3": "node1",
			},
		},
	}

	for _, test := range tests {
		// initialize schedulerCache
		binder := &utils.FakeBinder{
			Binds:   map[string]string{},
			Channel: make(chan string),
		}
		recorder := record.NewFakeRecorder(100)
		go func() {
			for {
				event := <-recorder.Events
				t.Logf("%s: [Event] %s", test.name, event)
			}
		}()
		schedulerCache := &cache.SchedulerCache{
			Nodes:           make(map[string]*api.NodeInfo),
			Jobs:            make(map[api.JobID]*api.JobInfo),
			PriorityClasses: make(map[string]*schedulingv1.PriorityClass),
			Queues:          make(map[api.QueueID]*api.QueueInfo),
			Binder:          binder,
			StatusUpdater:   &utils.FakeStatusUpdater{},
			VolumeBinder:    &utils.FakeVolumeBinder{},
			Recorder:        recorder,
		}
		for _, node := range test.nodes {
			schedulerCache.AddNode(node)
		}
		for _, pod := range test.pods {
			schedulerCache.AddPod(pod)
		}
		for _, pc := range test.pcs {
			schedulerCache.PriorityClasses[pc.Name] = pc
		}
		for _, pg := range test.pgs {
			pg.Status = schedulingv1alpha1.PodGroupStatus{
				Phase: schedulingv1alpha1.PodGroupInqueue,
			}
			schedulerCache.AddPodGroupV1alpha1(pg)
		}
		schedulerCache.AddQueueV1alpha1(queue1)
		// session
		trueValue := true
		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:             PluginName,
						EnabledPredicate: &trueValue,
					},
					{
						Name:                gang.PluginName,
						EnabledJobReady:     &trueValue,
						EnabledJobPipelined: &trueValue,
					},
					{
						Name:            priority.PluginName,
						EnabledJobOrder: &trueValue,
					},
				},
			},
		}, nil)
		// allocate
		allocator := allocate.New()
		allocator.Execute(ssn)
		framework.CloseSession(ssn)

		t.Logf("expected: %#v, got: %#v", test.expected, binder.Binds)
		if !reflect.DeepEqual(test.expected, binder.Binds) {
			t.Errorf("expected: %v, got %v ", test.expected, binder.Binds)
		}
	}
}
