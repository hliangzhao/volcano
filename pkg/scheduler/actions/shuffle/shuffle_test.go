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

package shuffle

// TODO: just copied.
//  Passed.

import (
	"github.com/golang/mock/gomock"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/cache"
	"github.com/hliangzhao/volcano/pkg/scheduler/conf"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	mockframework "github.com/hliangzhao/volcano/pkg/scheduler/framework/mock_gen"
	"github.com/hliangzhao/volcano/pkg/scheduler/utils"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"testing"
	"time"
)

func TestShuffle(t *testing.T) {
	var highPriority int32
	var lowPriority int32
	highPriority = 100
	lowPriority = 10

	ctl := gomock.NewController(t)
	fakePlugin := mockframework.NewMockPlugin(ctl)
	fakePlugin.EXPECT().Name().AnyTimes().Return("fake")
	fakePlugin.EXPECT().OnSessionOpen(gomock.Any()).Return()
	fakePlugin.EXPECT().OnSessionClose(gomock.Any()).Return()
	fakePluginBuilder := func(arguments framework.Arguments) framework.Plugin {
		return fakePlugin
	}
	framework.RegisterPluginBuilder("fake", fakePluginBuilder)

	tests := []struct {
		name      string
		podGroups []*schedulingv1alpha1.PodGroup
		pods      []*v1.Pod
		nodes     []*v1.Node
		queues    []*schedulingv1alpha1.Queue
		expected  int
	}{
		{
			name: "select pods with low priority and evict them",
			nodes: []*v1.Node{
				utils.BuildNode("node1", utils.BuildResourceList("4", "8Gi"), make(map[string]string)),
				utils.BuildNode("node2", utils.BuildResourceList("4", "8Gi"), make(map[string]string)),
			},
			queues: []*schedulingv1alpha1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "default",
					},
					Spec: schedulingv1alpha1.QueueSpec{
						Weight: 1,
					},
				},
			},
			podGroups: []*schedulingv1alpha1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "test",
					},
					Spec: schedulingv1alpha1.PodGroupSpec{
						Queue: "default",
					},
					Status: schedulingv1alpha1.PodGroupStatus{
						Phase: schedulingv1alpha1.PodGroupRunning,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "test",
					},
					Spec: schedulingv1alpha1.PodGroupSpec{
						Queue: "default",
					},
					Status: schedulingv1alpha1.PodGroupStatus{
						Phase: schedulingv1alpha1.PodGroupRunning,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg3",
						Namespace: "test",
					},
					Spec: schedulingv1alpha1.PodGroupSpec{
						Queue: "default",
					},
					Status: schedulingv1alpha1.PodGroupStatus{
						Phase: schedulingv1alpha1.PodGroupRunning,
					},
				},
			},
			pods: []*v1.Pod{
				utils.BuildPodWithPriority("test", "pod1-1", "node1", v1.PodRunning, utils.BuildResourceList("1", "2G"), "pg1", make(map[string]string), make(map[string]string), &lowPriority),
				utils.BuildPodWithPriority("test", "pod1-2", "node1", v1.PodRunning, utils.BuildResourceList("1", "2G"), "pg1", make(map[string]string), make(map[string]string), &highPriority),
				utils.BuildPodWithPriority("test", "pod1-3", "node1", v1.PodRunning, utils.BuildResourceList("1", "2G"), "pg1", make(map[string]string), make(map[string]string), &highPriority),
				utils.BuildPodWithPriority("test", "pod2-1", "node1", v1.PodRunning, utils.BuildResourceList("1", "2G"), "pg2", make(map[string]string), make(map[string]string), &lowPriority),
				utils.BuildPodWithPriority("test", "pod2-2", "node2", v1.PodRunning, utils.BuildResourceList("1", "2G"), "pg2", make(map[string]string), make(map[string]string), &highPriority),
				utils.BuildPodWithPriority("test", "pod3-1", "node2", v1.PodRunning, utils.BuildResourceList("1", "2G"), "pg3", make(map[string]string), make(map[string]string), &lowPriority),
				utils.BuildPodWithPriority("test", "pod3-2", "node2", v1.PodRunning, utils.BuildResourceList("1", "2G"), "pg3", make(map[string]string), make(map[string]string), &highPriority),
			},
			expected: 3,
		},
	}
	shuffle := New()

	for i, test := range tests {
		binder := &utils.FakeBinder{
			Binds:   map[string]string{},
			Channel: make(chan string, 1),
		}
		evictor := &utils.FakeEvictor{
			Channel: make(chan string),
		}
		schedulerCache := &cache.SchedulerCache{
			Nodes:           make(map[string]*apis.NodeInfo),
			Jobs:            make(map[apis.JobID]*apis.JobInfo),
			Queues:          make(map[apis.QueueID]*apis.QueueInfo),
			Binder:          binder,
			Evictor:         evictor,
			StatusUpdater:   &utils.FakeStatusUpdater{},
			VolumeBinder:    &utils.FakeVolumeBinder{},
			PriorityClasses: make(map[string]*schedulingv1.PriorityClass),

			Recorder: record.NewFakeRecorder(100),
		}
		schedulerCache.PriorityClasses["high-priority"] = &schedulingv1.PriorityClass{
			Value: highPriority,
		}
		schedulerCache.PriorityClasses["low-priority"] = &schedulingv1.PriorityClass{
			Value: lowPriority,
		}

		for _, node := range test.nodes {
			schedulerCache.AddNode(node)
		}
		for _, q := range test.queues {
			schedulerCache.AddQueueV1alpha1(q)
		}
		for _, ss := range test.podGroups {
			schedulerCache.AddPodGroupV1alpha1(ss)
		}
		for _, pod := range test.pods {
			schedulerCache.AddPod(pod)
		}

		trueValue := true
		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:          "fake",
						EnabledVictim: &trueValue,
					},
				},
			},
		}, nil)
		defer framework.CloseSession(ssn)

		fakePluginVictimFns := func() []apis.VictimTasksFn {
			victimTasksFn := func(candidates []*apis.TaskInfo) []*apis.TaskInfo {
				evicts := make([]*apis.TaskInfo, 0)
				for _, task := range candidates {
					if task.Priority == lowPriority {
						evicts = append(evicts, task)
					}
				}
				return evicts
			}

			victimTasksFns := make([]apis.VictimTasksFn, 0)
			victimTasksFns = append(victimTasksFns, victimTasksFn)
			return victimTasksFns
		}
		ssn.AddVictimTasksFns("fake", fakePluginVictimFns())

		shuffle.Execute(ssn)
		for {
			select {
			case <-evictor.Channel:
			case <-time.After(2 * time.Second):
				goto LOOP
			}
		}

	LOOP:
		if test.expected != len(evictor.Evicts()) {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, test.name, test.expected, len(evictor.Evicts()))
		}
	}
}
