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

package cache

// TODO: just copied.
//  Passed.

import (
	"fmt"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"reflect"
	"testing"
)

func buildNode(name string, alloc corev1.ResourceList) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			UID:  types.UID(name),
			Name: name,
		},
		Status: corev1.NodeStatus{
			Capacity:    alloc,
			Allocatable: alloc,
		},
	}
}

func buildPod(ns, n, nn string,
	p corev1.PodPhase, req corev1.ResourceList,
	owner []metav1.OwnerReference, labels map[string]string) *corev1.Pod {

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:             types.UID(fmt.Sprintf("%v-%v", ns, n)),
			Name:            n,
			Namespace:       ns,
			OwnerReferences: owner,
			Labels:          labels,
		},
		Status: corev1.PodStatus{
			Phase: p,
		},
		Spec: corev1.PodSpec{
			NodeName: nn,
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

func buildResourceList(cpu string, memory string) corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpu),
		corev1.ResourceMemory: resource.MustParse(memory),
	}
}

func buildOwnerReference(owner string) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		Controller: &controller,
		UID:        types.UID(owner),
	}
}

func TestGetOrCreateJob(t *testing.T) {
	owner1 := buildOwnerReference("j1")
	owner2 := buildOwnerReference("j2")

	pod1 := buildPod("c1", "p1", "n1", corev1.PodRunning, buildResourceList("1000m", "1G"),
		[]metav1.OwnerReference{owner1}, make(map[string]string))
	pi1 := apis.NewTaskInfo(pod1)
	pi1.Job = "j1" // The job name is set by cache.

	pod2 := buildPod("c1", "p2", "n1", corev1.PodRunning, buildResourceList("1000m", "1G"),
		[]metav1.OwnerReference{owner2}, make(map[string]string))
	pod2.Spec.SchedulerName = "volcano"
	pi2 := apis.NewTaskInfo(pod2)

	pod3 := buildPod("c3", "p3", "n1", corev1.PodRunning, buildResourceList("1000m", "1G"),
		[]metav1.OwnerReference{owner2}, make(map[string]string))
	pi3 := apis.NewTaskInfo(pod3)

	cache := &SchedulerCache{
		Nodes:         make(map[string]*apis.NodeInfo),
		Jobs:          make(map[apis.JobID]*apis.JobInfo),
		schedulerName: "volcano",
	}

	tests := []struct {
		task   *apis.TaskInfo
		gotJob bool // whether getOrCreateJob will return job for corresponding task
	}{
		{
			task:   pi1,
			gotJob: true,
		},
		{
			task:   pi2,
			gotJob: false,
		},
		{
			task:   pi3,
			gotJob: false,
		},
	}
	for i, test := range tests {
		result := cache.getOrCreateJob(test.task) != nil
		if result != test.gotJob {
			t.Errorf("case %d: \n expected %t, \n got %t \n",
				i, test.gotJob, result)
		}
	}
}

func TestSchedulerCache_Bind_NodeWithSufficientResources(t *testing.T) {
	owner := buildOwnerReference("j1")

	cache := &SchedulerCache{
		Jobs:  make(map[apis.JobID]*apis.JobInfo),
		Nodes: make(map[string]*apis.NodeInfo),
		Binder: &utils.FakeBinder{
			Binds:   map[string]string{},
			Channel: make(chan string),
		},
		BindFlowChannel: make(chan *apis.TaskInfo, 5000),
	}

	pod := buildPod("c1", "p1", "", corev1.PodPending, buildResourceList("1000m", "1G"),
		[]metav1.OwnerReference{owner}, make(map[string]string))
	cache.AddPod(pod)

	node := buildNode("n1", buildResourceList("2000m", "10G"))
	cache.AddNode(node)

	task := apis.NewTaskInfo(pod)
	task.Job = "j1"
	if err := cache.addTask(task); err != nil {
		t.Errorf("failed to add task %v", err)
	}
	task.NodeName = "n1"
	err := cache.AddBindTask(task)
	if err != nil {
		t.Errorf("failed to bind pod to node: %v", err)
	}
}

func TestSchedulerCache_Bind_NodeWithInsufficientResources(t *testing.T) {
	owner := buildOwnerReference("j1")

	cache := &SchedulerCache{
		Jobs:  make(map[apis.JobID]*apis.JobInfo),
		Nodes: make(map[string]*apis.NodeInfo),
		Binder: &utils.FakeBinder{
			Binds:   map[string]string{},
			Channel: make(chan string),
		},
		BindFlowChannel: make(chan *apis.TaskInfo, 5000),
	}

	pod := buildPod("c1", "p1", "", corev1.PodPending, buildResourceList("5000m", "50G"),
		[]metav1.OwnerReference{owner}, make(map[string]string))
	cache.AddPod(pod)

	node := buildNode("n1", buildResourceList("2000m", "10G"))
	cache.AddNode(node)

	task := apis.NewTaskInfo(pod)
	task.Job = "j1"

	if err := cache.addTask(task); err != nil {
		t.Errorf("failed to add task %v", err)
	}

	task.NodeName = "n1"
	taskBeforeBind := task.Clone()
	nodeBeforeBind := cache.Nodes["n1"].Clone()

	err := cache.AddBindTask(task)
	if err == nil {
		t.Errorf("expected bind to fail for node with insufficient resources")
	}

	_, taskAfterBind, err := cache.findJobAndTask(task)
	if err != nil {
		t.Errorf("expected to find task after failed bind")
	}
	if !reflect.DeepEqual(taskBeforeBind, taskAfterBind) {
		t.Errorf("expected task to remain the same after failed bind: \n %#v\n %#v", taskBeforeBind, taskAfterBind)
	}

	nodeAfterBind := cache.Nodes["n1"].Clone()
	if !reflect.DeepEqual(nodeBeforeBind, nodeAfterBind) {
		t.Errorf("expected node to remain the same after failed bind")
	}
}

func TestNodeOperation(t *testing.T) {
	// case 1
	node1 := buildNode("n1", buildResourceList("2000m", "10G"))
	node2 := buildNode("n2", buildResourceList("4000m", "16G"))
	node3 := buildNode("n3", buildResourceList("3000m", "12G"))
	nodeInfo1 := apis.NewNodeInfo(node1)
	nodeInfo2 := apis.NewNodeInfo(node2)
	nodeInfo3 := apis.NewNodeInfo(node3)
	tests := []struct {
		deletedNode *corev1.Node
		nodes       []*corev1.Node
		expected    *SchedulerCache
		delExpect   *SchedulerCache
	}{
		{
			deletedNode: node2,
			nodes:       []*corev1.Node{node1, node2, node3},
			expected: &SchedulerCache{
				Nodes: map[string]*apis.NodeInfo{
					"n1": nodeInfo1,
					"n2": nodeInfo2,
					"n3": nodeInfo3,
				},
				NodeList: []string{"n1", "n2", "n3"},
			},
			delExpect: &SchedulerCache{
				Nodes: map[string]*apis.NodeInfo{
					"n1": nodeInfo1,
					"n3": nodeInfo3,
				},
				NodeList: []string{"n1", "n3"},
			},
		},
		{
			deletedNode: node1,
			nodes:       []*corev1.Node{node1, node2, node3},
			expected: &SchedulerCache{
				Nodes: map[string]*apis.NodeInfo{
					"n1": nodeInfo1,
					"n2": nodeInfo2,
					"n3": nodeInfo3,
				},
				NodeList: []string{"n1", "n2", "n3"},
			},
			delExpect: &SchedulerCache{
				Nodes: map[string]*apis.NodeInfo{
					"n2": nodeInfo2,
					"n3": nodeInfo3,
				},
				NodeList: []string{"n2", "n3"},
			},
		},
		{
			deletedNode: node3,
			nodes:       []*corev1.Node{node1, node2, node3},
			expected: &SchedulerCache{
				Nodes: map[string]*apis.NodeInfo{
					"n1": nodeInfo1,
					"n2": nodeInfo2,
					"n3": nodeInfo3,
				},
				NodeList: []string{"n1", "n2", "n3"},
			},
			delExpect: &SchedulerCache{
				Nodes: map[string]*apis.NodeInfo{
					"n1": nodeInfo1,
					"n2": nodeInfo2,
				},
				NodeList: []string{"n1", "n2"},
			},
		},
	}

	for i, test := range tests {
		cache := &SchedulerCache{
			Nodes:    make(map[string]*apis.NodeInfo),
			NodeList: []string{},
		}

		for _, n := range test.nodes {
			cache.AddNode(n)
		}

		if !reflect.DeepEqual(cache, test.expected) {
			t.Errorf("case %d: \n expected %v, \n got %v \n",
				i, test.expected, cache)
		}

		// delete node
		cache.DeleteNode(test.deletedNode)
		if !reflect.DeepEqual(cache, test.delExpect) {
			t.Errorf("case %d: \n expected %v, \n got %v \n",
				i, test.delExpect, cache)
		}
	}
}
