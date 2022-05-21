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

import (
	"fmt"
	"github.com/hliangzhao/volcano/pkg/apis/scheduling"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	"strings"
	"testing"
)

// TODO: just copied.
//  Passed.

func TestSchedulerCache_updateTask(t *testing.T) {
	namespace := "test"
	owner := buildOwnerReference("j1")

	tests := []struct {
		Name        string
		OldPod      *v1.Pod
		NewPod      *v1.Pod
		Nodes       []*v1.Node
		JobInfo     *apis.JobInfo
		OldTaskInfo *apis.TaskInfo
		NewTaskInfo *apis.TaskInfo
		Expected    error
	}{
		{
			Name:   "Success Case",
			OldPod: buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			NewPod: buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "2G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			OldTaskInfo: &apis.TaskInfo{},
			NewTaskInfo: &apis.TaskInfo{},
			Expected:    nil,
		},
		{
			Name:   "Error Case",
			OldPod: buildPod(namespace, "p1", "n1", v1.PodSucceeded, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			NewPod: buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "2G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			OldTaskInfo: &apis.TaskInfo{},
			NewTaskInfo: &apis.TaskInfo{},
			Expected:    fmt.Errorf("failed to find task <%s/%s> on host <%s>", namespace, "p1", "n1"),
		},
	}

	for i, test := range tests {
		cache := &SchedulerCache{
			Jobs:  make(map[apis.JobID]*apis.JobInfo),
			Nodes: make(map[string]*apis.NodeInfo),
		}

		for _, n := range test.Nodes {
			cache.AddNode(n)
		}

		cache.AddPod(test.OldPod)
		test.OldTaskInfo = apis.NewTaskInfo(test.OldPod)
		test.NewTaskInfo = apis.NewTaskInfo(test.NewPod)

		new := cache.updateTask(test.OldTaskInfo, test.NewTaskInfo)

		if test.Expected != nil && new != nil && !strings.Contains(new.Error(), test.Expected.Error()) {
			t.Errorf("Expected Error to be %v but got %v in case %d", test.Expected, new, i)
		}
	}
}

func TestSchedulerCache_UpdatePod(t *testing.T) {
	namespace := "test"
	owner := buildOwnerReference("j1")

	tests := []struct {
		Name     string
		OldPod   *v1.Pod
		NewPod   *v1.Pod
		Nodes    []*v1.Node
		JobInfo  *apis.JobInfo
		Expected error
	}{
		{
			Name:   "Success Case",
			OldPod: buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			NewPod: buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "2G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			Expected: nil,
		},
		{
			Name:   "Error Case",
			OldPod: buildPod(namespace, "p1", "n1", v1.PodSucceeded, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			NewPod: buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "2G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			Expected: fmt.Errorf("failed to find task <%s/%s> on host <%s>", namespace, "p1", "n1"),
		},
	}

	for i, test := range tests {
		cache := &SchedulerCache{
			Jobs:  make(map[apis.JobID]*apis.JobInfo),
			Nodes: make(map[string]*apis.NodeInfo),
		}

		for _, n := range test.Nodes {
			cache.AddNode(n)
		}

		cache.AddPod(test.OldPod)

		new := cache.updatePod(test.OldPod, test.NewPod)

		if test.Expected != nil && new != nil && !strings.Contains(new.Error(), test.Expected.Error()) {
			t.Errorf("Expected Error to be %v but got %v in case %d", test.Expected, new, i)
		}
	}
}

func TestSchedulerCache_AddPodGroupV1beta1(t *testing.T) {
	namespace := "test"
	owner := buildOwnerReference("j1")

	tests := []struct {
		Name     string
		Pod      *v1.Pod
		Nodes    []*v1.Node
		PodGroup interface{}
		Expected *apis.PodGroup
	}{
		{
			Name: "Success Case",
			Pod:  buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			PodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "j1",
					Namespace: namespace,
				},
			},
			Expected: &apis.PodGroup{
				PodGroup: scheduling.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "j1",
						Namespace: namespace,
					},
				},
			},
		},
		{
			Name: "Error Case: 1 - Wrong Type",
			Pod:  buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			PodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "j1",
					Namespace: namespace,
				},
			},
			Expected: nil,
		},
		{
			Name: "Error Case: 2 - PodGroup Without Identity",
			Pod:  buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			PodGroup: &schedulingv1alpha1.PodGroup{
				Status: schedulingv1alpha1.PodGroupStatus{
					Running: int32(1),
				},
			},
			Expected: nil,
		},
	}

	for i, test := range tests {
		cache := &SchedulerCache{
			Jobs:  make(map[apis.JobID]*apis.JobInfo),
			Nodes: make(map[string]*apis.NodeInfo),
		}

		for _, n := range test.Nodes {
			cache.AddNode(n)
		}
		test.Pod.Annotations = map[string]string{
			"scheduling.k8s.io/group-name": "j1",
		}
		cache.AddPod(test.Pod)

		cache.AddPodGroupV1alpha1(test.PodGroup)
		jobID := apis.JobID("test/j1")

		job := cache.Jobs[jobID]
		pg := job.PodGroup

		if test.Expected != nil && (pg.Namespace != test.Expected.Namespace || pg.Name != test.Expected.Name) {
			t.Errorf("Expected pg to be: %v but got :%v in case %d", test.Expected, pg, i)
		}
	}
}

func TestSchedulerCache_UpdatePodGroupV1beta1(t *testing.T) {
	namespace := "test"
	owner := buildOwnerReference("j1")

	tests := []struct {
		Name        string
		Pod         *v1.Pod
		Nodes       []*v1.Node
		OldPodGroup interface{}
		NewPodGroup interface{}
		Expected    *apis.PodGroup
	}{
		{
			Name: "Success Case",
			Pod:  buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			OldPodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "j1",
					Namespace: namespace,
				},
			},
			NewPodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "j1-updated",
					Namespace: namespace,
				},
			},
			Expected: &apis.PodGroup{
				PodGroup: scheduling.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "j1-updated",
						Namespace: namespace,
					},
				},
			},
		},
		{
			Name: "Error Case: 1 - Wrong Type(OldPodGroup)",
			Pod:  buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			OldPodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "j1",
					Namespace: namespace,
				},
			},
			NewPodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "j1-updated",
					Namespace: namespace,
				},
			},
			Expected: nil,
		},
		{
			Name: "Error Case: 2 - PodGroup Without Identity",
			Pod:  buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			NewPodGroup: &schedulingv1alpha1.PodGroup{
				Status: schedulingv1alpha1.PodGroupStatus{
					Running: int32(1),
				},
			},
			OldPodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "j1-updated",
					Namespace: namespace,
				},
			},
			Expected: nil,
		},
		{
			Name: "Error Case: 3 - Wrong Type(NewPodGroup)",
			Pod:  buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			OldPodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "j1",
					Namespace: namespace,
				},
			},
			NewPodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "j1-updated",
					Namespace: namespace,
				},
			},
			Expected: nil,
		},
	}

	for i, test := range tests {
		cache := &SchedulerCache{
			Jobs:  make(map[apis.JobID]*apis.JobInfo),
			Nodes: make(map[string]*apis.NodeInfo),
		}

		for _, n := range test.Nodes {
			cache.AddNode(n)
		}
		test.Pod.Annotations = map[string]string{
			"scheduling.k8s.io/group-name": "j1",
		}
		cache.AddPod(test.Pod)

		cache.UpdatePodGroupV1alpha1(test.OldPodGroup, test.NewPodGroup)
		jobID := apis.JobID("test/j1")

		job := cache.Jobs[jobID]
		pg := job.PodGroup

		if test.Expected != nil && pg != nil && (pg.Namespace != test.Expected.Namespace || pg.Name != test.Expected.Name) {
			t.Errorf("Expected pg to be: %v but got :%v in case %d", test.Expected, pg, i)
		}
	}
}

func TestSchedulerCache_DeletePodGroupV1beta1(t *testing.T) {
	namespace := "test"
	owner := buildOwnerReference("j1")

	tests := []struct {
		Name     string
		Pod      *v1.Pod
		Nodes    []*v1.Node
		PodGroup interface{}
		Expected *apis.PodGroup
	}{
		{
			Name: "Success Case",
			Pod:  buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			PodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "j1",
					Namespace: namespace,
				},
			},
			Expected: nil,
		},
		{
			Name: "Error Case: 1 - Wrong Type",
			Pod:  buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			PodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "j1",
					Namespace: namespace,
				},
			},
			Expected: &apis.PodGroup{
				PodGroup: scheduling.PodGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "j1",
						Namespace: namespace,
					},
				},
			},
		},
		{
			Name: "Error Case: 2 - PodGroup Without Identity",
			Pod:  buildPod(namespace, "p1", "n1", v1.PodRunning, buildResourceList("1000m", "1G"), []metav1.OwnerReference{owner}, make(map[string]string)),
			Nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2000m", "10G")),
			},
			PodGroup: &schedulingv1alpha1.PodGroup{
				Status: schedulingv1alpha1.PodGroupStatus{
					Running: int32(1),
				},
			},
			Expected: &apis.PodGroup{
				PodGroup: scheduling.PodGroup{
					Status: scheduling.PodGroupStatus{
						Running: int32(1),
					},
				},
			},
		},
	}

	for i, test := range tests {
		cache := &SchedulerCache{
			Jobs:  make(map[apis.JobID]*apis.JobInfo),
			Nodes: make(map[string]*apis.NodeInfo),
		}

		cache.deletedJobs = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

		for _, n := range test.Nodes {
			cache.AddNode(n)
		}
		test.Pod.Annotations = map[string]string{
			"scheduling.k8s.io/group-name": "j1",
		}
		cache.AddPod(test.Pod)

		cache.AddPodGroupV1alpha1(test.PodGroup)

		cache.DeletePodGroupV1alpha1(test.PodGroup)
		jobID := apis.JobID("test/j1")

		job := cache.Jobs[jobID]

		if test.Expected == nil && job.PodGroup != nil {
			t.Errorf("Expected job  to be: %v but got :%v in case %d", test.Expected, job, i)
		}
	}
}

func TestSchedulerCache_AddQueueV1beta1(t *testing.T) {
	namespace := "test"

	tests := []struct {
		Name     string
		Queue    interface{}
		Expected *scheduling.Queue
	}{
		{
			Name: "Success Case",
			Queue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1",
					Namespace: namespace,
				},
			},
			Expected: &scheduling.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1",
					Namespace: namespace,
				},
			},
		},
		{
			Name: "Error Case: 1 - Wrong Type",
			Queue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1",
					Namespace: namespace,
				},
			},
			Expected: nil,
		},
	}

	for i, test := range tests {
		cache := &SchedulerCache{
			Jobs:   make(map[apis.JobID]*apis.JobInfo),
			Nodes:  make(map[string]*apis.NodeInfo),
			Queues: make(map[apis.QueueID]*apis.QueueInfo)}

		cache.AddQueueV1alpha1(test.Queue)

		queue := cache.Queues["q1"]

		if test.Expected != nil && queue != nil && queue.Queue != nil && (queue.Queue.Namespace != test.Expected.Namespace || queue.Queue.Name != test.Expected.Name) {
			t.Errorf("Expected: %v but got: %v in case %d", test.Expected, queue.Queue, i)
		}
	}
}

func TestSchedulerCache_UpdateQueueV1beta1(t *testing.T) {
	namespace := "test"

	tests := []struct {
		Name     string
		OldQueue interface{}
		NewQueue interface{}
		Expected *scheduling.Queue
	}{
		{
			Name: "Success Case",
			OldQueue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1",
					Namespace: namespace,
				},
			},
			NewQueue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1-updated",
					Namespace: namespace,
				},
			},
			Expected: &scheduling.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1-updated",
					Namespace: namespace,
				},
			},
		},
		{
			Name: "Error Case: 1 - Wrong Type(OldQueue)",
			OldQueue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1",
					Namespace: namespace,
				},
			},
			NewQueue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1-updated",
					Namespace: namespace,
				},
			},
			Expected: nil,
		},
		{
			Name: "Error Case: 2 - Wrong Type(NewQueue)",
			OldQueue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1",
					Namespace: namespace,
				},
			},
			NewQueue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1-updated",
					Namespace: namespace,
				},
			},
			Expected: nil,
		},
	}

	for i, test := range tests {
		cache := &SchedulerCache{
			Jobs:   make(map[apis.JobID]*apis.JobInfo),
			Nodes:  make(map[string]*apis.NodeInfo),
			Queues: make(map[apis.QueueID]*apis.QueueInfo),
		}

		cache.UpdateQueueV1alpha1(test.OldQueue, test.NewQueue)

		queue := cache.Queues["q1-updated"]

		if test.Expected != nil && queue != nil && queue.Queue != nil && (queue.Queue.Namespace != test.Expected.Namespace || queue.Queue.Name != test.Expected.Name) {
			t.Errorf("Expected: %v but got: %v in case %d", test.Expected, queue.Queue, i)
		}
	}
}

func TestSchedulerCache_DeleteQueueV1beta1(t *testing.T) {
	namespace := "test"

	tests := []struct {
		Name     string
		Queue    interface{}
		Expected *scheduling.Queue
	}{
		{
			Name: "Success Case",
			Queue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1",
					Namespace: namespace,
				},
			},
			Expected: &scheduling.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1",
					Namespace: namespace,
				},
			},
		},
		{
			Name: "Error Case: 1 - Wrong Type",
			Queue: &schedulingv1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "q1",
					Namespace: namespace,
				},
			},
			Expected: nil,
		},
	}

	for i, test := range tests {
		cache := &SchedulerCache{
			Jobs:   make(map[apis.JobID]*apis.JobInfo),
			Nodes:  make(map[string]*apis.NodeInfo),
			Queues: make(map[apis.QueueID]*apis.QueueInfo),
		}

		cache.AddQueueV1alpha1(test.Queue)
		cache.DeleteQueueV1alpha1(test.Queue)

		queue := cache.Queues["q1"]

		if test.Expected == nil && queue != nil {
			t.Errorf("Expected: %v but got: %v in case %d", test.Expected, queue, i)
		}

		if test.Expected != nil && queue != nil && queue.Queue != nil && (queue.Queue.Namespace != test.Expected.Namespace || queue.Queue.Name != test.Expected.Name) {
			t.Errorf("Expected: %v but got: %v in case %d", test.Expected, queue.Queue, i)
		}
	}
}
