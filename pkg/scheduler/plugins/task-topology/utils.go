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

package tasktopology

import (
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
)

const (
	GroupName = "hliangzhao.io"
	// PluginName indicates name of volcano scheduler plugin
	PluginName = "task-topology"
	// PluginWeight is task-topology plugin weight in nodeOrderFn
	PluginWeight = "task-topology.weight"
	// JobAffinityKey is the key to read in task-topology arguments from job annotations
	JobAffinityKey = GroupName + "/task-topology"
	// OutOfBucket indicates whether task is out of any bucket
	OutOfBucket = -1

	// JobAffinityAnnotations is the key to read in task-topology affinity arguments from podgroup annotations
	JobAffinityAnnotations = GroupName + "/task-topology-affinity"
	// JobAntiAffinityAnnotations is the key to read in task-topology anti-affinity arguments from podgroup annotations
	JobAntiAffinityAnnotations = GroupName + "/task-topology-anti-affinity"
	// TaskOrderAnnotations is the key to read in task-topology task order arguments from podgroup annotations
	TaskOrderAnnotations = GroupName + "/task-topology-task-order"
)

// TaskTopology is used to save affinity infos of a job read from job plugin or annotations
type TaskTopology struct {
	Affinity     [][]string `json:"affinity,omitempty"`
	AntiAffinity [][]string `json:"antiAffinity,omitempty"`
	TaskOrder    []string   `json:"taskOrder,omitempty"`
}

func calculateWeight(arguments framework.Arguments) int {
	/*
	   User Should give taskTopologyWeight in this format(task-topology.weight).

	   actions: "enqueue, reclaim, allocate, backfill, preempt"
	   tiers:
	   - plugins:
	     - name: task-topology
	       arguments:
	         task-topology.weight: 10
	*/
	// Values are initialized to 1.
	weight := 1
	arguments.GetInt(&weight, PluginWeight)
	return weight
}

func getTaskName(task *apis.TaskInfo) string {
	return task.Pod.Annotations[batchv1alpha1.TaskSpecKey]
}

func addAffinity(m map[string]map[string]struct{}, src, dst string) {
	srcMap, ok := m[src]
	if !ok {
		srcMap = map[string]struct{}{}
		m[src] = srcMap
	}
	srcMap[dst] = struct{}{}
}

func noPendingTasks(job *apis.JobInfo) bool {
	return len(job.TaskStatusIndex[apis.Pending]) == 0
}

type TaskOrder struct {
	tasks   []*apis.TaskInfo
	manager *JobManager
}

func (o *TaskOrder) Len() int {
	return len(o.tasks)
}

func (o *TaskOrder) Swap(l, r int) {
	o.tasks[l], o.tasks[r] = o.tasks[r], o.tasks[l]
}

func (o *TaskOrder) Less(l, r int) bool {
	L := o.tasks[l]
	R := o.tasks[r]

	LHasNode := L.NodeName != ""
	RHasNode := R.NodeName != ""
	if LHasNode || RHasNode {
		// the task bounded would have high priority
		if LHasNode != RHasNode {
			return !LHasNode
		}
		// all bound, any order is alright
		return L.NodeName > R.NodeName
	}

	result := o.manager.taskAffinityOrder(L, R)
	// they have the same taskAffinity order, any order is alright
	if result == 0 {
		return L.Name > R.Name
	}
	return result < 0
}
