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

package apis

import (
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// LessFn is the func declaration used by sort or priority queue.
type LessFn func(interface{}, interface{}) bool

// CompareFn is the func declaration used by sort or priority queue.
type CompareFn func(interface{}, interface{}) int

// ValidateFn is the func declaration used to check object's status.
type ValidateFn func(interface{}) bool

// ValidateResult is struct to which can used to determine the result
type ValidateResult struct {
	Pass    bool
	Reason  string
	Message string
}

// ValidateExFn is the func declaration used to validate the result.
type ValidateExFn func(interface{}) *ValidateResult

// VoteFn is the func declaration used to check object's complicated status.
type VoteFn func(interface{}) int

// JobEnqueuedFn is the func declaration used to call after job enqueued.
type JobEnqueuedFn func(interface{})

// PredicateFn is the func declaration used to predicate node for task.
type PredicateFn func(*TaskInfo, *NodeInfo) error

// BestNodeFn is the func declaration used to return the nodeScores to plugins.
type BestNodeFn func(*TaskInfo, map[float64][]*NodeInfo) *NodeInfo

// EvictableFn is the func declaration used to evict tasks.
type EvictableFn func(*TaskInfo, []*TaskInfo) ([]*TaskInfo, int)

// NodeOrderFn is the func declaration used to get priority score for a node for a particular task.
type NodeOrderFn func(*TaskInfo, *NodeInfo) (float64, error)

// BatchNodeOrderFn is the func declaration used to get priority score for ALL nodes for a particular task.
type BatchNodeOrderFn func(*TaskInfo, []*NodeInfo) (map[string]float64, error)

// NodeMapFn is the func declaration used to get priority score for a node for a particular task.
type NodeMapFn func(*TaskInfo, *NodeInfo) (float64, error)

// NodeReduceFn is the func declaration used to reduce priority score for a node for a particular task.
type NodeReduceFn func(*TaskInfo, k8sframework.NodeScoreList) error

// NodeOrderMapFn is the func declaration used to get priority score of all plugins for a node for a particular task.
type NodeOrderMapFn func(*TaskInfo, *NodeInfo) (map[string]float64, float64, error)

// NodeOrderReduceFn is the func declaration used to reduce priority score of all nodes for a plugin for a particular task.
type NodeOrderReduceFn func(*TaskInfo, map[string]k8sframework.NodeScoreList) (map[string]float64, error)

// TargetJobFn is the func declaration used to select the target job satisfies some conditions
type TargetJobFn func([]*JobInfo) *JobInfo

// ReservedNodesFn is the func declaration used to select the reserved nodes
type ReservedNodesFn func()

// VictimTasksFn is the func declaration used to select victim tasks
type VictimTasksFn func([]*TaskInfo) []*TaskInfo

// UnderUsedResourceFn is the func declaration used to get under used resource list for queue
// type UnderUsedResourceFn func(*QueueInfo) ResourceNameList

// AllocatableFn is the func declaration used to check whether the task can be allocated
type AllocatableFn func(*QueueInfo, *TaskInfo) bool
