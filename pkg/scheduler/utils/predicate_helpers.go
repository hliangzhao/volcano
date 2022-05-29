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

package utils

// fully checked and understood

import (
	"context"
	"fmt"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"sync"
	"sync/atomic"
)

type PredicateHelper interface {
	PredicateNodes(task *apis.TaskInfo, nodes []*apis.NodeInfo, fn apis.PredicateFn) ([]*apis.NodeInfo, *apis.FitErrors)
}

type predicateHelper struct {
	taskPredicateErrorCache map[string]map[string]error // taskGroupId: {node-name: error}
}

func NewPredicateHelper() PredicateHelper {
	return &predicateHelper{
		taskPredicateErrorCache: map[string]map[string]error{},
	}
}

// PredicateNodes returns the nodes that fit a task after the predicates.
// Specifically, it selects `numNodesToFind` nodes from the input node list for task.
func (ph *predicateHelper) PredicateNodes(task *apis.TaskInfo, nodes []*apis.NodeInfo,
	fn apis.PredicateFn) ([]*apis.NodeInfo, *apis.FitErrors) {

	var errLock sync.RWMutex
	fe := apis.NewFitErrors()

	allNodes := len(nodes)
	if allNodes == 0 {
		return make([]*apis.NodeInfo, 0), fe
	}
	numNodesToFind := CalculateNumOfFeasibleNodesToFind(int32(allNodes))

	predicateNodes := make([]*apis.NodeInfo, numNodesToFind)
	numFoundNodes := int32(0)
	numProcessedNodes := int32(0)

	taskGroupId := taskGroupID(task)
	nodeErrCache, taskFailedBefore := ph.taskPredicateErrorCache[taskGroupId]
	if nodeErrCache == nil {
		nodeErrCache = map[string]error{}
	}

	ctx, cancel := context.WithCancel(context.Background())

	checkNode := func(index int) {
		// Check the nodes starting from where is left off in the previous scheduling cycle,
		// to make sure all nodes have the same chance of being examined across pods.
		node := nodes[(lastProcessedNodeIndex+index)%allNodes]
		atomic.AddInt32(&numProcessedNodes, 1)
		klog.V(4).Infof("Considering Task <%v/%v> on node <%v>: <%v> vs. <%v>",
			task.Namespace, task.Name, node.Name, task.ResReq, node.Idle)

		// Check if the task had "predicate" failure before.
		// And then check if the task failed to predict on this node before.
		if taskFailedBefore {
			errLock.RLock()
			errC, ok := nodeErrCache[node.Name]
			errLock.RUnlock()

			if ok {
				errLock.Lock()
				fe.SetNodeError(node.Name, errC)
				errLock.Unlock()
				return
			}
		}

		// TODO: Enable errCache for performance improvement.
		if err := fn(task, node); err != nil {
			klog.V(3).Infof("Predicates failed for task <%s/%s> on node <%s>: %v",
				task.Namespace, task.Name, node.Name, err)
			errLock.Lock()
			nodeErrCache[node.Name] = err
			ph.taskPredicateErrorCache[taskGroupId] = nodeErrCache
			fe.SetNodeError(node.Name, err)
			errLock.Unlock()
			return
		}

		// check if the number of found nodes is more than the numNodesToFind
		length := atomic.AddInt32(&numFoundNodes, 1)
		if length > numNodesToFind {
			cancel()
			atomic.AddInt32(&numFoundNodes, -1)
		} else {
			predicateNodes[length-1] = node
		}
	}

	// check all nodes
	workqueue.ParallelizeUntil(ctx, 16, allNodes, checkNode)

	lastProcessedNodeIndex = (lastProcessedNodeIndex + int(numProcessedNodes)) % allNodes
	predicateNodes = predicateNodes[:numFoundNodes]
	return predicateNodes, fe
}

func taskGroupID(task *apis.TaskInfo) string {
	return fmt.Sprintf("%s/%s", task.Job, task.GetTaskSpecKey())
}
