/*
Copyright 2021 hliangzhao.

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

import (
	`context`
	`fmt`
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	`k8s.io/client-go/util/workqueue`
	`k8s.io/klog/v2`
	k8sframework `k8s.io/kubernetes/pkg/scheduler/framework`
	`math`
	`math/rand`
	`sort`
	`sync`
)

const baselinePercentageOfNodesToFind = 50

var lastProcessedNodeIndex int

// Reservation is used to record target job and locked nodes
var Reservation *ResourceReservation

func init() {
	Reservation = NewResourceReservation()
}

// CalculateNumOfFeasibleNodesToFind returns the number of feasible nodes that once found,
// the scheduler stops its search for more feasible nodes.
func CalculateNumOfFeasibleNodesToFind(numAllNodes int32) (numNodes int32) {
	// TODO: scheduler/app not implemented
	return 0
}

func GetMinInt(values ...int) int {
	if len(values) == 0 {
		return 0
	}

	min := values[0]
	for _, val := range values {
		if val <= min {
			min = val
		}
	}
	return min
}

// ResourceReservation is struct used for resource reservation
type ResourceReservation struct {
	TargetJob   *apis.JobInfo
	LockedNodes map[string]*apis.NodeInfo
}

func NewResourceReservation() *ResourceReservation {
	return &ResourceReservation{
		TargetJob:   nil,
		LockedNodes: map[string]*apis.NodeInfo{},
	}
}

// GetNodeList returns values of the map 'nodes'.
func GetNodeList(nodes map[string]*apis.NodeInfo, nodeList []string) []*apis.NodeInfo {
	result := make([]*apis.NodeInfo, 0, len(nodeList))
	for _, name := range nodeList {
		if ni, ok := nodes[name]; ok {
			result = append(result, ni)
		}
	}
	return result
}

// ValidateVictims returns an error if the resources of the victims can't satisfy the preemptor.
func ValidateVictims(preemptor *apis.TaskInfo, node *apis.NodeInfo, victims []*apis.TaskInfo) error {
	if len(victims) == 0 {
		return fmt.Errorf("no victims")
	}
	// update idle resource with the evicted resources
	futureIdle := node.FutureIdle()
	for _, victim := range victims {
		futureIdle.Add(victim.ResReq)
	}
	// Every resource of the preemptor needs to be less or equal than corresponding
	// idle resource after preemption.
	if !preemptor.InitResReq.LessEqual(futureIdle, apis.Zero) {
		return fmt.Errorf("not enough resources: requested <%v>, but future idle <%v>",
			preemptor.InitResReq, futureIdle)
	}
	return nil
}

// SelectBestNode returns the best node whose score is highest, pick one randomly if there are many nodes with same score.
func SelectBestNode(nodeScores map[float64][]*apis.NodeInfo) *apis.NodeInfo {
	var bestNodes []*apis.NodeInfo
	maxScore := -1.0
	for score, nodes := range nodeScores {
		if score > maxScore {
			maxScore = score
			bestNodes = nodes
		}
	}

	if len(bestNodes) == 0 {
		return nil
	}

	return bestNodes[rand.Intn(len(bestNodes))]
}

// SortNodes returns nodes by order of score.
func SortNodes(nodeScores map[float64][]*apis.NodeInfo) []*apis.NodeInfo {
	var nodesInorder []*apis.NodeInfo
	var keys []float64
	for key := range nodeScores {
		keys = append(keys, key)
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(keys)))
	for _, key := range keys {
		nodes := nodeScores[key]
		nodesInorder = append(nodesInorder, nodes...)
	}
	return nodesInorder
}

// PrioritizeNodes returns a map whose key is node's score and value are corresponding nodes.
func PrioritizeNodes(task *apis.TaskInfo, nodes []*apis.NodeInfo, batchFn apis.BatchNodeOrderFn, mapFn apis.NodeOrderMapFn,
	reduceFn apis.NodeOrderReduceFn) map[float64][]*apis.NodeInfo {

	pluginNodeScoreMap := map[string]k8sframework.NodeScoreList{} // plugin: NodeScoreList {node name: node score}
	nodeOrderScoreMap := map[string]float64{}                     // node name: node order score
	nodeScores := map[float64][]*apis.NodeInfo{}                  // node score: nodes
	var workerLock sync.Mutex

	// a function: get the scores of the index-th node of every plugin for running task
	scoreNode := func(index int) {
		node := nodes[index]
		mapScores, orderScore, err := mapFn(task, node)
		if err != nil {
			klog.Errorf("Error in Calculating Priority for the node: %v", err)
			return
		}

		workerLock.Lock()
		for plugin, score := range mapScores {
			nodeScoreMap, ok := pluginNodeScoreMap[plugin]
			if !ok {
				nodeScoreMap = k8sframework.NodeScoreList{}
			}
			hp := k8sframework.NodeScore{}
			hp.Name = node.Name
			hp.Score = int64(math.Floor(score))
			pluginNodeScoreMap[plugin] = append(nodeScoreMap, hp)
		}
		nodeOrderScoreMap[node.Name] = orderScore
		workerLock.Unlock()
	}
	workqueue.ParallelizeUntil(context.TODO(), 16, len(nodes), scoreNode)
	reduceScores, err := reduceFn(task, pluginNodeScoreMap)
	if err != nil {
		klog.Errorf("Error in Calculating Priority for the node: %v", err)
		return nodeScores
	}

	batchNodeScore, err := batchFn(task, nodes)
	if err != nil {
		klog.Errorf("Error in Calculating batch Priority for the node, err %v", err)
		return nodeScores
	}

	for _, node := range nodes {
		if score, found := reduceScores[node.Name]; found {
			if orderScore, ok := nodeOrderScoreMap[node.Name]; ok {
				score += orderScore
			}
			if batchScore, ok := batchNodeScore[node.Name]; ok {
				score += batchScore
			}
			nodeScores[score] = append(nodeScores[score], node)
		} else {
			// If no plugin is applied to this node, the default is 0.0
			score = 0.0
			if orderScore, ok := nodeOrderScoreMap[node.Name]; ok {
				score += orderScore
			}
			if batchScore, ok := batchNodeScore[node.Name]; ok {
				score += batchScore
			}
			nodeScores[score] = append(nodeScores[score], node)
		}
	}
	return nodeScores
}
