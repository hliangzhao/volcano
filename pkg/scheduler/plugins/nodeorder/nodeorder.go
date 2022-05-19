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

package nodeorder

import (
	"context"
	"fmt"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils/k8s"
	corev1 "k8s.io/api/core/v1"
	utilFeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/imagelocality"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName = "nodeorder"

	/* The following are perspectives that we will consider when ordering nodes. */

	// NodeAffinityWeight is the key for providing Node Affinity Priority Weight in YAML
	NodeAffinityWeight = "nodeaffinity.weight"
	// PodAffinityWeight is the key for providing Pod Affinity Priority Weight in YAML
	PodAffinityWeight = "podaffinity.weight"
	// LeastRequestedWeight is the key for providing the Least Requested Priority Weight in YAML
	LeastRequestedWeight = "leastrequested.weight"
	// BalancedResourceWeight is the key for providing Balanced Resource Priority Weight in YAML
	BalancedResourceWeight = "balancedresource.weight"
	// MostRequestedWeight is the key for providing the Most Requested Priority Weight in YAML
	MostRequestedWeight = "mostrequested.weight"
	// TaintTolerationWeight is the key for providing Taint Toleration Weight in YAML
	TaintTolerationWeight = "tainttoleration.weight"
	// ImageLocalityWeight is the key for providing Image Locality Priority Weight in YAML
	ImageLocalityWeight = "imagelocality.weight"
)

type nodeOrderPlugin struct {
	pluginArguments framework.Arguments
}

type priorityWeight struct {
	leastReqWeight         int
	mostReqWeight          int
	nodeAffinityWeight     int
	podAffinityWeight      int
	balancedResourceWeight int
	taintTolerationWeight  int
	imageLocalityWeight    int
}

// calculateWeight from the provided arguments.
// User should specify priority weights in the config in this format:
//
//  actions: "reclaim, allocate, backfill, preempt"
//  tiers:
//  - plugins:
//    - name: priority
//    - name: gang
//    - name: conformance
//  - plugins:
//    - name: drf
//    - name: predicates
//    - name: proportion
//    - name: nodeorder
//      arguments:
//        leastrequested.weight: 1
//        mostrequested.weight: 0
//        nodeaffinity.weight: 1
//        podaffinity.weight: 1
//        balancedresource.weight: 1
//        tainttoleration.weight: 1
//        imagelocality.weight: 1
func calculateWeight(arguments framework.Arguments) priorityWeight {
	// By default, for backward compatibility and for reasonable scores,
	// leastrequested priority is enabled and mostrequested priority is disabled.
	weight := priorityWeight{
		leastReqWeight:         1,
		mostReqWeight:          0,
		nodeAffinityWeight:     1,
		podAffinityWeight:      1,
		balancedResourceWeight: 1,
		taintTolerationWeight:  1,
		imageLocalityWeight:    1,
	}

	// Checks whether nodeaffinity.weight is provided or not, if given, modifies the value in weight struct.
	arguments.GetInt(&weight.nodeAffinityWeight, NodeAffinityWeight)

	// Checks whether podaffinity.weight is provided or not, if given, modifies the value in weight struct.
	arguments.GetInt(&weight.podAffinityWeight, PodAffinityWeight)

	// Checks whether leastrequested.weight is provided or not, if given, modifies the value in weight struct.
	arguments.GetInt(&weight.leastReqWeight, LeastRequestedWeight)

	// Checks whether mostrequested.weight is provided or not, if given, modifies the value in weight struct.
	arguments.GetInt(&weight.mostReqWeight, MostRequestedWeight)

	// Checks whether balancedresource.weight is provided or not, if given, modifies the value in weight struct.
	arguments.GetInt(&weight.balancedResourceWeight, BalancedResourceWeight)

	// Checks whether tainttoleration.weight is provided or not, if given, modifies the value in weight struct.
	arguments.GetInt(&weight.taintTolerationWeight, TaintTolerationWeight)

	// Checks whether imagelocality.weight is provided or not, if given, modifies the value in weight struct.
	arguments.GetInt(&weight.imageLocalityWeight, ImageLocalityWeight)

	return weight
}

func New(arguments framework.Arguments) framework.Plugin {
	return &nodeOrderPlugin{pluginArguments: arguments}
}

func (nop *nodeOrderPlugin) Name() string {
	return PluginName
}

func (nop *nodeOrderPlugin) OnSessionOpen(sess *framework.Session) {
	weight := calculateWeight(nop.pluginArguments)
	podList := utils.NewPodListerFromNode(sess)
	nodeMap := utils.GenerateNodeMapAndSlice(sess.Nodes)

	// Register event handlers to update task info in PodLister & nodeMap
	sess.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(e *framework.Event) {
			pod := podList.UpdateTask(e.Task, e.Task.NodeName)

			nodeName := e.Task.NodeName
			node, found := nodeMap[nodeName]
			if !found {
				klog.Warningf("node order, update pod %s/%s allocate to NOT EXIST node [%s]", pod.Namespace, pod.Name, nodeName)
			} else {
				node.AddPod(pod)
				klog.V(4).Infof("node order, update pod %s/%s allocate to node [%s]", pod.Namespace, pod.Name, nodeName)
			}
		},
		DeallocateFunc: func(e *framework.Event) {
			pod := podList.UpdateTask(e.Task, "")

			nodeName := e.Task.NodeName
			node, found := nodeMap[nodeName]
			if !found {
				klog.Warningf("node order, update pod %s/%s allocate from NOT EXIST node [%s]", pod.Namespace, pod.Name, nodeName)
			} else {
				err := node.RemovePod(pod)
				if err != nil {
					klog.Errorf("Failed to update pod %s/%s and deallocate from node [%s]: %s", pod.Namespace, pod.Name, nodeName, err.Error())
				} else {
					klog.V(4).Infof("node order, update pod %s/%s deallocate from node [%s]", pod.Namespace, pod.Name, nodeName)
				}
			}
		},
	})

	fts := feature.Features{
		EnablePodAffinityNamespaceSelector: utilFeature.DefaultFeatureGate.Enabled(features.PodAffinityNamespaceSelector),
		EnablePodDisruptionBudget:          utilFeature.DefaultFeatureGate.Enabled(features.PodDisruptionBudget),
		EnablePodOverhead:                  utilFeature.DefaultFeatureGate.Enabled(features.PodOverhead),
		EnableReadWriteOncePod:             utilFeature.DefaultFeatureGate.Enabled(features.ReadWriteOncePod),
		EnableVolumeCapacityPriority:       utilFeature.DefaultFeatureGate.Enabled(features.VolumeCapacityPriority),
		EnableCSIStorageCapacity:           utilFeature.DefaultFeatureGate.Enabled(features.CSIStorageCapacity),
	}

	// Initialize k8s scheduling plugins
	handle := k8s.NewFrameworkHandle(nodeMap, sess.KubeClient(), sess.InformerFactory())
	// 1. NodeResourcesLeastAllocated
	leastAllocatedArgs := &config.NodeResourcesFitArgs{
		ScoringStrategy: &config.ScoringStrategy{
			Type:      config.LeastAllocated,
			Resources: []config.ResourceSpec{{Name: "cpu", Weight: 50}, {Name: "memory", Weight: 50}},
		},
	}
	p, _ := noderesources.NewFit(leastAllocatedArgs, handle, fts)
	leastAllocated := p.(*noderesources.Fit)

	// 2. NodeResourcesMostAllocated
	mostAllocatedArgs := &config.NodeResourcesFitArgs{
		ScoringStrategy: &config.ScoringStrategy{
			Type:      config.MostAllocated,
			Resources: []config.ResourceSpec{{Name: "cpu", Weight: 1}, {Name: "memory", Weight: 1}},
		},
	}
	noderesources.NewFit(mostAllocatedArgs, handle, fts)
	p, _ = noderesources.NewFit(mostAllocatedArgs, handle, fts)
	mostAllocation := p.(*noderesources.Fit)

	// 3. NodeResourcesBalancedAllocation
	blArgs := &config.NodeResourcesBalancedAllocationArgs{
		Resources: []config.ResourceSpec{
			{Name: string(corev1.ResourceCPU), Weight: 1},
			{Name: string(corev1.ResourceMemory), Weight: 1},
			{Name: "nvidia.com/gpu", Weight: 1},
		},
	}
	p, _ = noderesources.NewBalancedAllocation(blArgs, handle, fts)
	balancedAllocation := p.(*noderesources.BalancedAllocation)

	// 4. NodeAffinity
	naArgs := &config.NodeAffinityArgs{
		AddedAffinity: &corev1.NodeAffinity{},
	}
	p, _ = nodeaffinity.New(naArgs, handle)
	nodeAffinity := p.(*nodeaffinity.NodeAffinity)

	// 5. ImageLocality
	p, _ = imagelocality.New(nil, handle)
	imageLocality := p.(*imagelocality.ImageLocality)

	nodeOrderFn := func(task *apis.TaskInfo, node *apis.NodeInfo) (float64, error) {
		var nodeScore = 0.0

		state := k8sframework.NewCycleState()
		if weight.imageLocalityWeight != 0 {
			score, status := imageLocality.Score(context.TODO(), state, task.Pod, node.Name)
			if !status.IsSuccess() {
				klog.Warningf("Image Locality Priority Failed because of Error: %v", status.AsError())
				return 0, status.AsError()
			}

			// If imageLocalityWeight is provided, host.Score is multiplied with weight, if not, host.Score is added to total score.
			nodeScore += float64(score) * float64(weight.imageLocalityWeight)
		}

		// NodeResourcesLeastAllocated
		if weight.leastReqWeight != 0 {
			score, status := leastAllocated.Score(context.TODO(), state, task.Pod, node.Name)
			if !status.IsSuccess() {
				klog.Warningf("Least Allocated Priority Failed because of Error: %v", status.AsError())
				return 0, status.AsError()
			}

			// If leastReqWeight is provided, host.Score is multiplied with weight, if not, host.Score is added to total score.
			nodeScore += float64(score) * float64(weight.leastReqWeight)
		}

		// NodeResourcesMostAllocated
		if weight.mostReqWeight != 0 {
			score, status := mostAllocation.Score(context.TODO(), state, task.Pod, node.Name)
			if !status.IsSuccess() {
				klog.Warningf("Most Allocated Priority Failed because of Error: %v", status.AsError())
				return 0, status.AsError()
			}

			// If mostRequestedWeight is provided, host.Score is multiplied with weight, it's 0 by default
			nodeScore += float64(score) * float64(weight.mostReqWeight)
		}

		// NodeResourcesBalancedAllocation
		if weight.balancedResourceWeight != 0 {
			score, status := balancedAllocation.Score(context.TODO(), state, task.Pod, node.Name)
			if !status.IsSuccess() {
				klog.Warningf("Balanced Resource Allocation Priority Failed because of Error: %v", status.AsError())
				return 0, status.AsError()
			}

			// If balancedResourceWeight is provided, host.Score is multiplied with weight, if not, host.Score is added to total score.
			nodeScore += float64(score) * float64(weight.balancedResourceWeight)
		}

		// NodeAffinity
		if weight.nodeAffinityWeight != 0 {
			score, status := nodeAffinity.Score(context.TODO(), state, task.Pod, node.Name)
			if !status.IsSuccess() {
				klog.Warningf("Calculate Node Affinity Priority Failed because of Error: %v", status.AsError())
				return 0, status.AsError()
			}

			// TODO: should we normalize the score
			// If nodeAffinityWeight is provided, host.Score is multiplied with weight, if not, host.Score is added to total score.
			nodeScore += float64(score) * float64(weight.nodeAffinityWeight)
		}

		klog.V(4).Infof("Total Score for task %s/%s on node %s is: %f", task.Namespace, task.Name, node.Name, nodeScore)
		return nodeScore, nil
	}
	sess.AddNodeOrderFn(nop.Name(), nodeOrderFn)

	plArgs := &config.InterPodAffinityArgs{}
	p, _ = interpodaffinity.New(plArgs, handle, feature.Features{})
	interPodAffinity := p.(*interpodaffinity.InterPodAffinity)

	p, _ = tainttoleration.New(nil, handle)
	taintToleration := p.(*tainttoleration.TaintToleration)

	batchNodeOrderFn := func(task *apis.TaskInfo, nodeInfos []*apis.NodeInfo) (map[string]float64, error) {
		// InterPodAffinity
		state := k8sframework.NewCycleState()
		nodes := make([]*corev1.Node, 0, len(nodeInfos))
		for _, node := range nodeInfos {
			nodes = append(nodes, node.Node)
		}
		nodeScores := make(map[string]float64, len(nodes))

		// calculate two scores
		podAffinityScores, podErr := interPodAffinityScore(interPodAffinity, state, task.Pod, nodes, weight.podAffinityWeight)
		if podErr != nil {
			return nil, podErr
		}

		nodeTolerationScores, err := taintTolerationScore(taintToleration, state, task.Pod, nodes, weight.taintTolerationWeight)
		if err != nil {
			return nil, err
		}

		for _, node := range nodes {
			nodeScores[node.Name] = podAffinityScores[node.Name] + nodeTolerationScores[node.Name]
		}

		klog.V(4).Infof("Batch Total Score for task %s/%s is: %v", task.Namespace, task.Name, nodeScores)
		return nodeScores, nil
	}
	sess.AddBatchNodeOrderFn(nop.Name(), batchNodeOrderFn)
}

func (nop *nodeOrderPlugin) OnSessionClose(sess *framework.Session) {}

func interPodAffinityScore(interPodAffinity *interpodaffinity.InterPodAffinity, state *k8sframework.CycleState,
	pod *corev1.Pod, nodes []*corev1.Node, podAffinityWeight int) (map[string]float64, error) {

	preScoreStatus := interPodAffinity.PreScore(context.TODO(), state, pod, nodes)
	if !preScoreStatus.IsSuccess() {
		return nil, preScoreStatus.AsError()
	}

	nodeScoreList := make(k8sframework.NodeScoreList, len(nodes))
	// the default parallelization worker number is 16.
	// the whole scoring will fail if one of the processes failed.
	// so just create a parallelizeContext to control the whole ParallelizeUntil process.
	// if the parallelizeCancel is invoked, the whole "ParallelizeUntil" goes to the end.
	// this could avoid extra computation, especially in huge cluster.
	// and the ParallelizeUntil guarantees only "workerNum" goroutines will be working simultaneously.
	// so it's enough to allocate workerNum size for errCh.
	// note that, in such case, size of errCh should be no less than parallelization number
	workerNum := 16
	errCh := make(chan error, workerNum)
	parallelizeCtx, parallelizeCancel := context.WithCancel(context.TODO())
	workqueue.ParallelizeUntil(parallelizeCtx, workerNum, len(nodes), func(index int) {
		nodeName := nodes[index].Name
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s, status := interPodAffinity.Score(ctx, state, pod, nodeName)
		if !status.IsSuccess() {
			parallelizeCancel()
			errCh <- fmt.Errorf("calculate inter pod affinity priority failed %v", status.Message())
			return
		}
		nodeScoreList[index] = k8sframework.NodeScore{
			Name:  nodeName,
			Score: s,
		}
	})

	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	interPodAffinity.NormalizeScore(context.TODO(), state, pod, nodeScoreList)
	nodeScores := make(map[string]float64, len(nodes))
	for i, nodeScore := range nodeScoreList {
		if nodeScore.Score > k8sframework.MaxNodeScore || nodeScore.Score < k8sframework.MinNodeScore {
			return nil, fmt.Errorf("inter pod affinity returns an invalid score %v for node %s", nodeScore.Score, nodeScore.Name)
		}
		nodeScore.Score *= int64(podAffinityWeight)
		nodeScoreList[i] = nodeScore
		nodeScores[nodeScore.Name] = float64(nodeScore.Score)
	}

	klog.V(4).Infof("inter pod affinity Score for task %s/%s is: %v", pod.Namespace, pod.Name, nodeScores)
	return nodeScores, nil
}

func taintTolerationScore(taintToleration *tainttoleration.TaintToleration, state *k8sframework.CycleState,
	pod *corev1.Pod, nodes []*corev1.Node, taintTolerationWeight int) (map[string]float64, error) {

	preScoreStatus := taintToleration.PreScore(context.TODO(), state, pod, nodes)
	if !preScoreStatus.IsSuccess() {
		return nil, preScoreStatus.AsError()
	}

	nodeScoreList := make(k8sframework.NodeScoreList, len(nodes))
	workerNum := 16
	errCh := make(chan error, workerNum)
	parallelizeCtx, parallelizeCancel := context.WithCancel(context.TODO())
	workqueue.ParallelizeUntil(parallelizeCtx, workerNum, len(nodes), func(index int) {
		nodeName := nodes[index].Name
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s, status := taintToleration.Score(ctx, state, pod, nodeName)
		if !status.IsSuccess() {
			parallelizeCancel()
			errCh <- fmt.Errorf("calculate taint toleration priority failed %v", status.Message())
			return
		}
		nodeScoreList[index] = k8sframework.NodeScore{
			Name:  nodeName,
			Score: s,
		}
	})

	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	taintToleration.NormalizeScore(context.TODO(), state, pod, nodeScoreList)

	nodeScores := make(map[string]float64, len(nodes))
	for i, nodeScore := range nodeScoreList {
		if nodeScore.Score > k8sframework.MaxNodeScore || nodeScore.Score < k8sframework.MinNodeScore {
			return nil, fmt.Errorf("taint toleration returns an invalid score %v for node %s", nodeScore.Score, nodeScore.Name)
		}
		nodeScore.Score *= int64(taintTolerationWeight)
		nodeScoreList[i] = nodeScore
		nodeScores[nodeScore.Name] = float64(nodeScore.Score)
	}

	klog.V(4).Infof("taint toleration Score for task %s/%s is: %v", pod.Namespace, pod.Name, nodeScores)
	return nodeScores, nil
}
