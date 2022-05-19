/*
Copyright 2021-2022 hliangzhao.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by anplicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package numaaware

import (
	`context`
	`fmt`
	nodeinfov1alpha1 `github.com/hliangzhao/volcano/pkg/apis/nodeinfo/v1alpha1`
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	`github.com/hliangzhao/volcano/pkg/scheduler/framework`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/numaaware/policy`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/numaaware/provider/cpumanager`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils`
	corev1 `k8s.io/api/core/v1`
	`k8s.io/client-go/util/workqueue`
	`k8s.io/klog/v2`
	`k8s.io/kubernetes/pkg/apis/core/v1/helper/qos`
	`k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology`
	`k8s.io/kubernetes/pkg/kubelet/cm/cpuset`
	`k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask`
	`sync`
)

const (
	PluginName     = "numa-aware"
	NumaTopoWeight = "weight"
)

type numaPlugin struct {
	sync.Mutex
	args            framework.Arguments
	hintProviders   []policy.HintProvider
	assignRes       map[apis.TaskID]map[string]apis.ResNumaSets // map[taskUID]map[nodeName][resourceName]cpuset.CPUSet
	nodeResSets     map[string]apis.ResNumaSets                 // map[nodeName][resourceName]cpuset.CPUSet
	taskBindNodeMap map[apis.TaskID]string
}

func New(args framework.Arguments) framework.Plugin {
	plugin := &numaPlugin{
		args:            args,
		assignRes:       make(map[apis.TaskID]map[string]apis.ResNumaSets),
		taskBindNodeMap: make(map[apis.TaskID]string),
	}

	plugin.hintProviders = append(plugin.hintProviders, cpumanager.NewProvider())
	return plugin
}

func (np *numaPlugin) Name() string {
	return PluginName
}

func (np *numaPlugin) OnSessionOpen(sess *framework.Session) {
	weight := calculateWeight(np.args)
	numaNodes := apis.GenerateNumaNodes(sess.Nodes)
	np.nodeResSets = apis.GenerateNodeResNumaSets(sess.Nodes)

	sess.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(event *framework.Event) {
			node := np.nodeResSets[event.Task.NodeName]
			if _, ok := np.assignRes[event.Task.UID]; !ok {
				return
			}

			resNumaSets, ok := np.assignRes[event.Task.UID][event.Task.NodeName]
			if !ok {
				return
			}

			node.Allocate(resNumaSets)
			np.taskBindNodeMap[event.Task.UID] = event.Task.NodeName
		},
		DeallocateFunc: func(event *framework.Event) {
			node := np.nodeResSets[event.Task.NodeName]
			if _, ok := np.assignRes[event.Task.UID]; !ok {
				return
			}

			resNumaSets, ok := np.assignRes[event.Task.UID][event.Task.NodeName]
			if !ok {
				return
			}

			delete(np.taskBindNodeMap, event.Task.UID)
			node.Release(resNumaSets)
		},
	})

	predicateFn := func(task *apis.TaskInfo, node *apis.NodeInfo) error {
		if qos.GetPodQOS(task.Pod) != corev1.PodQOSGuaranteed {
			klog.V(3).Infof("task %s isn't Guaranteed pod", task.Name)
			return nil
		}

		if fit, err := filterNodeByPolicy(task, node, np.nodeResSets); !fit {
			return err
		}

		resNumaSets := np.nodeResSets[node.Name].Clone()

		taskPolicy := policy.GetPolicy(node, numaNodes[node.Name])
		allResAssignMap := make(map[string]cpuset.CPUSet)
		for _, container := range task.Pod.Spec.Containers {
			providersHints := policy.AccumulateProvidersHints(&container, node.NumaSchedulerInfo, resNumaSets, np.hintProviders)
			hit, admit := taskPolicy.Predicate(providersHints)
			if !admit {
				return fmt.Errorf("plugin %s predicates failed for task %s container %s on node %s",
					np.Name(), task.Name, container.Name, node.Name)
			}

			klog.V(4).Infof("[numaaware] hits for task %s container '%v': %v on node %s, best hit: %v",
				task.Name, container.Name, providersHints, node.Name, hit)
			resAssignMap := policy.Allocate(&container, &hit, node.NumaSchedulerInfo, resNumaSets, np.hintProviders)
			for resName, assign := range resAssignMap {
				allResAssignMap[resName] = allResAssignMap[resName].Union(assign)
				resNumaSets[resName] = resNumaSets[resName].Difference(assign)
			}
		}

		np.Lock()
		defer np.Unlock()
		if _, ok := np.assignRes[task.UID]; !ok {
			np.assignRes[task.UID] = make(map[string]apis.ResNumaSets)
		}

		np.assignRes[task.UID][node.Name] = allResAssignMap

		klog.V(4).Infof(" task %s's on node<%s> resAssignMap: %v",
			task.Name, node.Name, np.assignRes[task.UID][node.Name])

		return nil
	}

	sess.AddPredicateFn(np.Name(), predicateFn)

	batchNodeOrderFn := func(task *apis.TaskInfo, nodeInfo []*apis.NodeInfo) (map[string]float64, error) {
		nodeScores := make(map[string]float64, len(nodeInfo))
		if task.NumaInfo == nil || task.NumaInfo.Policy == "" || task.NumaInfo.Policy == "none" {
			return nodeScores, nil
		}

		if _, found := np.assignRes[task.UID]; !found {
			return nodeScores, nil
		}

		scoreList := getNodeNumaNumForTask(nodeInfo, np.assignRes[task.UID])
		utils.NormalizeScore(apis.DefaultMaxNodeScore, true, scoreList)

		for idx, scoreNode := range scoreList {
			scoreNode.Score *= int64(weight)
			nodeName := nodeInfo[idx].Name
			nodeScores[nodeName] = float64(scoreNode.Score)
		}

		klog.V(4).Infof("numa-aware plugin Score for task %s/%s is: %v",
			task.Namespace, task.Name, nodeScores)
		return nodeScores, nil
	}

	sess.AddBatchNodeOrderFn(np.Name(), batchNodeOrderFn)
}

func (np *numaPlugin) OnSessionClose(sess *framework.Session) {
	if len(np.taskBindNodeMap) == 0 {
		return
	}

	allocatedResSet := make(map[string]apis.ResNumaSets)
	for taskID, nodeName := range np.taskBindNodeMap {
		if _, existed := np.assignRes[taskID]; !existed {
			continue
		}

		if _, existed := np.assignRes[taskID][nodeName]; !existed {
			continue
		}

		if _, existed := allocatedResSet[nodeName]; !existed {
			allocatedResSet[nodeName] = make(apis.ResNumaSets)
		}

		resSet := np.assignRes[taskID][nodeName]
		for resName, set := range resSet {
			if _, existed := allocatedResSet[nodeName][resName]; !existed {
				allocatedResSet[nodeName][resName] = cpuset.NewCPUSet()
			}

			allocatedResSet[nodeName][resName] = allocatedResSet[nodeName][resName].Union(set)
		}
	}

	klog.V(4).Infof("[numaPlugin] allocatedResSet: %v", allocatedResSet)
	sess.UpdateSchedulerNumaInfo(allocatedResSet)
}

func calculateWeight(args framework.Arguments) int {
	weight := 1
	args.GetInt(&weight, NumaTopoWeight)
	return weight
}

func filterNodeByPolicy(task *apis.TaskInfo, node *apis.NodeInfo, nodeResSets map[string]apis.ResNumaSets) (fit bool, err error) {
	if !(task.NumaInfo == nil || task.NumaInfo.Policy == "" || task.NumaInfo.Policy == "none") {
		if node.NumaSchedulerInfo == nil {
			return false, fmt.Errorf("numa info is empty")
		}

		if node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.CPUManagerPolicy] != "static" {
			return false, fmt.Errorf("cpu manager policy isn't static")
		}

		if task.NumaInfo.Policy != node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.TopologyManagerPolicy] {
			return false, fmt.Errorf("task topology polocy[%s] is different with node[%s]",
				task.NumaInfo.Policy, node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.TopologyManagerPolicy])
		}

		if _, ok := nodeResSets[node.Name]; !ok {
			return false, fmt.Errorf("no topo information")
		}

		if nodeResSets[node.Name][string(corev1.ResourceCPU)].Size() == 0 {
			return false, fmt.Errorf("cpu allocatable map is empty")
		}
	} else {
		if node.NumaSchedulerInfo == nil {
			return false, nil
		}

		if node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.CPUManagerPolicy] != "static" {
			return false, nil
		}

		if (node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.TopologyManagerPolicy] == "none") ||
			(node.NumaSchedulerInfo.Policies[nodeinfov1alpha1.TopologyManagerPolicy] == "") {
			return false, nil
		}
	}

	return true, nil
}

func getNodeNumaNumForTask(nodeInfo []*apis.NodeInfo, resAssignMap map[string]apis.ResNumaSets) []apis.ScoredNode {
	nodeNumaCounts := make([]apis.ScoredNode, len(nodeInfo))
	workqueue.ParallelizeUntil(context.TODO(), 16, len(nodeInfo), func(index int) {
		node := nodeInfo[index]
		assignCpus := resAssignMap[node.Name][string(corev1.ResourceCPU)]
		nodeNumaCounts[index] = apis.ScoredNode{
			NodeName: node.Name,
			Score:    int64(getNumaNodeCntForCPUID(assignCpus, node.NumaSchedulerInfo.CPUDetail)),
		}
	})

	return nodeNumaCounts
}

func getNumaNodeCntForCPUID(cpus cpuset.CPUSet, cpuDetails topology.CPUDetails) int {
	mask, _ := bitmask.NewBitMask()
	s := cpus.ToSlice()

	for _, cpuID := range s {
		_ = mask.Add(cpuDetails[cpuID].NUMANodeID)
	}

	return mask.Count()
}
