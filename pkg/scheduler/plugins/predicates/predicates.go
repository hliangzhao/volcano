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

import (
	"context"
	"fmt"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils/k8s"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeports"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeunschedulable"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
	"strings"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName = "predicates"

	// GPUSharingPredicate is the key for enabling GPU Sharing Predicate in YAML
	GPUSharingPredicate = "predicate.GPUSharingEnable"

	// CachePredicate control cache predicate feature
	CachePredicate = "predicate.CacheEnable"

	// ProportionalPredicate is the key for enabling Proportional Predicate in YAML
	ProportionalPredicate = "predicate.ProportionalEnable"

	// ProportionalResource is the key for additional resource key name
	ProportionalResource = "predicate.resources"

	// ProportionalResourcesPrefix is the key prefix for additional resource key name
	ProportionalResourcesPrefix = ProportionalResource + "."
)

type predicatePlugin struct {
	arguments framework.Arguments
}

func New(arguments framework.Arguments) framework.Plugin {
	return &predicatePlugin{arguments: arguments}
}

func (pp *predicatePlugin) Name() string {
	return PluginName
}

func (pp *predicatePlugin) OnSessionOpen(sess *framework.Session) {
	pl := utils.NewPodListerFromNode(sess)
	nodeMap := utils.GenerateNodeMapAndSlice(sess.Nodes)

	pCache := newPredicateCache()
	predicates := enablePredicate(pp.arguments)

	kubeClient := sess.KubeClient()
	sess.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(e *framework.Event) {
			pod := pl.UpdateTask(e.Task, e.Task.NodeName)

			nodeName := e.Task.NodeName
			node, found := nodeMap[nodeName]
			if !found {
				klog.Errorf("predicates, update pod %s/%s allocate to NOT EXIST node [%s]", pod.Namespace, pod.Name, nodeName)
				return
			}

			// Add GPU path
			if predicates.gpuSharingEnable && apis.GetGPUResourceOfPod(pod) > 0 {
				nodeInfo, ok := sess.Nodes[nodeName]
				if !ok {
					klog.Errorf("Failed to get node %s info from cache", nodeName)
					return
				}
				gpuId := predicateGPU(pod, nodeInfo)
				if gpuId < 0 {
					klog.Errorf("The node %s can't place the pod %s in namespace %s",
						pod.Spec.NodeName, pod.Name, pod.Namespace)
					return
				}
				dev, ok := nodeInfo.GPUDevices[gpuId]
				if !ok {
					klog.Errorf("Failed to get GPU %d from node %s", gpuId, nodeName)
					return
				}
				patch := apis.AddGPUIndexPatch(gpuId)
				pod, err := kubeClient.CoreV1().Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
				if err != nil {
					klog.Errorf("Patch pod %s failed with patch %s: %v", pod.Name, patch, err)
					return
				}
				dev.PodMap[string(pod.UID)] = pod
				klog.V(4).Infof("predicates with gpu sharing, update pod %s/%s allocate to node [%s]", pod.Namespace, pod.Name, nodeName)
			}

			node.AddPod(pod)
			klog.V(4).Infof("predicates, update pod %s/%s allocate to node [%s]", pod.Namespace, pod.Name, nodeName)
		},
		DeallocateFunc: func(e *framework.Event) {
			pod := pl.UpdateTask(e.Task, "")

			nodeName := e.Task.NodeName
			node, found := nodeMap[nodeName]
			if !found {
				klog.Errorf("predicates, update pod %s/%s allocate from NOT EXIST node [%s]", pod.Namespace, pod.Name, nodeName)
				return
			}

			// Remove GPU patch
			if predicates.gpuSharingEnable && apis.GetGPUResourceOfPod(pod) > 0 {
				gpuId := apis.GetGPUIndex(pod)
				patch := apis.RemoveGPUIndexPatch()
				_, err := kubeClient.CoreV1().Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
				if err != nil {
					klog.Errorf("Patch pod %s failed with patch %s: %v", pod.Name, patch, err)
					return
				}
				nodeInfo, ok := sess.Nodes[nodeName]
				if !ok {
					klog.Errorf("Failed to get node %s info from cache", nodeName)
					return
				}
				if dev, ok := nodeInfo.GPUDevices[gpuId]; ok {
					delete(dev.PodMap, string(pod.UID))
				}
				klog.V(4).Infof("predicates with gpu sharing, update pod %s/%s deallocate from node [%s]", pod.Namespace, pod.Name, nodeName)
			}

			err := node.RemovePod(pod)
			if err != nil {
				klog.Errorf("predicates, remove pod %s/%s from node [%s] error: %v", pod.Namespace, pod.Name, nodeName, err)
				return
			}
			klog.V(4).Infof("predicates, update pod %s/%s deallocate from node [%s]", pod.Namespace, pod.Name, nodeName)
		},
	})

	// Initialize k8s plugins
	// TODO: Add more predicates, k8s.io/kubernetes/pkg/scheduler/framework/plugins/legacy_registry.go
	handle := k8s.NewFrameworkHandle(nodeMap, sess.KubeClient(), sess.InformerFactory())

	// 1. NodeUnschedulable
	plugin, _ := nodeunschedulable.New(nil, handle)
	nodeUnschedulableFilter := plugin.(*nodeunschedulable.NodeUnschedulable)

	// 2. NodeAffinity
	nodeAffinityArgs := config.NodeAffinityArgs{
		AddedAffinity: &corev1.NodeAffinity{},
	}
	plugin, _ = nodeaffinity.New(&nodeAffinityArgs, handle)
	nodeAffinityFilter := plugin.(*nodeaffinity.NodeAffinity)

	// 3. NodePorts
	plugin, _ = nodeports.New(nil, handle)
	nodePortFilter := plugin.(*nodeports.NodePorts)

	// 4. TaintToleration
	plugin, _ = tainttoleration.New(nil, handle)
	tolerationFilter := plugin.(*tainttoleration.TaintToleration)

	// 5. InterPodAffinity
	interPodAffinityArgs := config.InterPodAffinityArgs{}
	fts := feature.Features{}
	plugin, _ = interpodaffinity.New(&interPodAffinityArgs, handle, fts)
	podAffinityFilter := plugin.(*interpodaffinity.InterPodAffinity)

	sess.AddPredicateFn(pp.Name(), func(taskInfo *apis.TaskInfo, node *apis.NodeInfo) error {
		nodeInfo, found := nodeMap[node.Name]
		if !found {
			return fmt.Errorf("failed to predicates, node info for %s not found", node.Name)
		}

		if node.Allocatable.MaxTaskNum <= len(nodeInfo.Pods) {
			klog.V(4).Infof("NodePodNumber predicates Task <%s/%s> on Node <%s> failed",
				taskInfo.Namespace, taskInfo.Name, node.Name)
			return apis.NewFitError(taskInfo, node, apis.NodePodNumberExceeded)
		}

		state := k8sframework.NewCycleState()
		predicateByStableFilter := func(pod *corev1.Pod, nodeInfo *k8sframework.NodeInfo) (bool, error) {
			// CheckNodeUnschedulable
			status := nodeUnschedulableFilter.Filter(context.TODO(), state, taskInfo.Pod, nodeInfo)
			if !status.IsSuccess() {
				return false, fmt.Errorf("plugin %s predicates failed %s", nodeunschedulable.Name, status.Message())
			}

			// Check NodeAffinity
			status = nodeAffinityFilter.Filter(context.TODO(), state, taskInfo.Pod, nodeInfo)
			if !status.IsSuccess() {
				return false, fmt.Errorf("plugin %s predicates failed %s", nodeaffinity.Name, status.Message())
			}

			// PodToleratesNodeTaints: TaintToleration
			status = tolerationFilter.Filter(context.TODO(), state, taskInfo.Pod, nodeInfo)
			if !status.IsSuccess() {
				return false, fmt.Errorf("plugin %s predicates failed %s", tainttoleration.Name, status.Message())
			}

			return true, nil
		}

		// Check PredicateWithCache
		{
			var err error
			var fit bool
			if predicates.cacheEnable {
				fit, err = pCache.PredicateWithCache(node.Name, taskInfo.Pod)
				if err != nil {
					fit, err = predicateByStableFilter(taskInfo.Pod, nodeInfo)
					pCache.UpdateCache(node.Name, taskInfo.Pod, fit)
				} else {
					if !fit {
						err = fmt.Errorf("plugin equivalence cache predicates failed")
					}
				}
			} else {
				fit, err = predicateByStableFilter(taskInfo.Pod, nodeInfo)
			}

			if !fit {
				return err
			}
		}

		// Check NodePorts
		nodePortFilter.PreFilter(context.TODO(), state, taskInfo.Pod)
		status := nodePortFilter.Filter(context.TODO(), state, nil, nodeInfo)
		if !status.IsSuccess() {
			return fmt.Errorf("plugin %s predicates failed %s", nodeaffinity.Name, status.Message())
		}

		// InterPodAffinity Predicate
		status = podAffinityFilter.PreFilter(context.TODO(), state, taskInfo.Pod)
		if !status.IsSuccess() {
			return fmt.Errorf("plugin %s pre-predicates failed %s", interpodaffinity.Name, status.Message())
		}

		status = podAffinityFilter.Filter(context.TODO(), state, taskInfo.Pod, nodeInfo)
		if !status.IsSuccess() {
			return fmt.Errorf("plugin %s predicates failed %s", interpodaffinity.Name, status.Message())
		}

		if predicates.gpuSharingEnable {
			// CheckGPUSharingPredicate
			fit, err := checkNodeGPUSharingPredicate(taskInfo.Pod, node)
			if err != nil {
				return err
			}

			klog.V(4).Infof("checkNodeGPUSharingPredicate predicates Task <%s/%s> on Node <%s>: fit %v",
				taskInfo.Namespace, taskInfo.Name, node.Name, fit)
		}
		if predicates.proportionalEnable {
			// Check ProportionalPredicate
			fit, err := checkNodeResourceIsProportional(taskInfo, node, predicates.proportional)
			if err != nil {
				return err
			}
			klog.V(4).Infof("checkNodeResourceIsProportional predicates Task <%s/%s> on Node <%s>: fit %v",
				taskInfo.Namespace, taskInfo.Name, node.Name, fit)
		}
		return nil
	})
}

func (pp *predicatePlugin) OnSessionClose(sess *framework.Session) {}

type baseResource struct {
	CPU    float64
	Memory float64
}

type predicateEnable struct {
	gpuSharingEnable   bool
	cacheEnable        bool
	proportionalEnable bool
	proportional       map[corev1.ResourceName]baseResource
}

/*
	User Should give predicatesEnable in this format: predicate.GPUSharingEnable.
	Currently, we only support GPUSharing predicate check.

	Example:
	actions: "reclaim, allocate, backfill, preempt"
	tiers:
	- plugins:
	 - name: priority
	 - name: gang
	 - name: conformance
	- plugins:
	 - name: drf
	 - name: predicates
	   arguments:
	     predicate.GPUSharingEnable: true
	     predicate.CacheEnable: true
	     predicate.ProportionalEnable: true
	     predicate.resources: nvidia.com/gpu
	     predicate.resources.nvidia.com/gpu.cpu: 4
	     predicate.resources.nvidia.com/gpu.memory: 8
	 - name: proportion
	 - name: nodeorder
*/

func enablePredicate(arguments framework.Arguments) predicateEnable {
	predicate := predicateEnable{
		gpuSharingEnable:   false,
		cacheEnable:        false,
		proportionalEnable: false,
	}

	arguments.GetBool(&predicate.gpuSharingEnable, GPUSharingPredicate)
	arguments.GetBool(&predicate.cacheEnable, CachePredicate)
	arguments.GetBool(&predicate.proportionalEnable, ProportionalPredicate)
	resProportional := make(map[corev1.ResourceName]baseResource)
	resStr, ok := arguments[ProportionalResource].(string)
	if !ok {
		resStr = ""
	}
	resources := strings.Split(resStr, ",")
	for _, res := range resources {
		res = strings.TrimSpace(res)
		if res == "" {
			continue
		}

		cpuResKey := ProportionalResourcesPrefix + res + ".cpu" // "predicate.resources.nvidia.com/gpu.cpu"
		cpuResRate := 1.0
		arguments.GetFloat64(&cpuResRate, cpuResKey)
		if cpuResRate < 0 {
			cpuResRate = 1.0
		}

		memResKey := ProportionalResourcesPrefix + res + ".memory" // "predicate.resources.nvidia.com/gpu.memory"
		memResRate := 1.0
		arguments.GetFloat64(&memResRate, memResKey)
		if memResRate < 0 {
			memResRate = 1.0
		}

		r := baseResource{
			CPU:    cpuResRate,
			Memory: memResRate,
		}
		resProportional[corev1.ResourceName(res)] = r
	}
	predicate.proportional = resProportional
	return predicate
}
