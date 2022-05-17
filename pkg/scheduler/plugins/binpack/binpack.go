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

package binpack

import (
	"fmt"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"strings"
)

const (
	PluginName = "binpack"

	// PluginWeight is the key for providing Binpack Priority Weight in YAML
	PluginWeight = "binpack.weight"

	// CPUWeight is the key for weight of cpu
	CPUWeight = "binpack.cpu"

	// MemoryWeight is the key for weight of memory
	MemoryWeight = "binpack.memory"

	// AdditionalResources is the key for additional resource key name
	AdditionalResources = "binpack.resources"

	// AdditionalResourcesPrefix is the key prefix for additional resource key name
	AdditionalResourcesPrefix = AdditionalResources + "."

	resourceFmt = "%s[%d]"
)

type priorityWeight struct {
	BinPackingWeight    int
	BinPackingCPU       int
	BinPackingMem       int
	BinPackingResources map[corev1.ResourceName]int
}

func (w *priorityWeight) String() string {
	length := 3
	if extendLength := len(w.BinPackingResources); extendLength == 0 {
		length++
	} else {
		length += extendLength
	}

	msg := make([]string, 0, length)
	msg = append(msg,
		fmt.Sprintf(resourceFmt, PluginWeight, w.BinPackingWeight),
		fmt.Sprintf(resourceFmt, CPUWeight, w.BinPackingCPU),
		fmt.Sprintf(resourceFmt, MemoryWeight, w.BinPackingMem),
	)

	if len(w.BinPackingResources) == 0 {
		msg = append(msg, "no extend resources.")
	} else {
		for name, weight := range w.BinPackingResources {
			msg = append(msg, fmt.Sprintf(resourceFmt, name, weight))
		}
	}
	return strings.Join(msg, ", ")
}

type binpackPlugin struct {
	weight priorityWeight
}

func New(args framework.Arguments) framework.Plugin {
	return &binpackPlugin{
		weight: calculateWeight(args),
	}
}

func (bp *binpackPlugin) Name() string {
	return PluginName
}

func calculateWeight(args framework.Arguments) priorityWeight {
	/*
	   User Should give priorityWeight in this format (binpack.weight, binpack.cpu, binpack.memory).
	   Support change the weight about cpu, memory and additional resource by arguments.

	   actions: "enqueue, reclaim, allocate, backfill, preempt"
	   tiers:
	   - plugins:
	     - name: binpack
	       arguments:
	         binpack.weight: 10
	         binpack.cpu: 5
	         binpack.memory: 1
	         binpack.resources: nvidia.com/gpu, example.com/foo
	         binpack.resources.nvidia.com/gpu: 2
	         binpack.resources.example.com/foo: 3
	*/
	// Values are initialized to 1.
	weight := priorityWeight{
		BinPackingWeight:    1,
		BinPackingCPU:       1,
		BinPackingMem:       1,
		BinPackingResources: map[corev1.ResourceName]int{},
	}
	// Checks whether binpack.weight is provided or not, if given, modifies the value in weight struct.
	args.GetInt(&weight.BinPackingWeight, PluginWeight)
	// Checks whether binpack.cpu is provided or not, if given, modifies the value in weight struct.
	args.GetInt(&weight.BinPackingCPU, CPUWeight)
	if weight.BinPackingCPU < 0 {
		weight.BinPackingCPU = 1
	}
	// Checks whether binpack.memory is provided or not, if given, modifies the value in weight struct.
	args.GetInt(&weight.BinPackingMem, MemoryWeight)
	if weight.BinPackingMem < 0 {
		weight.BinPackingMem = 1
	}

	resStr, ok := args[AdditionalResources].(string)
	if !ok {
		resStr = ""
	}
	resources := strings.Split(resStr, ",")
	for _, res := range resources {
		res = strings.TrimSpace(res)
		if res == "" {
			continue
		}

		resKey := AdditionalResourcesPrefix + res
		resWeight := 1
		args.GetInt(&resWeight, resKey)
		if resWeight < 0 {
			resWeight = 1
		}
		weight.BinPackingResources[corev1.ResourceName(res)] = resWeight
	}

	return weight
}

func (bp *binpackPlugin) OnSessionOpen(sess *framework.Session) {
	klog.V(4).Infof("Enter binpack plugin ...")
	if klog.V(4).Enabled() {
		defer func() {
			klog.V(4).Infof("Leaving binpack plugin. %s ...", bp.weight.String())
		}()

		// TODO: notFoundRes seems not used
		var notFoundRes []string
		for res := range bp.weight.BinPackingResources {
			found := false
			for _, nodeInfo := range sess.Nodes {
				if nodeInfo.Allocatable.Get(res) > 0 {
					found = true
					break
				}
			}
			if !found {
				notFoundRes = append(notFoundRes, string(res))
			}
		}
		klog.V(4).Infof("resources [%s] record in weight but not found on any node", strings.Join(notFoundRes, ", "))
	}

	nodeOrderFn := func(task *apis.TaskInfo, node *apis.NodeInfo) (float64, error) {
		binPackingScore := BinPackingScore(task, node, bp.weight)
		klog.V(4).Infof("Binpack score for Task %s/%s on node %s is: %v", task.Namespace, task.Name, node.Name, binPackingScore)
		return binPackingScore, nil
	}
	if bp.weight.BinPackingWeight != 0 {
		sess.AddNodeOrderFn(bp.Name(), nodeOrderFn)
	} else {
		klog.Infof("binpack weight is zero, skip node order function")
	}
}

func (bp *binpackPlugin) OnSessionClose(sess *framework.Session) {}

// BinPackingScore uses the best fit polices during scheduling.
// Goals:
// - Schedule Jobs using BestFit Policy using Resource Bin Packing Priority Function;
// - Reduce Fragmentation of scarce resources on the Cluster.
func BinPackingScore(task *apis.TaskInfo, node *apis.NodeInfo, weight priorityWeight) float64 {
	score := 0.0
	weightSum := 0

	// task requested
	requested := task.ResReq

	// node used and allocatable
	allocatable := node.Allocatable
	used := node.Used

	for _, res := range requested.ResourceNames() {
		req := requested.Get(res)
		if req == 0 {
			continue
		}
		allocate := allocatable.Get(res)
		nodeUsed := used.Get(res)

		resWeight := 0
		found := false
		switch res {
		case corev1.ResourceCPU:
			resWeight = weight.BinPackingCPU
			found = true
		case corev1.ResourceMemory:
			resWeight = weight.BinPackingMem
			found = true
		default:
			resWeight, found = weight.BinPackingResources[res]
		}
		if !found {
			continue
		}

		resScore := ResourceBinPackingScore(req, allocate, nodeUsed, resWeight)
		klog.V(5).Infof("task %s/%s on node %s resource %s, need %f, used %f, allocatable %f, weight %d, score %f",
			task.Namespace, task.Name, node.Name, res, req, nodeUsed, allocate, resWeight, resScore)

		score += resScore
		weightSum += resWeight
	}

	// mapping the result from [0, weightSum] to [0, 10(MaxPriority)]
	if weightSum > 0 {
		score /= float64(weightSum)
	}
	score *= float64(k8sframework.MaxNodeScore * int64(weight.BinPackingWeight))
	return score
}

// ResourceBinPackingScore calculate the binpack score for resource with provided info.
func ResourceBinPackingScore(requested, capacity, used float64, weight int) float64 {
	if capacity == 0 || weight == 0 {
		return 0
	}
	finallyUsed := requested + used
	if finallyUsed > capacity {
		return 0
	}
	return finallyUsed * float64(weight) / capacity
}
