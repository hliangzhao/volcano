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

package cpumanager

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/numaaware/policy"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpuset"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"math"
)

type cpuManage struct {
}

func NewProvider() policy.HintProvider {
	return &cpuManage{}
}

func (mng *cpuManage) Name() string {
	return "cpuManage"
}

func guaranteedCPUs(container *corev1.Container) int {
	cpuQuantity := container.Resources.Requests[corev1.ResourceCPU]
	if cpuQuantity.Value()*1000 != cpuQuantity.MilliValue() {
		return 0
	}

	return int(cpuQuantity.Value())
}

// generateCPUTopologyHints return the numa topology hints based on
// - availableCPUs
func generateCPUTopologyHints(availableCPUs cpuset.CPUSet, CPUDetails topology.CPUDetails, request int) []policy.TopologyHint {
	minAffinitySize := CPUDetails.NUMANodes().Size()
	var hints []policy.TopologyHint
	bitmask.IterateBitMasks(CPUDetails.NUMANodes().ToSlice(), func(mask bitmask.BitMask) {
		// First, update minAffinitySize for the current request size.
		cpusInMask := CPUDetails.CPUsInNUMANodes(mask.GetBits()...).Size()
		if cpusInMask >= request && mask.Count() < minAffinitySize {
			minAffinitySize = mask.Count()
		}

		// Then check to see if we have enough CPUs available on the current
		// numa node bitmask to satisfy the CPU request.
		numMatching := 0
		// Finally, check to see if enough available CPUs remain on the current
		// NUMA node combination to satisfy the CPU request.
		for _, c := range availableCPUs.ToSlice() {
			if mask.IsSet(CPUDetails[c].NUMANodeID) {
				numMatching++
			}
		}

		// If they don't, then move onto the next combination.
		if numMatching < request {
			return
		}

		// Otherwise, create a new hint from the numa node bitmask and add it to the
		// list of hints.  We set all hint preferences to 'false' on the first
		// pass through.
		hints = append(hints, policy.TopologyHint{
			NumaNodeAffinity: mask,
			Preferred:        false,
		})
	})

	// Loop back through all hints and update the 'Preferred' field based on
	// counting the number of bits sets in the affinity mask and comparing it
	// to the minAffinitySize. Only those with an equal number of bits set (and
	// with a minimal set of numa nodes) will be considered preferred.
	for i := range hints {
		if hints[i].NumaNodeAffinity.Count() == minAffinitySize {
			hints[i].Preferred = true
		}
	}

	return hints
}

func (mng *cpuManage) GetTopologyHints(container *corev1.Container,
	topoInfo *apis.NumaTopoInfo, resNumaSets apis.ResNumaSets) map[string][]policy.TopologyHint {

	if _, ok := container.Resources.Requests[corev1.ResourceCPU]; !ok {
		klog.Warningf("container %s has no cpu request", container.Name)
		return nil
	}

	requestNum := guaranteedCPUs(container)
	if requestNum == 0 {
		klog.Warningf(" the cpu request isn't  integer in container %s", container.Name)
		return nil
	}

	cputopo := &topology.CPUTopology{
		NumCPUs:    topoInfo.CPUDetail.CPUs().Size(),
		NumCores:   topoInfo.CPUDetail.Cores().Size() * topoInfo.CPUDetail.Sockets().Size(),
		NumSockets: topoInfo.CPUDetail.Sockets().Size(),
		CPUDetails: topoInfo.CPUDetail,
	}

	reserved := cpuset.NewCPUSet()
	reservedCPUs, ok := topoInfo.ResReserved[corev1.ResourceCPU]
	if ok {
		// Take the ceiling of the reservation, since fractional CPUs cannot be
		// exclusively allocated.
		reservedCPUsFloat := float64(reservedCPUs.MilliValue()) / 1000
		numReservedCPUs := int(math.Ceil(reservedCPUsFloat))
		reserved, _ = takeByTopology(cputopo, cputopo.CPUDetails.CPUs(), numReservedCPUs)
		klog.V(4).Infof("[cpumanager] reserve cpuset :%v", reserved)
	}

	availableCPUSet, ok := resNumaSets[string(corev1.ResourceCPU)]
	if !ok {
		klog.Warningf("no cpu resource")
		return nil
	}

	availableCPUSet = availableCPUSet.Difference(reserved)
	klog.V(4).Infof("requested: %d, availableCPUSet: %v", requestNum, availableCPUSet)
	return map[string][]policy.TopologyHint{
		string(corev1.ResourceCPU): generateCPUTopologyHints(availableCPUSet, topoInfo.CPUDetail, requestNum),
	}
}

func (mng *cpuManage) Allocate(container *corev1.Container, bestHit *policy.TopologyHint,
	topoInfo *apis.NumaTopoInfo, resNumaSets apis.ResNumaSets) map[string]cpuset.CPUSet {

	cpuTopo := &topology.CPUTopology{
		NumCPUs:    topoInfo.CPUDetail.CPUs().Size(),
		NumCores:   topoInfo.CPUDetail.Cores().Size() * topoInfo.CPUDetail.Sockets().Size(),
		NumSockets: topoInfo.CPUDetail.Sockets().Size(),
		CPUDetails: topoInfo.CPUDetail,
	}

	reserved := cpuset.NewCPUSet()
	reservedCPUs, ok := topoInfo.ResReserved[corev1.ResourceCPU]
	if ok {
		// Take the ceiling of the reservation, since fractional CPUs cannot be
		// exclusively allocated.
		reservedCPUsFloat := float64(reservedCPUs.MilliValue()) / 1000
		numReservedCPUs := int(math.Ceil(reservedCPUsFloat))
		reserved, _ = takeByTopology(cpuTopo, cpuTopo.CPUDetails.CPUs(), numReservedCPUs)
		klog.V(3).Infof("[cpumanager] reserve cpuset :%v", reserved)
	}

	requestNum := guaranteedCPUs(container)
	availableCPUSet := resNumaSets[string(corev1.ResourceCPU)]
	availableCPUSet = availableCPUSet.Difference(reserved)

	klog.V(4).Infof("alignedCPUs: %v requestNum: %v bestHit %v", availableCPUSet, requestNum, bestHit)

	result := cpuset.NewCPUSet()
	if bestHit.NumaNodeAffinity != nil {
		alignedCPUs := cpuset.NewCPUSet()
		for _, numaNodeID := range bestHit.NumaNodeAffinity.GetBits() {
			alignedCPUs = alignedCPUs.Union(availableCPUSet.Intersection(cpuTopo.CPUDetails.CPUsInNUMANodes(numaNodeID)))
		}

		numAlignedToAlloc := alignedCPUs.Size()
		if requestNum < numAlignedToAlloc {
			numAlignedToAlloc = requestNum
		}

		alignedCPUs, err := takeByTopology(cpuTopo, alignedCPUs, numAlignedToAlloc)
		if err != nil {
			return map[string]cpuset.CPUSet{
				string(corev1.ResourceCPU): cpuset.NewCPUSet(),
			}
		}

		result = result.Union(alignedCPUs)
	}

	// Get any remaining CPUs from what's leftover after attempting to grab aligned ones.
	remainingCPUs, err := takeByTopology(cpuTopo, availableCPUSet.Difference(result), requestNum-result.Size())
	if err != nil {
		return map[string]cpuset.CPUSet{
			string(corev1.ResourceCPU): cpuset.NewCPUSet(),
		}
	}

	result = result.Union(remainingCPUs)

	return map[string]cpuset.CPUSet{
		string(corev1.ResourceCPU): result,
	}
}
