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
	`fmt`
	`k8s.io/klog/v2`
	`k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology`
	`k8s.io/kubernetes/pkg/kubelet/cm/cpuset`
	`sort`
)

type cpuAccumulator struct {
	topo          *topology.CPUTopology
	details       topology.CPUDetails
	numCPUsNeeded int
	result        cpuset.CPUSet
}

func newCPUAccumulator(topo *topology.CPUTopology, availableCPUs cpuset.CPUSet, numCPUs int) *cpuAccumulator {
	return &cpuAccumulator{
		topo:          topo,
		details:       topo.CPUDetails.KeepOnly(availableCPUs),
		numCPUsNeeded: numCPUs,
		result:        cpuset.NewCPUSet(),
	}
}

func (ca *cpuAccumulator) take(cpus cpuset.CPUSet) {
	ca.result = ca.result.Union(cpus)
	ca.details = ca.details.KeepOnly(ca.details.CPUs().Difference(ca.result))
	ca.numCPUsNeeded -= cpus.Size()
}

// isSocketFree Returns true if the supplied socket is fully available in `topoDetails`.
func (ca *cpuAccumulator) isSocketFree(socketID int) bool {
	return ca.details.CPUsInSockets(socketID).Size() == ca.topo.CPUsPerSocket()
}

// isCoreFree Returns true if the supplied core is fully available in `topoDetails`.
func (ca *cpuAccumulator) isCoreFree(coreID int) bool {
	return ca.details.CPUsInCores(coreID).Size() == ca.topo.CPUsPerCore()
}

// freeSockets Returns free socket IDs as a slice sorted by:
// - socket ID, ascending.
func (ca *cpuAccumulator) freeSockets() []int {
	return ca.details.Sockets().Filter(ca.isSocketFree).ToSlice()
}

// freeCores Returns core IDs as a slice sorted by:
// - the number of whole available cores on the socket, ascending
// - socket ID, ascending
// - core ID, ascending
func (ca *cpuAccumulator) freeCores() []int {
	socketIDs := ca.details.Sockets().ToSliceNoSort()
	sort.Slice(socketIDs,
		func(i, j int) bool {
			iCores := ca.details.CoresInSockets(socketIDs[i]).Filter(ca.isCoreFree)
			jCores := ca.details.CoresInSockets(socketIDs[j]).Filter(ca.isCoreFree)
			return iCores.Size() < jCores.Size() || socketIDs[i] < socketIDs[j]
		})

	var coreIDs []int
	for _, s := range socketIDs {
		coreIDs = append(coreIDs, ca.details.CoresInSockets(s).Filter(ca.isCoreFree).ToSlice()...)
	}
	return coreIDs
}

// freeCPUs Returns CPU IDs as a slice sorted by:
// - socket affinity with result
// - number of CPUs available on the same socket
// - number of CPUs available on the same core
// - socket ID.
// - core ID.
func (ca *cpuAccumulator) freeCPUs() []int {
	var result []int
	cores := ca.details.Cores().ToSlice()

	sort.Slice(
		cores,
		func(i, j int) bool {
			iCore := cores[i]
			jCore := cores[j]

			iCPUs := ca.topo.CPUDetails.CPUsInCores(iCore).ToSlice()
			jCPUs := ca.topo.CPUDetails.CPUsInCores(jCore).ToSlice()

			iSocket := ca.topo.CPUDetails[iCPUs[0]].SocketID
			jSocket := ca.topo.CPUDetails[jCPUs[0]].SocketID

			// Compute the number of CPUs in the result reside on the same socket
			// as each core.
			iSocketColoScore := ca.topo.CPUDetails.CPUsInSockets(iSocket).Intersection(ca.result).Size()
			jSocketColoScore := ca.topo.CPUDetails.CPUsInSockets(jSocket).Intersection(ca.result).Size()

			// Compute the number of available CPUs available on the same socket
			// as each core.
			iSocketFreeScore := ca.details.CPUsInSockets(iSocket).Size()
			jSocketFreeScore := ca.details.CPUsInSockets(jSocket).Size()

			// Compute the number of available CPUs on each core.
			iCoreFreeScore := ca.details.CPUsInCores(iCore).Size()
			jCoreFreeScore := ca.details.CPUsInCores(jCore).Size()

			return iSocketColoScore > jSocketColoScore ||
				iSocketFreeScore < jSocketFreeScore ||
				iCoreFreeScore < jCoreFreeScore ||
				iSocket < jSocket ||
				iCore < jCore
		})

	// For each core, append sorted CPU IDs to result.
	for _, core := range cores {
		result = append(result, ca.details.CPUsInCores(core).ToSlice()...)
	}
	return result
}

func (ca *cpuAccumulator) needs(n int) bool {
	return ca.numCPUsNeeded >= n
}

func (ca *cpuAccumulator) isSatisfied() bool {
	return ca.numCPUsNeeded < 1
}

func (ca *cpuAccumulator) isFailed() bool {
	return ca.numCPUsNeeded > ca.details.CPUs().Size()
}

// takeByTopology return the assigned cpu set
func takeByTopology(topo *topology.CPUTopology, availableCPUs cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
	acc := newCPUAccumulator(topo, availableCPUs, numCPUs)
	if acc.isSatisfied() {
		return acc.result, nil
	}
	if acc.isFailed() {
		return cpuset.NewCPUSet(), fmt.Errorf("not enough cpus available to satisfy request")
	}

	// Algorithm: topology-aware best-fit
	// 1. Acquire whole sockets, if available and the container requires at
	//    least a socket's-worth of CPUs.
	if acc.needs(acc.topo.CPUsPerSocket()) {
		for _, s := range acc.freeSockets() {
			klog.V(4).Infof("[cpumanager] takeByTopology: claiming socket [%d]", s)
			acc.take(acc.details.CPUsInSockets(s))
			if acc.isSatisfied() {
				return acc.result, nil
			}
			if !acc.needs(acc.topo.CPUsPerSocket()) {
				break
			}
		}
	}

	// 2. Acquire whole cores, if available and the container requires at least
	//    a core's-worth of CPUs.
	if acc.needs(acc.topo.CPUsPerCore()) {
		for _, c := range acc.freeCores() {
			klog.V(4).Infof("[cpumanager] takeByTopology: claiming core [%d]", c)
			acc.take(acc.details.CPUsInCores(c))
			if acc.isSatisfied() {
				return acc.result, nil
			}
			if !acc.needs(acc.topo.CPUsPerCore()) {
				break
			}
		}
	}

	// 3. Acquire single threads, preferring to fill partially-allocated cores
	//    on the same sockets as the whole cores we have already taken in this
	//    allocation.
	for _, c := range acc.freeCPUs() {
		klog.V(4).Infof("[cpumanager] takeByTopology: claiming CPU [%d]", c)
		if acc.needs(1) {
			acc.take(cpuset.NewCPUSet(c))
		}
		if acc.isSatisfied() {
			return acc.result, nil
		}
	}

	return cpuset.NewCPUSet(), fmt.Errorf("failed to allocate cpus")
}
