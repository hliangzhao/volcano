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
	`k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology`
	`k8s.io/kubernetes/pkg/kubelet/cm/cpuset`
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
	// TODO
}

// isSocketFree Returns true if the supplied socket is fully available in `topoDetails`.
func (ca *cpuAccumulator) isSocketFree(socketID int) bool {
	// TODO
	return false
}

// isCoreFree Returns true if the supplied core is fully available in `topoDetails`.
func (ca *cpuAccumulator) isCoreFree(coreID int) bool {
	// TODO
	return false
}

// freeSockets Returns free socket IDs as a slice sorted by:
// - socket ID, ascending.
func (ca *cpuAccumulator) freeSockets() []int {
	// TODO
	return nil
}

// freeCores Returns core IDs as a slice sorted by:
// - the number of whole available cores on the socket, ascending
// - socket ID, ascending
// - core ID, ascending
func (ca *cpuAccumulator) freeCores() []int {
	// TODO
	return nil
}

// freeCPUs Returns CPU IDs as a slice sorted by:
// - socket affinity with result
// - number of CPUs available on the same socket
// - number of CPUs available on the same core
// - socket ID.
// - core ID.
func (ca *cpuAccumulator) freeCPUs() []int {
	// TODO
	return nil
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
	// TODO
	return cpuset.CPUSet{}, nil
}
