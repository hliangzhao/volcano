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

package policy

import `k8s.io/klog/v2`

type policySingleNumaNode struct {
	numaNodes []int
}

func NewPolicySingleNumaNode(numaNodes []int) Policy {
	return &policySingleNumaNode{
		numaNodes: numaNodes,
	}
}

func (p *policySingleNumaNode) canAdmitPodResult(hint *TopologyHint) bool {
	return hint.Preferred
}

func filterSingleNumaHints(allResHints [][]TopologyHint) [][]TopologyHint {
	var filteredResourcesHints [][]TopologyHint
	for _, oneResourceHints := range allResHints {
		var filtered []TopologyHint
		for _, hint := range oneResourceHints {
			if hint.NumaNodeAffinity == nil && hint.Preferred {
				filtered = append(filtered, hint)
			}
			if hint.NumaNodeAffinity != nil && hint.NumaNodeAffinity.Count() == 1 && hint.Preferred {
				filtered = append(filtered, hint)
			}
		}
		filteredResourcesHints = append(filteredResourcesHints, filtered)
	}
	return filteredResourcesHints
}

func (p *policySingleNumaNode) Predicate(providersHints []map[string][]TopologyHint) (TopologyHint, bool) {
	filteredHints := filterProvidersHints(providersHints)
	singleNumaHints := filterSingleNumaHints(filteredHints)
	bestHint := mergeFilteredHints(p.numaNodes, singleNumaHints)
	klog.V(4).Infof("bestHint: %v\n", bestHint)
	admit := p.canAdmitPodResult(&bestHint)
	return bestHint, admit
}
