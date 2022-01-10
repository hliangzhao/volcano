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

package apis

const (

	// VolcanoGPUResource extended gpu resource
	VolcanoGPUResource = "volcano.sh/gpu-memory"
	// VolcanoGPUNumber virtual GPU card number
	VolcanoGPUNumber = "volcano.sh/gpu-number"

	// PredicateTime is the key of predicate time
	PredicateTime = "volcano.sh/predicate-time"
	// GPUIndex is the key of gpu index
	GPUIndex = "volcano.sh/gpu-index"

	// OverSubscriptionNode is the key of node oversubscription
	OverSubscriptionNode = "volcano.sh/oversubscription"
	// OverSubscriptionCPU is the key of cpu oversubscription
	OverSubscriptionCPU = "volcano.sh/oversubscription-cpu"
	// OverSubscriptionMemory is the key of memory oversubscription
	OverSubscriptionMemory = "volcano.sh/oversubscription-memory"
	// OfflineJobEvicting node will not schedule pod due to offline job evicting
	OfflineJobEvicting = "volcano.sh/offline-job-evicting"

	// TopologyDecisionAnnotation is the key of topology decision about pod request resource
	TopologyDecisionAnnotation = "volcano.sh/topology-decision"
)
