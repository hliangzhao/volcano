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

package v1alpha1

const GroupName = "scheduling.hliangzhao.io"

// JobWaitingTime is the key of sla plugin to set maximum waiting time
// that a job could stay Pending in service level agreement
const JobWaitingTime = GroupName + "/sla-waiting-time"

const KubeHierarchyAnnotationKey = GroupName + "/hierarchy"

const KubeHierarchyWeightAnnotationKey = GroupName + "/hierarchy-weights"

// KubeGroupNameAnnotationKey is the annotation key of Pod to identify
// which PodGroup it belongs to.
const KubeGroupNameAnnotationKey = "scheduling.k8s.io/group-name"

// VolcanoGroupNameAnnotationKey is the annotation key of Pod to identify
// which PodGroup it belongs to.
const VolcanoGroupNameAnnotationKey = GroupName + "/group-name"

// QueueNameAnnotationKey is the annotation key of Pod to identify
// which queue it belongs to.
const QueueNameAnnotationKey = GroupName + "/queue-name"

// PodPreemptable is the key of preemptable
const PodPreemptable = GroupName + "/preemptable"

// PreemptStableTime is the key of preempt stable time, such as "60s", "10m".
// Valid time units are "ns", "µs" (or "us"), "ms", "s", "m", "h".
const PreemptStableTime = GroupName + "/preempt-stable-time"

// RevocableZone is the key of revocable-zone
const RevocableZone = GroupName + "/revocable-zone"

// JDBMinAvailable is the key of min available pod number
const JDBMinAvailable = GroupName + "/jdb-min-available"

// JDBMaxUnavailable is the key of max unavailable pod number
const JDBMaxUnavailable = GroupName + "/jdb-max-unavailable"

// NumaPolicyKey is the key of pod NUMA-topology policy
const NumaPolicyKey = GroupName + "/numa-topology-policy"

// TopologyDecisionAnnotation is the key of topology decision about pod request resource
const TopologyDecisionAnnotation = GroupName + "/topology-decision"
