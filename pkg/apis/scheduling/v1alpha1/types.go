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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

/* PodGroup specific */

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:path=podgroups,shortName=pg;podgroup-v1alpha1

// PodGroup is a collection of Pod; used for batch workload.
type PodGroup struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// +optional
	Spec PodGroupSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// This data may not be up-to-date.
	// +optional
	Status PodGroupStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// PodGroupSpec represents the template of a pod group.
type PodGroupSpec struct {
	// MinMember defines the minimal number of members/tasks to run the pod group;
	// if there's not enough resources to start all tasks, the scheduler
	// will not start anyone.
	// TODO: this could be improved
	MinMember int32 `json:"minMember,omitempty" protobuf:"bytes,1,opt,name=minMember"`

	// MinTaskMember defines the minimal number of pods to run each task in the pod group;
	// if there's not enough resources to start each task, the scheduler
	// will not start anyone.
	// TODO: this could be improved
	MinTaskMember map[string]int32 `json:"minTaskMember,omitempty" protobuf:"bytes,1,opt,name=minTaskMember"`

	// Queue defines the queue to allocate resource for PodGroup; if queue does not exist,
	// the PodGroup will not be scheduled. Defaults to `default` Queue with the lowest weight.
	// +optional
	Queue string `json:"queue,omitempty" protobuf:"bytes,2,opt,name=queue"`

	// If specified, indicates the PodGroup's priority. "system-node-critical" and
	// "system-cluster-critical" are two special keywords which indicate the
	// highest priorities with the former being the highest priority. Any other
	// name must be defined by creating a PriorityClass object with that name.
	// If not specified, the PodGroup priority will be default or zero if there is no
	// default.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,3,opt,name=priorityClassName"`

	// MinResources defines the minimal resource of members/tasks to run the pod group;
	// if there's not enough resources to start all tasks, the scheduler
	// will not start anyone.
	// TODO: the minimal resources to run all tasks?
	MinResources *corev1.ResourceList `json:"minResources,omitempty" protobuf:"bytes,4,opt,name=minResources"`
}

// PodGroupStatus represents the current state of a pod group.
type PodGroupStatus struct {
	// Current phase of PodGroup.
	Phase PodGroupPhase `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase"`

	// The conditions of PodGroup.
	// +optional
	Conditions []PodGroupCondition `json:"conditions,omitempty" protobuf:"bytes,2,opt,name=conditions"`

	// The number of actively running pods.
	// +optional
	Running int32 `json:"running,omitempty" protobuf:"bytes,3,opt,name=running"`

	// The number of pods which reached phase Succeeded.
	// +optional
	Succeeded int32 `json:"succeeded,omitempty" protobuf:"bytes,4,opt,name=succeeded"`

	// The number of pods which reached phase Failed.
	// +optional
	Failed int32 `json:"failed,omitempty" protobuf:"bytes,5,opt,name=failed"`
}

// PodGroupPhase is the phase of a pod group at the current time.
type PodGroupPhase string

// These are the valid phase of podGroups.
const (
	// PodGroupPending means the pod group has been accepted by the system, but scheduler can not allocate
	// enough resources to it.
	PodGroupPending PodGroupPhase = "Pending"

	// PodGroupRunning means `spec.minMember` pods of PodGroups has been in running phase.
	PodGroupRunning PodGroupPhase = "Running"

	// PodGroupUnknown means part of `spec.minMember` pods are running but the other part can not
	// be scheduled, e.g. not enough resource; scheduler will wait for related controller to recover it.
	PodGroupUnknown PodGroupPhase = "Unknown"

	// PodGroupInqueue means controllers can start to create pods,
	// is a new state between PodGroupPending and PodGroupRunning
	PodGroupInqueue PodGroupPhase = "Inqueue"
)

type PodGroupConditionType string

const (
	// PodGroupUnschedulableType is Unschedulable event type
	PodGroupUnschedulableType PodGroupConditionType = "Unschedulable"

	// PodGroupScheduled is scheduled event type
	PodGroupScheduled PodGroupConditionType = "Scheduled"
)

type PodGroupConditionDetail string

const (
	// PodGroupReady is that PodGroup has reached scheduling restriction
	PodGroupReady PodGroupConditionDetail = "pod group is ready"
	// PodGroupNotReady is that PodGroup has not yet reached the scheduling restriction
	PodGroupNotReady PodGroupConditionDetail = "pod group is not ready"
)

type ErrorReason string

const (
	// PodFailedReason is probed if pod of PodGroup failed
	PodFailedReason ErrorReason = "PodFailed"

	// PodDeletedReason is probed if pod of PodGroup deleted
	PodDeletedReason ErrorReason = "PodDeleted"

	// NotEnoughResourcesReason is probed if there are not enough resources to schedule pods
	NotEnoughResourcesReason ErrorReason = "NotEnoughResources"

	// NotEnoughPodsReason is probed if there are not enough tasks compared to `spec.minMember`
	NotEnoughPodsReason ErrorReason = "NotEnoughTasks"

	// NotEnoughPodsOfTaskReason is probed if there are not enough pods of task compared to `spec.minTaskMember`
	NotEnoughPodsOfTaskReason ErrorReason = "NotEnoughPodsOfTask"
)

// PodGroupCondition contains details for the current state of this pod group.
type PodGroupCondition struct {
	// Type is the type of the condition
	Type PodGroupConditionType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type"`

	// Status is the status of the condition.
	Status corev1.ConditionStatus `json:"status,omitempty" protobuf:"bytes,2,opt,name=status"`

	// The ID of condition transition.
	TransitionID string `json:"transitionID,omitempty" protobuf:"bytes,3,opt,name=transitionID"`

	// Last time the phase transitioned from another to current phase.
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,4,opt,name=lastTransitionTime"`

	// Unique, one-word, CamelCase reason for the phase's last transition.
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,5,opt,name=reason"`

	// Human-readable message indicating details about last transition.
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// PodGroupList is a collection of pod groups.
type PodGroupList struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items is the list of PodGroup
	Items []PodGroup `json:"items" protobuf:"bytes,2,rep,name=items"`
}

/* Queue specific */

const (
	// DefaultQueue constant stores the name of the queue as "default"
	DefaultQueue = "default"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:path=queues,scope=Cluster,shortName=q;queue-v1alpha1
// +kubebuilder:subresource:status

// Queue is a queue of PodGroup.
type Queue struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// +optional
	Spec QueueSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// +optional
	Status QueueStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// QueueSpec represents the template of Queue.
type QueueSpec struct {
	Weight     int32               `json:"weight,omitempty" protobuf:"bytes,1,opt,name=weight"`
	Capability corev1.ResourceList `json:"capability,omitempty" protobuf:"bytes,2,opt,name=capability"`

	// TODO: verify delete `State QueueState` is legal or not

	// Reclaimable indicate whether the queue can be reclaimed by other queue
	Reclaimable *bool `json:"reclaimable,omitempty" protobuf:"bytes,3,opt,name=reclaimable"`

	// extendCluster indicate the jobs in this Queue will be dispatched to these clusters.
	ExtendClusters []Cluster `json:"extendClusters,omitempty" protobuf:"bytes,4,opt,name=extendClusters"`

	// Guarantee indicate configuration about resource reservation
	Guarantee Guarantee `json:"guarantee,omitempty" protobuf:"bytes,4,opt,name=guarantee"`
}

type Cluster struct {
	Name     string              `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`
	Weight   int32               `json:"weight,omitempty" protobuf:"bytes,2,opt,name=weight"`
	Capacity corev1.ResourceList `json:"capacity,omitempty" protobuf:"bytes,3,opt,name=capacity"`
}

// Guarantee represents configuration of queue resource reservation
type Guarantee struct {
	// The amount of cluster resource reserved for queue. Just set either `percentage` or `resource`
	// +optional
	Resource corev1.ResourceList `json:"resource,omitempty" protobuf:"bytes,3,opt,name=resource"`
}

// QueueState is state type of queue.
type QueueState string

const (
	// QueueStateOpen indicate `Open` state of queue
	QueueStateOpen QueueState = "Open"
	// QueueStateClosed indicate `Closed` state of queue
	QueueStateClosed QueueState = "Closed"
	// QueueStateClosing indicate `Closing` state of queue
	QueueStateClosing QueueState = "Closing"
	// QueueStateUnknown indicate `Unknown` state of queue
	QueueStateUnknown QueueState = "Unknown"
)

// QueueAction is the action that queue controller will take according to the event.
type QueueAction string

const (
	// SyncQueueAction is the action to sync queue status.
	SyncQueueAction QueueAction = "SyncQueue"
	// OpenQueueAction is the action to open queue
	OpenQueueAction QueueAction = "OpenQueue"
	// CloseQueueAction is the action to close queue
	CloseQueueAction QueueAction = "CloseQueue"
)

// QueueEvent represent the phase of queue.
type QueueEvent string

const (
	// QueueOutOfSyncEvent is triggered if PodGroup/Queue were updated
	QueueOutOfSyncEvent QueueEvent = "OutOfSync"
	// QueueCommandIssuedEvent is triggered if a command is raised by user
	QueueCommandIssuedEvent QueueEvent = "CommandIssued"
)

// QueueStatus represents the status of Queue.
type QueueStatus struct {
	// State is state of queue
	State QueueState `json:"state,omitempty" protobuf:"bytes,1,opt,name=state"`

	// The number of 'Unknown' PodGroup in this queue.
	Unknown int32 `json:"unknown,omitempty" protobuf:"bytes,2,opt,name=unknown"`
	// The number of 'Pending' PodGroup in this queue.
	Pending int32 `json:"pending,omitempty" protobuf:"bytes,3,opt,name=pending"`
	// The number of 'Running' PodGroup in this queue.
	Running int32 `json:"running,omitempty" protobuf:"bytes,4,opt,name=running"`
	// The number of `Inqueue` PodGroup in this queue.
	Inqueue int32 `json:"inqueue,omitempty" protobuf:"bytes,5,opt,name=inqueue"`

	// Reservation is the profile of resource reservation for queue
	Reservation Reservation `json:"reservation,omitempty" protobuf:"bytes,6,opt,name=reservation"`
}

// Reservation represents current condition about resource reservation
type Reservation struct {
	// Nodes are Locked nodes for queue
	// +optional
	Nodes []string `json:"nodes,omitempty" protobuf:"bytes,1,opt,name=nodes"`
	// Resource is a list of total idle resource in locked nodes.
	// +optional
	Resource corev1.ResourceList `json:"resource,omitempty" protobuf:"bytes,2,opt,name=resource"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// QueueList is a collection of queues.
type QueueList struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items is the list of PodGroup
	Items []Queue `json:"items" protobuf:"bytes,2,rep,name=items"`
}
