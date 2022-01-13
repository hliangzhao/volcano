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

import corev1 `k8s.io/api/core/v1`

type AllocateFailError struct {
	Reason string
}

func (e *AllocateFailError) Error() string {
	return e.Reason
}

// NodeState defines the current state of node.
type NodeState struct {
	Phase  NodePhase
	Reason string
}

// NodeInfo is node level aggregated information.
type NodeInfo struct {
	Name string
	Node *corev1.Node

	State NodeState

	// The releasing resource on that node
	Releasing *Resource
	// The pipelined resource on that node
	Pipelined *Resource
	// The idle resource on that node
	Idle *Resource
	// The used resource on that node, including running and terminating pods
	Used *Resource

	Allocatable *Resource
	Capability  *Resource

	Tasks             map[TaskID]*TaskInfo
	NumaInfo          *NumaTopoInfo
	NumaChgFlag       NumaChangeFlag
	NumaSchedulerInfo *NumaTopoInfo
	RevocableZone     string

	// Used to store custom information
	Others map[string]interface{}
	GPUDevice map[int]*GPUDevice

	// enable node resource oversubscription
	OversubscriptionNode bool
	// if OfflineJobEvicting is true, then when node resource usage too high,
	// the dispatched pod can not use oversubscription resource
	OfflineJobEvicting bool

	// the Oversubscription Resource reported in annotation
	OversubscriptionResource *Resource
}
