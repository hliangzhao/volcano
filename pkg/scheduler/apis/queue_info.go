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

import (
	"github.com/hliangzhao/volcano/pkg/apis/scheduling"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

// TODO: fully checked

type QueueID types.UID

type QueueInfo struct {
	UID    QueueID
	Name   string
	Weight int32

	// Weights is a list of slash seperated float numbers.
	// Each of them is a weight corresponding the hierarchy level.
	Weights string

	// Hierarchy is a list of node name along the
	// path from the root to the node itself.
	Hierarchy string

	Queue *scheduling.Queue
}

func NewQueueInfo(queue *scheduling.Queue) *QueueInfo {
	return &QueueInfo{
		UID:    QueueID(queue.Name),
		Name:   queue.Name,
		Weight: queue.Spec.Weight,

		Weights:   queue.Annotations[schedulingv1alpha1.KubeHierarchyWeightAnnotationKey],
		Hierarchy: queue.Annotations[schedulingv1alpha1.KubeHierarchyAnnotationKey],

		Queue: queue,
	}
}

func (q *QueueInfo) Clone() *QueueInfo {
	return &QueueInfo{
		UID:       q.UID,
		Name:      q.Name,
		Weight:    q.Weight,
		Weights:   q.Weights,
		Hierarchy: q.Hierarchy,
		Queue:     q.Queue,
	}
}

// Reclaimable returns true if queue is reclaimable.
func (q *QueueInfo) Reclaimable() bool {
	if q == nil {
		return false
	}
	if q.Queue == nil {
		return false
	}
	if q.Queue.Spec.Reclaimable == nil {
		return true
	}
	return *q.Queue.Spec.Reclaimable
}
