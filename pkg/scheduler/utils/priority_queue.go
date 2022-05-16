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

package utils

import (
	"container/heap"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
)

/*
NOTE that the priority queues in this file has nothing to do with the CRD Queue.
The priorityQueue and PriorityQueue defined in this file are data structures used for push and pop things.
*/

type priorityQueue struct {
	items  []interface{}
	lessFn apis.LessFn
}

func (pq *priorityQueue) Len() int {
	return len(pq.items)
}

func (pq *priorityQueue) Less(i, j int) bool {
	if pq.lessFn == nil {
		return i < j
	}
	return pq.lessFn(pq.items[i], pq.items[j])
}

func (pq priorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

func (pq *priorityQueue) Push(obj interface{}) {
	(*pq).items = append((*pq).items, obj)
}

func (pq *priorityQueue) Pop() interface{} {
	old := (*pq).items
	n := len(old)
	item := old[n-1]
	(*pq).items = old[0 : n-1]
	return item
}

type PriorityQueue struct {
	queue priorityQueue
}

func NewPriorityQueue(lessFn apis.LessFn) *PriorityQueue {
	return &PriorityQueue{
		queue: priorityQueue{
			items:  make([]interface{}, 0),
			lessFn: lessFn,
		},
	}
}

func (q *PriorityQueue) Len() int {
	return q.queue.Len()
}

func (q *PriorityQueue) Empty() bool {
	return q.queue.Len() == 0
}

func (q *PriorityQueue) Push(it interface{}) {
	heap.Push(&q.queue, it)
}

func (q *PriorityQueue) Pop() interface{} {
	if q.Len() == 0 {
		return nil
	}
	return heap.Pop(&q.queue)
}
