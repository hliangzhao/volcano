/*
Copyright 2021-2022 The Volcano Authors.

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

package backfill

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/metrics"
	"k8s.io/klog/v2"
)

const (
	Backfill = "backfill"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (backfill *Action) Name() string {
	return Backfill
}

func (backfill *Action) Initialize() {}

func (backfill *Action) Execute(sess *framework.Session) {
	klog.V(3).Infof("Enter Backfill ...")
	defer klog.V(3).Infof("Leaving Backfill ...")

	// TODO: When backfill, it's also need to balance between Queues.
	for _, job := range sess.Jobs {
		if job.IsPending() {
			continue
		}

		if valid := sess.JobValid(job); valid != nil && !valid.Pass {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip backfill, reason: %v, message %v",
				job.Namespace, job.Name, job.Queue, valid.Reason, valid.Message)
			continue
		}

		for _, task := range job.TaskStatusIndex[apis.Pending] {
			if task.InitResReq.IsEmpty() {
				allocated := false
				fe := apis.NewFitErrors()

				// As task did not request resources, so it only need to meet predicates.
				// TODO: need to prioritize nodes to avoid pod hole.
				for _, node := range sess.Nodes {
					// TODO: predicates did not consider pod number for now, there'll be ping-pong case here.
					if err := sess.PredicateFn(task, node); err != nil {
						klog.V(3).Infof("Predicates failed for task <%s/%s> on node <%s>: %v",
							task.Namespace, task.Name, node.Name, err)
						fe.SetNodeError(node.Name, err)
						continue
					}

					// this node passed the predicate
					klog.V(3).Infof("Binding Task <%v/%v> to node <%v>", task.Namespace, task.Name, node.Name)
					if err := sess.Allocate(task, node); err != nil {
						klog.Errorf("Failed to bind Task %v on %v in Session %v", task.UID, node.Name, sess.UID)
						fe.SetNodeError(node.Name, err)
						continue
					}

					metrics.UpdateE2eSchedulingDurationByJob(
						job.Name,
						string(job.Queue),
						job.Namespace,
						metrics.Duration(job.CreationTimestamp.Time),
					)
					allocated = true
					break
				}

				if !allocated {
					job.NodesFitErrors[task.UID] = fe
				}
			}
			// TODO: backfill for other case.
		}
	}
}

func (backfill *Action) UnInitialize() {}
