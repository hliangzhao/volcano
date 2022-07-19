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

package reclaim

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/utils"
	"k8s.io/klog/v2"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (reclaim *Action) Name() string {
	return "reclaim"
}

func (reclaim *Action) Initialize() {}

func (reclaim *Action) Execute(sess *framework.Session) {
	klog.V(3).Infof("Enter Reclaim ...")
	defer klog.V(3).Infof("Leaving Reclaim ...")

	queues := utils.NewPriorityQueue(sess.QueueOrderFn)
	queueMap := map[apis.QueueID]*apis.QueueInfo{}

	preemptorsMap := map[apis.QueueID]*utils.PriorityQueue{} // {queue: jobs}
	preemptorsTasks := map[apis.JobID]*utils.PriorityQueue{} // {job: tasks}

	klog.V(3).Infof("There are <%d> Jobs and <%d> Queues in total for scheduling.",
		len(sess.Jobs), len(sess.Queues))

	for _, job := range sess.Jobs {
		if job.IsPending() {
			continue
		}
		if valid := sess.JobValid(job); valid != nil && !valid.Pass {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip reclaim, reason: %v, message %v",
				job.Namespace, job.Name, job.Queue, valid.Reason, valid.Message)
			continue
		}

		if queue, found := sess.Queues[job.Queue]; !found {
			klog.Errorf("Failed to find Queue <%s> for Job <%s/%s>",
				job.Queue, job.Namespace, job.Name)
			continue
		} else if _, exist := queueMap[queue.UID]; !exist {
			klog.V(4).Infof("Added Queue <%s> for Job <%s/%s>",
				queue.Name, job.Namespace, job.Name)
			queueMap[queue.UID] = queue
			queues.Push(queue)
		}

		if len(job.TaskStatusIndex[apis.Pending]) != 0 {
			if _, found := preemptorsMap[job.Queue]; !found {
				preemptorsMap[job.Queue] = utils.NewPriorityQueue(sess.JobOrderFn)
			}
			preemptorsMap[job.Queue].Push(job)
			preemptorsTasks[job.UID] = utils.NewPriorityQueue(sess.TaskOrderFn)
			for _, task := range job.TaskStatusIndex[apis.Pending] {
				preemptorsTasks[job.UID].Push(task)
			}
		}
	}

	for {
		if queues.Empty() {
			break
		}

		var job *apis.JobInfo
		var task *apis.TaskInfo

		queue := queues.Pop().(*apis.QueueInfo)
		if sess.Overused(queue) {
			klog.V(3).Infof("Queue <%s> is overused, ignore it.", queue.Name)
			continue
		}

		jobs, found := preemptorsMap[queue.UID]
		if !found || jobs.Empty() {
			continue
		} else {
			job = jobs.Pop().(*apis.JobInfo)
		}

		if tasks, found := preemptorsTasks[job.UID]; !found || tasks.Empty() {
			continue
		} else {
			task = tasks.Pop().(*apis.TaskInfo)
		}

		// taskRequest := task.ResReq.ResourceNames()
		// if underusedResources := sess.UnderusedResources(queue); underusedResources != nil && !underusedResources.Contains(taskRequest) {
		// 	klog.V(3).Infof("Queue <%s> is overused when considering task <%s>, ignore it.", queue.Name, task.Name)
		// 	continue
		// }
		if !sess.Allocatable(queue, task) {
			klog.V(3).Infof("Queue <%s> is overused when considering task <%s>, ignore it.", queue.Name, task.Name)
			continue
		}

		assigned := false
		for _, n := range sess.Nodes {
			if err := sess.PredicateFn(task, n); err != nil {
				continue
			}
			klog.V(3).Infof("Considering Task <%s/%s> on Node <%s>.",
				task.Namespace, task.Name, n.Name)

			var reclaimees []*apis.TaskInfo
			for _, task := range n.Tasks {
				if task.Status != apis.Running {
					continue
				}

				if j, found := sess.Jobs[task.Job]; !found {
					continue
				} else if j.Queue != job.Queue {
					q := sess.Queues[j.Queue]
					if !q.Reclaimable() {
						continue
					}
					reclaimees = append(reclaimees, task.Clone())
				}
			}
			victims := sess.Reclaimable(task, reclaimees)

			if err := utils.ValidateVictims(task, n, victims); err != nil {
				klog.V(3).Infof("No validated victims on Node <%s>: %v", n.Name, err)
				continue
			}

			resReq := task.InitResReq.Clone()
			reclaimed := apis.EmptyResource()

			// Reclaim victims for tasks.
			for _, reclaimee := range victims {
				klog.Errorf("Try to reclaim Task <%s/%s> for Tasks <%s/%s>",
					reclaimee.Namespace, reclaimee.Name, task.Namespace, task.Name)
				if err := sess.Evict(reclaimee, "reclaim"); err != nil {
					klog.Errorf("Failed to reclaim Task <%s/%s> for Tasks <%s/%s>: %v",
						reclaimee.Namespace, reclaimee.Name, task.Namespace, task.Name, err)
					continue
				}
				reclaimed.Add(reclaimee.ResReq)
				// If reclaimed enough resources, break loop to avoid Sub panic.
				if resReq.LessEqual(reclaimed, apis.Zero) {
					break
				}
			}

			klog.V(3).Infof("Reclaimed <%v> for task <%s/%s> requested <%v>.",
				reclaimed, task.Namespace, task.Name, task.InitResReq)

			if task.InitResReq.LessEqual(reclaimed, apis.Zero) {
				if err := sess.Pipeline(task, n.Name); err != nil {
					klog.Errorf("Failed to pipeline Task <%s/%s> on Node <%s>",
						task.Namespace, task.Name, n.Name)
				}
				// Ignore error of pipeline, will be corrected in next scheduling loop.
				assigned = true
				break
			}
		}
		if assigned {
			jobs.Push(job)
		}
		queues.Push(queue)
	}
}

func (reclaim *Action) UnInitialize() {}
