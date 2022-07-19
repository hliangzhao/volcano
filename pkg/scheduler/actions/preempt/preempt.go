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

package preempt

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/metrics"
	"github.com/hliangzhao/volcano/pkg/scheduler/utils"
	"k8s.io/klog/v2"
)

const (
	Preempt = "preempt"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (preempt *Action) Name() string {
	return Preempt
}

func (preempt *Action) Initialize() {}

func (preempt *Action) Execute(sess *framework.Session) {
	klog.V(3).Infof("Enter Preempt ...")
	defer klog.V(3).Infof("Leaving Preempt ...")

	preemptorsMap := map[apis.QueueID]*utils.PriorityQueue{} // {queue: jobs}
	preemptorTasks := map[apis.JobID]*utils.PriorityQueue{}  // {job: tasks}

	var underRequest []*apis.JobInfo
	queues := map[apis.QueueID]*apis.QueueInfo{}

	for _, job := range sess.Jobs {
		// job should be ready and valid
		if job.IsPending() {
			continue
		}
		if valid := sess.JobValid(job); valid != nil && !valid.Pass {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip preempt, reason: %v, message %v",
				job.Namespace, job.Name, job.Queue, valid.Reason, valid.Message)
			continue
		}

		// Get the queue that this job is in.
		// If not found, add it to queues.
		if queue, found := sess.Queues[job.Queue]; !found {
			continue
		} else if _, exist := queues[queue.UID]; !exist {
			klog.V(3).Infof("Added Queue <%s> for Job <%s/%s>",
				queue.Name, job.Namespace, job.Name)
			queues[queue.UID] = queue
		}

		// Check if job requests for more resources.
		if sess.JobStarving(job) {
			// create priority queue for the CRD Queue and push the job into it
			if _, found := preemptorsMap[job.Queue]; !found {
				preemptorsMap[job.Queue] = utils.NewPriorityQueue(sess.JobOrderFn)
			}
			preemptorsMap[job.Queue].Push(job)
			underRequest = append(underRequest, job)
			preemptorTasks[job.UID] = utils.NewPriorityQueue(sess.TaskOrderFn)
			for _, task := range job.TaskStatusIndex[apis.Pending] {
				preemptorTasks[job.UID].Push(task)
			}
		}
	}

	ph := utils.NewPredicateHelper()
	// Preemption between Jobs within Queue.
	for _, queue := range queues {
		for {
			preemptors := preemptorsMap[queue.UID]
			// If no preemptors, no preemption.
			if preemptors == nil || preemptors.Empty() {
				klog.V(4).Infof("No preemptors in Queue <%s>, break.", queue.Name)
				break
			}

			preemptorJob := preemptors.Pop().(*apis.JobInfo)
			stmt := framework.NewStatement(sess)
			assigned := false
			for {
				// If job is not request more resource, then stop preempting.
				if !sess.JobStarving(preemptorJob) {
					break
				}

				// If no preemptor tasks, next job.
				if preemptorTasks[preemptorJob.UID].Empty() {
					klog.V(3).Infof("No preemptor task in job <%s/%s>.",
						preemptorJob.Namespace, preemptorJob.Name)
					break
				}

				preemptor := preemptorTasks[preemptorJob.UID].Pop().(*apis.TaskInfo)
				if preempted, _ := preemption(sess, stmt, preemptor, func(task *apis.TaskInfo) bool {
					// Ignore non running task.
					if task.Status != apis.Running {
						return false
					}
					// Ignore task with empty resource request.
					if task.ResReq.IsEmpty() {
						return false
					}
					job, found := sess.Jobs[task.Job]
					if !found {
						return false
					}
					// Preempt other jobs within queue
					return job.Queue == preemptorJob.Queue && preemptor.Job != task.Job
				}, ph); preempted {
					assigned = true
				}
			}

			// Commit changes only if job is pipelined, otherwise try next job.
			if sess.JobPipelined(preemptorJob) {
				stmt.Commit()
			} else {
				stmt.Discard()
				continue
			}

			if assigned {
				preemptors.Push(preemptorJob)
			}
		}

		// Preemption between Task within Job.
		for _, job := range underRequest {
			// Fix: preemptor numbers lose when in same job
			preemptorTasks[job.UID] = utils.NewPriorityQueue(sess.TaskOrderFn)
			for _, task := range job.TaskStatusIndex[apis.Pending] {
				preemptorTasks[job.UID].Push(task)
			}
			for {
				if _, found := preemptorTasks[job.UID]; !found {
					break
				}
				if preemptorTasks[job.UID].Empty() {
					break
				}
				preemptor := preemptorTasks[job.UID].Pop().(*apis.TaskInfo)
				stmt := framework.NewStatement(sess)
				assigned, _ := preemption(sess, stmt, preemptor, func(task *apis.TaskInfo) bool {
					// Ignore non running task.
					if task.Status != apis.Running {
						return false
					}
					// Ignore task with empty resource request.
					if task.ResReq.IsEmpty() {
						return false
					}
					// Preempt tasks within job.
					return preemptor.Job == task.Job
				}, ph)
				stmt.Commit()

				if !assigned {
					break
				}
			}
		}
	}

	victimTasks(sess)
}

func (preempt *Action) UnInitialize() {}

func preemption(sess *framework.Session, stmt *framework.Statement, preemptor *apis.TaskInfo,
	filter func(*apis.TaskInfo) bool, predicateHelper utils.PredicateHelper) (bool, error) {

	// preempt is successful or not
	assigned := false

	allNodes := sess.NodeList
	predicateNodes, _ := predicateHelper.PredicateNodes(preemptor, allNodes, sess.PredicateFn)
	nodeScores := utils.PrioritizeNodes(
		preemptor,
		predicateNodes,
		sess.BatchNodeOrderFn,
		sess.NodeOrderMapFn,
		sess.NodeOrderReduceFn,
	)
	selectNodes := utils.SortNodes(nodeScores)
	for _, node := range selectNodes {
		klog.V(3).Infof("Considering Task <%s/%s> on Node <%s>.",
			preemptor.Namespace, preemptor.Name, node.Name)

		// get victims of this preempt
		var preemptees []*apis.TaskInfo
		for _, task := range node.Tasks {
			if filter == nil {
				preemptees = append(preemptees, task.Clone())
			} else if filter(task) {
				preemptees = append(preemptees, task.Clone())
			}
		}
		victims := sess.Preemptable(preemptor, preemptees)
		metrics.UpdatePreemptionVictimsCount(len(victims))

		if err := utils.ValidateVictims(preemptor, node, victims); err != nil {
			klog.V(3).Infof("No validated victims on Node <%s>: %v", node.Name, err)
			continue
		}

		victimsQueue := utils.NewPriorityQueue(func(l, r interface{}) bool {
			return !sess.TaskOrderFn(l, r)
		})
		for _, victim := range victims {
			victimsQueue.Push(victim)
		}

		// Preempt victims for tasks, pick the lowest priority task first.
		preempted := apis.EmptyResource()
		for !victimsQueue.Empty() {
			// If reclaimed enough resources, break loop to avoid Sub panic.
			if preemptor.InitResReq.LessEqual(node.FutureIdle(), apis.Zero) {
				break
			}
			preemptee := victimsQueue.Pop().(*apis.TaskInfo)
			klog.V(3).Infof("Try to preempt Task <%s/%s> for Task <%s/%s>",
				preemptee.Namespace, preemptee.Name, preemptor.Namespace, preemptor.Name)
			if err := stmt.Evict(preemptee, "preempt"); err != nil {
				klog.Errorf("Failed to preempt Task <%s/%s> for Task <%s/%s>: %v",
					preemptee.Namespace, preemptee.Name, preemptor.Namespace, preemptor.Name, err)
				continue
			}
			preempted.Add(preemptee.ResReq)
		}

		metrics.RegisterPreemptionAttempts()
		klog.V(3).Infof("Preempted <%v> for Task <%s/%s> requested <%v>.",
			preempted, preemptor.Namespace, preemptor.Name, preemptor.InitResReq)

		if preemptor.InitResReq.LessEqual(node.FutureIdle(), apis.Zero) {
			if err := stmt.Pipeline(preemptor, node.Name); err != nil {
				klog.Errorf("Failed to pipeline Task <%s/%s> on Node <%s>",
					preemptor.Namespace, preemptor.Name, node.Name)
			}

			// Ignore pipeline error, will be corrected in next scheduling loop.
			assigned = true
			break
		}
	}

	return assigned, nil
}

func victimTasks(sess *framework.Session) {
	stmt := framework.NewStatement(sess)
	tasks := make([]*apis.TaskInfo, 0)
	victimTasksMap := sess.VictimTasks(tasks)
	vts := make([]*apis.TaskInfo, 0)
	for task := range victimTasksMap {
		vts = append(vts, task)
	}
	for _, victim := range vts {
		if err := stmt.Evict(victim.Clone(), "evict"); err != nil {
			klog.Errorf("Failed to evict Task <%s/%s>: %v",
				victim.Namespace, victim.Name, err)
			continue
		}
	}
	stmt.Commit()
}
