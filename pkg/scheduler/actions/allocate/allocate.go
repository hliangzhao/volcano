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

package allocate

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/metrics"
	"github.com/hliangzhao/volcano/pkg/scheduler/utils"
	"k8s.io/klog/v2"
)

// targetJob stores the jobs which are of the highest priority and waits for the longest time.
var targetJob = utils.Reservation.TargetJob

type Action struct{}

func New() *Action {
	return &Action{}
}

func (alloc *Action) Name() string {
	return "allocate"
}

func (alloc *Action) Initialize() {}

func (alloc *Action) Execute(sess *framework.Session) {
	klog.V(3).Infof("Enter Allocate ...")
	defer klog.V(3).Infof("Leaving Allocate ...")

	// the allocation of node for pod haves many stages:
	// 1. pick a namespace named N (using sess.NamespaceOrderFn)
	// 2. pick a queue named Q from N (using sess.QueueOrderFn)
	// 3. pick a job named J from Q (using sess.JobOrderFn)
	// 4. pick a task T from J (using sess.TaskOrderFn)
	// 5. use predicateFn to filter out node that T can not be allocated on
	// 6. use sess.NodeOrderFn to judge the best node and assign it to T

	namespaces := utils.NewPriorityQueue(sess.NamespaceOrderFn)

	// jobsMap is used to find the job with the highest priority in given queue and namespace
	jobsMap := map[apis.NamespaceName]map[apis.QueueID]*utils.PriorityQueue{}

	for _, job := range sess.Jobs {
		if job.IsPending() {
			// pending job cannot be allocated, just skip it
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip allocate, reason: job status is pending.",
				job.Namespace, job.Name, job.Queue)
			continue
		}
		if valid := sess.JobValid(job); valid != nil && !valid.Pass {
			// invalid job cannot be allocated, just skip it
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip allocate, reason: %v, message %v",
				job.Namespace, job.Name, job.Queue, valid.Reason, valid.Message)
			continue
		}
		if _, found := sess.Queues[job.Queue]; !found {
			// only enqueued job can be allocated
			klog.Warningf("Skip adding Job <%s/%s> because its queue %s is not found",
				job.Namespace, job.Name, job.Queue)
			continue
		}

		// get the queues in the specific namespace
		namespace := apis.NamespaceName(job.Namespace)
		queueMap, found := jobsMap[namespace]
		if !found {
			namespaces.Push(namespace)
			// queueMap: {queueName: jobs}
			queueMap = map[apis.QueueID]*utils.PriorityQueue{}
			jobsMap[namespace] = queueMap
		}
		jobs, found := queueMap[job.Queue]
		if !found {
			jobs = utils.NewPriorityQueue(sess.JobOrderFn)
			queueMap[job.Queue] = jobs
		}
		klog.V(4).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
		jobs.Push(job)
	}

	klog.V(3).Infof("Try to allocate resource to %d Namespaces", len(jobsMap))
	pendingTasks := map[apis.JobID]*utils.PriorityQueue{}

	// Get all the unlocked nodes. Only unlocked nodes can be chosen for allocation.
	// The locked nodes are reserved for target job.
	allNodes := sess.NodeList
	unlockedNodes := allNodes
	if targetJob != nil && len(utils.Reservation.LockedNodes) != 0 {
		unlockedNodes = unlockedNodes[0:0]
		for _, node := range allNodes {
			if _, exist := utils.Reservation.LockedNodes[node.Name]; !exist {
				unlockedNodes = append(unlockedNodes, node)
			}
		}
	}
	for _, unlockedNode := range unlockedNodes {
		klog.V(4).Infof("unlockedNode ID: %s, Name: %s", unlockedNode.Node.UID, unlockedNode.Node.Name)
	}

	// predicateFn is a func to check whether node can fit task in resource requirements
	predicateFn := func(task *apis.TaskInfo, node *apis.NodeInfo) error {
		if !task.InitResReq.LessEqual(node.FutureIdle(), apis.Zero) {
			return apis.NewFitError(task, node, apis.NodeResourceFitFailed)
		}
		return sess.PredicateFn(task, node)
	}

	// To pick <namespace, queue> tuple for job, we choose to pick namespace firstly.
	// Because we believe that number of queues would less than namespaces in most case.
	// And, this action would make the resource usage among namespace balanced.
	for {
		if namespaces.Empty() {
			break
		}

		namespace := namespaces.Pop().(apis.NamespaceName)
		queuesInNamespace := jobsMap[namespace]

		// pick queue for given namespace
		// TODO: This block use an algorithm with time complex O(n).
		//  But at least PriorityQueue could not be used here,
		//  because the allocation of job would change the priority of queue among all namespaces,
		//  and the PriorityQueue have no ability to update priority for a special queue.
		var queue *apis.QueueInfo
		for queueID := range queuesInNamespace {
			currentQueue := sess.Queues[queueID]
			// TODO: currentQueue should not be nil. The judgement should be moved to here.
			if sess.Overused(currentQueue) {
				klog.V(3).Infof("Namespace <%s> Queue <%s> is overused, ignore it.", namespace, currentQueue.Name)
				delete(queuesInNamespace, queueID)
				continue
			}
			if jobs, found := queuesInNamespace[currentQueue.UID]; found && jobs.Empty() {
				continue
			}
			if queue == nil || sess.QueueOrderFn(currentQueue, queue) {
				queue = currentQueue
			}
		}

		if queue == nil {
			klog.V(3).Infof("Namespace <%s> have no queue, skip it", namespace)
			continue
		}

		// now we have a non-nil apis.QueueInfo now. We can allocate resources to the jobs in it now!
		klog.V(3).Infof("Try to allocate resource to Jobs in Namespace <%s> Queue <%v>", namespace, queue.Name)

		jobs, found := queuesInNamespace[queue.UID]
		// TODO: why judge again here?
		if !found || jobs.Empty() {
			delete(queuesInNamespace, queue.UID)
			namespaces.Push(namespace)
			klog.V(4).Infof("Can not find jobs for queue %s.", queue.Name)
			continue
		}

		// finally, we get a job to allocate resources
		job := jobs.Pop().(*apis.JobInfo)
		var nodes []*apis.NodeInfo
		if targetJob != nil && job.UID == targetJob.UID {
			klog.V(4).Infof("Try to allocate resource to target job: %s", job.Name)
			nodes = allNodes
		} else {
			nodes = unlockedNodes
		}

		// note that hte allocation is happened to each pending task of this job
		// thus, we should get pending tasks in that job first
		if _, found = pendingTasks[job.UID]; !found {
			tasks := utils.NewPriorityQueue(sess.TaskOrderFn)
			for _, task := range job.TaskStatusIndex[apis.Pending] {
				// Skip BestEffort task in 'allocate' action.
				if task.ResReq.IsEmpty() {
					klog.V(4).Infof("Task <%v/%v> is BestEffort task, skip it.",
						task.Namespace, task.Name)
					continue
				}
				tasks.Push(task)
			}
			pendingTasks[job.UID] = tasks
		}
		tasks := pendingTasks[job.UID]

		// now we can allocate for a task
		klog.V(3).Infof("Try to allocate resource to %d tasks of Job <%v/%v>",
			tasks.Len(), job.Namespace, job.Name)
		stmt := framework.NewStatement(sess)
		ph := utils.NewPredicateHelper()
		for !tasks.Empty() {
			task := tasks.Pop().(*apis.TaskInfo)

			// Check whether the queue is overused on the dimension that the task requested
			taskRequest := task.ResReq.ResourceNames()
			if underUsedResources := sess.UnderusedResources(queue); underUsedResources != nil && !underUsedResources.Contains(taskRequest) {
				klog.V(3).Infof("Queue <%s> is overused when considering task <%s>, ignore it.", queue.Name, task.Name)
				continue
			}

			// for this task, choose the best node for it
			klog.V(3).Infof("There are <%d> nodes for Job <%v/%v>", len(nodes), job.Namespace, job.Name)

			// firstly, use predicate func to filter out the nodes that cannot be used
			predicateNodes, fitErrors := ph.PredicateNodes(task, nodes, predicateFn)
			if len(predicateNodes) == 0 {
				job.NodesFitErrors[task.UID] = fitErrors
				break
			}

			// after predicate, we get the candidate nodes
			var candidateNodes []*apis.NodeInfo
			for _, n := range predicateNodes {
				if task.InitResReq.LessEqual(n.Idle, apis.Zero) || task.InitResReq.LessEqual(n.FutureIdle(), apis.Zero) {
					candidateNodes = append(candidateNodes, n)
				}
			}
			if len(candidateNodes) == 0 {
				continue
			}

			// secondly, scoring each node and sort them for obtaining the best node
			nodeScores := utils.PrioritizeNodes(
				task,
				candidateNodes,
				sess.BatchNodeOrderFn,
				sess.NodeOrderMapFn,
				sess.NodeOrderReduceFn,
			)
			node := sess.BestNodeFn(task, nodeScores)
			if node == nil {
				node = utils.SelectBestNode(nodeScores)
			}

			// allocate the resources of the best nodes to the task
			if task.InitResReq.LessEqual(node.Idle, apis.Zero) {
				klog.V(3).Infof("Binding Task <%v/%v> to node <%v>",
					task.Namespace, task.Name, node.Name)
				if err := stmt.Allocate(task, node); err != nil {
					klog.Errorf("Failed to bind Task %v on %v in Session %v, err: %v",
						task.UID, node.Name, sess.UID, err)
				} else {
					metrics.UpdateE2eSchedulingDurationByJob(
						job.Name,
						string(job.Queue),
						job.Namespace,
						metrics.Duration(job.CreationTimestamp.Time),
					)
				}
			} else {
				klog.V(3).Infof("Predicates failed for task <%s/%s> on node <%s> with limited resources",
					task.Namespace, task.Name, node.Name)

				// Allocate releasing resource to the task if any.
				if task.InitResReq.LessEqual(node.FutureIdle(), apis.Zero) {
					// TODO: here we know the difference between Pending and Pipelined
					klog.V(3).Infof("Pipelining Task <%v/%v> to node <%v> for <%v> on <%v>",
						task.Namespace, task.Name, node.Name, task.InitResReq, node.Releasing)
					if err := stmt.Pipeline(task, node.Name); err != nil {
						klog.Errorf("Failed to pipeline Task %v on %v in Session %v for %v.",
							task.UID, node.Name, sess.UID, err)
					} else {
						metrics.UpdateE2eSchedulingDurationByJob(
							job.Name,
							string(job.Queue),
							job.Namespace,
							metrics.Duration(job.CreationTimestamp.Time),
						)
					}
				}
			}

			if sess.JobReady(job) && !tasks.Empty() {
				jobs.Push(job)
				break
			}
		}

		if sess.JobReady(job) {
			stmt.Commit()
		} else {
			if !sess.JobPipelined(job) {
				stmt.Discard()
			}
		}

		// Added Namespace back until no job in Namespace.
		namespaces.Push(namespace)
	}
}

func (alloc *Action) UnInitialize() {}
