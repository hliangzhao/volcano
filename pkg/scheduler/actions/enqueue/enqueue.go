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

package enqueue

import (
	`github.com/hliangzhao/volcano/pkg/apis/scheduling`
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	`github.com/hliangzhao/volcano/pkg/scheduler/framework`
	`github.com/hliangzhao/volcano/pkg/scheduler/utils`
	metav1 `k8s.io/apimachinery/pkg/apis/meta/v1`
	`k8s.io/klog/v2`
	`time`
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (enqueue *Action) Name() string {
	return "enqueue"
}

func (enqueue *Action) Initialize() {}

func (enqueue *Action) Execute(sess *framework.Session) {
	klog.V(3).Infof("Enter Enqueue ...")
	defer klog.V(3).Infof("Leaving Enqueue ...")

	queues := utils.NewPriorityQueue(sess.QueueOrderFn)
	queueMap := map[apis.QueueID]*apis.QueueInfo{}
	jobsMap := map[apis.QueueID]*utils.PriorityQueue{}

	for _, job := range sess.Jobs {
		// set schedule start time
		if job.ScheduleStartTimestamp.IsZero() {
			sess.Jobs[job.UID].ScheduleStartTimestamp = metav1.Time{Time: time.Now()}
		}
		// the CRD queue for this job not found, create it, and add it to queueMap and queues
		if queue, found := sess.Queues[job.Queue]; !found {
			klog.Errorf("Failed to find Queue <%s> for Job <%s/%s>",
				job.Queue, job.Namespace, job.Name)
			continue
		} else if _, existed := queueMap[queue.UID]; !existed {
			klog.V(3).Infof("Added Queue <%s> for Job <%s/%s>",
				queue.Name, job.Namespace, job.Name)
			queueMap[queue.UID] = queue
			queues.Push(queue)
		}

		// if this job is pending, add it to jobsMap
		if job.IsPending() {
			if _, found := jobsMap[job.Queue]; !found {
				jobsMap[job.Queue] = utils.NewPriorityQueue(sess.JobOrderFn)
			}
			klog.V(3).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
			jobsMap[job.Queue].Push(job)
		}
	}

	klog.V(3).Infof("Try to enqueue PodGroup to %d Queues", len(jobsMap))
	for {
		if queues.Empty() {
			break
		}
		queue := queues.Pop().(*apis.QueueInfo)
		jobs, found := jobsMap[queue.UID]
		if !found || jobs.Empty() {
			continue
		}

		// enqueue the job with the highest priority to the corresponding CRD Queue
		job := jobs.Pop().(*apis.JobInfo)
		if job.PodGroup.Spec.MinResources == nil || sess.JobEnqueuable(job) {
			sess.JobEnqueued(job)
			job.PodGroup.Status.Phase = scheduling.PodGroupInqueue
			sess.Jobs[job.UID] = job
		}

		// Added Queue back until no job in Queue.
		queues.Push(queue)
	}
}

func (enqueue *Action) UnInitialize() {}
