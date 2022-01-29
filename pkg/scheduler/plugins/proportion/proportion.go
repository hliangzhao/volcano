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

package proportion

import (
	`github.com/hliangzhao/volcano/pkg/apis/scheduling`
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	`github.com/hliangzhao/volcano/pkg/scheduler/apis/helpers`
	`github.com/hliangzhao/volcano/pkg/scheduler/framework`
	`github.com/hliangzhao/volcano/pkg/scheduler/metrics`
	`k8s.io/klog/v2`
	`math`
)

const PluginName = "proportion"

type queueAttr struct {
	queueID apis.QueueID
	name    string
	weight  int32
	share   float64

	deserved  *apis.Resource
	allocated *apis.Resource
	request   *apis.Resource
	// inqueue represents the resource request of the inqueue job
	inqueue    *apis.Resource
	capability *apis.Resource
	// realCapability represents the resource limit of the queue, LessEqual capability
	realCapability *apis.Resource
	guarantee      *apis.Resource
}

type proportionPlugin struct {
	totalResource  *apis.Resource
	totalGuarantee *apis.Resource
	queueOpts      map[apis.QueueID]*queueAttr

	pluginArguments framework.Arguments
}

func New(arguments framework.Arguments) framework.Plugin {
	return &proportionPlugin{
		totalResource:   apis.EmptyResource(),
		totalGuarantee:  apis.EmptyResource(),
		queueOpts:       map[apis.QueueID]*queueAttr{},
		pluginArguments: arguments,
	}
}

func (pp *proportionPlugin) Name() string {
	return PluginName
}

func (pp *proportionPlugin) OnSessionOpen(sess *framework.Session) {
	pp.totalResource.Add(sess.TotalResource)
	klog.V(4).Infof("The total resource is <%v>", pp.totalResource)

	for _, queue := range sess.Queues {
		if len(queue.Queue.Spec.Guarantee.Resource) == 0 {
			continue
		}
		guarantee := apis.NewResource(queue.Queue.Spec.Guarantee.Resource)
		pp.totalGuarantee.Add(guarantee)
	}
	klog.V(4).Infof("The total guarantee resource is <%v>", pp.totalGuarantee)

	// Build attributes for Queues.
	for _, job := range sess.Jobs {
		klog.V(4).Infof("Considering Job <%s/%s>.", job.Namespace, job.Name)
		if _, found := pp.queueOpts[job.Queue]; !found {
			queue := sess.Queues[job.Queue]
			attr := &queueAttr{
				queueID: queue.UID,
				name:    queue.Name,
				weight:  queue.Weight,

				deserved:  apis.EmptyResource(),
				allocated: apis.EmptyResource(),
				request:   apis.EmptyResource(),
				inqueue:   apis.EmptyResource(),
				guarantee: apis.EmptyResource(),
			}

			// set capacity
			if len(queue.Queue.Spec.Capability) != 0 {
				attr.capability = apis.NewResource(queue.Queue.Spec.Capability)
				if attr.capability.MilliCPU <= 0 {
					attr.capability.MilliCPU = math.MaxFloat64
				}
				if attr.capability.Memory <= 0 {
					attr.capability.Memory = math.MaxFloat64
				}
			}
			if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
				attr.guarantee = apis.NewResource(queue.Queue.Spec.Guarantee.Resource)
			}

			// set real capacity
			realCapacity := pp.totalResource.Clone().Sub(pp.totalGuarantee).Add(attr.guarantee)
			if attr.capability == nil {
				attr.realCapability = realCapacity
			} else {
				attr.realCapability = helpers.Min(realCapacity, attr.capability)
			}

			pp.queueOpts[job.Queue] = attr
			klog.V(4).Infof("Added Queue <%s> attributes.", job.Queue)
		}

		attr := pp.queueOpts[job.Queue]
		for status, tasks := range job.TaskStatusIndex {
			if apis.AllocatedStatus(status) {
				for _, task := range tasks {
					attr.allocated.Add(task.ResReq)
					attr.request.Add(task.ResReq)
				}
			} else if status == apis.Pending {
				for _, task := range tasks {
					attr.request.Add(task.ResReq)
				}
			}
		}

		if job.PodGroup.Status.Phase == scheduling.PodGroupInqueue {
			attr.inqueue.Add(job.GetMinResources())
		}
	}

	// record metrics
	for _, attr := range pp.queueOpts {
		metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Memory)
		metrics.UpdateQueueRequest(attr.name, attr.request.MilliCPU, attr.request.Memory)
		metrics.UpdateQueueWeight(attr.name, attr.weight)
		queue := sess.Queues[attr.queueID]
		metrics.UpdateQueuePodGroupInqueueCount(attr.name, queue.Queue.Status.Inqueue)
		metrics.UpdateQueuePodGroupPendingCount(attr.name, queue.Queue.Status.Pending)
		metrics.UpdateQueuePodGroupRunningCount(attr.name, queue.Queue.Status.Running)
		metrics.UpdateQueuePodGroupUnknownCount(attr.name, queue.Queue.Status.Unknown)
	}

	// TODO
}

func (pp *proportionPlugin) OnSessionClose(sess *framework.Session) {
	pp.totalResource = nil
	pp.totalGuarantee = nil
	pp.queueOpts = nil
}

func (pp *proportionPlugin) updateShare(attr *queueAttr) {
	res := float64(0)

	// TODO: how to handle fragment issues?
	for _, resName := range attr.deserved.ResourceNames() {
		share := helpers.Share(attr.allocated.Get(resName), attr.deserved.Get(resName))
		if share > res {
			res = share
		}
	}

	attr.share = res
	metrics.UpdateQueueShare(attr.name, attr.share)
}
