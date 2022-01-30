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
	"github.com/hliangzhao/volcano/pkg/apis/scheduling"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis/helpers"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/metrics"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"math"
	"reflect"
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

	remaining := pp.totalResource.Clone()
	meet := map[apis.QueueID]struct{}{}
	for {
		totalWeight := int32(0)
		for _, attr := range pp.queueOpts {
			if _, found := meet[attr.queueID]; found {
				continue
			}
			totalWeight += attr.weight
		}

		if totalWeight == 0 {
			klog.V(4).Infof("Exiting when total weight is 0")
			break
		}

		oldRemaining := remaining.Clone()
		// Calculates the deserved resources of each Queue.
		// increasedDeserved is the increased value for attr.deserved of processed queues
		// decreasedDeserved is the decreased value for attr.deserved of processed queues
		increasedDeserved := apis.EmptyResource()
		decreasedDeserved := apis.EmptyResource()
		for _, attr := range pp.queueOpts {
			klog.V(4).Infof("Considering Queue <%s>: weight <%d>, total weight <%d>.",
				attr.name, attr.weight, totalWeight)
			if _, found := meet[attr.queueID]; found {
				continue
			}

			oldDeserved := attr.deserved.Clone()
			// add resources to this queue according to the proportional weight
			attr.deserved.Add(remaining.Clone().Multi(float64(attr.weight) / float64(totalWeight)))

			if attr.realCapability != nil {
				attr.deserved.MinDimensionResource(attr.realCapability, apis.Infinity)
			}
			attr.deserved.MinDimensionResource(attr.request, apis.Zero)
			klog.V(4).Infof("Format queue <%s> deserved resource to <%v>", attr.name, attr.deserved)

			// check this queue's request is meet or not
			if attr.request.LessEqual(attr.deserved, apis.Zero) {
				meet[attr.queueID] = struct{}{}
				klog.V(4).Infof("queue <%s> is meet", attr.name)
			} else if reflect.DeepEqual(attr.deserved, oldDeserved) {
				meet[attr.queueID] = struct{}{}
				klog.V(4).Infof("queue <%s> is meet cause of the capability", attr.name)
			}
			attr.deserved = helpers.Max(attr.deserved, attr.guarantee)
			pp.updateShare(attr)
			klog.V(4).Infof("The attributes of queue <%s> in proportion: deserved <%v>, realCapability <%v>, allocate <%v>, request <%v>, share <%0.2f>",
				attr.name, attr.deserved, attr.realCapability, attr.allocated, attr.request, attr.share)

			increased, decreased := attr.deserved.Diff(oldDeserved, apis.Zero)
			increasedDeserved.Add(increased)
			decreasedDeserved.Add(decreased)

			// Record metrics
			metrics.UpdateQueueDeserved(attr.name, attr.deserved.MilliCPU, attr.deserved.Memory)
		}

		remaining.Sub(increasedDeserved).Add(decreasedDeserved)
		klog.V(4).Infof("Remaining resource is <%s>", remaining)
		if remaining.IsEmpty() || reflect.DeepEqual(remaining, oldRemaining) {
			klog.V(4).Infof("Exiting when remaining is empty or no queue has more resource request:  <%v>", remaining)
			break
		}
	}

	sess.AddQueueOrderFn(pp.Name(), func(l, r interface{}) int {
		lv := l.(*apis.QueueInfo)
		rv := r.(*apis.QueueInfo)

		if pp.queueOpts[lv.UID].share == pp.queueOpts[rv.UID].share {
			return 0
		} else if pp.queueOpts[lv.UID].share < pp.queueOpts[rv.UID].share {
			return -1
		}
		return 1
	})

	sess.AddReclaimableFn(pp.Name(), func(reclaimer *apis.TaskInfo, reclaimees []*apis.TaskInfo) ([]*apis.TaskInfo, int) {
		var victims []*apis.TaskInfo
		allocations := map[apis.QueueID]*apis.Resource{}

		for _, reclaimee := range reclaimees {
			job := sess.Jobs[reclaimee.Job]
			attr := pp.queueOpts[job.Queue]

			if _, found := allocations[job.Queue]; !found {
				allocations[job.Queue] = attr.allocated.Clone()
			}
			allocated := allocations[job.Queue]
			if allocated.LessPartly(reclaimer.ResReq, apis.Zero) {
				klog.V(3).Infof("Failed to allocate resource for Task <%s/%s> in Queue <%s>, not enough resource.",
					reclaimee.Namespace, reclaimee.Name, job.Queue)
				continue
			}

			// only allocated greater than deserved, this queue's resource will be retrieved
			if !allocated.LessEqual(attr.deserved, apis.Zero) {
				allocated.Sub(reclaimee.ResReq)
				victims = append(victims, reclaimee)
			}
		}
		klog.V(4).Infof("Victims from proportion plugins are %+v", victims)
		return victims, utils.Permit
	})

	sess.AddOverusedFn(pp.Name(), func(obj interface{}) bool {
		queue := obj.(*apis.QueueInfo)
		attr := pp.queueOpts[queue.UID]

		overused := attr.deserved.LessEqual(attr.allocated, apis.Zero)
		metrics.UpdateQueueOverused(attr.name, overused)
		if overused {
			klog.V(3).Infof("Queue <%v>: deserved <%v>, allocated <%v>, share <%v>",
				queue.Name, attr.deserved, attr.allocated, attr.share)
		}

		return overused
	})

	sess.AddUnderusedResourceFn(pp.Name(), func(queue *apis.QueueInfo) apis.ResourceNameList {
		underusedResNames := apis.ResourceNameList{}
		attr := pp.queueOpts[queue.UID]

		_, underusedRes := attr.allocated.Diff(attr.deserved, apis.Zero)
		if underusedRes.MilliCPU >= apis.GetMinResource() {
			underusedResNames = append(underusedResNames, corev1.ResourceCPU)
		}
		if underusedRes.Memory >= apis.GetMinResource() {
			underusedResNames = append(underusedResNames, corev1.ResourceMemory)
		}
		for resName, resValue := range underusedRes.ScalarResources {
			if resValue > 0 {
				underusedResNames = append(underusedResNames, resName)
			}
		}
		klog.V(3).Infof("Queue <%v>: deserved <%v>, allocated <%v>, share <%v>, underUsedResName %v",
			queue.Name, attr.deserved, attr.allocated, attr.share, underusedResNames)

		return underusedResNames
	})

	sess.AddJobEnqueuableFn(pp.Name(), func(obj interface{}) int {
		job := obj.(*apis.JobInfo)
		queueID := job.Queue
		attr := pp.queueOpts[queueID]
		queue := sess.Queues[queueID]

		// If no capability is set, always enqueue the job.
		if attr.realCapability == nil {
			klog.V(4).Infof("Capability of queue <%s> was not set, allow job <%s/%s> to Inqueue.",
				queue.Name, job.Namespace, job.Name)
			return utils.Permit
		}

		// If no minResources is set, always enqueue the job.
		if job.PodGroup.Spec.MinResources == nil {
			klog.V(4).Infof("job %s MinResources is null.", job.Name)
			return utils.Permit
		}
		minReq := job.GetMinResources()

		klog.V(5).Infof("job %s min resource <%s>, queue %s capability <%s> allocated <%s>",
			job.Name, minReq.String(), queue.Name, attr.realCapability.String(), attr.allocated.String())

		// The queue resource quota limit has not reached
		inqueue := minReq.Add(attr.allocated).Add(attr.inqueue).LessEqual(attr.realCapability, apis.Infinity)
		klog.V(5).Infof("job %s inqueue %v", job.Name, inqueue)
		if inqueue {
			attr.inqueue.Add(job.GetMinResources())
			return utils.Permit
		}
		return utils.Reject
	})

	sess.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(e *framework.Event) {
			job := sess.Jobs[e.Task.Job]
			attr := pp.queueOpts[job.Queue]
			attr.allocated.Add(e.Task.ResReq)
			metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Memory)

			pp.updateShare(attr)

			klog.V(4).Infof("Proportion AllocateFunc: task <%v/%v>, resReq <%v>,  share <%v>",
				e.Task.Namespace, e.Task.Name, e.Task.ResReq, attr.share)
		},
		DeallocateFunc: func(e *framework.Event) {
			job := sess.Jobs[e.Task.Job]
			attr := pp.queueOpts[job.Queue]
			attr.allocated.Sub(e.Task.ResReq)
			metrics.UpdateQueueAllocated(attr.name, attr.allocated.MilliCPU, attr.allocated.Memory)

			pp.updateShare(attr)

			klog.V(4).Infof("Proportion DeallocateFunc: task <%v/%v>, resReq <%v>,  share <%v>",
				e.Task.Namespace, e.Task.Name, e.Task.ResReq, attr.share)
		},
	})
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
