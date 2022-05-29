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

package drf

import (
	"fmt"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/metrics"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"math"
	"strconv"
	"strings"
)

// PluginName indicates name of volcano scheduler plugin.
const PluginName = "drf"

var shareDelta = 0.000001

type drfAttr struct {
	share            float64
	dominantResource string
	allocated        *apis.Resource
}

func (attr *drfAttr) String() string {
	return fmt.Sprintf("dominant resource <%s>, dominant share %f, allocated %s",
		attr.dominantResource, attr.share, attr.allocated)
}

// hierarchicalNode represents the node hierarchy
// and the corresponding weight and drf attribute.
type hierarchicalNode struct {
	parent *hierarchicalNode
	attr   *drfAttr

	// If the node is a leaf node,
	// request represents the request of the job.
	request   *apis.Resource
	weight    float64
	saturated bool
	hierarchy string
	children  map[string]*hierarchicalNode
}

func (hn *hierarchicalNode) Clone(parent *hierarchicalNode) *hierarchicalNode {
	newNode := &hierarchicalNode{
		parent: parent,
		attr: &drfAttr{
			share:            hn.attr.share,
			dominantResource: hn.attr.dominantResource,
			allocated:        hn.attr.allocated.Clone(),
		},
		request:   hn.request.Clone(),
		weight:    hn.weight,
		saturated: hn.saturated,
		hierarchy: hn.hierarchy,
		children:  nil,
	}
	if hn.children != nil {
		newNode.children = map[string]*hierarchicalNode{}
		for _, child := range hn.children {
			newNode.children[child.hierarchy] = child.Clone(newNode)
		}
	}
	return newNode
}

// resourceSaturated returns true if any requested resource of the job is saturated or the job demands fully allocated resource.
func resourceSaturated(allocated *apis.Resource, jobRequest *apis.Resource, demandingResources map[corev1.ResourceName]bool) bool {
	for _, resName := range allocated.ResourceNames() {
		allocatedQuantity := allocated.Get(resName)
		requestQuantity := jobRequest.Get(resName)
		if allocatedQuantity != 0 && requestQuantity != 0 && allocatedQuantity >= requestQuantity {
			return true
		}
		if !demandingResources[resName] && requestQuantity != 0 {
			return true
		}
	}
	return false
}

type drfPlugin struct {
	totalResource  *apis.Resource
	totalAllocated *apis.Resource

	jobAttrs         map[apis.JobID]*drfAttr
	namespaceOpts    map[string]*drfAttr
	hierarchicalRoot *hierarchicalNode
	pluginArguments  framework.Arguments
}

func New(arguments framework.Arguments) framework.Plugin {
	return &drfPlugin{
		totalResource:  apis.EmptyResource(),
		totalAllocated: apis.EmptyResource(),
		jobAttrs:       map[apis.JobID]*drfAttr{},
		namespaceOpts:  map[string]*drfAttr{},
		hierarchicalRoot: &hierarchicalNode{
			attr:      &drfAttr{allocated: apis.EmptyResource()},
			request:   apis.EmptyResource(),
			hierarchy: "root",
			weight:    1,
			children:  map[string]*hierarchicalNode{},
		},
		pluginArguments: arguments,
	}
}

func (dp *drfPlugin) Name() string {
	return PluginName
}

func (dp *drfPlugin) OnSessionOpen(sess *framework.Session) {
	// Prepare scheduling data for this session.
	dp.totalResource.Add(sess.TotalResource)
	klog.V(4).Infof("Total Allocatable %s", dp.totalResource)

	namespaceOrderEnabled := dp.NamespaceOrderEnabled(sess)
	hierarchyEnabled := dp.HierarchyEnabled(sess)

	for _, job := range sess.Jobs {
		attr := &drfAttr{
			allocated: apis.EmptyResource(),
		}
		for status, tasks := range job.TaskStatusIndex {
			if apis.AllocatedStatus(status) {
				for _, t := range tasks {
					attr.allocated.Add(t.ResReq)
				}
			}
		}
		// Calculate the init share of Job
		dp.updateJobShare(job.Namespace, job.Name, attr)
		dp.jobAttrs[job.UID] = attr

		if namespaceOrderEnabled {
			nsOpts, found := dp.namespaceOpts[job.Namespace]
			if !found {
				nsOpts = &drfAttr{
					allocated: apis.EmptyResource(),
				}
				dp.namespaceOpts[job.Namespace] = nsOpts
			}
			// all tasks in job should have the same namespace with job
			nsOpts.allocated.Add(attr.allocated)
			dp.updateNamespaceShare(job.Namespace, nsOpts)
		}

		if hierarchyEnabled {
			queue := sess.Queues[job.Queue]
			dp.totalAllocated.Add(attr.allocated)
			dp.UpdateHierarchicalShare(
				dp.hierarchicalRoot,
				dp.totalAllocated,
				job,
				attr,
				queue.Hierarchy,
				queue.Weights,
			)
		}
	}

	preemptableFn := func(preemptor *apis.TaskInfo, preemptees []*apis.TaskInfo) ([]*apis.TaskInfo, int) {
		var victims []*apis.TaskInfo

		addVictim := func(candidate *apis.TaskInfo) {
			victims = append(victims, candidate)
		}

		if namespaceOrderEnabled {
			// apply the namespace share policy on preemptee firstly
			lWeight := sess.NamespaceInfo[apis.NamespaceName(preemptor.Namespace)].GetWeight()
			lNSAttr := dp.namespaceOpts[preemptor.Namespace]
			lNSAlloc := lNSAttr.allocated.Clone().Add(preemptor.ResReq)
			_, lNSShare := dp.calculateShare(lNSAlloc, dp.totalResource)
			lNSShareWeighted := lNSShare / float64(lWeight)

			namespaceAllocation := map[string]*apis.Resource{}

			// undecidedPreemptees means this policy could not judge preemptee is preemptable or not
			// and left it to next policy
			var undecidedPreemptees []*apis.TaskInfo

			for _, preemptee := range preemptees {
				if preemptor.Namespace == preemptee.Namespace {
					// policy is disabled when they are in the same namespace
					undecidedPreemptees = append(undecidedPreemptees, preemptee)
					continue
				}

				// compute the preemptee namespace weighted share after preemption
				nsAllocation, found := namespaceAllocation[preemptee.Namespace]
				if !found {
					rNSAttr := dp.namespaceOpts[preemptee.Namespace]
					nsAllocation = rNSAttr.allocated.Clone()
					namespaceAllocation[preemptee.Namespace] = nsAllocation
				}
				rWeight := sess.NamespaceInfo[apis.NamespaceName(preemptee.Namespace)].GetWeight()
				rNSAlloc := nsAllocation.Sub(preemptee.ResReq)
				_, rNSShare := dp.calculateShare(rNSAlloc, dp.totalResource)
				rNSShareWeighted := rNSShare / float64(rWeight)

				// to avoid ping pong actions, the preemptee namespace should
				// have the higher weighted share after preemption.
				if lNSShareWeighted < rNSShareWeighted {
					addVictim(preemptee)
					continue
				}
				if lNSShareWeighted-rNSShareWeighted > shareDelta {
					continue
				}

				// equal namespace order leads to judgement of jobOrder
				undecidedPreemptees = append(undecidedPreemptees, preemptee)
			}

			preemptees = undecidedPreemptees
		}

		lAttr := dp.jobAttrs[preemptor.Job]
		lAlloc := lAttr.allocated.Clone().Add(preemptor.ResReq)
		_, ls := dp.calculateShare(lAlloc, dp.totalResource)

		allocations := map[apis.JobID]*apis.Resource{}

		for _, preemptee := range preemptees {
			if _, found := allocations[preemptee.Job]; !found {
				rAttr := dp.jobAttrs[preemptee.Job]
				allocations[preemptee.Job] = rAttr.allocated.Clone()
			}
			rAlloc := allocations[preemptee.Job].Sub(preemptee.ResReq)
			_, rs := dp.calculateShare(rAlloc, dp.totalResource)
			if ls < rs || math.Abs(ls-rs) <= shareDelta {
				addVictim(preemptee)
			}
		}
		klog.V(4).Infof("Victims from DRF plugins are %+v", victims)

		return victims, utils.Permit
	}
	sess.AddPreemptableFn(dp.Name(), preemptableFn)

	if hierarchyEnabled {
		queueOrderFn := func(l, r interface{}) int {
			lv := l.(*apis.QueueInfo)
			rv := r.(*apis.QueueInfo)
			ret := dp.compareQueues(dp.hierarchicalRoot, lv, rv)
			if ret < 0 {
				return -1
			}
			if ret > 0 {
				return 1
			}
			return 0
		}
		sess.AddQueueOrderFn(dp.Name(), queueOrderFn)

		reclaimFn := func(reclaimer *apis.TaskInfo, reclaimees []*apis.TaskInfo) ([]*apis.TaskInfo, int) {
			var victims []*apis.TaskInfo
			// clone h-drf tree
			totalAllocated := dp.totalAllocated.Clone()
			root := dp.hierarchicalRoot.Clone(nil)

			// update reclaimer h-drf
			lJob := sess.Jobs[reclaimer.Job]
			lQueue := sess.Queues[lJob.Queue]
			lJob = lJob.Clone()

			attr := dp.jobAttrs[lJob.UID]
			lAttr := &drfAttr{allocated: attr.allocated.Clone()}
			lAttr.allocated.Add(reclaimer.ResReq)
			totalAllocated.Add(reclaimer.ResReq)

			dp.updateShare(lAttr)
			dp.UpdateHierarchicalShare(
				root,
				totalAllocated,
				lJob,
				lAttr,
				lQueue.Hierarchy,
				lQueue.Weights,
			)

			for _, preemptee := range reclaimees {
				rJob := sess.Jobs[preemptee.Job]
				rQueue := sess.Queues[rJob.Queue]
				rJob = rJob.Clone()

				// update reclaimee h-drf
				attr := dp.jobAttrs[rJob.UID]
				rAttr := &drfAttr{allocated: attr.allocated.Clone()}
				rAttr.allocated.Sub(preemptee.ResReq)
				totalAllocated.Sub(preemptee.ResReq)

				dp.updateShare(rAttr)
				dp.UpdateHierarchicalShare(
					root,
					totalAllocated,
					rJob,
					rAttr,
					rQueue.Hierarchy,
					rQueue.Weights,
				)

				// compare h-drf of queues
				ret := dp.compareQueues(root, lQueue, rQueue)

				// resume h-drf of reclaimee job
				totalAllocated.Add(preemptee.ResReq)
				rAttr.allocated.Add(preemptee.ResReq)
				dp.updateShare(rAttr)
				dp.UpdateHierarchicalShare(
					root,
					totalAllocated,
					rJob,
					rAttr,
					rQueue.Hierarchy,
					rQueue.Weights,
				)

				if ret < 0 {
					victims = append(victims, preemptee)
				}

				if ret > shareDelta {
					continue
				}
			}
			klog.V(4).Infof("Victims from HDRF plugins are %+v", victims)

			return victims, utils.Permit
		}
		sess.AddReclaimableFn(dp.Name(), reclaimFn)
	}

	jobOrderFn := func(l, r interface{}) int {
		lv := l.(*apis.JobInfo)
		rv := r.(*apis.JobInfo)

		klog.V(4).Infof("DRF JobOrderFn: <%v/%v> share state: %v, <%v/%v> share state: %v",
			lv.Namespace, lv.Name, dp.jobAttrs[lv.UID].share, rv.Namespace, rv.Name, dp.jobAttrs[rv.UID].share)

		if dp.jobAttrs[lv.UID].share == dp.jobAttrs[rv.UID].share {
			return 0
		}

		if dp.jobAttrs[lv.UID].share < dp.jobAttrs[rv.UID].share {
			return -1
		}

		return 1
	}
	sess.AddJobOrderFn(dp.Name(), jobOrderFn)

	if namespaceOrderEnabled {
		namespaceOrderFn := func(l, r interface{}) int {
			lv := l.(apis.NamespaceName)
			rv := r.(apis.NamespaceName)

			lOpt := dp.namespaceOpts[string(lv)]
			rOpt := dp.namespaceOpts[string(rv)]

			lWeight := sess.NamespaceInfo[lv].GetWeight()
			rWeight := sess.NamespaceInfo[rv].GetWeight()

			klog.V(4).Infof("DRF NamespaceOrderFn: <%v> share state: %f, weight %v, <%v> share state: %f, weight %v",
				lv, lOpt.share, lWeight, rv, rOpt.share, rWeight)

			lWeightedShare := lOpt.share / float64(lWeight)
			rWeightedShare := rOpt.share / float64(rWeight)

			metrics.UpdateNamespaceWeight(string(lv), lWeight)
			metrics.UpdateNamespaceWeight(string(rv), rWeight)
			metrics.UpdateNamespaceWeightedShare(string(lv), lWeightedShare)
			metrics.UpdateNamespaceWeightedShare(string(rv), rWeightedShare)

			if lWeightedShare == rWeightedShare {
				return 0
			}

			if lWeightedShare < rWeightedShare {
				return -1
			}

			return 1
		}
		sess.AddNamespaceOrderFn(dp.Name(), namespaceOrderFn)
	}

	// Register event handlers.
	sess.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(e *framework.Event) {
			attr := dp.jobAttrs[e.Task.Job]
			attr.allocated.Add(e.Task.ResReq)

			job := sess.Jobs[e.Task.Job]
			dp.updateJobShare(job.Namespace, job.Name, attr)

			nsShare := -1.0
			if namespaceOrderEnabled {
				nsOpt := dp.namespaceOpts[e.Task.Namespace]
				nsOpt.allocated.Add(e.Task.ResReq)

				dp.updateNamespaceShare(e.Task.Namespace, nsOpt)
				nsShare = nsOpt.share
			}
			if hierarchyEnabled {
				queue := sess.Queues[job.Queue]
				dp.totalAllocated.Add(e.Task.ResReq)
				dp.UpdateHierarchicalShare(
					dp.hierarchicalRoot,
					dp.totalAllocated,
					job,
					attr,
					queue.Hierarchy,
					queue.Weights,
				)
			}
			klog.V(4).Infof("DRF AllocateFunc: task <%v/%v>, resReq <%v>,  share <%v>, namespace share <%v>",
				e.Task.Namespace, e.Task.Name, e.Task.ResReq, attr.share, nsShare)
		},
		DeallocateFunc: func(e *framework.Event) {
			attr := dp.jobAttrs[e.Task.Job]
			attr.allocated.Sub(e.Task.ResReq)

			job := sess.Jobs[e.Task.Job]
			dp.updateJobShare(job.Namespace, job.Name, attr)

			nsShare := -1.0
			if namespaceOrderEnabled {
				nsOpt := dp.namespaceOpts[e.Task.Namespace]
				nsOpt.allocated.Sub(e.Task.ResReq)

				dp.updateNamespaceShare(e.Task.Namespace, nsOpt)
				nsShare = nsOpt.share
			}
			if hierarchyEnabled {
				queue := sess.Queues[job.Queue]
				dp.totalAllocated.Sub(e.Task.ResReq)
				dp.UpdateHierarchicalShare(
					dp.hierarchicalRoot,
					dp.totalAllocated,
					job,
					attr,
					queue.Hierarchy,
					queue.Weights,
				)
			}
			klog.V(4).Infof("DRF EvictFunc: task <%v/%v>, resReq <%v>,  share <%v>, namespace share <%v>",
				e.Task.Namespace, e.Task.Name, e.Task.ResReq, attr.share, nsShare)
		},
	})
}

func (dp *drfPlugin) OnSessionClose(sess *framework.Session) {
	// Clean schedule data.
	dp.totalResource = apis.EmptyResource()
	dp.totalAllocated = apis.EmptyResource()
	dp.jobAttrs = map[apis.JobID]*drfAttr{}
}

func (dp *drfPlugin) HierarchyEnabled(sess *framework.Session) bool {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if plugin.Name != PluginName {
				continue
			}
			return plugin.EnabledHierarchy != nil && *plugin.EnabledHierarchy
		}
	}
	return false
}

func (dp *drfPlugin) NamespaceOrderEnabled(sess *framework.Session) bool {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if plugin.Name != PluginName {
				continue
			}
			return plugin.EnabledNamespaceOrder != nil && *plugin.EnabledNamespaceOrder
		}
	}
	return false
}

// calculateShare returns the resource name and its share if it has the maximum share in total kinds of resources.
func (dp *drfPlugin) calculateShare(allocated, totalResource *apis.Resource) (string, float64) {
	res := float64(0)
	dominantRes := ""
	for _, resName := range totalResource.ResourceNames() {
		share := apis.Share(allocated.Get(resName), totalResource.Get(resName))
		if share > res {
			res = share
			dominantRes = string(resName)
		}
	}
	return dominantRes, res
}

func (dp *drfPlugin) updateShare(attr *drfAttr) {
	attr.dominantResource, attr.share = dp.calculateShare(attr.allocated, dp.totalResource)
}

func (dp *drfPlugin) updateNamespaceShare(namespaceName string, attr *drfAttr) {
	dp.updateShare(attr)
	metrics.UpdateNamespaceShare(namespaceName, attr.share)
}

func (dp *drfPlugin) updateJobShare(jobNS, jobName string, attr *drfAttr) {
	dp.updateShare(attr)
	metrics.UpdateJobShare(jobNS, jobName, attr.share)
}

func (dp *drfPlugin) updateHierarchicalShare(node *hierarchicalNode, demandingResources map[corev1.ResourceName]bool) {
	if node.children == nil {
		node.saturated = resourceSaturated(node.attr.allocated, node.request, demandingResources)
		klog.V(4).Infof("Update hierarchical node %s, share %f, dominant %s, resource %v, saturated: %t",
			node.hierarchy, node.attr.share, node.attr.dominantResource, node.attr.allocated, node.saturated)
	} else {
		// get minimum dominant resource share
		var mdrShare float64 = 1
		for _, child := range node.children {
			dp.updateHierarchicalShare(child, demandingResources)
			// skip empty child and saturated child
			if child.attr.share != 0 && !child.saturated {
				_, resShare := dp.calculateShare(child.attr.allocated, dp.totalResource)
				if resShare < mdrShare {
					mdrShare = resShare
				}
			}
		}

		node.attr.allocated = apis.EmptyResource()
		saturated := true
		for _, child := range node.children {
			if !child.saturated {
				saturated = false
			}
			if child.attr.share != 0 {
				// saturated child is not scaled
				if child.saturated {
					t := child.attr.allocated
					node.attr.allocated.Add(t)
				} else {
					t := child.attr.allocated.Clone().Multi(mdrShare / child.attr.share)
					node.attr.allocated.Add(t)
				}
			}
		}
		node.attr.dominantResource, node.attr.share = dp.calculateShare(
			node.attr.allocated,
			dp.totalResource,
		)
		node.saturated = saturated
		klog.V(4).Infof("Update hierarchical node %s, share %f, dominant resource %s, resource %v, saturated: %t",
			node.hierarchy, node.attr.share, node.attr.dominantResource, node.attr.allocated, node.saturated)
	}
}

func (dp *drfPlugin) UpdateHierarchicalShare(root *hierarchicalNode, totalAllocated *apis.Resource, job *apis.JobInfo,
	attr *drfAttr, hierarchy, hierarchicalWeights string) {

	// get demanding resources first
	demandingResources := map[corev1.ResourceName]bool{}
	for _, resName := range dp.totalResource.ResourceNames() {
		if totalAllocated.Get(resName) < dp.totalResource.Get(resName) {
			demandingResources[resName] = true
		}
	}
	// build hierarchy and update share
	dp.buildHierarchy(root, job, attr, hierarchy, hierarchicalWeights)
	dp.updateHierarchicalShare(root, demandingResources)
}

func (dp *drfPlugin) compareQueues(root *hierarchicalNode, lQueue, rQueue *apis.QueueInfo) float64 {
	lNode := root
	lPaths := strings.Split(lQueue.Hierarchy, "/")
	rNode := root
	rPaths := strings.Split(rQueue.Hierarchy, "/")

	depth := 0
	if len(lPaths) < len(rPaths) {
		depth = len(lPaths)
	} else {
		depth = len(rPaths)
	}
	for i := 0; i < depth; i++ {
		// Saturated nodes who have the minimum priority,
		// so that demanding nodes will be popped first.
		if !lNode.saturated && rNode.saturated {
			return -1
		}
		if lNode.saturated && !rNode.saturated {
			return 1
		}
		if lNode.attr.share/lNode.weight == rNode.attr.share/rNode.weight {
			if i < depth-1 {
				lNode = lNode.children[lPaths[i+1]]
				rNode = rNode.children[rPaths[i+1]]
			}
		} else {
			return lNode.attr.share/lNode.weight - rNode.attr.share/rNode.weight
		}
	}
	return 0
}

func (dp *drfPlugin) buildHierarchy(root *hierarchicalNode, job *apis.JobInfo, attr *drfAttr, hierarchy, hierarchicalWeights string) {
	inode := root
	paths := strings.Split(hierarchy, "/")
	weights := strings.Split(hierarchicalWeights, "/")

	for i := 1; i < len(paths); i++ {
		if child, ok := inode.children[paths[i]]; ok {
			inode = child
		} else {
			// create child node
			fWeight, _ := strconv.ParseFloat(weights[i], 64)
			if fWeight < 1 {
				fWeight = 1
			}
			child = &hierarchicalNode{
				weight:    fWeight,
				hierarchy: paths[i],
				request:   apis.EmptyResource(),
				attr: &drfAttr{
					allocated: apis.EmptyResource(),
				},
				children: map[string]*hierarchicalNode{},
			}
			klog.V(4).Infof("Node %s added to %s, weight %f",
				child.hierarchy, inode.hierarchy, fWeight)
			inode.children[paths[i]] = child
			child.parent = inode
			inode = child
		}
	}

	child := &hierarchicalNode{
		weight:    1,
		attr:      attr,
		hierarchy: string(job.UID),
		request:   job.TotalRequest.Clone(),
		children:  nil,
	}
	inode.children[string(job.UID)] = child
	// update drf attribute bottom up
	klog.V(4).Infof("Job <%s/%s> added to %s, weights %s, attr %v, total request: %s",
		job.Namespace, job.Name, inode.hierarchy, hierarchicalWeights, child.attr, job.TotalRequest)
}
