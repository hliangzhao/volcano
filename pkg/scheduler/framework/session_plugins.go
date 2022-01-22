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

package framework

import (
	"github.com/hliangzhao/volcano/pkg/apis/scheduling"
	"github.com/hliangzhao/volcano/pkg/controllers/job/helpers"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

func isEnabled(enabled *bool) bool {
	return enabled != nil && *enabled
}

/* Add plugin functions of different topics to session */

// AddJobOrderFn add job order function.
func (sess *Session) AddJobOrderFn(name string, fn apis.CompareFn) {
	sess.jobOrderFns[name] = fn
}

// AddQueueOrderFn add queue order function.
func (sess *Session) AddQueueOrderFn(name string, fn apis.CompareFn) {
	sess.queueOrderFns[name] = fn
}

// AddClusterOrderFn add queue order function.
func (sess *Session) AddClusterOrderFn(name string, fn apis.CompareFn) {
	sess.clusterOrderFns[name] = fn
}

// AddTaskOrderFn add task order function.
func (sess *Session) AddTaskOrderFn(name string, fn apis.CompareFn) {
	sess.taskOrderFns[name] = fn
}

// AddNamespaceOrderFn add namespace order function.
func (sess *Session) AddNamespaceOrderFn(name string, fn apis.CompareFn) {
	sess.namespaceOrderFns[name] = fn
}

// AddPreemptableFn add preemptable function.
func (sess *Session) AddPreemptableFn(name string, fn apis.EvictableFn) {
	sess.preemptableFns[name] = fn
}

// AddReclaimableFn add Reclaimable function.
func (sess *Session) AddReclaimableFn(name string, fn apis.EvictableFn) {
	sess.reclaimableFns[name] = fn
}

// AddJobReadyFn add JobReady function.
func (sess *Session) AddJobReadyFn(name string, fn apis.ValidateFn) {
	sess.jobReadyFns[name] = fn
}

// AddJobPipelinedFn add pipelined function.
func (sess *Session) AddJobPipelinedFn(name string, fn apis.VoteFn) {
	sess.jobPipelinedFns[name] = fn
}

// AddPredicateFn add Predicate function.
func (sess *Session) AddPredicateFn(name string, fn apis.PredicateFn) {
	sess.predicateFns[name] = fn
}

// AddBestNodeFn add BestNode function.
func (sess *Session) AddBestNodeFn(name string, fn apis.BestNodeFn) {
	sess.bestNodeFns[name] = fn
}

// AddNodeOrderFn add Node order function.
func (sess *Session) AddNodeOrderFn(name string, fn apis.NodeOrderFn) {
	sess.nodeOrderFns[name] = fn
}

// AddBatchNodeOrderFn add Batch Node order function.
func (sess *Session) AddBatchNodeOrderFn(name string, fn apis.BatchNodeOrderFn) {
	sess.batchNodeOrderFns[name] = fn
}

// AddNodeMapFn add Node map function.
func (sess *Session) AddNodeMapFn(name string, fn apis.NodeMapFn) {
	sess.nodeMapFns[name] = fn
}

// AddNodeReduceFn add Node reduce function.
func (sess *Session) AddNodeReduceFn(name string, fn apis.NodeReduceFn) {
	sess.nodeReduceFns[name] = fn
}

// AddOverusedFn add overused function.
func (sess *Session) AddOverusedFn(name string, fn apis.ValidateFn) {
	sess.overUsedFns[name] = fn
}

// AddUnderusedResourceFn add underused function.
func (sess *Session) AddUnderusedResourceFn(name string, fn apis.UnderUsedResourceFn) {
	sess.underUsedFns[name] = fn
}

// AddJobValidFn add job valid function.
func (sess *Session) AddJobValidFn(name string, fn apis.ValidateExFn) {
	sess.jobValidFns[name] = fn
}

// AddJobEnqueuableFn add job enqueuable function.
func (sess *Session) AddJobEnqueuableFn(name string, fn apis.VoteFn) {
	sess.jobEnqueuableFns[name] = fn
}

// AddJobEnqueuedFn add jobEnqueued function.
func (sess *Session) AddJobEnqueuedFn(name string, fn apis.JobEnqueuedFn) {
	sess.jobEnqueuedFns[name] = fn
}

// AddTargetJobFn add target job function.
func (sess *Session) AddTargetJobFn(name string, fn apis.TargetJobFn) {
	sess.targetJobFns[name] = fn
}

// AddReservedNodesFn add reservedNodesFn function.
func (sess *Session) AddReservedNodesFn(name string, fn apis.ReservedNodesFn) {
	sess.reservedNodesFns[name] = fn
}

// AddVictimTasksFns add victimTasksFns function.
func (sess *Session) AddVictimTasksFns(name string, fn apis.VictimTasksFn) {
	sess.victimTasksFns[name] = fn
}

// AddJobStarvingFns add jobStarvingFns function.
func (sess *Session) AddJobStarvingFns(name string, fn apis.ValidateFn) {
	sess.jobStarvingFns[name] = fn
}

/* Retrieve functions from all registered plugins of specific topics */

// Reclaimable invokes the reclaimable func of the plugins and returns the reclaimed tasks.
func (sess *Session) Reclaimable(reclaimer *apis.TaskInfo, reclaimees []*apis.TaskInfo) []*apis.TaskInfo {
	var victims []*apis.TaskInfo
	var init bool

	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			// get reclaim func if allowed
			if !isEnabled(plugin.EnabledReclaimable) {
				continue
			}
			reclaimFunc, found := sess.reclaimableFns[plugin.Name]
			if !found {
				continue
			}

			candidates, abstain := reclaimFunc(reclaimer, reclaimees)
			if abstain == 0 {
				continue
			}
			if len(candidates) == 0 {
				victims = nil
				break
			}
			if !init {
				victims = candidates
				init = true
			} else {
				var intersection []*apis.TaskInfo
				// Get intersection of victims and candidates.
				for _, v := range victims {
					for _, c := range candidates {
						if v.UID == c.UID {
							intersection = append(intersection, v)
						}
					}
				}

				// Update victims to intersection
				victims = intersection
			}
		}
		// Plugins in this tier made decision if victims is not nil
		if victims != nil {
			return victims
		}
	}
	return victims
}

// Preemptable invokes the preemptable func of the plugins and returns the preemptable tasks.
func (sess *Session) Preemptable(preemptor *apis.TaskInfo, preemptees []*apis.TaskInfo) []*apis.TaskInfo {
	var victims []*apis.TaskInfo
	var init bool

	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledPreemptable) {
				continue
			}

			pf, found := sess.preemptableFns[plugin.Name]
			if !found {
				continue
			}
			candidates, abstain := pf(preemptor, preemptees)
			if abstain == 0 {
				continue
			}
			// intersection will be nil if length is 0, don't need to do any more check
			if len(candidates) == 0 {
				victims = nil
				break
			}

			if !init {
				victims = candidates
				init = true
			} else {
				var intersection []*apis.TaskInfo
				// Get intersection of victims and candidates.
				for _, v := range victims {
					for _, c := range candidates {
						if v.UID == c.UID {
							intersection = append(intersection, v)
						}
					}
				}

				// Update victims to intersection
				victims = intersection
			}
		}
		// Plugins in this tier made decision if victims is not nil
		if victims != nil {
			return victims
		}
	}

	return victims
}

// Overused invokes the overused function of the plugins, and returns true if the input queue is indeed overused.
func (sess *Session) Overused(queue *apis.QueueInfo) bool {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			of, found := sess.overUsedFns[plugin.Name]
			if !found {
				continue
			}
			if of(queue) {
				return true
			}
		}
	}
	return false
}

// UnderusedResources invokes the underused func of the plugins, and it returns underUsedResourceList.
func (sess *Session) UnderusedResources(queue *apis.QueueInfo) apis.ResourceNameList {
	if len(sess.underUsedFns) == 0 {
		return nil
	}
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			of, found := sess.underUsedFns[plugin.Name]
			if !found {
				continue
			}
			underUsedResourceList := of(queue)
			return underUsedResourceList
		}
	}
	return apis.ResourceNameList{}
}

// JobReady invoke job ready function of the plugins, and it returns true is the input obj is ready.
func (sess *Session) JobReady(obj interface{}) bool {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledJobReady) {
				continue
			}
			jrf, found := sess.jobReadyFns[plugin.Name]
			if !found {
				continue
			}
			if !jrf(obj) {
				return false
			}
		}
	}
	return true
}

// JobPipelined invoke pipelined function of the plugins.
// Check if job has get enough resource to run, it returns true if at least one plugin votes permit.
func (sess *Session) JobPipelined(obj interface{}) bool {
	var hasFound bool
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledJobPipelined) {
				continue
			}
			jrf, found := sess.jobPipelinedFns[plugin.Name]
			if !found {
				continue
			}

			res := jrf(obj)
			if res < 0 {
				return false
			}
			if res > 0 {
				hasFound = true
			}
		}
		// if plugin exists that votes permit, meanwhile other plugin votes abstention,
		// permit job to be pipelined, do not check next tier
		if hasFound {
			return true
		}
	}
	return true
}

// JobStarving invoke jobStarving function of the plugins.
// Check if job still need more resource.
func (sess *Session) JobStarving(obj interface{}) bool {
	var hasFound bool
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledJobStarving) {
				continue
			}
			jrf, found := sess.jobStarvingFns[plugin.Name]
			if !found {
				continue
			}
			hasFound = true

			if !jrf(obj) {
				return false
			}
		}
		// this tier registered function
		if hasFound {
			return true
		}
	}

	return false
}

// JobValid invokes job valid function of the plugins.
func (sess *Session) JobValid(obj interface{}) *apis.ValidateResult {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			jrf, found := sess.jobValidFns[plugin.Name]
			if !found {
				continue
			}

			if vr := jrf(obj); vr != nil && !vr.Pass {
				return vr
			}
		}
	}

	return nil
}

// JobEnqueuable invokes jobEnqueuableFns function of the plugins.
func (sess *Session) JobEnqueuable(obj interface{}) bool {
	var hasFound bool
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledJobEnqueued) {
				continue
			}
			fn, found := sess.jobEnqueuableFns[plugin.Name]
			if !found {
				continue
			}

			res := fn(obj)
			if res < 0 {
				return false
			}
			if res > 0 {
				hasFound = true
			}
		}
		// if plugin exists that votes permit, meanwhile other plugin votes abstention,
		// permit job to be enqueuable, do not check next tier
		if hasFound {
			return true
		}
	}
	return true
}

// JobEnqueued invokes jobEnqueuedFns function of the plugins.
func (sess *Session) JobEnqueued(obj interface{}) {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledJobEnqueued) {
				continue
			}
			fn, found := sess.jobEnqueuedFns[plugin.Name]
			if !found {
				continue
			}

			fn(obj)
		}
	}
}

// TargetJob invokes targetJobFns function of the plugins. It returns the target job from the job list.
func (sess *Session) TargetJob(jobs []*apis.JobInfo) *apis.JobInfo {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledTargetJob) {
				continue
			}
			fn, found := sess.targetJobFns[plugin.Name]
			if !found {
				continue
			}
			return fn(jobs)
		}
	}
	return nil
}

// VictimTasks invokes ReservedNodes function of the plugins.
func (sess *Session) VictimTasks() []*apis.TaskInfo {
	var victims []*apis.TaskInfo
	var init bool

	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledVictim) {
				continue
			}

			pf, found := sess.victimTasksFns[plugin.Name]
			if !found {
				continue
			}
			candidates := pf()
			if !init {
				victims = candidates
				init = true
			} else {
				var intersection []*apis.TaskInfo
				// Get intersection of victims and candidates.
				for _, v := range victims {
					for _, c := range candidates {
						if v.UID == c.UID {
							intersection = append(intersection, v)
						}
					}
				}

				// Update victims to intersection
				victims = intersection
			}
		}
		// Plugins in this tier made decision if victims is not nil
		if victims != nil {
			return victims
		}
	}

	return victims
}

// ReservedNodes invokes ReservedNodes function of the plugins.
func (sess *Session) ReservedNodes() {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledReservedNodes) {
				continue
			}
			fn, found := sess.reservedNodesFns[plugin.Name]
			if !found {
				continue
			}
			fn()
		}
	}
}

// JobOrderFn invokes job order function of the plugins.
func (sess *Session) JobOrderFn(l, r interface{}) bool {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledJobOrder) {
				continue
			}
			jof, found := sess.jobOrderFns[plugin.Name]
			if !found {
				continue
			}
			if j := jof(l, r); j != 0 {
				return j < 0
			}
		}
	}

	// If no job order funcs, order job by CreationTimestamp first, then by UID.
	lv := l.(*apis.JobInfo)
	rv := r.(*apis.JobInfo)
	if lv.CreationTimestamp.Equal(&rv.CreationTimestamp) {
		return lv.UID < rv.UID
	}
	return lv.CreationTimestamp.Before(&rv.CreationTimestamp)
}

// NamespaceOrderFn invokes namespace order function of the plugins.
func (sess *Session) NamespaceOrderFn(l, r interface{}) bool {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledNamespaceOrder) {
				continue
			}
			nof, found := sess.namespaceOrderFns[plugin.Name]
			if !found {
				continue
			}
			if j := nof(l, r); j != 0 {
				return j < 0
			}
		}
	}

	// TODO: if all NamespaceOrderFn treat these two namespaces as the same,
	//  we should make the job order have its affect among namespaces.
	//  or just schedule namespace one by one
	lv := l.(apis.NamespaceName)
	rv := r.(apis.NamespaceName)
	return lv < rv
}

// ClusterOrderFn invokes ClusterOrderFn function of the plugins.
func (sess *Session) ClusterOrderFn(l, r interface{}) bool {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledClusterOrder) {
				continue
			}
			cof, found := sess.clusterOrderFns[plugin.Name]
			if !found {
				continue
			}
			if j := cof(l, r); j != 0 {
				return j < 0
			}
		}
	}

	// If no cluster order funcs, order cluster by ClusterID
	lv := l.(*scheduling.Cluster)
	rv := r.(*scheduling.Cluster)
	return lv.Name < rv.Name
}

// QueueOrderFn invokes queue order function of the plugins.
func (sess *Session) QueueOrderFn(l, r interface{}) bool {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledQueueOrder) {
				continue
			}
			qof, found := sess.queueOrderFns[plugin.Name]
			if !found {
				continue
			}
			if j := qof(l, r); j != 0 {
				return j < 0
			}
		}
	}

	// If no queue order funcs, order queue by CreationTimestamp first, then by UID.
	lv := l.(*apis.QueueInfo)
	rv := r.(*apis.QueueInfo)
	if lv.Queue.CreationTimestamp.Equal(&rv.Queue.CreationTimestamp) {
		return lv.UID < rv.UID
	}
	return lv.Queue.CreationTimestamp.Before(&rv.Queue.CreationTimestamp)
}

// TaskCompareFns invokes task order function of the plugins.
func (sess *Session) TaskCompareFns(l, r interface{}) int {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledTaskOrder) {
				continue
			}
			tof, found := sess.taskOrderFns[plugin.Name]
			if !found {
				continue
			}
			if j := tof(l, r); j != 0 {
				return j
			}
		}
	}

	return 0
}

// TaskOrderFn invokes task order function of the plugins.
func (sess *Session) TaskOrderFn(l, r interface{}) bool {
	if res := sess.TaskCompareFns(l, r); res != 0 {
		return res < 0
	}

	// If no task order funcs, order task by default func.
	lv := l.(*apis.TaskInfo)
	rv := r.(*apis.TaskInfo)
	return helpers.CompareTask(lv, rv)
}

// PredicateFn invokes predicate function of the plugins.
func (sess *Session) PredicateFn(task *apis.TaskInfo, node *apis.NodeInfo) error {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledPredicate) {
				continue
			}
			pfn, found := sess.predicateFns[plugin.Name]
			if !found {
				continue
			}
			err := pfn(task, node)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// BestNodeFn invokes bestNode function of the plugins.
func (sess *Session) BestNodeFn(task *apis.TaskInfo, nodeScores map[float64][]*apis.NodeInfo) *apis.NodeInfo {
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledBestNode) {
				continue
			}
			pfn, found := sess.bestNodeFns[plugin.Name]
			if !found {
				continue
			}
			// TODO: Only the first plugin that enables and realizes bestNodeFn is allowed to choose the best node for task
			//  Maybe a iterative algorithm is required to get the best node among all plugins
			if bestNode := pfn(task, nodeScores); bestNode != nil {
				return bestNode
			}
		}
	}
	return nil
}

// NodeOrderFn invokes node order function of the plugins.
func (sess *Session) NodeOrderFn(task *apis.TaskInfo, node *apis.NodeInfo) (float64, error) {
	priorityScore := 0.0
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledNodeOrder) {
				continue
			}
			pfn, found := sess.nodeOrderFns[plugin.Name]
			if !found {
				continue
			}
			score, err := pfn(task, node)
			if err != nil {
				return 0, err
			}
			priorityScore += score
		}
	}
	return priorityScore, nil
}

// BatchNodeOrderFn invokes node order function of the plugins.
func (sess *Session) BatchNodeOrderFn(task *apis.TaskInfo, nodes []*apis.NodeInfo) (map[string]float64, error) {
	priorityScore := make(map[string]float64, len(nodes))
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledNodeOrder) {
				continue
			}
			pfn, found := sess.batchNodeOrderFns[plugin.Name]
			if !found {
				continue
			}
			score, err := pfn(task, nodes)
			if err != nil {
				return nil, err
			}
			for nodeName, score := range score {
				priorityScore[nodeName] += score
			}
		}
	}
	return priorityScore, nil
}

// NodeOrderMapFn invokes node order function of the plugins.
func (sess *Session) NodeOrderMapFn(task *apis.TaskInfo, node *apis.NodeInfo) (map[string]float64, float64, error) {
	nodeScoreMap := map[string]float64{}
	var priorityScore float64
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledNodeOrder) {
				continue
			}
			if pfn, found := sess.nodeOrderFns[plugin.Name]; found {
				score, err := pfn(task, node)
				if err != nil {
					return nodeScoreMap, priorityScore, err
				}
				priorityScore += score
			}
			if pfn, found := sess.nodeMapFns[plugin.Name]; found {
				score, err := pfn(task, node)
				if err != nil {
					return nodeScoreMap, priorityScore, err
				}
				nodeScoreMap[plugin.Name] = score
			}
		}
	}
	return nodeScoreMap, priorityScore, nil
}

// NodeOrderReduceFn invokes node order function of the plugins.
func (sess *Session) NodeOrderReduceFn(task *apis.TaskInfo, pluginNodeScoreMap map[string]k8sframework.NodeScoreList) (map[string]float64, error) {
	nodeScoreMap := map[string]float64{}
	for _, tier := range sess.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledNodeOrder) {
				continue
			}
			pfn, found := sess.nodeReduceFns[plugin.Name]
			if !found {
				continue
			}
			if err := pfn(task, pluginNodeScoreMap[plugin.Name]); err != nil {
				return nodeScoreMap, err
			}
			for _, hp := range pluginNodeScoreMap[plugin.Name] {
				nodeScoreMap[hp.Name] += float64(hp.Score)
			}
		}
	}
	return nodeScoreMap, nil
}
