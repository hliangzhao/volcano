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

package tasktopology

import (
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	`github.com/hliangzhao/volcano/pkg/scheduler/framework`
	`k8s.io/klog/v2`
)

type taskTopologyPlugin struct {
	arguments framework.Arguments
	weight    int
	managers  map[apis.JobID]*JobManager
}

func New(arguments framework.Arguments) framework.Plugin {
	return &taskTopologyPlugin{
		arguments: arguments,
		weight:    calculateWeight(arguments),
		managers:  map[apis.JobID]*JobManager{},
	}
}

func (ttp *taskTopologyPlugin) Name() string {
	return PluginName
}

func (ttp *taskTopologyPlugin) OnSessionOpen(sess *framework.Session) {}

func (ttp *taskTopologyPlugin) OnSessionClose(sess *framework.Session) {
	ttp.managers = nil
}

// TaskOrderFn returns -1 to make l prior to r.
//
// for example:
// A:
//  | bucket1   | bucket2   | out of bucket
//  | a1 a3     | a2        | a4
// B:
//  | bucket1   | out of bucket
//  | b1 b2     | b3
// the right task order should be:
//   a1 a3 a2 b1 b2 a4 b3
func (ttp *taskTopologyPlugin) TaskOrderFn(l, r interface{}) int {
	lv, ok := l.(*apis.TaskInfo)
	if !ok {
		klog.Errorf("Object l is not an apis.TaskInfo")
	}
	rv, ok := r.(*apis.TaskInfo)
	if !ok {
		klog.Errorf("Object r is not an apis.TaskInfo")
	}

	// get the corresponding job managers
	lvJobManager := ttp.managers[lv.Job]
	rvJobManager := ttp.managers[rv.Job]

	// get the corresponding buckets
	var lvBucket, rvBucket *Bucket
	if lvJobManager != nil {
		lvBucket = lvJobManager.GetBucket(lv)
	} else {
		klog.V(4).Infof("No job manager for job <ID: %s>, do not return task order.", lv.Job)
		return 0
	}
	if rvJobManager != nil {
		rvBucket = rvJobManager.GetBucket(rv)
	} else {
		klog.V(4).Infof("No job manager for job <ID: %s>, do not return task order.", rv.Job)
		return 0
	}

	// the one in bucket would always prior to another
	lvInBucket := lvBucket != nil
	rvInBucket := rvBucket != nil
	if lvInBucket != rvInBucket {
		if lvInBucket {
			return -1
		}
		return 1
	}

	// comparison between job is not the duty of this plugin
	if lv.Job != rv.Job {
		return 0
	}

	// task out of bucket have no order
	if !lvInBucket && !rvInBucket {
		return 0
	}

	// the big bucket should prior to small one
	lvHasTask := len(lvBucket.tasks)
	rvHasTask := len(rvBucket.tasks)
	if lvHasTask != rvHasTask {
		if lvHasTask > rvHasTask {
			return -1
		}
		return 1
	}

	lvBucketIndex := lvBucket.index
	rvBucketIndex := rvBucket.index
	// in the same bucket, the affinityOrder is ok
	if lvBucketIndex == rvBucketIndex {
		affinityOrder := lvJobManager.taskAffinityOrder(lv, rv)
		return -affinityOrder
	}

	// the old bucket should prior to young one
	if lvBucketIndex < rvBucketIndex {
		return -1
	}
	return 1
}

func (ttp *taskTopologyPlugin) calcBucketScore(task *apis.TaskInfo, node *apis.NodeInfo) (int, *JobManager, error) {
	// task could not fit the node, socre is 0
	maxResource := node.Idle.Clone().Add(node.Releasing)
	if req := task.ResReq; req != nil && maxResource.LessPartly(req, apis.Zero) {
		return 0, nil, nil
	}

	jobManager, hasManager := ttp.managers[task.Job]
	if !hasManager {
		return 0, nil, nil
	}

	bucket := jobManager.GetBucket(task)
	if bucket == nil {
		return 0, jobManager, nil
	}

	// 1. bound task in bucket is the base score of this node
	score := bucket.node[node.Name]

	// 2. task inter/self anti-affinity should be calculated
	if nodeTaskSet := jobManager.nodeTaskSet[node.Name]; nodeTaskSet != nil {
		taskName := getTaskName(task)
		affinityScore := jobManager.checkTaskSetAffinity(taskName, nodeTaskSet, true)
		if affinityScore < 0 {
			score += affinityScore
		}
	}
	klog.V(4).Infof("task %s/%s, node %s, additional score %d, task %d",
		task.Namespace, task.Name, node.Name, score, len(bucket.tasks))

	// 3. the other tasks in bucket take into considering
	score += len(bucket.tasks)
	if bucket.request == nil || bucket.request.LessEqual(maxResource, apis.Zero) {
		return score, jobManager, nil
	}

	remains := bucket.request.Clone()
	// randomly (by map) take out task to make the bucket fits the node
	for bucketTaskID, bucketTask := range bucket.tasks {
		// current task should keep in bucket
		if bucketTaskID == task.Pod.UID || bucketTask.ResReq == nil {
			continue
		}
		remains.Sub(bucketTask.ResReq)
		score--
		if remains.LessEqual(maxResource, apis.Zero) {
			break
		}
	}
	// here, the bucket remained request will always fit the maxResource
	return score, jobManager, nil
}
