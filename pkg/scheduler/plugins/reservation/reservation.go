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

package reservation

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/utils"
	"k8s.io/klog/v2"
	"time"
)

const PluginName = "reservation"

type reservationPlugin struct {
	pluginArguments framework.Arguments
}

func New(arguments framework.Arguments) framework.Plugin {
	return &reservationPlugin{pluginArguments: arguments}
}

func (rp *reservationPlugin) Name() string {
	return PluginName
}

func (rp *reservationPlugin) OnSessionOpen(sess *framework.Session) {
	// select the job which has the highest priority and waits for the longest duration
	sess.AddTargetJobFn(rp.Name(), func(jobs []*apis.JobInfo) *apis.JobInfo {
		if len(jobs) == 0 {
			return nil
		}
		priority := rp.getHighestPriority(jobs)
		// firstly, get the highest priority jobs
		highestPriorityJobs := rp.getHighestPriorityJobs(priority, jobs)
		// secondly, get the job with the longest waiting time from these jobs
		return rp.getTargetJob(highestPriorityJobs)
	})

	// select the nodes with the maximum idle resource from unlocked node list
	sess.AddReservedNodesFn(rp.Name(), func() {
		node := rp.getUnlockedNodesWithMaxIdle(sess)
		if node != nil {
			utils.Reservation.LockedNodes[node.Name] = node
			klog.V(3).Infof("locked node: %s", node.Name)
		}
	})
}

func (rp *reservationPlugin) OnSessionClose(sess *framework.Session) {}

func (rp *reservationPlugin) getJobWaitingTime(job *apis.JobInfo) time.Duration {
	if job == nil {
		return -1
	}
	now := time.Now()
	return now.Sub(job.ScheduleStartTimestamp.Time)
}

// getUnlockedNodesWithMaxIdle gets the node with the maximum idle resource from unlocked node list.
func (rp *reservationPlugin) getUnlockedNodesWithMaxIdle(sess *framework.Session) *apis.NodeInfo {
	var maxIdleNode *apis.NodeInfo
	for _, node := range sess.Nodes {
		hasLocked := false
		for _, lockNode := range utils.Reservation.LockedNodes {
			if node.Node.UID == lockNode.Node.UID {
				hasLocked = true
				break
			}
		}
		if !hasLocked && (maxIdleNode == nil || maxIdleNode.Idle.LessEqual(node.Idle, apis.Zero)) {
			maxIdleNode = node
		}
	}
	if maxIdleNode != nil {
		klog.V(3).Infof("Max idle node: %s:", maxIdleNode.Name)
	} else {
		klog.V(3).Info("Max idle node: nil")
	}

	return maxIdleNode
}

func (rp *reservationPlugin) getTargetJob(jobs []*apis.JobInfo) *apis.JobInfo {
	if len(jobs) == 0 {
		return nil
	}

	// init as the first job
	maxWaitDuration := rp.getJobWaitingTime(jobs[0])
	targetJob := jobs[0]

	for _, job := range jobs {
		waitingDuration := rp.getJobWaitingTime(job)
		if waitingDuration > maxWaitDuration {
			maxWaitDuration = waitingDuration
			targetJob = job
		}
	}
	klog.V(3).Infof("Target job ID: %s, Name: %s", targetJob.UID, targetJob.Name)
	return targetJob
}

func (rp *reservationPlugin) getHighestPriorityJobs(priority int32, jobs []*apis.JobInfo) []*apis.JobInfo {
	var highestPriorityJobs []*apis.JobInfo
	if len(jobs) == 0 {
		return highestPriorityJobs
	}
	for _, job := range jobs {
		if job.Priority == priority {
			highestPriorityJobs = append(highestPriorityJobs, job)
		}
	}
	return highestPriorityJobs
}

func (rp *reservationPlugin) getHighestPriority(jobs []*apis.JobInfo) int32 {
	if len(jobs) == 0 {
		return -1
	}
	highestPriority := jobs[0].Priority
	for _, job := range jobs {
		if job.Priority > highestPriority {
			highestPriority = job.Priority
		}
	}
	return highestPriority
}
