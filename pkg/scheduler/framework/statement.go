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
	"fmt"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/metrics"
	"k8s.io/klog/v2"
)

type Operation int8

const (
	Evict = iota
	Pipeline
	Allocate
)

type operation struct {
	name   Operation
	task   *apis.TaskInfo
	reason string
}

type Statement struct {
	operations []operation
	sess       *Session
}

func NewStatement(sess *Session) *Statement {
	return &Statement{
		sess: sess,
	}
}

func (s *Statement) Discard() {
	klog.V(3).Info("Discarding operations ...")
	for i := len(s.operations) - 1; i >= 0; i-- {
		op := s.operations[i]
		op.task.GenerateLastTxContext()
		switch op.name {
		case Evict:
			if err := s.unevict(op.task); err != nil {
				klog.Errorf("Failed to unevict task: %s", err.Error())
			}
		case Pipeline:
			if err := s.unpipeline(op.task); err != nil {
				klog.Errorf("Failed to unpipeline task: %s", err.Error())
			}
		case Allocate:
			if err := s.unallocate(op.task); err != nil {
				klog.Errorf("Failed to unallocate task: %s", err.Error())
			}
		}
	}
}

func (s *Statement) Commit() {
	klog.V(3).Info("Committing operations ...")
	for _, op := range s.operations {
		op.task.ClearLastTxContext()
		switch op.name {
		case Evict:
			err := s.evict(op.task, op.reason)
			if err != nil {
				klog.Errorf("Failed to evict task: %s", err.Error())
			}
		case Pipeline:
			s.pipeline(op.task)
		case Allocate:
			err := s.allocate(op.task)
			if err != nil {
				klog.Errorf("Failed to allocate task: for %s", err.Error())
			}
		}
	}
}

/* Evict, Pipeline, and Allocate implementation */

func (s *Statement) unevict(reclaimee *apis.TaskInfo) error {
	// update job status
	if job, found := s.sess.Jobs[reclaimee.Job]; found {
		if err := job.UpdateTaskStatus(reclaimee, apis.Running); err != nil {
			klog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
				reclaimee.Namespace, reclaimee.Name, apis.Releasing, s.sess.UID, err)
		}
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when binding.",
			reclaimee.Job, s.sess.UID)
	}

	// update node info that this job is dispatched
	if node, found := s.sess.Nodes[reclaimee.NodeName]; found {
		err := node.UpdateTask(reclaimee)
		if err != nil {
			klog.Errorf("Failed to update task <%v/%v> in node %v for: %s",
				reclaimee.Namespace, reclaimee.Name, reclaimee.NodeName, err.Error())
			return err
		}
	}

	// handle allocation event
	for _, handler := range s.sess.eventHandlers {
		if handler.AllocateFunc != nil {
			handler.AllocateFunc(&Event{
				Task: reclaimee,
			})
		}
	}

	return nil
}

func (s *Statement) evict(reclaimee *apis.TaskInfo, reason string) error {
	if err := s.sess.cache.Evict(reclaimee, reason); err != nil {
		// TODO: why in evict func, we call unevict func?
		//  there may exists problem when naming the `unevict` func!
		if e := s.unevict(reclaimee); e != nil {
			klog.Errorf("Failed to unevict task <%v/%v>: %v.",
				reclaimee.Namespace, reclaimee.Name, e)
		}
		return err
	}
	return nil
}

func (s *Statement) Evict(reclaimee *apis.TaskInfo, reason string) error {
	// update job status
	if job, found := s.sess.Jobs[reclaimee.Job]; found {
		if err := job.UpdateTaskStatus(reclaimee, apis.Releasing); err != nil {
			klog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
				reclaimee.Namespace, reclaimee.Name, apis.Releasing, s.sess.UID, err)
		}
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when binding.",
			reclaimee.Job, s.sess.UID)
	}

	// update node info that this job is dispatched
	if node, found := s.sess.Nodes[reclaimee.NodeName]; found {
		err := node.UpdateTask(reclaimee)
		if err != nil {
			klog.Errorf("Failed to update task <%v/%v> in node %v for: %s",
				reclaimee.Namespace, reclaimee.Name, reclaimee.NodeName, err.Error())
			return err
		}
	}

	// handle allocation event
	for _, handler := range s.sess.eventHandlers {
		if handler.DeallocateFunc != nil {
			handler.DeallocateFunc(&Event{
				Task: reclaimee,
			})
		}
	}

	s.operations = append(s.operations, operation{
		name:   Evict,
		task:   reclaimee,
		reason: reason,
	})

	return nil
}

func (s *Statement) pipeline(task *apis.TaskInfo) {
	// TODO: not implemented?
}

func (s *Statement) Pipeline(task *apis.TaskInfo, hostname string) error {
	if job, found := s.sess.Jobs[task.Job]; found {
		if err := job.UpdateTaskStatus(task, apis.Pipelined); err != nil {
			klog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
				task.Namespace, task.Name, apis.Pipelined, s.sess.UID, err)
		}
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when binding.",
			task.Job, s.sess.UID)
	}

	task.NodeName = hostname

	if node, found := s.sess.Nodes[hostname]; found {
		if err := node.AddTask(task); err != nil {
			klog.Errorf("Failed to pipeline task <%v/%v> to node <%v> in Session <%v>: %v",
				task.Namespace, task.Name, hostname, s.sess.UID, err)
		}
		klog.V(3).Infof("After pipelined Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		klog.Errorf("Failed to find Node <%s> in Session <%s> index when binding.",
			hostname, s.sess.UID)
	}

	for _, handler := range s.sess.eventHandlers {
		if handler.AllocateFunc != nil {
			handler.AllocateFunc(&Event{
				Task: task,
			})
		}
	}

	s.operations = append(s.operations, operation{
		name: Pipeline,
		task: task,
	})

	return nil
}

func (s *Statement) unpipeline(task *apis.TaskInfo) error {
	if job, found := s.sess.Jobs[task.Job]; found {
		if err := job.UpdateTaskStatus(task, apis.Pending); err != nil {
			klog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
				// TODO: should be apis.Pending! [changed]
				task.Namespace, task.Name, apis.Pending, s.sess.UID, err)
		}
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when binding.",
			task.Job, s.sess.UID)
	}

	if node, found := s.sess.Nodes[task.NodeName]; found {
		if err := node.RemoveTask(task); err != nil {
			// TODO: `pipeline` -> `remove` [changed]
			klog.Errorf("Failed to remove task <%v/%v> to node <%v> in Session <%v>: %v",
				task.Namespace, task.Name, task.NodeName, s.sess.UID, err)
		}
		// TODO: `pipeline` -> `remove` [changed]
		klog.V(3).Infof("After remove Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		klog.Errorf("Failed to find Node <%s> in Session <%s> index when binding.",
			task.NodeName, s.sess.UID)
	}

	for _, eh := range s.sess.eventHandlers {
		if eh.DeallocateFunc != nil {
			eh.DeallocateFunc(&Event{
				Task: task,
			})
		}
	}
	task.NodeName = ""

	return nil
}

func (s *Statement) allocate(task *apis.TaskInfo) error {
	// bind task to node and then update task status
	if err := s.sess.cache.AddBindTask(task); err != nil {
		return err
	}
	if job, found := s.sess.Jobs[task.Job]; found {
		if err := job.UpdateTaskStatus(task, apis.Binding); err != nil {
			klog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
				task.Namespace, task.Name, apis.Binding, s.sess.UID, err)
			return err
		}
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when binding.",
			task.Job, s.sess.UID)
		return fmt.Errorf("failed to find job %s", task.Job)
	}

	// update metrics
	metrics.UpdateTaskScheduleDuration(metrics.Duration(task.Pod.CreationTimestamp.Time))
	return nil
}

func (s *Statement) Allocate(task *apis.TaskInfo, nodeInfo *apis.NodeInfo) error {
	// get pod volumes and allocate it on the host, then, update task status
	podVolumes, err := s.sess.cache.GetPodVolumes(task, nodeInfo.Node)
	if err != nil {
		return err
	}
	hostname := nodeInfo.Name
	if err = s.sess.cache.AllocateVolumes(task, hostname, podVolumes); err != nil {
		return err
	}
	task.Pod.Spec.NodeName = hostname
	task.PodVolumes = podVolumes

	// update status
	if job, found := s.sess.Jobs[task.Job]; found {
		if err = job.UpdateTaskStatus(task, apis.Allocated); err != nil {
			klog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
				task.Namespace, task.Name, apis.Allocated, s.sess.UID, err)
			return err
		}
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when binding.",
			task.Job, s.sess.UID)
		return fmt.Errorf("failed to find job %s", task.Job)
	}

	// update node
	task.NodeName = hostname
	if node, found := s.sess.Nodes[hostname]; found {
		if err = node.AddTask(task); err != nil {
			klog.Errorf("Failed to add task <%v/%v> to node <%v> in Session <%v>: %v",
				task.Namespace, task.Name, hostname, s.sess.UID, err)
			return err
		}
		klog.V(3).Infof("After allocated Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		klog.Errorf("Failed to find Node <%s> in Session <%s> index when binding.",
			hostname, s.sess.UID)
		return fmt.Errorf("failed to find node %s", hostname)
	}

	for _, eh := range s.sess.eventHandlers {
		if eh.AllocateFunc != nil {
			eh.AllocateFunc(&Event{
				Task: task,
			})
		}
	}

	klog.V(3).Info("Allocating operations ...")
	s.operations = append(s.operations, operation{
		name: Allocate,
		task: task,
	})

	return nil
}

func (s *Statement) unallocate(task *apis.TaskInfo) error {
	if job, found := s.sess.Jobs[task.Job]; found {
		if err := job.UpdateTaskStatus(task, apis.Pending); err != nil {
			klog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
				task.Namespace, task.Name, apis.Pending, s.sess.UID, err)
		}
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when unallocating.",
			task.Job, s.sess.UID)
	}

	if node, found := s.sess.Nodes[task.NodeName]; found {
		klog.V(3).Infof("Remove Task <%v> on node <%v>", task.Name, task.NodeName)
		err := node.RemoveTask(task)
		if err != nil {
			klog.Errorf("Failed to remove Task <%v> on node <%v>: %s", task.Name, task.NodeName, err.Error())
		}
	}

	for _, eh := range s.sess.eventHandlers {
		if eh.DeallocateFunc != nil {
			eh.DeallocateFunc(&Event{
				Task: task,
			})
		}
	}
	task.NodeName = ""

	return nil
}
