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

package priority

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils"
	"k8s.io/klog/v2"
)

const PluginName = "priority"

type priorityPlugin struct {
	args framework.Arguments
}

func New(arguments framework.Arguments) framework.Plugin {
	return &priorityPlugin{
		args: arguments,
	}
}

func (pp *priorityPlugin) Name() string {
	return PluginName
}

func (pp *priorityPlugin) OnSessionOpen(sess *framework.Session) {
	taskOrderFn := func(l, r interface{}) int {
		lv := l.(*apis.TaskInfo)
		rv := l.(*apis.TaskInfo)

		klog.V(4).Infof("Priority TaskOrder: <%v/%v> priority is %v, <%v/%v> priority is %v",
			lv.Namespace, lv.Name, lv.Priority, rv.Namespace, rv.Name, rv.Priority)

		if lv.Priority == rv.Priority {
			return 0
		}
		if lv.Priority > rv.Priority {
			return -1
		}
		return 1
	}
	sess.AddTaskOrderFn(pp.Name(), taskOrderFn)

	jobOrderFn := func(l, r interface{}) int {
		lv := l.(*apis.JobInfo)
		rv := r.(*apis.JobInfo)

		klog.V(4).Infof("Priority JobOrderFn: <%v/%v> priority: %d, <%v/%v> priority: %d",
			lv.Namespace, lv.Name, lv.Priority, rv.Namespace, rv.Name, rv.Priority)

		if lv.Priority > rv.Priority {
			return -1
		}
		if lv.Priority < rv.Priority {
			return 1
		}
		return 0
	}
	sess.AddJobOrderFn(pp.Name(), jobOrderFn)

	preemptableFn := func(preemptor *apis.TaskInfo, preemptees []*apis.TaskInfo) ([]*apis.TaskInfo, int) {
		preemptorJob := sess.Jobs[preemptor.Job]

		var victims []*apis.TaskInfo
		for _, preemptee := range preemptees {
			preempteeJob := sess.Jobs[preemptee.Job]
			if preempteeJob.UID != preemptorJob.UID {
				// Preemption between Jobs within Queue
				if preempteeJob.Priority >= preemptorJob.Priority {
					klog.V(4).Infof("Can not preempt task <%v/%v>"+
						"because preemptee job has greater or equal job priority (%d) than preemptor (%d)",
						preemptee.Namespace, preemptee.Name, preempteeJob.Priority, preemptorJob.Priority)
				} else {
					victims = append(victims, preemptee)
				}
			} else {
				// same job's different tasks should compare task's priority
				if preemptee.Priority >= preemptor.Priority {
					klog.V(4).Infof("Can not preempt task <%v/%v>"+
						"because preemptee task has greater or equal task priority (%d) than preemptor (%d)",
						preemptee.Namespace, preemptee.Name, preemptee.Priority, preemptor.Priority)
				} else {
					victims = append(victims, preemptee)
				}
			}
		}
		klog.V(4).Infof("Victims from Priority plugins are %+v", victims)
		return victims, utils.Permit
	}
	sess.AddPreemptableFn(pp.Name(), preemptableFn)

	jobStarvingFn := func(obj interface{}) bool {
		ji := obj.(*apis.JobInfo)
		return ji.ReadyTaskNum()+ji.WaitingTaskNum() < int32(len(ji.Tasks))
	}
	sess.AddJobStarvingFns(pp.Name(), jobStarvingFn)
}

func (pp *priorityPlugin) OnSessionClose(sess *framework.Session) {}
