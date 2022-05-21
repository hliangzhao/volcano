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

package shuffle

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"k8s.io/klog/v2"
)

const (
	Shuffle = "shuffle"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (shuffle *Action) Name() string {
	return Shuffle
}

func (shuffle *Action) Initialize() {}

func (shuffle *Action) Execute(sess *framework.Session) {
	klog.V(3).Infoln("Enter Shuffle ...")
	defer klog.V(3).Infoln("Leaving Shuffle ...")

	tasks := make([]*apis.TaskInfo, 0)
	for _, jobInfo := range sess.Jobs {
		for _, taskInfo := range jobInfo.Tasks {
			if taskInfo.Status == apis.Running {
				tasks = append(tasks, taskInfo)
			}
		}
	}

	victims := sess.VictimTasks(tasks)
	for victim := range victims {
		klog.V(5).Infof("Victim %s: [namespace: %s, job: %s]\n", victim.Name, victim.Namespace, victim.Job)
		if err := sess.Evict(victim, "shuffle"); err != nil {
			klog.Errorf("Failed to evict Task <%s/%s>: %v\n", victim.Namespace, victim.Name, err)
			continue
		}
	}
}

func (shuffle *Action) UnInitialize() {}
