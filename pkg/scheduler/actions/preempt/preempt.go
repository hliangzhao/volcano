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

package preempt

import (
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	`github.com/hliangzhao/volcano/pkg/scheduler/framework`
	`github.com/hliangzhao/volcano/pkg/scheduler/utils`
	`k8s.io/klog/v2`
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (preempt *Action) Name() string {
	return "preempt"
}

func (preempt *Action) Initialize() {}

func (preempt *Action) Execute(sess *framework.Session) {
	// TODO
}

func (preempt *Action) UnInitialize() {}

func preempt(sess *framework.Session, stmt *framework.Statement, preemptor *apis.TaskInfo,
	filter func(*apis.TaskInfo) bool, predicateHelper utils.PredicateHelper) (bool, error) {

	// TODO
	return false, nil
}

func victimTasks(sess *framework.Session) {
	stmt := framework.NewStatement(sess)
	vTasks := sess.VictimTasks()
	for _, v := range vTasks {
		if err := stmt.Evict(v.Clone(), "evict"); err != nil {
			klog.Errorf("Failed to evict Task <%s/%s>: %v",
				v.Namespace, v.Name, err)
			continue
		}
	}
	stmt.Commit()
}
