/*
Copyright 2021-2022 hliangzhao.

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

package apis

import (
	"fmt"
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
)

type Request struct {
	Namespace  string
	JobName    string
	TaskName   string
	QueueName  string
	Event      busv1alpha1.Event
	ExitCode   int32
	Action     busv1alpha1.Action
	JobVersion int32
}

func (r Request) String() string {
	return fmt.Sprintf("Queue: %s, Job: %s/%s, Task:%s, Event:%s, ExitCode:%d, Action:%s, JobVersion: %d",
		r.QueueName, r.Namespace, r.JobName, r.TaskName, r.Event, r.ExitCode, r.Action, r.JobVersion)
}
