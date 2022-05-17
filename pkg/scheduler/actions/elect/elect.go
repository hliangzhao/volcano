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

package elect

import (
	"github.com/hliangzhao/volcano/pkg/apis/scheduling"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/utils"
	"k8s.io/klog/v2"
)

const (
	Elect = "elect"
)

// Action elect selects the target job which is of the highest priority and waits for the longest time.
type Action struct{}

func New() *Action {
	return &Action{}
}

func (elect *Action) Name() string {
	return Elect
}

func (elect *Action) Initialize() {}

func (elect *Action) Execute(sess *framework.Session) {
	klog.V(3).Infof("Enter Elect ...")
	defer klog.V(3).Infof("Leaving Elect ...")

	if utils.Reservation.TargetJob == nil {
		klog.V(4).Infof("Start select Target Job")
		var pendingJobs []*apis.JobInfo
		for _, job := range sess.Jobs {
			if job.PodGroup.Status.Phase == scheduling.PodGroupPending {
				pendingJobs = append(pendingJobs, job)
			}
		}
		utils.Reservation.TargetJob = sess.TargetJob(pendingJobs)
		if utils.Reservation.TargetJob != nil {
			klog.V(3).Infof("Target Job name: %s", utils.Reservation.TargetJob.Name)
		} else {
			klog.V(3).Infof("Target Job name: nil")
		}
	}
}

func (elect *Action) UnInitialize() {}
