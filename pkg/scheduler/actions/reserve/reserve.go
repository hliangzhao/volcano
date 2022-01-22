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

package reserve

import (
	`github.com/hliangzhao/volcano/pkg/scheduler/framework`
	`github.com/hliangzhao/volcano/pkg/scheduler/utils`
	`k8s.io/klog/v2`
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (reserve *Action) Name() string {
	return "reserve"
}

func (reserve *Action) Initialize() {}

func (reserve *Action) Execute(sess *framework.Session) {
	klog.V(3).Infof("Enter Reserve ...")
	defer klog.V(3).Infof("Leaving Reserve ...")

	if utils.Reservation.TargetJob == nil {
		klog.V(4).Infof("No target job, skip reserve action")
		return
	}

	// if target job has not been scheduled, select a locked node for it
	// else reset target job and locked nodes
	targetJob := sess.Jobs[utils.Reservation.TargetJob.UID]
	if targetJob == nil {
		// target job scheduled, release all locked nodes
		klog.V(3).Infof("Target Job has been deleted. Reset target job")
		utils.Reservation.TargetJob = nil
		for node := range utils.Reservation.LockedNodes {
			delete(utils.Reservation.LockedNodes, node)
		}
		return
	}

	utils.Reservation.TargetJob = targetJob
	if !utils.Reservation.TargetJob.Ready() {
		// reserve nodes for target job
		sess.ReservedNodes()
	} else {
		klog.V(3).Infof("Target Job has been scheduled. Reset target job")
		utils.Reservation.TargetJob = nil
		for node := range utils.Reservation.LockedNodes {
			delete(utils.Reservation.LockedNodes, node)
		}
	}
}

func (reserve *Action) UnInitialize() {}
