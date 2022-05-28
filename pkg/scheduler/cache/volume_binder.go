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

package cache

// fully checked and understood

import (
	`fmt`
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	corev1 `k8s.io/api/core/v1`
	`k8s.io/klog/v2`
	`k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding`
	`strings`
)

/* VolumeBinder is used to bind volumes to task pods. */

type defaultVolumeBinder struct {
	volumeBinder volumebinding.SchedulerVolumeBinder
}

// AllocateVolumes allocates volume on the host to the task.
func (vb *defaultVolumeBinder) AllocateVolumes(task *apis.TaskInfo, hostname string, podVolumes *volumebinding.PodVolumes) error {
	allBound, err := vb.volumeBinder.AssumePodVolumes(task.Pod, hostname, podVolumes)
	task.VolumeReady = allBound
	return err
}

// RevertVolumes reverts the allocated volume from task.
func (vb *defaultVolumeBinder) RevertVolumes(task *apis.TaskInfo, podVolumes *volumebinding.PodVolumes) {
	if podVolumes != nil {
		klog.Infof("Revert assumed volumes for task %v/%v on node %s", task.Namespace, task.Name, task.NodeName)
		vb.volumeBinder.RevertAssumedPodVolumes(podVolumes)
		task.VolumeReady = false
		task.PodVolumes = nil
	}
}

// GetPodVolumes gets pod volume binding status of task on the given node.
func (vb *defaultVolumeBinder) GetPodVolumes(task *apis.TaskInfo, node *corev1.Node) (*volumebinding.PodVolumes, error) {
	boundClaims, claimsToBind, unboundClaimsImmediate, err := vb.volumeBinder.GetPodVolumes(task.Pod)
	if err != nil {
		return nil, err
	}
	if len(unboundClaimsImmediate) > 0 {
		return nil, fmt.Errorf("pod has unbound immeidate PersistentVolumeClaims")
	}

	podVolumes, reasons, err := vb.volumeBinder.FindPodVolumes(task.Pod, boundClaims, claimsToBind, node)
	if err != nil {
		return nil, err
	} else if len(reasons) > 0 {
		var errors []string
		for _, reason := range reasons {
			errors = append(errors, string(reason))
		}
		return nil, fmt.Errorf(strings.Join(errors, ","))
	}
	return podVolumes, err
}

// BindVolumes binds volumes to the task.
func (vb *defaultVolumeBinder) BindVolumes(task *apis.TaskInfo, podVolumes *volumebinding.PodVolumes) error {
	// If task's volumes are ready, did not bind them again.
	if task.VolumeReady {
		return nil
	}
	return vb.volumeBinder.BindPodVolumes(task.Pod, podVolumes)
}
