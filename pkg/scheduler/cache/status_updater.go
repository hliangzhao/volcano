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
	`context`
	schedulingscheme `github.com/hliangzhao/volcano/pkg/apis/scheduling/scheme`
	schedulingv1alpha1 `github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1`
	volcanoclient `github.com/hliangzhao/volcano/pkg/client/clientset/versioned`
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	corev1 `k8s.io/api/core/v1`
	metav1 `k8s.io/apimachinery/pkg/apis/meta/v1`
	`k8s.io/client-go/kubernetes`
	`k8s.io/klog/v2`
	podutil `k8s.io/kubernetes/pkg/api/v1/pod`
)

/* StatusUpdater is used to update the status of pods and podgroups. */

// defaultStatusUpdater is the default implementation of the StatusUpdater interface
type defaultStatusUpdater struct {
	kubeClient *kubernetes.Clientset
	vcClient   *volcanoclient.Clientset
}

// UpdatePodCondition updates pod with podCondition in the cluster.
func (su *defaultStatusUpdater) UpdatePodCondition(pod *corev1.Pod, condition *corev1.PodCondition) (*corev1.Pod, error) {
	klog.V(3).Infof("Updating pod condition for %s/%s to (%s==%s)",
		pod.Namespace, pod.Name, condition.Type, condition.Status)
	if podutil.UpdatePodCondition(&pod.Status, condition) {
		return su.kubeClient.CoreV1().Pods(pod.Namespace).UpdateStatus(context.TODO(), pod, metav1.UpdateOptions{})
	}
	return pod, nil
}

// UpdatePodGroup updates the podgroup that represented by pg.
func (su *defaultStatusUpdater) UpdatePodGroup(pg *apis.PodGroup) (*apis.PodGroup, error) {
	// convert apis.podgroup into kind PodGroup
	podgroup := &schedulingv1alpha1.PodGroup{}
	if err := schedulingscheme.Scheme.Convert(&pg.PodGroup, podgroup, nil); err != nil {
		klog.Errorf("Error while converting apis.PodGroup to v1alpha1.PodGroup with error: %v", err)
		return nil, err
	}

	// call the client to update the kind resource
	updated, err := su.vcClient.SchedulingV1alpha1().PodGroups(podgroup.Namespace).Update(context.TODO(),
		podgroup, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Error while updating PodGroup with error: %v", err)
		return nil, err
	}

	// get the updated podgroup info
	pgInfo := &apis.PodGroup{Version: apis.PodGroupVersionV1Alpha1}
	if err := schedulingscheme.Scheme.Convert(updated, &pgInfo, nil); err != nil {
		klog.Errorf("Error while converting v1alpha.PodGroup to apis.PodGroup with error: %v", err)
		return nil, err
	}

	return pgInfo, nil
}
