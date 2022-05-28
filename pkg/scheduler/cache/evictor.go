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
	`fmt`
	corev1 `k8s.io/api/core/v1`
	metav1 `k8s.io/apimachinery/pkg/apis/meta/v1`
	`k8s.io/client-go/kubernetes`
	`k8s.io/client-go/tools/record`
	`k8s.io/klog/v2`
	podutil `k8s.io/kubernetes/pkg/api/v1/pod`
)

/* Evictor is used to evict pod. */

type defaultEvictor struct {
	kubeClient *kubernetes.Clientset
	recorder   record.EventRecorder
}

// Evict evicts pod because of the given reason.
// The pod resource will be deleted from the cluster.
// Evict is used to guarantee that both task and job could be in the original state if error happens.
func (evictor *defaultEvictor) Evict(pod *corev1.Pod, reason string) error {
	klog.V(3).Infof("Evicting pod %v/%v, because of %v", pod.Namespace, pod.Name, reason)

	evictMsg := fmt.Sprintf("Pod is evicted, because of %v", reason)
	annotations := map[string]string{}
	// record event and add annotation simultaneously
	evictor.recorder.AnnotatedEventf(pod, annotations, corev1.EventTypeWarning, "Evict", evictMsg)

	newPod := pod.DeepCopy()
	cond := &corev1.PodCondition{
		Type:    corev1.PodReady,
		Status:  corev1.ConditionFalse,
		Reason:  "Evict",
		Message: evictMsg,
	}
	if !podutil.UpdatePodCondition(&newPod.Status, cond) {
		klog.V(1).Infof("UpdatePodCondition: existed condition, not update")
		klog.V(1).Infof("%+v", newPod.Status.Conditions)
		return nil
	}

	// update pod status then delete
	if _, err := evictor.kubeClient.CoreV1().Pods(pod.Namespace).UpdateStatus(context.TODO(),
		newPod, metav1.UpdateOptions{}); err != nil {
		klog.Errorf("Failed to update pod <%v/%v> status: %v", newPod.Namespace, newPod.Name, err)
		return err
	}
	if err := evictor.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(),
		pod.Name, metav1.DeleteOptions{}); err != nil {
		klog.Errorf("Failed to evict pod <%v/%v>: %#v", pod.Namespace, pod.Name, err)
		return err
	}

	return nil
}
