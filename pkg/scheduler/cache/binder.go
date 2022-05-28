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
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	corev1 `k8s.io/api/core/v1`
	metav1 `k8s.io/apimachinery/pkg/apis/meta/v1`
	`k8s.io/client-go/kubernetes`
	`k8s.io/klog/v2`
)

/* Binder is used to bind task pod to host (node) in the cluster. */

type defaultBinder struct {
	// kubeClient *kubernetes.Clientset
}

func NewBinder() *defaultBinder {
	return &defaultBinder{}
}

// Bind sends bind request to apiserver through the kube client.
func (binder *defaultBinder) Bind(kubeClient *kubernetes.Clientset, tasks []*apis.TaskInfo) ([]*apis.TaskInfo, error) {
	var errTasks []*apis.TaskInfo
	// for each task, the node to bind is already set in `task.NodeName`
	for _, task := range tasks {
		pod := task.Pod
		if err := kubeClient.CoreV1().Pods(pod.Namespace).Bind(
			context.TODO(),
			&corev1.Binding{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   pod.Namespace,
					Name:        pod.Name,
					UID:         pod.UID,
					Annotations: pod.Annotations,
				},
				Target: corev1.ObjectReference{
					Kind: "Node",
					Name: task.NodeName, // the to-be-bind node has already been written into task.NodeName
				},
			},
			metav1.CreateOptions{},
		); err != nil {
			klog.Errorf("Failed to bind pod <%v/%v> to node %s : %#v", pod.Namespace, pod.Name, task.NodeName, err)
			errTasks = append(errTasks, task)
		}
	}

	if len(errTasks) > 0 {
		return errTasks, fmt.Errorf("failed to bind pods")
	}

	return nil, nil
}
