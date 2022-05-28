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
	batchv1alpha1 `github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1`
	schedulingscheme `github.com/hliangzhao/volcano/pkg/apis/scheduling/scheme`
	schedulingv1alpha1 `github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1`
	volcanoclient `github.com/hliangzhao/volcano/pkg/client/clientset/versioned`
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	metav1 `k8s.io/apimachinery/pkg/apis/meta/v1`
	`k8s.io/client-go/kubernetes`
	`k8s.io/klog/v2`
)

/* pgBinder is used to bind job to podgroup. */

type pgBinder struct {
	kubeClient *kubernetes.Clientset
	vcClient   *volcanoclient.Clientset
}

// Bind binds job to podgroup. Specifically, it adds silo cluster annotation on pod and podgroup.
func (pgb *pgBinder) Bind(job *apis.JobInfo, cluster string) (*apis.JobInfo, error) {
	if len(job.Tasks) == 0 {
		klog.V(4).Infof("Job pods have not been created yet")
		return job, nil
	}

	for _, task := range job.Tasks {
		// update each task's annotation and resourceVersion
		pod := task.Pod
		pod.Annotations[batchv1alpha1.ForwardClusterKey] = cluster
		pod.ResourceVersion = ""

		_, err := pgb.kubeClient.CoreV1().Pods(pod.Namespace).UpdateStatus(context.TODO(), pod, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Error while update pod annotation with error: %v", err)
			return nil, err
		}
	}

	// set annotation on the podgroup and update
	pg := job.PodGroup
	pg.Annotations[batchv1alpha1.ForwardClusterKey] = cluster
	podgroup := &schedulingv1alpha1.PodGroup{}

	if err := schedulingscheme.Scheme.Convert(&pg.PodGroup, podgroup, nil); err != nil {
		klog.Errorf("Error while converting apis.PodGroup to v1alpha1.PodGroup with error: %v", err)
		return nil, err
	}

	newPg, err := pgb.vcClient.SchedulingV1alpha1().PodGroups(pg.Namespace).Update(context.TODO(),
		podgroup, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Error while update PodGroup annotation with error: %v", err)
		return nil, err
	}
	job.PodGroup.ResourceVersion = newPg.ResourceVersion

	klog.V(4).Infof("Bind PodGroup <%s> successfully", job.PodGroup.Name)
	return job, nil
}
