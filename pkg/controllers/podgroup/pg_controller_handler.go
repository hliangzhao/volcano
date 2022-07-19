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

package podgroup

// fully checked and understood

import (
	"context"
	"github.com/hliangzhao/volcano/pkg/apis/helpers"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/quota/v1/evaluator/core"
	"k8s.io/utils/clock"
)

type podRequest struct {
	podName      string
	podNamespace string
}

// addPod adds an obj (a pod actually) to the work-podQueue of the pgController.
func (pgC *pgController) addPod(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		klog.Errorf("Failed to convert %v to corev1.Pod", obj)
		return
	}
	pgC.podQueue.Add(podRequest{
		podName:      pod.Name,
		podNamespace: pod.Namespace,
	})
}

// updatePodAnnotations updates the annotation which denotes the podgroup that this pod belongs to.
func (pgC *pgController) updatePodAnnotations(pod *corev1.Pod, pgName string) error {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	if pod.Annotations[schedulingv1alpha1.KubeGroupNameAnnotationKey] == "" {
		pod.Annotations[schedulingv1alpha1.KubeGroupNameAnnotationKey] = pgName
	} else {
		if pod.Annotations[schedulingv1alpha1.KubeGroupNameAnnotationKey] != pgName {
			klog.Errorf("normal pod %s/%s annotations %s value is not %s, but %s", pod.Namespace, pod.Name,
				schedulingv1alpha1.KubeGroupNameAnnotationKey, pgName, pod.Annotations[schedulingv1alpha1.KubeGroupNameAnnotationKey])
		}
		return nil
	}
	// update the pod resource in cluster
	if _, err := pgC.kubeClient.CoreV1().Pods(pod.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{}); err != nil {
		klog.Errorf("Failed to update pod <%s/%s>: %v", pod.Namespace, pod.Name, err)
		return err
	}
	return nil
}

// createNormalPodPGIfNotExist creates a podgroup resource in cluster if the given pod does not belong to one.
// The main logic in this func is setting the relations between the newly created podgroup and the input pod.
func (pgC *pgController) createNormalPodPGIfNotExist(pod *corev1.Pod) error {
	// TODO: judge `pod.Annotations == nil` should be placed here
	pgName := helpers.GeneratePodGroupName(pod)
	if _, err := pgC.pgLister.PodGroups(pod.Namespace).Get(pgName); err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to get normal PodGroup for Pod <%s/%s>: %v", pod.Namespace, pod.Name, err)
			return err
		}

		// podgroup not found, create one for the pod
		obj := &schedulingv1alpha1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       pod.Namespace,
				Name:            pgName,
				OwnerReferences: newPGOwnerReferences(pod), // set the OwnerReferences of the podgroup as the same as the pod
				Annotations:     map[string]string{},
				Labels:          map[string]string{},
			},
			Spec: schedulingv1alpha1.PodGroupSpec{
				MinMember:         1,
				PriorityClassName: pod.Spec.PriorityClassName,
				MinResources:      calcPGMinResources(pod), // NOTE that a podgroup main contain multiple pods
			},
			// a newly created podgroup is in Pending phase
			Status: schedulingv1alpha1.PodGroupStatus{
				Phase: schedulingv1alpha1.PodGroupPending,
			},
		}

		// set the podQueue name which this podgroup belongs to
		if queueName, ok := pod.Annotations[schedulingv1alpha1.QueueNameAnnotationKey]; ok {
			obj.Spec.Queue = queueName
		}

		// set other annotations of the podgroup according to the annotations of the pod
		if value, ok := pod.Annotations[schedulingv1alpha1.PodPreemptable]; ok {
			obj.Annotations[schedulingv1alpha1.PodPreemptable] = value
		}
		if value, ok := pod.Annotations[schedulingv1alpha1.RevocableZone]; ok {
			obj.Annotations[schedulingv1alpha1.RevocableZone] = value
		}
		if value, ok := pod.Labels[schedulingv1alpha1.PodPreemptable]; ok {
			obj.Labels[schedulingv1alpha1.PodPreemptable] = value
		}
		if value, found := pod.Annotations[schedulingv1alpha1.JDBMinAvailable]; found {
			obj.Annotations[schedulingv1alpha1.JDBMinAvailable] = value
		} else if value, found = pod.Annotations[schedulingv1alpha1.JDBMaxUnavailable]; found {
			obj.Annotations[schedulingv1alpha1.JDBMaxUnavailable] = value
		}

		// finally, we create the podgroup resource in cluster
		if _, err = pgC.volcanoClient.SchedulingV1alpha1().PodGroups(pod.Namespace).Create(context.TODO(),
			obj, metav1.CreateOptions{}); err != nil {
			klog.Errorf("Failed to create normal PodGroup for Pod <%s/%s>: %v",
				pod.Namespace, pod.Name, err)
			return err
		}
	}
	// do not forget to update the annotation which indicates the podgroup that the pod belongs to
	return pgC.updatePodAnnotations(pod, pgName)
}

// newPGOwnerReferences return the OwnerReferences of the given pod.
func newPGOwnerReferences(pod *corev1.Pod) []metav1.OwnerReference {
	if len(pod.OwnerReferences) != 0 {
		// if this pod is managed by a controller, then an entry in the list `pod.OwnerReferences`
		// will point to this controller, with the controller field set to true.
		for _, ownerRef := range pod.OwnerReferences {
			if ownerRef.Controller != nil && *ownerRef.Controller {
				return pod.OwnerReferences
			}
		}
	}
	// if we reach here, it means that the input pod is not managed by any controller,
	// just set a new controller reference for it
	gvk := schema.GroupVersionKind{
		Group:   corev1.SchemeGroupVersion.Group,
		Version: corev1.SchemeGroupVersion.Version,
		Kind:    "Pod",
	}
	ref := metav1.NewControllerRef(pod, gvk)
	return []metav1.OwnerReference{*ref}
}

// func addResourceList(list, req, limit corev1.ResourceList) {
// 	// update Requests to list
// 	for name, quantity := range req {
// 		if value, ok := list[name]; !ok {
// 			list[name] = quantity.DeepCopy()
// 		} else {
// 			value.Add(quantity)
// 			list[name] = value
// 		}
// 	}
//
// 	if req != nil {
// 		return
// 	}
// 	// If Requests is omitted for a container,
// 	// it defaults to Limits if that is explicitly specified.
// 	for name, quantity := range limit {
// 		if value, ok := list[name]; !ok {
// 			list[name] = quantity.DeepCopy()
// 		} else {
// 			value.Add(quantity)
// 			list[name] = value
// 		}
// 	}
// }

// calcPGMinResources returns the minimal ResourceList to successfully start the pod.
func calcPGMinResources(pod *corev1.Pod) *corev1.ResourceList {
	// pgMinRes := corev1.ResourceList{}
	// for _, c := range pod.Spec.Containers {
	// 	addResourceList(pgMinRes, c.Resources.Requests, c.Resources.Limits)
	// }
	pgMinRes, _ := core.PodUsageFunc(pod, clock.RealClock{})
	return &pgMinRes
}
