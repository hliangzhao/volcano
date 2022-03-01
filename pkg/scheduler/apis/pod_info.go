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

package apis

import (
	"fmt"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"gopkg.in/square/go-jose.v2/json"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"strconv"
	"strings"
	"time"
)

// Refer k8s.io/kubernetes/pkg/scheduler/algorithm/predicates/predicates.go#GetResourceRequest.
//
// GetResourceRequest returns a *Resource that covers the largest width in each resource dimension.
// Because init-containers run sequentially, we collect the max in each dimension iteratively.
// In contrast, we sum the resource vectors for regular containers since they run simultaneously.
//
// To be consistent with kubernetes default scheduler, it is only used for predicates of actions (e.g.
// allocate, backfill, preempt, reclaim), please use GetPodResourceWithoutInitContainers for other cases.
//
// Example:
//
// Pod:
//   InitContainers
//     IC1:
//       CPU: 2
//       Memory: 1G
//     IC2:
//       CPU: 2
//       Memory: 3G
//   Containers
//     C1:
//       CPU: 2
//       Memory: 1G
//     C2:
//       CPU: 1
//       Memory: 1G
//
// Result: CPU: 3, Memory: 3G

// GetPodResourceWithoutInitContainers returns Pod's resource request, it does not contain
// init containers' resource request.
func GetPodResourceWithoutInitContainers(pod *corev1.Pod) *Resource {
	result := EmptyResource()
	for _, container := range pod.Spec.Containers {
		result.Add(NewResource(container.Resources.Requests))
	}
	return result
}

// GetPodResourceRequest returns all the resource required for that pod.
func GetPodResourceRequest(pod *corev1.Pod) *Resource {
	result := GetPodResourceWithoutInitContainers(pod)
	for _, container := range pod.Spec.InitContainers {
		result.SetMaxResource(NewResource(container.Resources.Requests))
	}
	return result
}

// GetPodPreemptable returns the value of annotation/label `volcano.sh/preemptable` of pod.
func GetPodPreemptable(pod *corev1.Pod) bool {
	// check annotations
	if len(pod.Annotations) > 0 {
		if value, found := pod.Annotations[schedulingv1alpha1.PodPreemptable]; found {
			b, err := strconv.ParseBool(value)
			if err != nil {
				klog.Warningf("invalid %s=%s", schedulingv1alpha1.PodPreemptable, value)
				return false
			}
			return b
		}
	}

	// check labels
	if len(pod.Labels) > 0 {
		if value, found := pod.Labels[schedulingv1alpha1.PodPreemptable]; found {
			b, err := strconv.ParseBool(value)
			if err != nil {
				klog.Warningf("invalid %s=%s", schedulingv1alpha1.PodPreemptable, value)
				return false
			}
			return b
		}
	}

	return false
}

// GetPodRevocableZone return `volcano.sh/revocable-zone` value for pod/podgroup.
func GetPodRevocableZone(pod *corev1.Pod) string {
	if len(pod.Annotations) > 0 {
		if value, found := pod.Annotations[schedulingv1alpha1.RevocableZone]; found {
			if value != "*" {
				return ""
			}
			return value
		}
		if value, found := pod.Annotations[schedulingv1alpha1.PodPreemptable]; found {
			if b, err := strconv.ParseBool(value); err == nil && b {
				return "*"
			}
		}
	}
	return ""
}

// GetPodTopologyInfo return `volcano.sh/numa-topology-policy` value for pod.
func GetPodTopologyInfo(pod *corev1.Pod) *TopologyInfo {
	info := TopologyInfo{
		ResMap: map[int]corev1.ResourceList{},
	}

	if len(pod.Annotations) > 0 {
		// set policy and res map
		if value, found := pod.Annotations[schedulingv1alpha1.NumaPolicyKey]; found {
			info.Policy = value
		}
		if value, found := pod.Annotations[TopologyDecisionAnnotation]; found {
			decision := PodResourceDecision{}
			if err := json.Unmarshal([]byte(value), &decision); err != nil {
				info.ResMap = decision.NumaResources
			}
		}
	}
	return &info
}

// GetGPUIndex returns the index of GPU that pod consumed.
func GetGPUIndex(pod *corev1.Pod) int {
	// TODO: what about a pod consumes more than 1 GPU? Should returns a slice
	if len(pod.Annotations) > 0 {
		value, found := pod.Annotations[GPUIndex]
		if found {
			id, err := strconv.Atoi(value)
			if err != nil {
				klog.Errorf("invalid %s=%s", GPUIndex, value)
				return -1
			}
			return id
		}
	}
	return -1
}

func escapeJSONPointer(p string) string {
	// Escaping reference name using https://tools.ietf.org/html/rfc6901
	p = strings.Replace(p, "~", "~0", -1)
	p = strings.Replace(p, "/", "~1", -1)
	return p
}

// AddGPUIndexPatch returns the patch adding GPU index.
func AddGPUIndexPatch(id int) string {
	return fmt.Sprintf(
		`[{"op": "add", "path": "/metadata/annotations/%s", "value":"%d"},`+
			`{"op": "add", "path": "/metadata/annotations/%s", "value": "%d"}]`,
		escapeJSONPointer(PredicateTime),
		time.Now().UnixNano(),
		escapeJSONPointer(GPUIndex),
		id,
	)
}

// RemoveGPUIndexPatch returns the patch removing GPU index.
func RemoveGPUIndexPatch() string {
	return fmt.Sprintf(
		`[{"op": "remove", "path": "/metadata/annotations/%s"},`+
			`{"op": "remove", "path": "/metadata/annotations/%s"}]`,
		escapeJSONPointer(PredicateTime),
		escapeJSONPointer(GPUIndex),
	)
}
