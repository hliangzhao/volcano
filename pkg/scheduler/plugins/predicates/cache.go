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

package predicates

import (
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sync"
)

type predicateCache struct {
	sync.RWMutex
	cache map[string]map[string]bool // node: pod: true/false
}

func newPredicateCache() *predicateCache {
	return &predicateCache{
		cache: map[string]map[string]bool{},
	}
}

func getPodTemplateUID(pod *corev1.Pod) string {
	uid, found := pod.Annotations[batchv1alpha1.PodTemplateKey]
	if !found {
		return ""
	}
	return uid
}

func (pc *predicateCache) PredicateWithCache(nodeName string, pod *corev1.Pod) (bool, error) {
	podTemplateUID := getPodTemplateUID(pod)
	if podTemplateUID == "" {
		return false, fmt.Errorf("no anonation of hliangzhao.io/template-uid in pod %s", pod.Name)
	}
	pc.RLock()
	defer pc.RUnlock()

	if nodeCache, exist := pc.cache[nodeName]; exist {
		if result, exist := nodeCache[podTemplateUID]; exist {
			klog.V(4).Infof("Predicate node %s and pod %s result %v", nodeName, pod.Name, result)
			return result, nil
		}
	}
	return false, fmt.Errorf("no information of node %s and pod %s in predicate cache", nodeName, pod.Name)
}

func (pc *predicateCache) UpdateCache(nodeName string, pod *corev1.Pod, fit bool) {
	podTemplateUID := getPodTemplateUID(pod)
	if podTemplateUID == "" {
		klog.V(3).Infof("Don't find pod %s template uid", pod.Name)
		return
	}
	pc.Lock()
	defer pc.Unlock()

	if _, exist := pc.cache[nodeName]; !exist {
		podCache := make(map[string]bool)
		podCache[podTemplateUID] = fit
		pc.cache[nodeName] = podCache
	} else {
		pc.cache[nodeName][podTemplateUID] = fit
	}
}
