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

package resourcequota

import (
	`fmt`
	schedulingv1alpha1 `github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1`
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	`github.com/hliangzhao/volcano/pkg/scheduler/framework`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils`
	corev1 `k8s.io/api/core/v1`
	quotav1 `k8s.io/apiserver/pkg/quota/v1`
	`k8s.io/klog/v2`
)

const PluginName = "resourcequota"

type resourcequotaPlugin struct {
	args framework.Arguments
}

func New(args framework.Arguments) framework.Plugin {
	return &resourcequotaPlugin{
		args: args,
	}
}

func (rp *resourcequotaPlugin) Name() string {
	return PluginName
}

func (rp *resourcequotaPlugin) OnSessionOpen(sess *framework.Session) {
	klog.V(4).Infof("Enter resourcequota plugin ...")
	defer klog.V(4).Infof("Leaving resourcequota plugin.")

	pendingResources := make(map[string]corev1.ResourceList)
	sess.AddJobEnqueuableFn(rp.Name(), func(obj interface{}) int {
		job := obj.(*apis.JobInfo)

		resourcesRequests := job.PodGroup.Spec.MinResources

		if resourcesRequests == nil {
			return utils.Permit
		}

		quotas := sess.NamespaceInfo[apis.NamespaceName(job.Namespace)].QuotaStatus
		for _, resourceQuota := range quotas {
			hardResources := quotav1.ResourceNames(resourceQuota.Hard)
			requestedUsage := quotav1.Mask(*resourcesRequests, hardResources)

			var resourcesUsed = resourceQuota.Used
			if pendingUse, found := pendingResources[job.Namespace]; found {
				resourcesUsed = quotav1.Add(pendingUse, resourcesUsed)
			}
			newUsage := quotav1.Add(resourcesUsed, requestedUsage)
			maskedNewUsage := quotav1.Mask(newUsage, quotav1.ResourceNames(requestedUsage))

			if allowed, exceeded := quotav1.LessThanOrEqual(maskedNewUsage, resourceQuota.Hard); !allowed {
				failedRequestedUsage := quotav1.Mask(requestedUsage, exceeded)
				failedUsed := quotav1.Mask(resourceQuota.Used, exceeded)
				failedHard := quotav1.Mask(resourceQuota.Hard, exceeded)
				msg := fmt.Sprintf("resource quota insufficient, requested: %v, used: %v, limited: %v",
					failedRequestedUsage,
					failedUsed,
					failedHard,
				)
				klog.V(4).Infof("enqueueable false for job: %s/%s, because :%s", job.Namespace, job.Name, msg)
				sess.RecordPodGroupEvent(job.PodGroup, corev1.EventTypeNormal, string(schedulingv1alpha1.PodGroupUnschedulableType), msg)
				return utils.Reject
			}
		}
		if _, found := pendingResources[job.Namespace]; !found {
			pendingResources[job.Namespace] = corev1.ResourceList{}
		}
		pendingResources[job.Namespace] = quotav1.Add(pendingResources[job.Namespace], *resourcesRequests)
		return utils.Permit
	})
}

func (rp *resourcequotaPlugin) OnSessionClose(sess *framework.Session) {}
