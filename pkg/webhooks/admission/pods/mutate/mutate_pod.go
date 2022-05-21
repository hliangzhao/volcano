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

package mutate

import (
	"encoding/json"
	"fmt"
	webhooksconfig "github.com/hliangzhao/volcano/pkg/webhooks/config"
	"github.com/hliangzhao/volcano/pkg/webhooks/router"
	"github.com/hliangzhao/volcano/pkg/webhooks/schema"
	"github.com/hliangzhao/volcano/pkg/webhooks/utils"
	admissionv1 "k8s.io/api/admission/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// patchOperation define the patch operation structure
type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

// init register mutate pod
func init() {
	_ = router.RegisterAdmission(service)
}

var service = &router.AdmissionService{
	Path:   "/pods/mutate",
	Func:   Pods,
	Config: config,
	MutatingConfig: &admissionregistrationv1.MutatingWebhookConfiguration{
		Webhooks: []admissionregistrationv1.MutatingWebhook{{
			Name: "mutatepod.volcano.sh",
			Rules: []admissionregistrationv1.RuleWithOperations{
				{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{""},
						APIVersions: []string{"v1"},
						Resources:   []string{"pods"},
					},
				},
			},
		}},
	},
}

var config = &router.AdmissionServiceConfig{}

// Pods mutate pods.
func Pods(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	klog.V(3).Infof("mutating pods -- %s", ar.Request.Operation)
	pod, err := schema.DecodePod(ar.Request.Object, ar.Request.Resource)
	if err != nil {
		return utils.ToAdmissionResponse(err)
	}

	if pod.Namespace == "" {
		pod.Namespace = ar.Request.Namespace
	}

	var patchBytes []byte
	switch ar.Request.Operation {
	case admissionv1.Create:
		patchBytes, _ = createPatch(pod)
	default:
		err = fmt.Errorf("expect operation to be 'CREATE' ")
		return utils.ToAdmissionResponse(err)
	}

	reviewResponse := admissionv1.AdmissionResponse{
		Allowed: true,
		Patch:   patchBytes,
	}
	pt := admissionv1.PatchTypeJSONPatch
	reviewResponse.PatchType = &pt

	return &reviewResponse
}

// createPatch patch pod
func createPatch(pod *corev1.Pod) ([]byte, error) {
	if config.ConfigData == nil {
		klog.V(5).Infof("admission configuration is empty.")
		return nil, nil
	}

	var patch []patchOperation
	config.ConfigData.Lock()
	defer config.ConfigData.Unlock()

	for _, resourceGroup := range config.ConfigData.ResGroupsConfig {
		klog.V(3).Infof("resourceGroup %s", resourceGroup.ResourceGroup)
		group := GetResGroup(resourceGroup)
		if !group.IsBelongResGroup(pod, resourceGroup) {
			continue
		}

		patchLabel := patchLabels(pod, resourceGroup)
		if patchLabel != nil {
			patch = append(patch, *patchLabel)
		}

		patchToleration := patchTaintToleration(pod, resourceGroup)
		if patchToleration != nil {
			patch = append(patch, *patchToleration)
		}
		patchScheduler := patchSchedulerName(resourceGroup)
		if patchScheduler != nil {
			patch = append(patch, *patchScheduler)
		}

		klog.V(5).Infof("pod patch %v", patch)
		return json.Marshal(patch)
	}

	return json.Marshal(patch)
}

// patchLabels patch label
func patchLabels(pod *corev1.Pod, resGroupConfig webhooksconfig.ResGroupConfig) *patchOperation {
	if len(resGroupConfig.Labels) == 0 {
		return nil
	}

	nodeSelector := make(map[string]string)
	for key, label := range pod.Spec.NodeSelector {
		nodeSelector[key] = label
	}

	for key, label := range resGroupConfig.Labels {
		nodeSelector[key] = label
	}

	return &patchOperation{Op: "add", Path: "/spec/nodeSelector", Value: nodeSelector}
}

// patchTaintToleration patch taint toleration
func patchTaintToleration(pod *corev1.Pod, resGroupConfig webhooksconfig.ResGroupConfig) *patchOperation {
	if len(resGroupConfig.Tolerations) == 0 {
		return nil
	}

	var dst []corev1.Toleration
	dst = append(dst, pod.Spec.Tolerations...)
	dst = append(dst, resGroupConfig.Tolerations...)

	return &patchOperation{Op: "add", Path: "/spec/tolerations", Value: dst}
}

// patchSchedulerName patch scheduler
func patchSchedulerName(resGroupConfig webhooksconfig.ResGroupConfig) *patchOperation {
	if resGroupConfig.SchedulerName == "" {
		return nil
	}

	return &patchOperation{Op: "add", Path: "/spec/schedulerName", Value: resGroupConfig.SchedulerName}
}
