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

package mutate

import (
	"encoding/json"
	"fmt"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/webhooks/router"
	"github.com/hliangzhao/volcano/pkg/webhooks/schema"
	"github.com/hliangzhao/volcano/pkg/webhooks/utils"
	admissionv1 "k8s.io/api/admission/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

func init() {
	_ = router.RegisterAdmission(service)
}

var service = &router.AdmissionService{
	Path: "/podgroups/mutate",
	Func: PodGroups,

	MutatingConfig: &admissionregistrationv1.MutatingWebhookConfiguration{
		Webhooks: []admissionregistrationv1.MutatingWebhook{{
			Name: "mutatepodgroup.hliangzhao.io",
			Rules: []admissionregistrationv1.RuleWithOperations{
				{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{schedulingv1alpha1.SchemeGroupVersion.Group},
						APIVersions: []string{schedulingv1alpha1.SchemeGroupVersion.Version},
						Resources:   []string{"podgroups"},
					},
				},
			},
		}},
	},
}

type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

// PodGroups mutate podgroups.
func PodGroups(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	klog.V(3).Infof("Mutating %s podgroup %s.", ar.Request.Operation, ar.Request.Name)

	podgroup, err := schema.DecodePodGroup(ar.Request.Object, ar.Request.Resource)
	if err != nil {
		return utils.ToAdmissionResponse(err)
	}

	var patchBytes []byte
	switch ar.Request.Operation {
	case admissionv1.Create:
		patchBytes, err = createPodGroupPatch(podgroup)
	default:
		return utils.ToAdmissionResponse(fmt.Errorf("invalid operation `%s`, "+
			"expect operation to be `CREATE`", ar.Request.Operation))
	}

	if err != nil {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result:  &metav1.Status{Message: err.Error()},
		}
	}

	pt := admissionv1.PatchTypeJSONPatch
	return &admissionv1.AdmissionResponse{
		Allowed:   true,
		Patch:     patchBytes,
		PatchType: &pt,
	}
}

func createPodGroupPatch(podgroup *schedulingv1alpha1.PodGroup) ([]byte, error) {
	var patch []patchOperation

	if len(podgroup.Spec.Queue) == 0 {
		patch = append(patch, patchOperation{
			Op:    "add",
			Path:  "/spec/queue",
			Value: schedulingv1alpha1.DefaultQueue,
		})
	}

	return json.Marshal(patch)
}
