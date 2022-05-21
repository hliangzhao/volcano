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

package validate

import (
	"context"
	"fmt"
	"github.com/hliangzhao/volcano/pkg/apis/helpers"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/webhooks/router"
	"github.com/hliangzhao/volcano/pkg/webhooks/schema"
	"github.com/hliangzhao/volcano/pkg/webhooks/utils"
	admissionv1 "k8s.io/api/admission/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog/v2"
	"strconv"
	"strings"
)

func init() {
	_ = router.RegisterAdmission(service)
}

var service = &router.AdmissionService{
	Path: "/pods/validate",
	Func: AdmitPods,

	Config: config,

	ValidatingConfig: &admissionregistrationv1.ValidatingWebhookConfiguration{
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name: "validatepod.volcano.sh",
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

// AdmitPods is to admit pods and return response.
func AdmitPods(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	klog.V(3).Infof("admitting pods -- %s", ar.Request.Operation)

	pod, err := schema.DecodePod(ar.Request.Object, ar.Request.Resource)
	if err != nil {
		return utils.ToAdmissionResponse(err)
	}

	var msg string
	reviewResponse := admissionv1.AdmissionResponse{}
	reviewResponse.Allowed = true

	switch ar.Request.Operation {
	case admissionv1.Create:
		msg = validatePod(pod, &reviewResponse)
	default:
		err := fmt.Errorf("expect operation to be 'CREATE'")
		return utils.ToAdmissionResponse(err)
	}

	if !reviewResponse.Allowed {
		reviewResponse.Result = &metav1.Status{Message: strings.TrimSpace(msg)}
	}
	return &reviewResponse
}

/*
	Allow pods to create when
	1. schedulerName of pod isn't volcano
	2. normal pods whose schedulerName is volcano don't have podgroup.
	3. check pod budget annotations configure
*/
func validatePod(pod *corev1.Pod, reviewResponse *admissionv1.AdmissionResponse) string {
	if pod.Spec.SchedulerName != config.SchedulerName {
		return ""
	}

	pgName := ""
	msg := ""

	// vc-job, SN == volcano
	if pod.Annotations != nil {
		pgName = pod.Annotations[schedulingv1alpha1.KubeGroupNameAnnotationKey]
	}
	if pgName != "" {
		if err := checkPG(pod, pgName, true); err != nil {
			msg = err.Error()
			reviewResponse.Allowed = false
		}
		return msg
	}

	// normal pod, SN == volcano
	pgName = helpers.GeneratePodGroupName(pod)
	if err := checkPG(pod, pgName, false); err != nil {
		msg = err.Error()
		reviewResponse.Allowed = false
	}

	// check pod annotations
	if err := validateAnnotation(pod); err != nil {
		msg = err.Error()
		reviewResponse.Allowed = false
	}

	return msg
}

func checkPG(pod *corev1.Pod, pgName string, isVCJob bool) error {
	_, err := config.VolcanoClient.SchedulingV1alpha1().PodGroups(pod.Namespace).Get(context.TODO(), pgName, metav1.GetOptions{})
	if err != nil {
		if isVCJob || (!isVCJob && !apierrors.IsNotFound(err)) {
			return fmt.Errorf("failed to get PodGroup for pod <%s/%s>: %v", pod.Namespace, pod.Name, err)
		}
		return nil
	}
	return nil
}

func validateAnnotation(pod *corev1.Pod) error {
	num := 0
	if len(pod.Annotations) > 0 {
		keys := []string{
			schedulingv1alpha1.JDBMinAvailable,
			schedulingv1alpha1.JDBMaxUnavailable,
		}
		for _, key := range keys {
			if value, found := pod.Annotations[key]; found {
				num++
				if err := validateIntPercentageStr(key, value); err != nil {
					recordEvent(err)
					return err
				}
			}
		}
		if num > 1 {
			return fmt.Errorf("not allow configure multiple annotations <%v> at same time", keys)
		}
	}
	return nil
}

func recordEvent(err error) {
	config.Recorder.Eventf(nil, corev1.EventTypeWarning, "Admit", "Create pod failed due to %v", err)
}

func validateIntPercentageStr(key, value string) error {
	tmp := intstr.Parse(value)
	switch tmp.Type {
	case intstr.Int:
		if tmp.IntValue() <= 0 {
			return fmt.Errorf("invalid value <%q> for %v, it must be a positive integer", value, key)
		}
		return nil
	case intstr.String:
		s := strings.Replace(tmp.StrVal, "%", "", -1)
		v, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("invalid value %v for %v", err, key)
		}
		if v <= 0 || v >= 100 {
			return fmt.Errorf("invalid value <%q> for %v, it must be a valid percentage which between 1%% ~ 99%%", tmp.StrVal, key)
		}
		return nil
	}
	return fmt.Errorf("invalid type: neither int nor percentage for %v", key)
}
