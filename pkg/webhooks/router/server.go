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

package router

import (
	"encoding/json"
	"github.com/hliangzhao/volcano/pkg/webhooks/schema"
	"github.com/hliangzhao/volcano/pkg/webhooks/utils"
	"io"
	"io/ioutil"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"net/http"
)

var ContentType = "Content-Type"
var ApplicationJson = "application/json"

func Serve(w io.Writer, r *http.Request, admit AdmitFunc) {
	var body []byte
	if r.Body != nil {
		if data, err := ioutil.ReadAll(r.Body); err == nil {
			body = data
		}
	}

	contentType := r.Header.Get(ContentType)
	if contentType != ApplicationJson {
		klog.Errorf("contentType is not application/json")
		return
	}

	var reviewResponse *admissionv1.AdmissionResponse
	ar := admissionv1.AdmissionReview{}
	deserializer := schema.Codecs.UniversalDeserializer()
	if _, _, err := deserializer.Decode(body, nil, &ar); err != nil {
		reviewResponse = utils.ToAdmissionResponse(err)
	} else {
		reviewResponse = admit(ar)
	}
	klog.V(3).Infof("sending response: %v", reviewResponse)

	response := createResponse(reviewResponse, &ar)
	resp, err := json.Marshal(response)
	if err != nil {
		klog.Error(err)
	}
	if _, err := w.Write(resp); err != nil {
		klog.Error(err)
	}
}

func createResponse(reviewResponse *admissionv1.AdmissionResponse, ar *admissionv1.AdmissionReview) admissionv1.AdmissionReview {
	response := admissionv1.AdmissionReview{}
	if reviewResponse != nil {
		response.APIVersion = "admission.k8s.io/v1"
		response.Kind = "AdmissionReview"
		response.Response = reviewResponse
		response.Response.UID = ar.Request.UID
	}
	// reset the Object and OldObject, they are not needed in a response.
	ar.Request.Object = runtime.RawExtension{}
	ar.Request.OldObject = runtime.RawExtension{}

	return response
}
