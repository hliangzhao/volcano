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
	"fmt"
	"sync"
)

var admissionMap = make(map[string]*AdmissionService)
var admissionMutex sync.Mutex

func RegisterAdmission(service *AdmissionService) error {
	admissionMutex.Lock()
	defer admissionMutex.Unlock()

	if _, found := admissionMap[service.Path]; found {
		return fmt.Errorf("duplicated admission service for %s", service.Path)
	}

	admissionMap[service.Path] = service
	return nil
}

// TODO: cmd/webhook-manager not implemented
// func ForEachAdmission(config *options.Config, handler func(*AdmissionService)) {
// 	admissions := strings.Split(strings.TrimSpace(config.EnabledAdmission), ",")
// 	for _, admission := range admissions {
// 		if service, found := admissionMap[admission]; found {
// 			handler(service)
// 		}
// 	}
// }
