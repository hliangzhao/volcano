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

package utils

import (
	`fmt`
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	`k8s.io/client-go/kubernetes`
)

// FakeBinder is used as fake binder
type FakeBinder struct {
	Binds   map[string]string
	Channel chan string
}

// Bind used by fake binder struct to bind pods
func (fb *FakeBinder) Bind(kubeClient *kubernetes.Clientset, tasks []*apis.TaskInfo) (error, []*apis.TaskInfo) {
	for _, p := range tasks {
		key := fmt.Sprintf("%v/%v", p.Namespace, p.Name)
		fb.Binds[key] = p.NodeName
	}
	return nil, nil
}
