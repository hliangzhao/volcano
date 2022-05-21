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

package extender

import "github.com/hliangzhao/volcano/pkg/scheduler/apis"

type OnSessionOpenRequest struct {
	Jobs           map[apis.JobID]*apis.JobInfo
	Nodes          map[string]*apis.NodeInfo
	Queues         map[apis.QueueID]*apis.QueueInfo
	NamespaceInfo  map[apis.NamespaceName]*apis.NamespaceInfo
	RevocableNodes map[string]*apis.NodeInfo
	NodeList       []string
}

type OnSessionOpenResponse struct{}
type OnSessionCloseRequest struct{}
type OnSessionCloseResponse struct{}

type PredicateRequest struct {
	Task *apis.TaskInfo `json:"task"`
	Node *apis.NodeInfo `json:"node"`
}

type PredicateResponse struct {
	ErrorMessage string `json:"errorMessage"`
}

type PrioritizeRequest struct {
	Task  *apis.TaskInfo   `json:"task"`
	Nodes []*apis.NodeInfo `json:"nodes"`
}

type PrioritizeResponse struct {
	NodeScore    map[string]float64 `json:"nodeScore"`
	ErrorMessage string             `json:"errorMessage"`
}

type PreemptableRequest struct {
	Evictor  *apis.TaskInfo   `json:"evictor"`
	Evictees []*apis.TaskInfo `json:"evictees"`
}

type PreemptableResponse struct {
	Status  int              `json:"status"`
	Victims []*apis.TaskInfo `json:"victims"`
}

type ReclaimableRequest PreemptableRequest
type ReclaimableResponse PreemptableResponse

type JobEnqueueableRequest struct {
	Job *apis.JobInfo `json:"job"`
}

type JobEnqueueableResponse struct {
	Status int `json:"status"`
}

type QueueOverusedRequest struct {
	Queue *apis.QueueInfo `json:"queue"`
}
type QueueOverusedResponse struct {
	Overused bool `json:"overused"`
}
