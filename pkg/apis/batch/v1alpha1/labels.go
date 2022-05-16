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

package v1alpha1

const (
	GroupName = "hliangzhao.io"

	// TaskSpecKey task spec key used in pod annotation
	TaskSpecKey = GroupName + "/task-spec"
	// JobNameKey job name key used in pod annotation/labels
	JobNameKey = GroupName + "/job-name"
	// QueueNameKey queue name key used in pod annotation/labels
	QueueNameKey = GroupName + "/queue-name"
	// JobNamespaceKey job namespace key
	JobNamespaceKey = GroupName + "/job-namespace"
	// DefaultTaskSpec default task spec value
	DefaultTaskSpec = "default"
	// JobVersion job version key used in pod annotation
	JobVersion = GroupName + "/job-version"
	// JobTypeKey job type key used in labels
	JobTypeKey = GroupName + "/job-type"
	// PodgroupNamePrefix podgroup name prefix
	PodgroupNamePrefix = "podgroup-"
	// PodTemplateKey type specify an equivalence pod class
	PodTemplateKey = GroupName + "/template-uid"
	// JobForwardingKey job forwarding key used in job annotation
	JobForwardingKey = GroupName + "/job-forwarding"
	// ForwardClusterKey cluster key used in pod annotation
	ForwardClusterKey = GroupName + "/forward-cluster"
	// OriginalNameKey annotation key for resource name
	OriginalNameKey = GroupName + "/burst-name"
	// BurstToSiloClusterAnnotation labels key for resource only in silo cluster
	BurstToSiloClusterAnnotation = GroupName + "/silo-resource"
)
