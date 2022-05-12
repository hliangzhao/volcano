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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

/* Command is defined as a CRD: which action that will be taken on which resource object. */

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// Command is the Schema for the commands API
type Command struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Action defines the action that will be taken to the target object.
	Action string `json:"action,omitempty" protobuf:"bytes,2,opt,name=action"`

	// TargetObject defines the target object of this command.
	TargetObject *metav1.OwnerReference `json:"target,omitempty" protobuf:"bytes,3,opt,name=target"`

	// Unique, one-word, CamelCase reason for this command.
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`

	// Human-readable message indicating details of this command.
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,4,opt,name=message"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// CommandList contains a list of Command
type CommandList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []Command `json:"items" protobuf:"bytes,2,rep,name=items"`
}
