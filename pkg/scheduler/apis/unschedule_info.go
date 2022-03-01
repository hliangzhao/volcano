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

package apis

import (
	"fmt"
	"sort"
	"strings"
)

const (
	// NodePodNumberExceeded means pods in node exceed the allocatable pod number
	NodePodNumberExceeded = "node(s) pod number exceeded"

	// NodeResourceFitFailed means node could not fit the request of pod
	NodeResourceFitFailed = "node(s) resource fit failed"

	// AllNodeUnavailableMsg is the default error message
	AllNodeUnavailableMsg = "all nodes are unavailable"
)

// These are reasons for a pod's transition to a condition.
const (
	// PodReasonUnschedulable reason in PodScheduled PodCondition means that the scheduler
	// can't schedule the pod right now, for example due to insufficient resources in the cluster.
	PodReasonUnschedulable = "Unschedulable"

	// PodReasonSchedulable reason in PodScheduled PodCondition means that the scheduler
	// can schedule the pod right now, but not bind yet.
	PodReasonSchedulable = "Schedulable"

	// PodReasonUndetermined reason in PodScheduled PodCondition means that the scheduler
	// skips scheduling the pod which left the pod `Undetermined`, for example due to unschedulable pod already occurred.
	PodReasonUndetermined = "Undetermined"
)

// FitError describes the reason why task could not fit that node
type FitError struct {
	taskNamespace string
	taskName      string
	NodeName      string
	Reasons       []string
}

// NewFitError creates FitError with msg.
func NewFitError(ti *TaskInfo, ni *NodeInfo, msg ...string) *FitError {
	return &FitError{
		taskName:      ti.Name,
		taskNamespace: ti.Namespace,
		NodeName:      ni.Name,
		Reasons:       msg,
	}
}

// Error returns the final error message
func (fe *FitError) Error() string {
	return fmt.Sprintf("task %s/%s on node %s fit failed: %s",
		fe.taskNamespace, fe.taskName, fe.NodeName, strings.Join(fe.Reasons, ", "))
}

// FitErrors is the set of FitError of multiple nodes.
type FitErrors struct {
	nodes map[string]*FitError
	err   string // common err msg
}

func NewFitErrors() *FitErrors {
	f := new(FitErrors)
	f.nodes = map[string]*FitError{}
	return f
}

// SetError sets the common error message in FitErrors.
func (f *FitErrors) SetError(err string) {
	f.err = err
}

// SetNodeError sets the node error in FitErrors.
func (f *FitErrors) SetNodeError(nodeName string, err error) {
	var fe *FitError
	switch obj := err.(type) {
	case *FitError:
		obj.NodeName = nodeName
		fe = obj
	default:
		fe = &FitError{
			NodeName: nodeName,
			Reasons:  []string{obj.Error()},
		}
	}
	f.nodes[nodeName] = fe
}

// Error returns the final error message.
func (f *FitErrors) Error() string {
	reasons := make(map[string]int)

	for _, node := range f.nodes {
		for _, reason := range node.Reasons {
			reasons[reason]++
		}
	}

	sortReasonsHistogram := func() []string {
		var reasonStrings []string
		for k, v := range reasons {
			reasonStrings = append(reasonStrings, fmt.Sprintf("%v %v", v, k))
		}
		sort.Strings(reasonStrings)
		return reasonStrings
	}

	if f.err == "" {
		f.err = AllNodeUnavailableMsg
	}
	reasonMsg := fmt.Sprintf(f.err+": %v.", strings.Join(sortReasonsHistogram(), ", "))
	return reasonMsg
}
