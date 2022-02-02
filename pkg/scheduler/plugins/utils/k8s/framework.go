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

package k8s

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
)

// Framework is a K8S framework who mainly provides some methods
// about snapshot and plugins such as predicates
type Framework struct {
	snapshot        k8sframework.SharedLister
	kubeClient      kubernetes.Interface
	informerFactory informers.SharedInformerFactory
}

// TODO: To make Framework legal, we need to implement the inherited functions.
//  Currently, they re not implemented.

var _ k8sframework.Handle = &Framework{}

func (f *Framework) AddNominatedPod(pod *k8sframework.PodInfo, nodeName string) {
	panic("not implemented")
}

func (f *Framework) DeleteNominatedPodIfExists(pod *corev1.Pod) {
	panic("not implemented")
}

func (f *Framework) UpdateNominatedPod(oldPod *corev1.Pod, newPodInfo *k8sframework.PodInfo) {
	panic("not implemented")
}

func (f *Framework) NominatedPodsForNode(nodeName string) []*k8sframework.PodInfo {
	panic("not implemented")
}

func (f *Framework) RunPreScorePlugins(context.Context, *k8sframework.CycleState, *corev1.Pod, []*corev1.Node) *k8sframework.Status {
	panic("not implemented")
}

func (f *Framework) RunScorePlugins(context.Context, *k8sframework.CycleState, *corev1.Pod, []*corev1.Node) (k8sframework.PluginToNodeScores, *k8sframework.Status) {
	panic("not implemented")
}

func (f *Framework) RunFilterPlugins(context.Context, *k8sframework.CycleState, *corev1.Pod, *k8sframework.NodeInfo) k8sframework.PluginToStatus {
	panic("not implemented")
}

func (f *Framework) RunPreFilterExtensionAddPod(ctx context.Context, state *k8sframework.CycleState, podToSchedule *corev1.Pod, podInfoToAdd *k8sframework.PodInfo, nodeInfo *k8sframework.NodeInfo) *k8sframework.Status {
	panic("not implemented")
}

func (f *Framework) RunPreFilterExtensionRemovePod(ctx context.Context, state *k8sframework.CycleState, podToSchedule *corev1.Pod, podInfoToRemove *k8sframework.PodInfo, nodeInfo *k8sframework.NodeInfo) *k8sframework.Status {
	panic("not implemented")
}

// SnapshotSharedLister returns the scheduler's SharedLister of the latest NodeInfo
// snapshot. The snapshot is taken at the beginning of a scheduling cycle and remains
// unchanged until a pod finishes "Reserve". There is no guarantee that the information
// remains unchanged after "Reserve".
func (f *Framework) SnapshotSharedLister() k8sframework.SharedLister {
	return f.snapshot
}

// IterateOverWaitingPods acquires a read lock and iterates over the WaitingPods map.
func (f *Framework) IterateOverWaitingPods(callback func(k8sframework.WaitingPod)) {
	panic("not implemented")
}

// GetWaitingPod returns a reference to a WaitingPod given its UID.
func (f *Framework) GetWaitingPod(uid types.UID) k8sframework.WaitingPod {
	panic("not implemented")
}

// RejectWaitingPod rejects a WaitingPod given its UID.
func (f *Framework) RejectWaitingPod(uid types.UID) bool {
	panic("not implemented")
}

// HasFilterPlugins returns true if at least one filter plugin is defined.
func (f *Framework) HasFilterPlugins() bool {
	panic("not implemented")
}

// HasScorePlugins returns true if at least one score plugin is defined.
func (f *Framework) HasScorePlugins() bool {
	panic("not implemented")
}

// ListPlugins returns a map of extension point name to plugin names configured at each extension
// point. Returns nil if no plugins where configured.
func (f *Framework) ListPlugins() map[string][]config.Plugin {
	panic("not implemented")
}

// ClientSet returns a kubernetes clientset.
func (f *Framework) ClientSet() kubernetes.Interface {
	return f.kubeClient
}

func (f *Framework) KubeConfig() *rest.Config {
	panic("not implemented")
}

func (f *Framework) Extenders() []k8sframework.Extender {
	panic("not implemented")
}

func (f *Framework) Parallelizer() parallelize.Parallelizer {
	panic("not implemented")
}

// SharedInformerFactory returns a shared informer factory.
func (f *Framework) SharedInformerFactory() informers.SharedInformerFactory {
	return f.informerFactory
}

// VolumeBinder returns the volume binder used by scheduler.
func (f *Framework) VolumeBinder() volumebinding.SchedulerVolumeBinder {
	panic("not implemented")
}

// EventRecorder was introduced in k8s v1.19.6 and to be implemented
func (f *Framework) EventRecorder() events.EventRecorder {
	return nil
}

func (f *Framework) RunFilterPluginsWithNominatedPods(ctx context.Context, state *k8sframework.CycleState, pod *corev1.Pod, info *k8sframework.NodeInfo) *k8sframework.Status {
	panic("not implemented")
}

// PreemptHandle incorporates all needed logic to run preemption logic.
type PreemptHandle interface {
	// PodNominator abstracts operations to maintain nominated Pods.
	k8sframework.PodNominator
	// PluginsRunner abstracts operations to run some plugins.
	k8sframework.PluginsRunner
	// Extenders returns registered scheduler extenders.
	Extenders() []k8sframework.Extender
}

// PreemptHandle was introduced in k8s v1.19.6 and to be implemented
func (f *Framework) PreemptHandle() PreemptHandle {
	return nil
}

// NewFrameworkHandle creates a FrameworkHandle interface, which is used by k8s plugins.
func NewFrameworkHandle(nodeMap map[string]*k8sframework.NodeInfo, client kubernetes.Interface, informerFactory informers.SharedInformerFactory) k8sframework.Handle {
	snapshot := NewSnapshot(nodeMap)
	return &Framework{
		snapshot:        snapshot,
		kubeClient:      client,
		informerFactory: informerFactory,
	}
}
