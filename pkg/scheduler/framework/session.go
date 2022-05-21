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

package framework

import (
	"fmt"
	"github.com/hliangzhao/volcano/pkg/apis/scheduling"
	schedulingscheme "github.com/hliangzhao/volcano/pkg/apis/scheduling/scheme"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/cache"
	"github.com/hliangzhao/volcano/pkg/scheduler/conf"
	"github.com/hliangzhao/volcano/pkg/scheduler/metrics"
	"github.com/hliangzhao/volcano/pkg/scheduler/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
)

// Session defines a scheduling session.
type Session struct {
	// session ID
	UID types.UID

	// kube client, informers, and volcano scheduler cache
	kubeClient      kubernetes.Interface
	recorder        record.EventRecorder
	cache           cache.Cache
	informerFactory informers.SharedInformerFactory

	TotalResource *apis.Resource
	// podGroupStatus caches the status of podgroups during schedule
	// This should not be mutated after initiated.
	podGroupStatus map[apis.JobID]scheduling.PodGroupStatus

	Jobs           map[apis.JobID]*apis.JobInfo
	Nodes          map[string]*apis.NodeInfo
	RevocableNodes map[string]*apis.NodeInfo
	Queues         map[apis.QueueID]*apis.QueueInfo
	NamespaceInfo  map[apis.NamespaceName]*apis.NamespaceInfo

	// scheduling configs
	Tiers          []conf.Tier
	Configurations []conf.Configuration
	NodeList       []*apis.NodeInfo

	plugins       map[string]Plugin
	eventHandlers []*EventHandler

	// predicates for scheduling
	jobOrderFns       map[string]apis.CompareFn
	queueOrderFns     map[string]apis.CompareFn
	taskOrderFns      map[string]apis.CompareFn
	namespaceOrderFns map[string]apis.CompareFn
	clusterOrderFns   map[string]apis.CompareFn
	predicateFns      map[string]apis.PredicateFn
	bestNodeFns       map[string]apis.BestNodeFn
	nodeOrderFns      map[string]apis.NodeOrderFn
	batchNodeOrderFns map[string]apis.BatchNodeOrderFn
	nodeMapFns        map[string]apis.NodeMapFn
	nodeReduceFns     map[string]apis.NodeReduceFn
	preemptableFns    map[string]apis.EvictableFn
	reclaimableFns    map[string]apis.EvictableFn
	overUsedFns       map[string]apis.ValidateFn
	allocatableFns    map[string]apis.AllocatableFn
	// underUsedFns      map[string]apis.UnderUsedResourceFn
	jobReadyFns      map[string]apis.ValidateFn
	jobPipelinedFns  map[string]apis.VoteFn
	jobValidFns      map[string]apis.ValidateExFn
	jobEnqueuableFns map[string]apis.VoteFn
	jobEnqueuedFns   map[string]apis.JobEnqueuedFn
	targetJobFns     map[string]apis.TargetJobFn
	reservedNodesFns map[string]apis.ReservedNodesFn
	victimTasksFns   map[string][]apis.VictimTasksFn
	jobStarvingFns   map[string]apis.ValidateFn
}

// openSession opens a scheduling session based on the input cluster cache info.
func openSession(cache cache.Cache) *Session {
	sess := &Session{
		UID:             uuid.NewUUID(),
		kubeClient:      cache.Client(),
		recorder:        cache.EventRecorder(),
		cache:           cache,
		informerFactory: cache.SharedInformerFactory(),

		TotalResource:  apis.EmptyResource(),
		podGroupStatus: map[apis.JobID]scheduling.PodGroupStatus{},

		Jobs:           map[apis.JobID]*apis.JobInfo{},
		Nodes:          map[string]*apis.NodeInfo{},
		RevocableNodes: map[string]*apis.NodeInfo{},
		Queues:         map[apis.QueueID]*apis.QueueInfo{},

		plugins:           map[string]Plugin{},
		jobOrderFns:       map[string]apis.CompareFn{},
		queueOrderFns:     map[string]apis.CompareFn{},
		taskOrderFns:      map[string]apis.CompareFn{},
		namespaceOrderFns: map[string]apis.CompareFn{},
		clusterOrderFns:   map[string]apis.CompareFn{},
		predicateFns:      map[string]apis.PredicateFn{},
		bestNodeFns:       map[string]apis.BestNodeFn{},
		nodeOrderFns:      map[string]apis.NodeOrderFn{},
		batchNodeOrderFns: map[string]apis.BatchNodeOrderFn{},
		nodeMapFns:        map[string]apis.NodeMapFn{},
		nodeReduceFns:     map[string]apis.NodeReduceFn{},
		preemptableFns:    map[string]apis.EvictableFn{},
		reclaimableFns:    map[string]apis.EvictableFn{},
		overUsedFns:       map[string]apis.ValidateFn{},
		allocatableFns:    map[string]apis.AllocatableFn{},
		// underUsedFns:      map[string]apis.UnderUsedResourceFn{},
		jobReadyFns:      map[string]apis.ValidateFn{},
		jobPipelinedFns:  map[string]apis.VoteFn{},
		jobValidFns:      map[string]apis.ValidateExFn{},
		jobEnqueuableFns: map[string]apis.VoteFn{},
		jobEnqueuedFns:   map[string]apis.JobEnqueuedFn{},
		targetJobFns:     map[string]apis.TargetJobFn{},
		reservedNodesFns: map[string]apis.ReservedNodesFn{},
		victimTasksFns:   map[string][]apis.VictimTasksFn{},
		jobStarvingFns:   map[string]apis.ValidateFn{},
	}

	snapshot := cache.Snapshot()

	sess.Jobs = snapshot.Jobs
	for _, job := range sess.Jobs {
		// only conditions will be updated periodically
		if job.PodGroup != nil && job.PodGroup.Status.Conditions != nil {
			sess.podGroupStatus[job.UID] = *job.PodGroup.Status.DeepCopy()
		}

		if vjr := sess.JobValid(job); vjr != nil {
			// job not passed, update its condition to unschedulable
			if !vjr.Pass {
				jc := &scheduling.PodGroupCondition{
					Type:               scheduling.PodGroupUnschedulableType,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
					TransitionID:       string(sess.UID),
					Reason:             vjr.Reason,
					Message:            vjr.Message,
				}

				if err := sess.UpdatePodGroupCondition(job, jc); err != nil {
					klog.Errorf("Failed to update job condition: %v", err)
				}
			}

			delete(sess.Jobs, job.UID)
		}
	}

	sess.NodeList = utils.GetNodeList(snapshot.Nodes, snapshot.NodeList)
	sess.Nodes = snapshot.Nodes
	sess.RevocableNodes = snapshot.RevocableNodes
	sess.Queues = snapshot.Queues
	sess.NamespaceInfo = snapshot.NamespaceInfo

	// calculate all nodes' resource only once in each schedule cycle, other plugins can clone it when need
	for _, node := range sess.Nodes {
		sess.TotalResource.Add(node.Allocatable)
	}

	klog.V(3).Infof("Open Session %v with <%d> Job and <%d> Queues",
		sess.UID, len(sess.Jobs), len(sess.Queues))

	return sess
}

// closeSession closes the input scheduling session.
func closeSession(sess *Session) {
	ju := newJobUpdater(sess)
	ju.UpdateAll()

	sess.Jobs = nil
	sess.Nodes = nil
	sess.RevocableNodes = nil
	sess.plugins = nil
	sess.eventHandlers = nil
	sess.jobOrderFns = nil
	sess.namespaceOrderFns = nil
	sess.queueOrderFns = nil
	sess.clusterOrderFns = nil
	sess.NodeList = nil
	sess.TotalResource = nil

	klog.V(3).Infof("Close Session %v", sess.UID)
}

// RecordPodGroupEvent records podGroup events.
func (sess Session) RecordPodGroupEvent(podGroup *apis.PodGroup, eventType, reason, msg string) {
	if podGroup == nil {
		return
	}

	pg := &schedulingv1alpha1.PodGroup{}
	if err := schedulingscheme.Scheme.Convert(&podGroup.PodGroup, pg, nil); err != nil {
		klog.Errorf("Error while converting PodGroup to v1alpha1.PodGroup with error: %v", err)
		return
	}
	sess.recorder.Eventf(pg, eventType, reason, msg)
}

// String returns nodes and jobs information in the session.
func (sess Session) String() string {
	msg := fmt.Sprintf("Session %v: \n", sess.UID)

	for _, job := range sess.Jobs {
		msg = fmt.Sprintf("%s%v\n", msg, job)
	}

	for _, node := range sess.Nodes {
		msg = fmt.Sprintf("%s%v\n", msg, node)
	}

	return msg
}

// InformerFactory returns the session's ShareInformerFactory.
func (sess Session) InformerFactory() informers.SharedInformerFactory {
	return sess.informerFactory
}

// KubeClient returns the kubernetes client.
func (sess Session) KubeClient() kubernetes.Interface {
	return sess.kubeClient
}

// AddEventHandler adds event handlers to current session.
func (sess *Session) AddEventHandler(eh *EventHandler) {
	sess.eventHandlers = append(sess.eventHandlers, eh)
}

// UpdateSchedulerNumaInfo updates SchedulerNumaInfo.
func (sess *Session) UpdateSchedulerNumaInfo(AllocatedSets map[string]apis.ResNumaSets) {
	_ = sess.cache.UpdateSchedulerNumaInfo(AllocatedSets)
}

// UpdatePodGroupCondition updates job condition accordingly.
func (sess *Session) UpdatePodGroupCondition(jobInfo *apis.JobInfo, cond *scheduling.PodGroupCondition) error {
	job, ok := sess.Jobs[jobInfo.UID]
	if !ok {
		return fmt.Errorf("failed to find job <%s/%s>", jobInfo.Namespace, jobInfo.Name)
	}

	index := -1
	for i, c := range job.PodGroup.Status.Conditions {
		if c.Type == cond.Type {
			index = i
			break
		}
	}

	// Update condition to the new condition.
	if index < 0 {
		job.PodGroup.Status.Conditions = append(job.PodGroup.Status.Conditions, *cond)
	} else {
		job.PodGroup.Status.Conditions[index] = *cond
	}

	return nil
}

// BindPodGroup binds PodGroup to the specified cluster.
func (sess *Session) BindPodGroup(job *apis.JobInfo, cluster string) error {
	return sess.cache.BindPodGroup(job, cluster)
}

// jobStatus sets status of given jobInfo.
func jobStatus(sess *Session, jobInfo *apis.JobInfo) scheduling.PodGroupStatus {
	status := jobInfo.PodGroup.Status

	unschedulable := false
	for _, c := range status.Conditions {
		if c.Type == scheduling.PodGroupUnschedulableType &&
			c.Status == corev1.ConditionTrue &&
			c.TransitionID == string(sess.UID) {
			unschedulable = true
			break
		}
	}

	// If running tasks && unschedulable, unknown phase
	if len(jobInfo.TaskStatusIndex[apis.Running]) != 0 && unschedulable {
		status.Phase = scheduling.PodGroupUnknown
	} else {
		allocated := 0
		for status, tasks := range jobInfo.TaskStatusIndex {
			if apis.AllocatedStatus(status) || status == apis.Succeeded {
				allocated += len(tasks)
			}
		}

		// If there are enough allocated resource, it's running
		if int32(allocated) >= jobInfo.PodGroup.Spec.MinMember {
			status.Phase = scheduling.PodGroupRunning
		} else if jobInfo.PodGroup.Status.Phase != scheduling.PodGroupInqueue {
			status.Phase = scheduling.PodGroupPending
		}
	}

	status.Running = int32(len(jobInfo.TaskStatusIndex[apis.Running]))
	status.Failed = int32(len(jobInfo.TaskStatusIndex[apis.Failed]))
	status.Succeeded = int32(len(jobInfo.TaskStatusIndex[apis.Succeeded]))

	return status
}

// Statement constructs a Statement instance with sess.
func (sess *Session) Statement() *Statement {
	return &Statement{
		sess: sess,
	}
}

// dispatch binds task to the scheduled node and updates related metrics.
func (sess *Session) dispatch(task *apis.TaskInfo, volumes *volumebinding.PodVolumes) error {
	if err := sess.cache.AddBindTask(task); err != nil {
		return err
	}

	// Update status in session
	if job, found := sess.Jobs[task.Job]; found {
		if err := job.UpdateTaskStatus(task, apis.Binding); err != nil {
			klog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
				task.Namespace, task.Name, apis.Binding, sess.UID, err)
			return err
		}
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when binding.",
			task.Job, sess.UID)
		return fmt.Errorf("failed to find job %s", task.Job)
	}

	metrics.UpdateTaskScheduleDuration(metrics.Duration(task.Pod.CreationTimestamp.Time))
	return nil
}

/* Statement Allocate, Evict, and Pipeline implementations */

func (sess *Session) Allocate(task *apis.TaskInfo, nodeInfo *apis.NodeInfo) error {
	// TODO: this Allocate is similar to statement.Allocate. Why statement.Allocate calls this directly?

	podVolumes, err := sess.cache.GetPodVolumes(task, nodeInfo.Node)
	if err != nil {
		return err
	}

	hostname := nodeInfo.Name
	if err := sess.cache.AllocateVolumes(task, hostname, podVolumes); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			sess.cache.RevertVolumes(task, podVolumes)
		}
	}()

	task.Pod.Spec.NodeName = hostname
	task.PodVolumes = podVolumes

	job, found := sess.Jobs[task.Job]
	if found {
		if err := job.UpdateTaskStatus(task, apis.Allocated); err != nil {
			klog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
				task.Namespace, task.Name, apis.Allocated, sess.UID, err)
			return err
		}
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when binding.",
			task.Job, sess.UID)
		return fmt.Errorf("failed to find job %s", task.Job)
	}

	task.NodeName = hostname

	if node, found := sess.Nodes[hostname]; found {
		if err := node.AddTask(task); err != nil {
			klog.Errorf("Failed to add task <%v/%v> to node <%v> in Session <%v>: %v",
				task.Namespace, task.Name, hostname, sess.UID, err)
			return err
		}
		klog.V(3).Infof("After allocated Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		klog.Errorf("Failed to find Node <%s> in Session <%s> index when binding.",
			hostname, sess.UID)
		return fmt.Errorf("failed to find node %s", hostname)
	}

	// Callbacks
	for _, eh := range sess.eventHandlers {
		if eh.AllocateFunc != nil {
			eh.AllocateFunc(&Event{
				Task: task,
			})
		}
	}

	if sess.JobReady(job) {
		for _, task := range job.TaskStatusIndex[apis.Allocated] {
			if err := sess.dispatch(task, podVolumes); err != nil {
				klog.Errorf("Failed to dispatch task <%v/%v>: %v",
					task.Namespace, task.Name, err)
				return err
			}
		}
	} else {
		sess.cache.RevertVolumes(task, podVolumes)
	}

	return nil
}

func (sess *Session) Evict(reclaimee *apis.TaskInfo, reason string) error {
	// TODO: this Evict is similar to statement.Evict. Why statement.Evict calls this directly?

	if err := sess.cache.Evict(reclaimee, reason); err != nil {
		return err
	}

	job, found := sess.Jobs[reclaimee.Job]
	if found {
		if err := job.UpdateTaskStatus(reclaimee, apis.Releasing); err != nil {
			klog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
				reclaimee.Namespace, reclaimee.Name, apis.Releasing, sess.UID, err)
			return err
		}
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when binding.",
			reclaimee.Job, sess.UID)
		return fmt.Errorf("failed to find job %s", reclaimee.Job)
	}

	if node, found := sess.Nodes[reclaimee.NodeName]; found {
		if err := node.UpdateTask(reclaimee); err != nil {
			klog.Errorf("Failed to update task <%v/%v> in Session <%v>: %v",
				reclaimee.Namespace, reclaimee.Name, sess.UID, err)
			return err
		}
	}

	for _, eh := range sess.eventHandlers {
		if eh.DeallocateFunc != nil {
			eh.DeallocateFunc(&Event{
				Task: reclaimee,
			})
		}
	}

	return nil
}

func (sess *Session) Pipeline(task *apis.TaskInfo, hostname string) error {
	// TODO: this Pipeline is similar to statement.Pipeline. Why statement.Pipeline calls this directly?

	job, found := sess.Jobs[task.Job]
	if found {
		if err := job.UpdateTaskStatus(task, apis.Pipelined); err != nil {
			klog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
				task.Namespace, task.Name, apis.Pipelined, sess.UID, err)
			return err
		}
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when binding.",
			task.Job, sess.UID)
		return fmt.Errorf("failed to find job %s when binding", task.Job)
	}

	task.NodeName = hostname

	if node, found := sess.Nodes[hostname]; found {
		if err := node.AddTask(task); err != nil {
			klog.Errorf("Failed to add task <%v/%v> to node <%v> in Session <%v>: %v",
				task.Namespace, task.Name, hostname, sess.UID, err)
			return err
		}
		klog.V(3).Infof("After added Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		klog.Errorf("Failed to find Node <%s> in Session <%s> index when binding.",
			hostname, sess.UID)
		return fmt.Errorf("failed to find node %s", hostname)
	}

	for _, eh := range sess.eventHandlers {
		if eh.AllocateFunc != nil {
			eh.AllocateFunc(&Event{
				Task: task,
			})
		}
	}

	return nil
}
