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

package overcommit

import (
	"github.com/hliangzhao/volcano/pkg/apis/scheduling"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils"
	corev1 `k8s.io/api/core/v1`
	"k8s.io/klog/v2"
)

const (
	// PluginName is name of plugin
	PluginName = "overcommit"

	// overCommitFactor is resource overCommit factor for enqueue action
	// It determines the number of `pending` pods that the scheduler will tolerate
	// when the resources of the cluster is insufficient
	overCommitFactor = "overcommit-factor"

	// defaultOverCommitFactor defines the default overCommit resource factor for enqueue action
	defaultOverCommitFactor = 1.2
)

type overcommitPlugin struct {
	pluginArguments  framework.Arguments
	idleResource     *apis.Resource
	inqueueResource  *apis.Resource
	overCommitFactor float64
}

func New(arguments framework.Arguments) framework.Plugin {
	return &overcommitPlugin{
		pluginArguments:  arguments,
		idleResource:     apis.EmptyResource(),
		inqueueResource:  apis.EmptyResource(),
		overCommitFactor: defaultOverCommitFactor,
	}
}

func (op *overcommitPlugin) Name() string {
	return PluginName
}

/*
User should give overcommit-factor through overcommit plugin arguments as format below:

actions: "enqueue, allocate, backfill"
tiers:
- plugins:
  - name: overcommit
      arguments:
        overcommit-factor: 1.0
*/

func (op *overcommitPlugin) OnSessionOpen(sess *framework.Session) {
	klog.V(4).Infof("Enter overcommit plugin ...")
	defer klog.V(4).Infof("Leaving overcommit plugin.")

	op.pluginArguments.GetFloat64(&op.overCommitFactor, overCommitFactor)
	if op.overCommitFactor < 1.0 {
		klog.Warningf("Invalid input %f for overcommit-factor, reason: overcommit-factor cannot be less than 1,"+
			" using default value: %f.", op.overCommitFactor, defaultOverCommitFactor)
		op.overCommitFactor = defaultOverCommitFactor
	}

	// calculate idle resources of total cluster, overcommit resources included
	total := apis.EmptyResource()
	used := apis.EmptyResource()
	for _, node := range sess.Nodes {
		total.Add(node.Allocatable)
		used.Add(node.Used)
	}
	op.idleResource = total.Clone().Multi(op.overCommitFactor).Sub(used)

	// calculate inqueue job resources
	for _, job := range sess.Jobs {
		if job.PodGroup.Status.Phase == scheduling.PodGroupInqueue && job.PodGroup.Spec.MinResources != nil {
			op.inqueueResource.Add(apis.NewResource(*job.PodGroup.Spec.MinResources))
			continue
		}
		// calculate inqueue resource for running jobs
		// the judgement 'job.PodGroup.Status.Running >= job.PodGroup.Spec.MinMember' will work on cases such as the following condition:
		// Considering a Spark job is completed(driver pod is completed) while the podgroup keeps running, the allocated resource will be reserved again if without the judgement.
		if job.PodGroup.Status.Phase == scheduling.PodGroupRunning &&
			job.PodGroup.Spec.MinResources != nil &&
			job.PodGroup.Status.Running >= job.PodGroup.Spec.MinMember {
			allocated := utils.GetAllocatedResource(job)
			inqueued := utils.GetInqueueResource(job, allocated)
			op.inqueueResource.Add(inqueued)
		}
	}

	sess.AddJobEnqueuableFn(op.Name(), func(obj interface{}) int {
		job := obj.(*apis.JobInfo)
		idle := op.idleResource
		inqueue := apis.EmptyResource()
		inqueue.Add(op.inqueueResource)
		if job.PodGroup.Spec.MinResources == nil {
			klog.V(4).Infof("Job <%s/%s> is bestEffort, permit to be inqueue.", job.Namespace, job.Name)
			return utils.Permit
		}

		// TODO: if allow 1 more job to be inqueue beyond overcommit-factor, large job may be inqueue and create pods
		jobMinReq := apis.NewResource(*job.PodGroup.Spec.MinResources)
		if inqueue.Add(jobMinReq).LessEqual(idle, apis.Zero) {
			klog.V(4).Infof("Sufficient resources, permit job <%s/%s> to be inqueue", job.Namespace, job.Name)
			return utils.Permit
		}
		klog.V(4).Infof("Resource in cluster is overused, reject job <%s/%s> to be inqueue",
			job.Namespace, job.Name)
		sess.RecordPodGroupEvent(job.PodGroup, corev1.EventTypeNormal, string(scheduling.PodGroupUnschedulableType), "resource in cluster is overused")
		return utils.Reject
	})

	sess.AddJobEnqueuedFn(op.Name(), func(obj interface{}) {
		job := obj.(*apis.JobInfo)
		if job.PodGroup.Spec.MinResources == nil {
			return
		}
		jobMinReq := apis.NewResource(*job.PodGroup.Spec.MinResources)
		op.inqueueResource.Add(jobMinReq)
	})
}

func (op *overcommitPlugin) OnSessionClose(sess *framework.Session) {
	op.idleResource = nil
	op.inqueueResource = nil
}
