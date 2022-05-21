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

package job

import (
	"context"
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	apishelpers "github.com/hliangzhao/volcano/pkg/apis/helpers"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	volcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned"
	volcanoscheme "github.com/hliangzhao/volcano/pkg/client/clientset/versioned/scheme"
	"github.com/hliangzhao/volcano/pkg/client/informers/externalversions"
	batchinformerv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/batch/v1alpha1"
	businformerv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/bus/v1alpha1"
	schedulinginformerv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/scheduling/v1alpha1"
	batchlisterv1alpha1 "github.com/hliangzhao/volcano/pkg/client/listers/batch/v1alpha1"
	buslisterv1alpha1 "github.com/hliangzhao/volcano/pkg/client/listers/bus/v1alpha1"
	schedulinglisterv1alpha1 "github.com/hliangzhao/volcano/pkg/client/listers/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/controllers/apis"
	controllercache "github.com/hliangzhao/volcano/pkg/controllers/cache"
	"github.com/hliangzhao/volcano/pkg/controllers/framework"
	"github.com/hliangzhao/volcano/pkg/controllers/job/helpers"
	"github.com/hliangzhao/volcano/pkg/controllers/job/state"
	"golang.org/x/time/rate"
	"hash"
	"hash/fnv"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformersv1 "k8s.io/client-go/informers/core/v1"
	schedulinginformerv1 "k8s.io/client-go/informers/scheduling/v1"
	"k8s.io/client-go/kubernetes"
	coretypedv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelistersv1 "k8s.io/client-go/listers/core/v1"
	schedulinglisterv1 "k8s.io/client-go/listers/scheduling/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"time"
)

func init() {
	_ = framework.RegisterController(&jobController{})
}

type jobController struct {
	kubeClient    kubernetes.Interface
	volcanoClient volcanoclient.Interface

	// informers and listers
	jobInformer           batchinformerv1alpha1.JobInformer
	podInformer           coreinformersv1.PodInformer
	pvcInformer           coreinformersv1.PersistentVolumeClaimInformer
	pgInformer            schedulinginformerv1alpha1.PodGroupInformer
	svcInformer           coreinformersv1.ServiceInformer
	cmdInformer           businformerv1alpha1.CommandInformer
	priorityclassInformer schedulinginformerv1.PriorityClassInformer
	queueInformer         schedulinginformerv1alpha1.QueueInformer

	jobLister batchlisterv1alpha1.JobLister
	jobSynced func() bool // TODO: change to cache.InformerSynced

	podLister corelistersv1.PodLister
	podSynced func() bool

	pgLister schedulinglisterv1alpha1.PodGroupLister
	pgSynced func() bool

	pvcLister corelistersv1.PersistentVolumeClaimLister
	pvcSynced func() bool

	svcLister corelistersv1.ServiceLister
	svcSynced func() bool

	cmdLister buslisterv1alpha1.CommandLister
	cmdSynced func() bool

	priorityclassLister schedulinglisterv1.PriorityClassLister
	priorityclassSynced func() bool

	queueLister schedulinglisterv1alpha1.QueueLister
	queueSynced func() bool

	// work queues
	queueList []workqueue.RateLimitingInterface
	cmdQueue  workqueue.RateLimitingInterface
	cache     controllercache.Cache

	// event recorder
	recorder record.EventRecorder

	errTasks      workqueue.RateLimitingInterface
	workers       uint32
	maxRequeueNum int
}

func (jc *jobController) Name() string {
	return "job-controller"
}

func (jc *jobController) Initialize(opt *framework.ControllerOption) error {
	jc.kubeClient = opt.KubeClient
	jc.volcanoClient = opt.VolcanoClient

	sharedInformers := opt.SharedInformerFactory
	workers := opt.WorkerNum

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&coretypedv1.EventSinkImpl{Interface: jc.kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(volcanoscheme.Scheme, corev1.EventSource{Component: "vc-controller-manager"})

	jc.queueList = make([]workqueue.RateLimitingInterface, workers)
	jc.cmdQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	jc.cache = controllercache.NewCache()
	jc.errTasks = newRateLimitingQueue()
	jc.recorder = recorder
	jc.workers = workers
	jc.maxRequeueNum = opt.MaxRequeueNum
	if jc.maxRequeueNum < 0 {
		jc.maxRequeueNum = -1
	}

	var i uint32
	for i = 0; i < workers; i++ {
		jc.queueList[i] = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	}

	jc.jobInformer = externalversions.NewSharedInformerFactory(jc.volcanoClient, 0).Batch().V1alpha1().Jobs()
	jc.jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    jc.addJob,
		UpdateFunc: jc.updateJob,
		DeleteFunc: jc.deleteJob,
	})
	jc.jobLister = jc.jobInformer.Lister()
	jc.jobSynced = jc.jobInformer.Informer().HasSynced

	jc.cmdInformer = externalversions.NewSharedInformerFactory(jc.volcanoClient, 0).Bus().V1alpha1().Commands()
	jc.cmdInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			switch v := obj.(type) {
			case *busv1alpha1.Command:
				if v.TargetObject != nil &&
					v.TargetObject.APIVersion == batchv1alpha1.SchemeGroupVersion.String() &&
					v.TargetObject.Kind == "Job" {
					return true
				}
				return false
			default:
				return false
			}
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: jc.addCommand,
		},
	})
	jc.cmdLister = jc.cmdInformer.Lister()
	jc.cmdSynced = jc.cmdInformer.Informer().HasSynced

	jc.pgInformer = externalversions.NewSharedInformerFactory(jc.volcanoClient, 0).Scheduling().V1alpha1().PodGroups()
	jc.pgInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: jc.updatePodGroup,
	})
	jc.pgLister = jc.pgInformer.Lister()
	jc.pgSynced = jc.pgInformer.Informer().HasSynced

	jc.queueInformer = externalversions.NewSharedInformerFactory(jc.volcanoClient, 0).Scheduling().V1alpha1().Queues()
	jc.queueLister = jc.queueInformer.Lister()
	jc.queueSynced = jc.queueInformer.Informer().HasSynced

	jc.podInformer = sharedInformers.Core().V1().Pods()
	jc.podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    jc.addPod,
		UpdateFunc: jc.updatePod,
		DeleteFunc: jc.deletePod,
	})
	jc.podLister = jc.podInformer.Lister()
	jc.podSynced = jc.podInformer.Informer().HasSynced

	jc.pvcInformer = sharedInformers.Core().V1().PersistentVolumeClaims()
	jc.pvcLister = jc.pvcInformer.Lister()
	jc.pvcSynced = jc.pvcInformer.Informer().HasSynced

	jc.svcInformer = sharedInformers.Core().V1().Services()
	jc.svcLister = jc.svcInformer.Lister()
	jc.svcSynced = jc.svcInformer.Informer().HasSynced

	jc.priorityclassInformer = sharedInformers.Scheduling().V1().PriorityClasses()
	jc.priorityclassLister = jc.priorityclassInformer.Lister()
	jc.priorityclassSynced = jc.priorityclassInformer.Informer().HasSynced

	state.SyncJob = jc.syncJob
	state.KillJob = jc.killJob

	return nil
}

// Run starts the job-controller.
func (jc *jobController) Run(stopCh <-chan struct{}) {
	// start informer as a goroutine
	go jc.jobInformer.Informer().Run(stopCh)
	go jc.podInformer.Informer().Run(stopCh)
	go jc.pvcInformer.Informer().Run(stopCh)
	go jc.pgInformer.Informer().Run(stopCh)
	go jc.svcInformer.Informer().Run(stopCh)
	go jc.cmdInformer.Informer().Run(stopCh)
	go jc.priorityclassInformer.Informer().Run(stopCh)
	go jc.queueInformer.Informer().Run(stopCh)

	// wait for cache sync
	cache.WaitForCacheSync(stopCh, jc.jobSynced, jc.podSynced, jc.pvcSynced, jc.pgSynced,
		jc.svcSynced, jc.cmdSynced, jc.priorityclassSynced, jc.queueSynced)

	// finally, start each worker as seperated goroutine and run forever
	go wait.Until(jc.handleCommands, 0, stopCh)
	var i uint32
	for i = 0; i < jc.workers; i++ {
		go func(num uint32) {
			wait.Until(
				func() {
					jc.worker(num)
				},
				time.Second,
				stopCh,
			)
		}(i)
	}

	go jc.cache.Run(stopCh)

	// Re-sync error tasks
	go wait.Until(jc.processReSyncedTask, 0, stopCh)

	klog.Infof("job-controller is running...")
}

func (jc *jobController) worker(i uint32) {
	klog.Infof("worker %d starting...", i)
	for jc.processNextReq(i) {
	}
}

func (jc *jobController) belongsToThisRoutine(key string, count uint32) bool {
	var hashVal hash.Hash32
	var val uint32

	hashVal = fnv.New32()
	_, _ = hashVal.Write([]byte(key))
	val = hashVal.Sum32()

	return val%jc.workers == count
}

func (jc *jobController) getWorkerQueue(key string) workqueue.RateLimitingInterface {
	var hashVal hash.Hash32
	var val uint32

	hashVal = fnv.New32()
	_, _ = hashVal.Write([]byte(key))
	val = hashVal.Sum32()

	return jc.queueList[val%jc.workers]
}

// processNextReq retrieves a callback from work queue and process it.
// `false` is returned only if the request cannot be retrieved.
func (jc *jobController) processNextReq(count uint32) bool {
	queue := jc.queueList[count]
	obj, shutdown := queue.Get()
	if shutdown {
		klog.Errorf("Failed to pop item from queue")
		return false
	}

	req := obj.(apis.Request)
	defer queue.Done(req)

	key := controllercache.JobKeyByRequest(&req)
	if !jc.belongsToThisRoutine(key, count) {
		// add this req to the right queue
		klog.Errorf("Should not occur. The job does not belongs to this routine key:%s, worker:%d...... ", key, count)
		queueLocal := jc.getWorkerQueue(key)
		queueLocal.Add(req)
		return true
	}

	klog.V(3).Infof("Try to handle request <%v>", req)
	jobInfo, err := jc.cache.Get(key)
	if err != nil {
		// TODO: ignore not-ready error.
		klog.Errorf("Failed to get job by <%v> from cache: %v", req, err)
		return true
	}

	stt := state.NewState(jobInfo)
	if stt == nil {
		klog.Errorf("Invalid state <%s> of Job <%v/%v>",
			jobInfo.Job.Status.State, jobInfo.Job.Namespace, jobInfo.Job.Name)
		return true
	}

	act := applyPolices(jobInfo.Job, &req)
	klog.V(3).Infof("Execute <%v> on Job <%s/%s> in <%s> by <%T>.",
		act, req.Namespace, req.JobName, jobInfo.Job.Status.State.Phase, stt)

	if act != busv1alpha1.SyncJobAction {
		jc.recordJobEvent(jobInfo.Job.Namespace, jobInfo.Job.Name, batchv1alpha1.ExecuteAction, fmt.Sprintf(
			"Start to execute action %s ", act))
	}

	if err = stt.Execute(act); err != nil {
		if jc.maxRequeueNum == -1 || queue.NumRequeues(req) < jc.maxRequeueNum {
			klog.V(2).Infof("Failed to handle Job <%s/%s>: %v",
				jobInfo.Job.Namespace, jobInfo.Job.Name, err)
			// If any error, requeue it.
			queue.AddRateLimited(req)
			return true
		}
		jc.recordJobEvent(jobInfo.Job.Namespace, jobInfo.Job.Name, batchv1alpha1.ExecuteAction, fmt.Sprintf(
			"Job failed on action %s for retry limit reached", act))
		klog.Warningf("Terminating Job <%s/%s> and releasing resources", jobInfo.Job.Namespace, jobInfo.Job.Name)

		if err = stt.Execute(busv1alpha1.TerminateJobAction); err != nil {
			klog.Errorf("Failed to terminate Job<%s/%s>: %v", jobInfo.Job.Namespace, jobInfo.Job.Name, err)
		}
		klog.Warningf("Dropping job <%s/%s> out of the queue: %v because max retries has reached",
			jobInfo.Job.Namespace, jobInfo.Job.Name, err)
	}

	// If no error, forget it.
	queue.Forget(req)

	return true
}

func newRateLimitingQueue() workqueue.RateLimitingInterface {
	return workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 180*time.Second),
		// 10 qps, 100 bucket size. This is only for retry speed and the overall factor (not per item)
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	))
}

func (jc *jobController) processReSyncedTask() {
	obj, shutdown := jc.errTasks.Get()
	if shutdown {
		return
	}

	// reSync only 10 times
	if jc.errTasks.NumRequeues(obj) > 10 {
		jc.errTasks.Forget(obj)
		return
	}
	defer jc.errTasks.Done(obj)

	task, ok := obj.(*corev1.Pod)
	if !ok {
		klog.Errorf("failed to convert %v to *corev1.Pod", obj)
		return
	}

	if err := jc.syncTask(task); err != nil {
		klog.Errorf("Failed to sync pod <%v/%v>, retry it, err %v", task.Namespace, task.Name, err)
		jc.reSyncTask(task)
	}
}

func (jc *jobController) syncTask(oldTask *corev1.Pod) error {
	// get the latest pod from cluster and update it to job-controller's cache
	newPod, err := jc.kubeClient.CoreV1().Pods(oldTask.Namespace).Get(context.TODO(), oldTask.Name, metav1.GetOptions{})
	if err != nil {
		// pod not found in cluster, delete it from cache
		if errors.IsNotFound(err) {
			if err = jc.cache.DeletePod(oldTask); err != nil {
				klog.Errorf("failed to delete cache pod <%v/%v>, err %v.", oldTask.Namespace, oldTask.Name, err)
				return err
			}
			klog.V(3).Infof("Pod <%v/%v> was deleted, removed from cache.", oldTask.Namespace, oldTask.Name)
			return nil
		}
		return fmt.Errorf("failed to get Pod <%v/%v>: err %v", oldTask.Namespace, oldTask.Name, err)
	}
	return jc.cache.UpdatePod(newPod)
}

func (jc *jobController) reSyncTask(task *corev1.Pod) {
	jc.errTasks.AddRateLimited(task)
}

/* Some auxiliary utilities for job-controller. */

var detectionPeriodOfDependsOnTask time.Duration

func SetDetectionPeriodOfDependsOnTask(period time.Duration) {
	detectionPeriodOfDependsOnTask = period
}

// MakePodName append podName, jobName, taskName and index and returns the string.
func MakePodName(jobName, taskName string, index int) string {
	return fmt.Sprintf(helpers.PodNameFmt, jobName, taskName, index)
}

func createJobPod(job *batchv1alpha1.Job, template *corev1.PodTemplateSpec, topologyPolicy batchv1alpha1.NumaPolicy,
	index int, jobForwarding bool) *corev1.Pod {

	templateCopy := template.DeepCopy()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            helpers.MakePodName(job.Name, template.Name, index),
			Namespace:       job.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(job, apishelpers.JobKind)},
			Labels:          templateCopy.Labels,
			Annotations:     templateCopy.Annotations,
		},
		Spec: templateCopy.Spec,
	}

	// If no scheduler name in Pod, use scheduler name from Job.
	if len(pod.Spec.SchedulerName) == 0 {
		pod.Spec.SchedulerName = job.Spec.SchedulerName
	}

	// bind volumes
	volumeMap := make(map[string]string)
	for _, volume := range job.Spec.Volumes {
		vcName := volume.VolumeClaimName
		name := fmt.Sprintf("%s-%s", job.Name, helpers.GenRandomStr(12))
		if _, ok := volumeMap[vcName]; !ok {
			pod.Spec.Volumes = append(pod.Spec.Volumes,
				corev1.Volume{
					Name: name,
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: vcName,
						},
					},
				},
			)
			volumeMap[vcName] = name
		} else {
			continue
		}

		for i, c := range pod.Spec.Containers {
			vm := corev1.VolumeMount{
				MountPath: volume.MountPath,
				Name:      name,
			}
			pod.Spec.Containers[i].VolumeMounts = append(c.VolumeMounts, vm)
		}
	}

	tsKey := templateCopy.Name
	if len(tsKey) == 0 {
		tsKey = batchv1alpha1.DefaultTaskSpec
	}

	if len(pod.Annotations) == 0 {
		pod.Annotations = make(map[string]string)
	}

	pod.Annotations[batchv1alpha1.TaskSpecKey] = tsKey
	pod.Annotations[schedulingv1alpha1.KubeGroupNameAnnotationKey] = job.Name
	pod.Annotations[batchv1alpha1.JobNameKey] = job.Name
	pod.Annotations[batchv1alpha1.QueueNameKey] = job.Spec.Queue
	pod.Annotations[batchv1alpha1.JobVersion] = fmt.Sprintf("%d", job.Status.Version)
	pod.Annotations[batchv1alpha1.PodTemplateKey] = fmt.Sprintf("%s-%s", job.Name, template.Name)

	if topologyPolicy != "" {
		pod.Annotations[schedulingv1alpha1.NumaPolicyKey] = string(topologyPolicy)
	}

	if len(job.Annotations) > 0 {
		if value, found := job.Annotations[schedulingv1alpha1.PodPreemptable]; found {
			pod.Annotations[schedulingv1alpha1.PodPreemptable] = value
		}
		if value, found := job.Annotations[schedulingv1alpha1.RevocableZone]; found {
			pod.Annotations[schedulingv1alpha1.RevocableZone] = value
		}

		if value, found := job.Annotations[schedulingv1alpha1.JDBMinAvailable]; found {
			pod.Annotations[schedulingv1alpha1.JDBMinAvailable] = value
		} else if value, found := job.Annotations[schedulingv1alpha1.JDBMaxUnavailable]; found {
			pod.Annotations[schedulingv1alpha1.JDBMaxUnavailable] = value
		}
	}

	if len(pod.Labels) == 0 {
		pod.Labels = make(map[string]string)
	}

	// Set pod labels for Service.
	pod.Labels[batchv1alpha1.JobNameKey] = job.Name
	pod.Labels[batchv1alpha1.TaskSpecKey] = tsKey
	pod.Labels[batchv1alpha1.JobNamespaceKey] = job.Namespace
	pod.Labels[batchv1alpha1.QueueNameKey] = job.Spec.Queue
	if len(job.Labels) > 0 {
		if value, found := job.Labels[schedulingv1alpha1.PodPreemptable]; found {
			pod.Labels[schedulingv1alpha1.PodPreemptable] = value
		}
	}

	if jobForwarding {
		pod.Annotations[batchv1alpha1.JobForwardingKey] = "true"
		pod.Labels[batchv1alpha1.JobForwardingKey] = "true"
	}

	return pod
}

func applyPolices(job *batchv1alpha1.Job, req *apis.Request) busv1alpha1.Action {
	if len(req.Action) != 0 {
		return req.Action
	}

	if req.Event == busv1alpha1.OutOfSyncEvent {
		return busv1alpha1.SyncJobAction
	}

	// For all the requests triggered from discarded job resources will perform sync action instead
	if req.JobVersion < job.Status.Version {
		klog.Infof("Request %s is outdated, will perform sync instead.", req)
		return busv1alpha1.SyncJobAction
	}

	// Overwrite Job level policies for the task in request.
	if len(req.TaskName) != 0 {
		for _, task := range job.Spec.Tasks {
			if task.Name == req.TaskName {
				for _, policy := range task.Policies {
					policyEvents := getEventList(policy)
					if len(policyEvents) > 0 && len(req.Event) > 0 {
						if checkEventExist(policyEvents, req.Event) ||
							checkEventExist(policyEvents, busv1alpha1.AnyEvent) {
							return policy.Action
						}
					}
					if policy.ExitCode != nil && *policy.ExitCode == req.ExitCode {
						return policy.Action
					}
				}
				break
			}
		}
	}

	// Parse Job level policies
	for _, policy := range job.Spec.Policies {
		policyEvents := getEventList(policy)
		if len(policyEvents) > 0 && len(req.Event) > 0 {
			if checkEventExist(policyEvents, req.Event) ||
				checkEventExist(policyEvents, busv1alpha1.AnyEvent) {
				return policy.Action
			}
		}
		if policy.ExitCode != nil && *policy.ExitCode == req.ExitCode {
			return policy.Action
		}
	}

	return busv1alpha1.SyncJobAction
}

func getEventList(policy batchv1alpha1.LifecyclePolicy) []busv1alpha1.Event {
	el := policy.Events
	if len(policy.Event) > 0 {
		el = append(el, policy.Event)
	}
	return el
}

func checkEventExist(policyEvents []busv1alpha1.Event, reqEvent busv1alpha1.Event) bool {
	for _, e := range policyEvents {
		if e == reqEvent {
			return true
		}
	}
	return false
}

func addResourceList(list, req, limit corev1.ResourceList) {
	for resName, resQuantity := range req {
		if val, ok := list[resName]; !ok {
			list[resName] = resQuantity.DeepCopy()
		} else {
			val.Add(resQuantity)
			list[resName] = val
		}
	}

	if req != nil {
		return
	}

	// If Requests is omitted for a container,
	// it defaults to Limits if that is explicitly specified.
	for resName, resQuantity := range limit {
		if val, ok := list[resName]; !ok {
			list[resName] = resQuantity.DeepCopy()
		} else {
			val.Add(resQuantity)
			list[resName] = val
		}
	}
}

type TaskPriority struct {
	priority int32
	batchv1alpha1.TaskSpec
}

type TasksPriority []TaskPriority

func (p TasksPriority) Len() int {
	return len(p)
}

func (p TasksPriority) Less(i, j int) bool {
	return p[i].priority > p[j].priority
}

func (p TasksPriority) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func isControlledBy(obj metav1.Object, gvk schema.GroupVersionKind) bool {
	controllerRef := metav1.GetControllerOf(obj)
	if controllerRef == nil {
		return false
	}
	if controllerRef.APIVersion == gvk.GroupVersion().String() && controllerRef.Kind == gvk.Kind {
		return true
	}
	return false
}
