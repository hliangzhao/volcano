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

package job

import (
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	volcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned"
	volcanoscheme "github.com/hliangzhao/volcano/pkg/client/clientset/versioned/scheme"
	"github.com/hliangzhao/volcano/pkg/client/informers/externalversions"
	batchinformerv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/batch/v1alpha1"
	businformerv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/bus/v1alpha1"
	schedulinginformerv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/scheduling/v1alpha1"
	batchlisterv1alpha1 "github.com/hliangzhao/volcano/pkg/client/listers/batch/v1alpha1"
	buslisterv1alpha1 "github.com/hliangzhao/volcano/pkg/client/listers/bus/v1alpha1"
	schedulinglisterv1alpha1 "github.com/hliangzhao/volcano/pkg/client/listers/scheduling/v1alpha1"
	controllercache "github.com/hliangzhao/volcano/pkg/controllers/cache"
	"github.com/hliangzhao/volcano/pkg/controllers/framework"
	"github.com/hliangzhao/volcano/pkg/controllers/job/state"
	corev1 "k8s.io/api/core/v1"
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
)

func init() {

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

func (jc *jobController) Run(stopCh <-chan struct{}) {}

func (jc *jobController) worker(i uint32) {}

func (jc *jobController) belongsToThisRoutine(key string, count uint32) bool {
	return false
}

func (jc *jobController) getWorkerQueue(key string) workqueue.RateLimitingInterface {
	return nil
}

func (jc *jobController) processNextReq(count uint32) bool {
	return false
}

func newRateLimitingQueue() workqueue.RateLimitingInterface {
	// TODO
	return nil
}

func (jc *jobController) processReSyncedTask() {}

func (jc *jobController) syncTask(oldTask *corev1.Pod) error {
	return nil
}

func (jc *jobController) reSyncTask(task *corev1.Pod) {
	jc.errTasks.AddRateLimited(task)
}
