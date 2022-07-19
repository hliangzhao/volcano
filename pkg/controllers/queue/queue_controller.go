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

package queue

// fully checked and understood

import (
	"context"
	"fmt"
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	schedulingv1alpha1 `github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1`
	volcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned"
	volcanoscheme "github.com/hliangzhao/volcano/pkg/client/clientset/versioned/scheme"
	volcanoinformers "github.com/hliangzhao/volcano/pkg/client/informers/externalversions"
	businformerv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/bus/v1alpha1"
	schedulinginformerv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/scheduling/v1alpha1"
	buslisterv1alpha1 "github.com/hliangzhao/volcano/pkg/client/listers/bus/v1alpha1"
	schedulinglisterv1alpha1 "github.com/hliangzhao/volcano/pkg/client/listers/scheduling/v1alpha1"
	controllerapis "github.com/hliangzhao/volcano/pkg/controllers/apis"
	"github.com/hliangzhao/volcano/pkg/controllers/framework"
	"github.com/hliangzhao/volcano/pkg/controllers/queue/state"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"sync"
	"time"
)

func init() {
	_ = framework.RegisterController(&queueController{})
}

type queueController struct {
	// clients
	kubeClient    kubernetes.Interface
	volcanoClient volcanoclient.Interface

	// informers
	queueInformer schedulinginformerv1alpha1.QueueInformer
	pgInformer    schedulinginformerv1alpha1.PodGroupInformer
	cmdInformer   businformerv1alpha1.CommandInformer

	queueLister schedulinglisterv1alpha1.QueueLister
	queueSynced cache.InformerSynced
	pgLister    schedulinglisterv1alpha1.PodGroupLister
	pgSynced    cache.InformerSynced
	cmdLister   buslisterv1alpha1.CommandLister
	cmdSynced   cache.InformerSynced

	queueQueue workqueue.RateLimitingInterface // used for enqueue queue req
	cmdQueue   workqueue.RateLimitingInterface // used for enqueue cmd req

	pgMutex   sync.RWMutex
	podgroups map[string]map[string]struct{} // queue-name: podgroup-ns/podgroup-name: podgroup{}

	syncHandler    func(req *controllerapis.Request) error
	syncCmdHandler func(cmd *busv1alpha1.Command) error

	enqueueQueue func(req *controllerapis.Request)

	recorder record.EventRecorder

	maxRequeueNum int
}

func (qc *queueController) Name() string {
	return "queue-controller"
}

func (qc *queueController) Initialize(opt *framework.ControllerOption) error {
	qc.kubeClient = opt.KubeClient
	qc.volcanoClient = opt.VolcanoClient

	factory := volcanoinformers.NewSharedInformerFactory(qc.volcanoClient, 0)

	qc.queueInformer = factory.Scheduling().V1alpha1().Queues()
	qc.pgInformer = factory.Scheduling().V1alpha1().PodGroups()
	qc.cmdInformer = factory.Bus().V1alpha1().Commands()

	qc.queueLister = qc.queueInformer.Lister()
	qc.queueSynced = qc.queueInformer.Informer().HasSynced

	qc.pgLister = qc.pgInformer.Lister()
	qc.pgSynced = qc.pgInformer.Informer().HasSynced

	qc.cmdLister = qc.cmdInformer.Lister()
	qc.cmdSynced = qc.cmdInformer.Informer().HasSynced

	qc.queueQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	qc.cmdQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	qc.podgroups = make(map[string]map[string]struct{})

	eventBC := record.NewBroadcaster()
	eventBC.StartLogging(klog.Infof)
	eventBC.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: qc.kubeClient.CoreV1().Events("")})
	qc.recorder = eventBC.NewRecorder(volcanoscheme.Scheme, corev1.EventSource{Component: "vc-controller-manager"})

	qc.maxRequeueNum = opt.MaxRequeueNum
	if qc.maxRequeueNum < 0 {
		qc.maxRequeueNum = -1
	}

	// set event handlers
	qc.queueInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    qc.addQueue,
		UpdateFunc: qc.updateQueue,
		DeleteFunc: qc.deleteQueue,
	})
	qc.pgInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    qc.addPodgroup,
		UpdateFunc: qc.updatePodgroup,
		DeleteFunc: qc.deletePodgroup,
	})
	qc.cmdInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		// only care about the cmd that owned by queue
		FilterFunc: func(obj interface{}) bool {
			switch v := obj.(type) {
			case *busv1alpha1.Command:
				return IsQueueReference(v.TargetObject)
			default:
				return false
			}
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: qc.addCmd,
		},
	})
	state.SyncQueue = qc.syncQueue
	state.OpenQueue = qc.openQueue
	state.CloseQueue = qc.closeQueue

	qc.syncHandler = qc.handleQueue
	qc.syncCmdHandler = qc.handleCmd

	qc.enqueueQueue = qc.enqueue

	return nil
}

// Run starts the workers and informers in separate coroutines.
func (qc *queueController) Run(stopCh <-chan struct{}) {
	defer runtime.HandleCrash()
	defer qc.queueQueue.ShutDown()
	defer qc.cmdQueue.ShutDown()

	klog.Infof("Starting queue controller.")
	defer klog.Infof("Shutting down queue controller.")

	// start informers with coroutines
	go qc.queueInformer.Informer().Run(stopCh)
	go qc.pgInformer.Informer().Run(stopCh)
	go qc.cmdInformer.Informer().Run(stopCh)

	// wait for cache setting up
	if !cache.WaitForCacheSync(stopCh, qc.queueSynced, qc.pgSynced, qc.cmdSynced) {
		klog.Errorf("unable to sync caches for queue controller.")
		return
	}

	// start workers with coroutines
	go wait.Until(qc.worker, 0, stopCh)
	go wait.Until(qc.cmdWorker, 0, stopCh)

	klog.Infof("queue-controller is running...")

	// if stopCh has content, exit
	<-stopCh
}

func (qc *queueController) worker() {
	for qc.processNextReq() {
	}
}

// processNextReq acquires a queue request from the work-queue and handle it.
func (qc *queueController) processNextReq() bool {
	obj, shutdown := qc.queueQueue.Get()
	if shutdown {
		klog.Errorf("Failed to retrieve callback from work-queue")
		return false
	}
	defer qc.queueQueue.Done(obj)

	req, ok := obj.(*controllerapis.Request)
	if !ok {
		klog.Errorf("%v is not a valid queue struct.", obj)
		// processNextReq always return true because we need the for-loop runs forever
		return true
	}

	// call handleQueue() to handle the req
	err := qc.syncHandler(req)
	qc.handleQueueErr(err, obj)
	return true
}

// handleQueue handles req. Specifically, it creates a new state according to the request and calls the Execute function to update the queue's status.
func (qc *queueController) handleQueue(req *controllerapis.Request) error {
	startTime := time.Now()
	defer func() {
		klog.V(4).Infof("Finished syncing queue %s (%v).", req.QueueName, time.Since(startTime))
	}()

	queue, err := qc.queueLister.Get(req.QueueName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.V(4).Infof("Queue %s has been deleted.", req.QueueName)
			return nil
		}
		return fmt.Errorf("get queue %s failed for %v", req.QueueName, err)
	}

	queueState := state.NewState(queue)
	if queueState == nil {
		return fmt.Errorf("queue %s state %s is invalid", queue.Name, queue.Status.State)
	}
	klog.V(4).Infof("Begin execute %s action for queue %s, current status %s", req.Action, req.QueueName, queue.Status.State)
	if err = queueState.Execute(req.Action); err != nil {
		return fmt.Errorf("sync queue %s failed for %v, event is %v, action is %s", req.QueueName, err, req.Event, req.Action)
	}
	return nil
}

// handleQueueErr will re-enqueue the request if the specific queue still has chance.
func (qc *queueController) handleQueueErr(err error, obj interface{}) {
	// no error happened
	if err == nil {
		qc.queueQueue.Forget(obj)
		return
	}

	// obj should be requeue, it still has chance
	if qc.maxRequeueNum == -1 || qc.queueQueue.NumRequeues(obj) < qc.maxRequeueNum {
		klog.V(4).Infof("Error syncing queue request %v for %v", obj, err)
		qc.queueQueue.AddRateLimited(obj)
		return
	}

	req, _ := obj.(*controllerapis.Request)
	qc.recordEventsForQueue(
		req.QueueName,
		corev1.EventTypeWarning,
		string(req.Action),
		fmt.Sprintf("%v queue failed for %v", req.Action, err),
	)
	klog.V(2).Infof("Dropping queue request %v out of the queue for %v.", obj, err)
	qc.queueQueue.Forget(obj)
}

func (qc *queueController) cmdWorker() {
	for qc.cmdProcessNextReq() {
	}
}

// cmdProcessNextReq acquires a command request from the work-queue and handle it.
func (qc *queueController) cmdProcessNextReq() bool {
	obj, shutdown := qc.cmdQueue.Get()
	if shutdown {
		return false
	}
	defer qc.cmdQueue.Done(obj)

	cmd, ok := obj.(*busv1alpha1.Command)
	if !ok {
		klog.Errorf("%^v is not a valid Command struct.", obj)
		return true
	}

	// call handleCmd() to handle it
	err := qc.syncCmdHandler(cmd)
	qc.handleCmdErr(err, obj)
	return true
}

func (qc *queueController) handleCmd(cmd *busv1alpha1.Command) error {
	startTime := time.Now()
	defer func() {
		klog.V(4).Infof("Finished syncing command %s/%s (%v).", cmd.Namespace, cmd.Name, time.Since(startTime))
	}()

	// Parse the queue request from the cmd and enqueue the request to the work-queue.
	// After enqueued, just delete the cmd resource from the cluster.
	err := qc.volcanoClient.BusV1alpha1().Commands(cmd.Namespace).Delete(context.TODO(), cmd.Name, metav1.DeleteOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete command <%s/%s> for %v", cmd.Namespace, cmd.Name, err)
	}
	req := &controllerapis.Request{
		QueueName: cmd.TargetObject.Name,
		Event:     busv1alpha1.CommandIssuedEvent,
		Action:    busv1alpha1.Action(cmd.Action),
	}
	qc.enqueueQueue(req)

	return nil
}

// handleCmdErr will re-enqueue the request if the specific cmd still has chance.
func (qc *queueController) handleCmdErr(err error, obj interface{}) {
	if err == nil {
		qc.cmdQueue.Forget(obj)
		return
	}

	if qc.maxRequeueNum == -1 || qc.cmdQueue.NumRequeues(obj) < qc.maxRequeueNum {
		klog.V(4).Infof("Error syncing command %v for %v.", obj, err)
		qc.cmdQueue.AddRateLimited(obj)
		return
	}

	klog.V(2).Infof("Dropping command %v out of the queue for %v.", obj, err)
	qc.cmdQueue.Forget(obj)
}

// IsQueueReference judges whether ref is Queue.
func IsQueueReference(ref *metav1.OwnerReference) bool {
	if ref == nil {
		return false
	}
	if ref.APIVersion != schedulingv1alpha1.SchemeGroupVersion.String() || ref.Kind != "Queue" {
		return false
	}
	return true
}
