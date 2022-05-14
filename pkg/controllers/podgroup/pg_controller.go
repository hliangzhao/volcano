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

package podgroup

import (
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	volcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned"
	volcanoinformers "github.com/hliangzhao/volcano/pkg/client/informers/externalversions"
	schedulinginformerv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/scheduling/v1alpha1"
	schedulinglisterv1alpha1 "github.com/hliangzhao/volcano/pkg/client/listers/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/controllers/framework"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelistersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

func init() {
	_ = framework.RegisterController(&pgController{})
}

type pgController struct {
	// clients
	kubeClient    kubernetes.Interface
	volcanoClient volcanoclient.Interface

	// informers
	podInformer coreinformersv1.PodInformer
	pgInformer  schedulinginformerv1alpha1.PodGroupInformer

	podLister corelistersv1.PodLister
	podSynced func() bool // TODO: change to cache.InformerSynced
	pgLister  schedulinglisterv1alpha1.PodGroupLister
	pgSynced  func() bool

	// work queue
	queue workqueue.RateLimitingInterface

	// schedulers
	schedulerNames []string
}

func (pgC *pgController) Name() string {
	return "pg-controller"
}

func (pgC *pgController) Initialize(opt *framework.ControllerOption) error {
	pgC.kubeClient = opt.KubeClient
	pgC.volcanoClient = opt.VolcanoClient

	pgC.queue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	pgC.schedulerNames = make([]string, len(opt.SchedulerNames))
	copy(pgC.schedulerNames, opt.SchedulerNames)

	// init for pods
	pgC.podInformer = opt.SharedInformerFactory.Core().V1().Pods()
	pgC.podLister = pgC.podInformer.Lister()
	pgC.podSynced = pgC.podInformer.Informer().HasSynced
	pgC.podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: pgC.addPod})

	// init for podgroups
	pgC.pgInformer = volcanoinformers.NewSharedInformerFactory(pgC.volcanoClient, 0).Scheduling().V1alpha1().PodGroups()
	pgC.pgLister = pgC.pgInformer.Lister()
	pgC.pgSynced = pgC.pgInformer.Informer().HasSynced

	return nil
}

func (pgC *pgController) Run(stopCh <-chan struct{}) {
	// start informer as a goroutine
	go pgC.podInformer.Informer().Run(stopCh)
	go pgC.pgInformer.Informer().Run(stopCh)

	// wait for cache sync
	cache.WaitForCacheSync(stopCh, pgC.podSynced, pgC.pgSynced)

	// finally, start the worker and run forever
	go wait.Until(pgC.worker, 0, stopCh)

	klog.Infof("pg-controller is running...")
}

func (pgC *pgController) worker() {
	// loop for retrieving callbacks forever
	for pgC.processNextReq() {
	}
}

// processNextReq retrieves a callback from work queue and process it.
// `false` is returned only if the request cannot be retrieved.
func (pgC *pgController) processNextReq() bool {
	obj, shutdown := pgC.queue.Get()
	if shutdown {
		klog.Errorf("Failed to retrieve callback from queue")
		return false
	}
	req := obj.(podRequest)
	defer pgC.queue.Done(req)

	pod, err := pgC.podLister.Pods(req.podNamespace).Get(req.podName)
	if err != nil {
		klog.Errorf("Failed to get pod by <%v> from cache: %v", req, err)
		return true
	}
	if !contains(pgC.schedulerNames, pod.Spec.SchedulerName) {
		klog.V(5).Infof("pod %v/%v field SchedulerName is not matched", pod.Namespace, pod.Name)
		return true
	}
	if pod.Annotations != nil && pod.Annotations[schedulingv1alpha1.KubeGroupNameAnnotationKey] != "" {
		klog.V(5).Infof("pod %v/%v has created podgroup", pod.Namespace, pod.Name)
		return true
	}

	// now we add this pod to an existing podgroup or a newly created podgroup
	if err := pgC.createNormalPodPGIfNotExist(pod); err != nil {
		klog.Errorf("Failed to handle pod <%s/%s>: %v", pod.Namespace, pod.Name, err)
		pgC.queue.AddRateLimited(req)
		return true
	}

	pgC.queue.Forget(req)
	return true
}

func contains(slice []string, element string) bool {
	for _, item := range slice {
		if item == element {
			return true
		}
	}
	return false
}
