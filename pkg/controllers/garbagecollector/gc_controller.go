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

package garbagecollector

import (
	"context"
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	volcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned"
	volcanoinformers "github.com/hliangzhao/volcano/pkg/client/informers/externalversions"
	batchinformerv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/batch/v1alpha1"
	batchlisterv1alpha1 "github.com/hliangzhao/volcano/pkg/client/listers/batch/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/controllers/framework"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"time"
)

func init() {
	_ = framework.RegisterController(&gcController{})
}

// gcController runs reflectors to watch for changes of managed API
// objects. Currently, it only watches Jobs. Triggered by Job creation
// and updates, it enqueues Jobs that have non-nil `.spec.ttlSecondsAfterFinished`
// to the `queue`. The gcController has workers who consume `queue`, check whether
// the Job TTL has expired or not; if the Job TTL hasn't expired, it will add the
// Job to the queue after the TTL is expected to expire; if the TTL has expired, the
// worker will send requests to the API server to delete the Jobs accordingly.
// This is implemented outside of Job controller for separation of concerns, and
// because it will be extended to handle other finishable resource types.
type gcController struct {
	volcanoClient volcanoclient.Interface

	jobInformer batchinformerv1alpha1.JobInformer
	jobLister   batchlisterv1alpha1.JobLister
	jobSynced   func() bool

	queue workqueue.RateLimitingInterface
}

func (gc *gcController) Name() string {
	return "gc-controller"
}

func (gc *gcController) Initialize(opt *framework.ControllerOption) error {
	gc.volcanoClient = opt.VolcanoClient

	jobInformer := volcanoinformers.NewSharedInformerFactory(gc.volcanoClient, 0).Batch().V1alpha1().Jobs()
	gc.jobInformer = jobInformer
	gc.jobLister = jobInformer.Lister()
	gc.jobSynced = jobInformer.Informer().HasSynced
	gc.queue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    gc.addJob,
		UpdateFunc: gc.updateJob,
	})

	return nil
}

func (gc *gcController) addJob(obj interface{}) {
	job := obj.(*batchv1alpha1.Job)
	klog.V(4).Infof("Adding job %s/%s", job.Namespace, job.Name)
	if job.DeletionTimestamp == nil && needsCleanup(job) {
		gc.enqueue(job)
	}
}

func needsCleanup(job *batchv1alpha1.Job) bool {
	return job.Spec.TTLSecondsAfterFinished != nil && isJobFinished(job)
}

func isJobFinished(job *batchv1alpha1.Job) bool {
	return job.Status.State.Phase == batchv1alpha1.Completed ||
		job.Status.State.Phase == batchv1alpha1.Failed ||
		job.Status.State.Phase == batchv1alpha1.Terminated
}

func (gc *gcController) enqueue(job *batchv1alpha1.Job) {
	klog.V(4).Infof("Add job %s/%s to cleanup", job.Namespace, job.Name)

	key, err := cache.MetaNamespaceKeyFunc(job)
	if err != nil {
		klog.Errorf("couldn't get key for object %#v: %v", job, err)
		return
	}
	gc.queue.Add(key)
}

func (gc *gcController) updateJob(oldObj, newObj interface{}) {
	job := newObj.(*batchv1alpha1.Job)
	klog.V(4).Infof("Updating job %s/%s", job.Namespace, job.Name)
	if job.DeletionTimestamp == nil && needsCleanup(job) {
		gc.enqueue(job)
	}
}

func (gc *gcController) Run(stopCh <-chan struct{}) {
	defer gc.queue.ShutDown()

	klog.Infof("Starting garbage collector")
	defer klog.Infof("Shutting down garbage collector")

	go gc.jobInformer.Informer().Run(stopCh)
	if !cache.WaitForCacheSync(stopCh, gc.jobSynced) {
		return
	}
	go wait.Until(gc.worker, time.Second, stopCh)

	<-stopCh
}

func (gc *gcController) worker() {
	for gc.processNextWorkItem() {
	}
}

func (gc *gcController) processNextWorkItem() bool {
	key, shutdown := gc.queue.Get()
	if shutdown {
		return false
	}
	defer gc.queue.Done(key)

	err := gc.processJob(key.(string))
	gc.handleErr(err, key)
	return true
}

// processJob will check the Job's state and TTL and delete the Job when it
// finishes and its TTL after finished has expired. If the Job hasn't finished or
// its TTL hasn't expired, it will be added to the queue after the TTL is expected
// to expire.
// This function is not meant to be invoked concurrently with the same key.
func (gc *gcController) processJob(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	klog.V(4).Infof("Checking if Job %s/%s is ready for cleanup", namespace, name)
	// Ignore the Jobs that are already deleted or being deleted, or the ones that don't need to clean up.
	job, err := gc.jobLister.Jobs(namespace).Get(name)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	if expired, err := gc.processTTL(job); err != nil {
		return err
	} else if !expired {
		return nil
	}

	// The Job's TTL is assumed to have expired, but the Job TTL might be stale.
	// Before deleting the Job, do a final sanity check.
	// If TTL is modified before we do this check, we cannot be sure if the TTL truly expires.
	// The latest Job may have a different UID, but it's fine because the checks will be run again.
	fresh, err := gc.volcanoClient.BatchV1alpha1().Jobs(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	// Use the latest Job TTL to see if the TTL truly expires.
	if expired, err := gc.processTTL(fresh); err != nil {
		return err
	} else if !expired {
		return nil
	}
	// Cascade deletes the Jobs if TTL truly expires.
	policy := metav1.DeletePropagationForeground
	options := metav1.DeleteOptions{
		PropagationPolicy: &policy,
		Preconditions:     &metav1.Preconditions{UID: &fresh.UID},
	}
	klog.V(4).Infof("Cleaning up Job %s/%s", namespace, name)
	return gc.volcanoClient.BatchV1alpha1().Jobs(fresh.Namespace).Delete(context.TODO(), fresh.Name, options)
}

func (gc *gcController) handleErr(err error, key interface{}) {
	if err == nil {
		gc.queue.Forget(key)
		return
	}
	klog.Errorf("error cleaning up Job %v, will retry: %v", key, err)
	gc.queue.AddRateLimited(key)
}

// processTTL checks whether a given Job's TTL has expired, and add it to the queue after the TTL is expected to expire
// if the TTL will expire later.
func (gc *gcController) processTTL(job *batchv1alpha1.Job) (expired bool, err error) {
	if job.DeletionTimestamp != nil || !needsCleanup(job) {
		return false, nil
	}

	now := time.Now()
	remaining, err := timeLeft(job, &now)
	if err != nil {
		return false, err
	}

	if *remaining <= 0 {
		return true, nil
	}

	gc.enqueueAfter(job, *remaining)
	return false, nil
}

func timeLeft(job *batchv1alpha1.Job, since *time.Time) (*time.Duration, error) {
	finishAt, expireAt, err := getFinishAndExpireTime(job)
	if err != nil {
		return nil, err
	}
	if finishAt.UTC().After(since.UTC()) {
		klog.Warningf("Warning: Found Job %s/%s finished in the future. "+
			"This is likely due to time skew in the cluster. Job cleanup will be deferred.", job.Namespace, job.Name)
	}
	remainingTime := expireAt.UTC().Sub(since.UTC())
	klog.V(4).Infof("Found Job %s/%s finished at %v, remaining TTL %v since %v, TTL will expire at %v",
		job.Namespace, job.Name, finishAt.UTC(), remainingTime, since.UTC(), expireAt.UTC())
	return &remainingTime, nil
}

func getFinishAndExpireTime(job *batchv1alpha1.Job) (*time.Time, *time.Time, error) {
	if !needsCleanup(job) {
		return nil, nil, fmt.Errorf("job %s/%s should not be cleaned up", job.Namespace, job.Name)
	}
	finishAt, err := jobFinishTime(job)
	if err != nil {
		return nil, nil, err
	}
	finishAtUTC := finishAt.UTC()
	expireAtUTC := finishAt.Add(time.Duration(*job.Spec.TTLSecondsAfterFinished) * time.Second)
	return &finishAtUTC, &expireAtUTC, nil
}

// jobFinishTime takes an already finished Job and returns the time it finishes.
func jobFinishTime(finishedJob *batchv1alpha1.Job) (metav1.Time, error) {
	if finishedJob.Status.State.LastTransitionTime.IsZero() {
		return metav1.Time{}, fmt.Errorf("unable to find the time when the Job %s/%s finished", finishedJob.Namespace, finishedJob.Name)
	}
	return finishedJob.Status.State.LastTransitionTime, nil
}

func (gc *gcController) enqueueAfter(job *batchv1alpha1.Job, after time.Duration) {
	key, err := cache.MetaNamespaceKeyFunc(job)
	if err != nil {
		klog.Errorf("couldn't get key for object %#v: %v", job, err)
		return
	}
	gc.queue.AddAfter(key, after)
}
