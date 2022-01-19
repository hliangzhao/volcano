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

package cache

import (
	"context"
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	schedulingscheme "github.com/hliangzhao/volcano/pkg/apis/scheduling/scheme"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	volcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned"
	volcanoinformers "github.com/hliangzhao/volcano/pkg/client/informers/externalversions"
	nodeinfoinformersv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/nodeinfo/v1alpha1"
	schedulinginformersv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	coreinformersv1 "k8s.io/client-go/informers/core/v1"
	schedulinginformersv1 "k8s.io/client-go/informers/scheduling/v1"
	storageinformersv1 "k8s.io/client-go/informers/storage/v1"
	"k8s.io/client-go/informers/storage/v1alpha1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
	"sync"
)

// podConditionHaveUpdate checks whether condition is changed from status.
func podConditionHaveUpdate(status *corev1.PodStatus, condition *corev1.PodCondition) bool {
	lastTransitTime := metav1.Now()
	_, oldCondition := podutil.GetPodCondition(status, condition.Type)
	if oldCondition == nil {
		return true
	}

	// before updating an existing condition, check if it has changed
	if condition.Status == oldCondition.Status {
		lastTransitTime = oldCondition.LastTransitionTime
	}
	isEqual := condition.Status == oldCondition.Status && condition.Reason == oldCondition.Reason &&
		condition.Message == oldCondition.Message && condition.LastProbeTime.Equal(&oldCondition.LastProbeTime) &&
		lastTransitTime.Equal(&oldCondition.LastTransitionTime)
	return !isEqual
}

/* Binder related. Binder is used to bind task to node. */

type defaultBinder struct {
	kubeClient *kubernetes.Clientset
}

func NewBinder() *defaultBinder {
	return &defaultBinder{}
}

// Bind sends bind request to api server.
func (binder *defaultBinder) Bind(kubeClient *kubernetes.Clientset, tasks []*apis.TaskInfo) (error, []*apis.TaskInfo) {
	var errTasks []*apis.TaskInfo
	for _, task := range tasks {
		pod := task.Pod
		if err := kubeClient.CoreV1().Pods(pod.Namespace).Bind(
			context.TODO(),
			&corev1.Binding{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   pod.Namespace,
					Name:        pod.Name,
					UID:         pod.UID,
					Annotations: pod.Annotations,
				},
				Target: corev1.ObjectReference{
					Kind: "Node",
					Name: task.NodeName,
				},
			},
			metav1.CreateOptions{},
		); err != nil {
			klog.Errorf("Failed to bind pod <%v/%v> to node %s : %#v", pod.Namespace, pod.Name, task.NodeName, err)
			errTasks = append(errTasks, task)
		}
	}

	if len(errTasks) > 0 {
		return fmt.Errorf("failed to bind pods"), errTasks
	}

	return nil, nil
}

/* Evictor related. Evictor is used to evict pod. */

type defaultEvictor struct {
	kubeClient *kubernetes.Clientset
	recorder   record.EventRecorder
}

func (evictor *defaultEvictor) Evict(pod *corev1.Pod, reason string) error {
	klog.V(3).Infof("Evicting pod %v/%v, because of %v", pod.Namespace, pod.Name, reason)

	evictMsg := fmt.Sprintf("Pod is evicted, because of %v", reason)
	annotations := map[string]string{}
	evictor.recorder.AnnotatedEventf(pod, annotations, corev1.EventTypeWarning, "Evict", evictMsg)

	newPod := pod.DeepCopy()
	cond := &corev1.PodCondition{
		Type:    corev1.PodReady,
		Status:  corev1.ConditionFalse,
		Reason:  "Evict",
		Message: evictMsg,
	}
	if !podutil.UpdatePodCondition(&newPod.Status, cond) {
		klog.V(1).Infof("UpdatePodCondition: existed condition, not update")
		klog.V(1).Infof("%+v", newPod.Status.Conditions)
		return nil
	}

	// update pod status then delete
	if _, err := evictor.kubeClient.CoreV1().Pods(pod.Namespace).UpdateStatus(context.TODO(),
		newPod, metav1.UpdateOptions{}); err != nil {
		klog.Errorf("Failed to update pod <%v/%v> status: %v", newPod.Namespace, newPod.Name, err)
		return err
	}
	if err := evictor.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(),
		pod.Name, metav1.DeleteOptions{}); err != nil {
		klog.Errorf("Failed to evict pod <%v/%v>: %#v", pod.Namespace, pod.Name, err)
		return err
	}

	return nil
}

/* StatusUpdater related */

// defaultStatusUpdater is the default implementation of the StatusUpdater interface
type defaultStatusUpdater struct {
	kubeClient *kubernetes.Clientset
	vcClient   *volcanoclient.Clientset
}

// UpdatePodCondition updates pod with podCondition.
func (su *defaultStatusUpdater) UpdatePodCondition(pod *corev1.Pod, condition *corev1.PodCondition) (*corev1.Pod, error) {
	klog.V(3).Infof("Updating pod condition for %s/%s to (%s==%s)",
		pod.Namespace, pod.Name, condition.Type, condition.Status)
	if podutil.UpdatePodCondition(&pod.Status, condition) {
		return su.kubeClient.CoreV1().Pods(pod.Namespace).UpdateStatus(context.TODO(), pod, metav1.UpdateOptions{})
	}
	return pod, nil
}

// UpdatePodGroup updates pod with podCondition.
func (su *defaultStatusUpdater) UpdatePodGroup(pg *apis.PodGroup) (*apis.PodGroup, error) {
	podgroup := &schedulingv1alpha1.PodGroup{}
	if err := schedulingscheme.Scheme.Convert(&pg.PodGroup, podgroup, nil); err != nil {
		klog.Errorf("Error while converting apis.PodGroup to v1alpha1.PodGroup with error: %v", err)
		return nil, err
	}

	updated, err := su.vcClient.SchedulingV1alpha1().PodGroups(podgroup.Namespace).Update(context.TODO(),
		podgroup, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Error while updating PodGroup with error: %v", err)
		return nil, err
	}

	pgInfo := &apis.PodGroup{Version: apis.PodGroupVersionV1Alpha1}
	if err := schedulingscheme.Scheme.Convert(updated, &pgInfo, nil); err != nil {
		klog.Errorf("Error while converting v1alpha.PodGroup to apis.PodGroup with error: %v", err)
		return nil, err
	}

	return pgInfo, nil
}

/* VolumeBinder related */

type defaultVolumeBinder struct {
	volumeBinder volumebinding.SchedulerVolumeBinder
}

// AllocateVolumes allocates volume on the host to the task.
func (vb *defaultVolumeBinder) AllocateVolumes(task *apis.TaskInfo, hostname string, podVolumes *volumebinding.PodVolumes) error {
	allBound, err := vb.volumeBinder.AssumePodVolumes(task.Pod, hostname, podVolumes)
	task.VolumeReady = allBound
	return err
}

// GetPodVolumes get pod volume on the host.
func (vb *defaultVolumeBinder) GetPodVolumes(task *apis.TaskInfo, node *corev1.Node) (*volumebinding.PodVolumes, error) {
	boundClaims, claimsToBind, _, err := vb.volumeBinder.GetPodVolumes(task.Pod)
	if err != nil {
		return nil, err
	}

	podVolumes, _, err := vb.volumeBinder.FindPodVolumes(task.Pod, boundClaims, claimsToBind, node)
	return podVolumes, err
}

// BindVolumes binds volumes to the task.
func (vb *defaultVolumeBinder) BindVolumes(task *apis.TaskInfo, podVolumes *volumebinding.PodVolumes) error {
	// If task's volumes are ready, did not bind them again.
	if task.VolumeReady {
		return nil
	}
	return vb.volumeBinder.BindPodVolumes(task.Pod, podVolumes)
}

/* Batch Binder related */

type pgBinder struct {
	kubeClient *kubernetes.Clientset
	vcClient   *volcanoclient.Clientset
}

// Bind adds silo cluster annotation on pod and podgroup.
func (pgb *pgBinder) Bind(job *apis.JobInfo, cluster string) (*apis.JobInfo, error) {
	if len(job.Tasks) == 0 {
		klog.V(4).Infof("Job pods have not been created yet")
		return job, nil
	}

	for _, task := range job.Tasks {
		// set annotation on each pod and update
		pod := task.Pod
		pod.Annotations[batchv1alpha1.ForwardClusterKey] = cluster
		pod.ResourceVersion = ""

		_, err := pgb.kubeClient.CoreV1().Pods(pod.Namespace).UpdateStatus(context.TODO(), pod, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Error while update pod annotation with error: %v", err)
			return nil, err
		}
	}

	// set annotation on the podgroup and update
	pg := job.PodGroup
	pg.Annotations[batchv1alpha1.ForwardClusterKey] = cluster
	podgroup := &schedulingv1alpha1.PodGroup{}

	if err := schedulingscheme.Scheme.Convert(&pg.PodGroup, podgroup, nil); err != nil {
		klog.Errorf("Error while converting apis.PodGroup to v1alpha1.PodGroup with error: %v", err)
		return nil, err
	}

	newPg, err := pgb.vcClient.SchedulingV1alpha1().PodGroups(pg.Namespace).Update(context.TODO(),
		podgroup, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Error while update PodGroup annotation with error: %v", err)
		return nil, err
	}
	job.PodGroup.ResourceVersion = newPg.ResourceVersion

	klog.V(4).Infof("Bind PodGroup <%s> successfully", job.PodGroup.Name)
	return job, nil
}

type SchedulerCache struct {
	sync.Mutex

	kubeClient         *kubernetes.Clientset
	vcClient           *volcanoclient.Clientset
	defaultQueue       string
	schedulerName      string
	nodeSelectorLabels map[string]string

	// informers
	podInformer       coreinformersv1.PodInformer
	nodeInformer      coreinformersv1.NodeInformer
	pgInformer        schedulinginformersv1alpha1.PodGroupInformer
	queueInformer     schedulinginformersv1alpha1.QueueInformer
	pvInformer        coreinformersv1.PersistentVolumeInformer
	pvcInformer       coreinformersv1.PersistentVolumeClaimInformer
	scInformer        storageinformersv1.StorageClassInformer
	pcInformer        schedulinginformersv1.PriorityClassInformer
	quotaInformer     coreinformersv1.ResourceQuotaInformer
	csiNodeInformer   storageinformersv1.CSINodeInformer
	csiDriverInformer storageinformersv1.CSIDriverInformer
	csiSCInformer     v1alpha1.CSIStorageCapacityInformer
	cpuInformer       nodeinfoinformersv1alpha1.NumatopologyInformer

	Binder         Binder
	Evictor        Evictor
	StatusUpdater  StatusUpdater
	PodGroupBinder BatchBinder
	VolumeBinder   VolumeBinder

	Recorder record.EventRecorder

	Jobs                 map[apis.JobID]*apis.JobInfo
	Nodes                map[string]*apis.NodeInfo
	Queues               map[apis.QueueID]*apis.QueueInfo
	PriorityClasses      map[string]*schedulingv1.PriorityClass
	defaultPriorityClass *schedulingv1.PriorityClass
	defaultPriority      int32
	NodeList             []string

	NamespaceCollection map[string]*apis.NamespaceCollection

	errTasks    workqueue.RateLimitingInterface
	deletedJobs workqueue.RateLimitingInterface

	kubeInformerFactory informers.SharedInformerFactory
	vcInformerFactory   volcanoinformers.SharedInformerFactory

	BindFlowChannel chan *apis.TaskInfo
	bindCache       []*apis.TaskInfo
	batchNum        int
}

func newSchedulerCache(config *rest.Config, schedulerName string, defaultQueue string, nodeSelectors []string) *SchedulerCache {
	// TODO
	return nil
}

// taskUnschedulable updates pod status of pending task
func (sc *SchedulerCache) taskUnschedulable(task *apis.TaskInfo, reason, msg string) error {
	pod := task.Pod
	condition := &corev1.PodCondition{
		Type:    corev1.PodScheduled,
		Status:  corev1.ConditionFalse,
		Reason:  reason, // TODO: Add more reasons in order to distinguish more specific scenario of pending tasks
		Message: msg,
	}

	if podConditionHaveUpdate(&pod.Status, condition) {
		newPod := pod.DeepCopy()

		// The reason field in 'Events' should be "FailedScheduling", there is no constant defined for this in
		// k8s core, so using the same string here.
		// The reason field in PodCondition can be "Unschedulable"
		sc.Recorder.Event(pod, corev1.EventTypeWarning, "FailedScheduling", msg)
		if _, err := sc.StatusUpdater.UpdatePodCondition(newPod, condition); err != nil {
			return err
		}
	} else {
		klog.V(4).Infof("task unschedulable %s/%s, message: %s, skip by no condition update", pod.Namespace, pod.Name, msg)
	}

	return nil
}

func (sc *SchedulerCache) deleteJob(job *apis.JobInfo) {
	klog.V(3).Infof("Try to delete Job <%v:%v/%v>", job.UID, job.Namespace, job.Name)
	sc.deletedJobs.AddRateLimited(job)
}

func (sc *SchedulerCache) reSyncTask(task *apis.TaskInfo) {
	sc.errTasks.AddRateLimited(task)
}

func (sc *SchedulerCache) processReSyncTask() {
	obj, shutdown := sc.errTasks.Get()
	if shutdown {
		return
	}
	defer sc.errTasks.Done(obj)

	task, ok := obj.(*apis.TaskInfo)
	if !ok {
		klog.Errorf("failed to convert %v to *apis.TaskInfo", obj)
		return
	}
	if err := sc.syncTask(task); err != nil {
		klog.Errorf("Failed to sync pod <%v/%v>, retry it.", task.Namespace, task.Name)
		sc.reSyncTask(task)
	}
}
