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
	`github.com/hliangzhao/volcano/pkg/apis/scheduling`
	schedulingscheme "github.com/hliangzhao/volcano/pkg/apis/scheduling/scheme"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	volcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned"
	`github.com/hliangzhao/volcano/pkg/client/clientset/versioned/scheme`
	vcinformer "github.com/hliangzhao/volcano/pkg/client/informers/externalversions"
	volcanoinformers "github.com/hliangzhao/volcano/pkg/client/informers/externalversions"
	nodeinfoinformersv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/nodeinfo/v1alpha1"
	schedulinginformersv1alpha1 "github.com/hliangzhao/volcano/pkg/client/informers/externalversions/scheduling/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	`github.com/hliangzhao/volcano/pkg/scheduler/metrics`
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	apierrors `k8s.io/apimachinery/pkg/api/errors`
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	`k8s.io/apimachinery/pkg/runtime`
	utilruntime `k8s.io/apimachinery/pkg/util/runtime`
	`k8s.io/apimachinery/pkg/util/wait`
	"k8s.io/client-go/informers"
	coreinformersv1 "k8s.io/client-go/informers/core/v1"
	schedulinginformersv1 "k8s.io/client-go/informers/scheduling/v1"
	storageinformersv1 "k8s.io/client-go/informers/storage/v1"
	"k8s.io/client-go/informers/storage/v1alpha1"
	"k8s.io/client-go/kubernetes"
	typedcorev1 `k8s.io/client-go/kubernetes/typed/core/v1`
	"k8s.io/client-go/rest"
	`k8s.io/client-go/tools/cache`
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
	`os`
	`strconv`
	`strings`
	"sync"
	`time`
)

func init() {
	schemeBuilder := runtime.SchemeBuilder{
		corev1.AddToScheme,
	}

	utilruntime.Must(schemeBuilder.AddToScheme(scheme.Scheme))
}

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

	// clients
	kubeClient *kubernetes.Clientset
	vcClient   *volcanoclient.Clientset

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

	// cache interfaces
	Binder         Binder
	Evictor        Evictor
	StatusUpdater  StatusUpdater
	PodGroupBinder BatchBinder
	VolumeBinder   VolumeBinder

	// event recorder
	Recorder record.EventRecorder

	// contents
	Jobs                 map[apis.JobID]*apis.JobInfo
	Nodes                map[string]*apis.NodeInfo
	Queues               map[apis.QueueID]*apis.QueueInfo
	PriorityClasses      map[string]*schedulingv1.PriorityClass
	defaultPriorityClass *schedulingv1.PriorityClass
	defaultPriority      int32
	NodeList             []string
	defaultQueue         string
	schedulerName        string
	nodeSelectorLabels   map[string]string

	NamespaceCollection map[string]*apis.NamespaceCollection

	// error work queues
	errTasks    workqueue.RateLimitingInterface
	deletedJobs workqueue.RateLimitingInterface

	// informers
	kubeInformerFactory informers.SharedInformerFactory
	vcInformerFactory   volcanoinformers.SharedInformerFactory

	// other related info
	BindFlowChannel chan *apis.TaskInfo
	bindCache       []*apis.TaskInfo
	batchNum        int
}

func New(config *rest.Config, schedulerName string, defaultQueue string, nodeSelectors []string) Cache {
	return newSchedulerCache(config, schedulerName, defaultQueue, nodeSelectors)
}

func newSchedulerCache(config *rest.Config, schedulerName string, defaultQueue string, nodeSelectors []string) *SchedulerCache {
	// get clients
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(fmt.Sprintf("failed init kubeClient, with err: %v", err))
	}
	vcClient, err := volcanoclient.NewForConfig(config)
	if err != nil {
		panic(fmt.Sprintf("failed init vcClient, with err: %v", err))
	}
	eventClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(fmt.Sprintf("failed init eventClient, with err: %v", err))
	}

	// create default queue
	reclaimable := true
	q := schedulingv1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: defaultQueue,
		},
		Spec: schedulingv1alpha1.QueueSpec{
			Reclaimable: &reclaimable,
			Weight:      1,
		},
	}
	if _, err = vcClient.SchedulingV1alpha1().Queues().Create(context.TODO(),
		&q, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		panic(fmt.Sprintf("failed init default queue, with err: %v", err))
	}

	sc := &SchedulerCache{
		Jobs:            map[apis.JobID]*apis.JobInfo{},
		Nodes:           map[string]*apis.NodeInfo{},
		Queues:          map[apis.QueueID]*apis.QueueInfo{},
		PriorityClasses: map[string]*schedulingv1.PriorityClass{},

		errTasks:    workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		deletedJobs: workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),

		kubeClient: kubeClient,
		vcClient:   vcClient,

		defaultQueue:        defaultQueue,
		schedulerName:       schedulerName,
		nodeSelectorLabels:  map[string]string{},
		NamespaceCollection: map[string]*apis.NamespaceCollection{},

		NodeList: []string{},
	}

	// set node selectors
	if len(nodeSelectors) > 0 {
		for _, label := range nodeSelectors {
			labelLen := len(label)
			if labelLen <= 0 {
				continue
			}
			index := strings.Index(label, ":")
			if index < 0 || index >= (labelLen-1) {
				continue
			}
			// legal labels should be here. Trim space for each of them.
			labelName := strings.TrimSpace(label[:index])
			labelVal := strings.TrimSpace(label[index+1:])
			key := labelName + ":" + labelVal
			sc.nodeSelectorLabels[key] = ""
		}
	}

	// prepare event clients
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: eventClient.CoreV1().Events("")})
	sc.Recorder = broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: schedulerName})

	sc.BindFlowChannel = make(chan *apis.TaskInfo, 5000)
	sc.Binder = GetBindMethod()

	// get batch bind num from os ENV
	var batchNum int
	batchNum, err = strconv.Atoi(os.Getenv("BATCH_BIND_NUM"))
	if err == nil && batchNum > 0 {
		sc.batchNum = batchNum
	} else {
		sc.batchNum = 1
	}

	sc.Evictor = &defaultEvictor{
		kubeClient: sc.kubeClient,
		recorder:   sc.Recorder,
	}

	sc.StatusUpdater = &defaultStatusUpdater{
		kubeClient: sc.kubeClient,
		vcClient:   sc.vcClient,
	}

	sc.PodGroupBinder = &pgBinder{
		kubeClient: sc.kubeClient,
		vcClient:   sc.vcClient,
	}

	informerFactory := informers.NewSharedInformerFactory(sc.kubeClient, 0)
	sc.kubeInformerFactory = informerFactory
	mySchedulerPodName, c := getMultiSchedulerInfo()

	// create informer for node information
	sc.nodeInformer = informerFactory.Core().V1().Nodes()
	sc.nodeInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				node, ok := obj.(*corev1.Node)
				if !ok {
					klog.Errorf("Cannot convert to *v1.Node: %v", obj)
					return false
				}
				if !responsibleForNode(node.Name, mySchedulerPodName, c) {
					return false
				}
				if len(sc.nodeSelectorLabels) == 0 {
					return true
				}
				for labelName, labelValue := range node.Labels {
					key := labelName + ":" + labelValue
					if _, ok := sc.nodeSelectorLabels[key]; ok {
						return true
					}
				}
				klog.Infof("node %s ignore add/update/delete into schedulerCache", node.Name)
				return false
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sc.AddNode,
				UpdateFunc: sc.UpdateNode,
				DeleteFunc: sc.DeleteNode,
			},
		},
		0,
	)

	sc.podInformer = informerFactory.Core().V1().Pods()
	sc.pvcInformer = informerFactory.Core().V1().PersistentVolumeClaims()
	sc.pvInformer = informerFactory.Core().V1().PersistentVolumes()
	sc.scInformer = informerFactory.Storage().V1().StorageClasses()
	sc.csiNodeInformer = informerFactory.Storage().V1().CSINodes()
	sc.csiDriverInformer = informerFactory.Storage().V1().CSIDrivers()
	sc.csiSCInformer = informerFactory.Storage().V1alpha1().CSIStorageCapacities()

	var capacityCheck *volumebinding.CapacityCheck
	// TODO: the following should be un-comment after the cmd/scheduler is finished
	// if options.ServerOpts.EnableCSIStorage {
	// 	capacityCheck = &volumebinding.CapacityCheck{
	// 		CSIDriverInformer:          sc.csiDriverInformer,
	// 		CSIStorageCapacityInformer: sc.csiSCInformer,
	// 	}
	// } else {
	// 	capacityCheck = nil
	// }
	// TODO: temporarily replacement
	capacityCheck = nil
	sc.VolumeBinder = &defaultVolumeBinder{
		volumeBinder: volumebinding.NewVolumeBinder(
			sc.kubeClient,
			sc.podInformer,
			sc.nodeInformer,
			sc.csiNodeInformer,
			sc.pvcInformer,
			sc.pvInformer,
			sc.scInformer,
			capacityCheck,
			30*time.Second,
		),
	}

	// create informer for pod information
	sc.podInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch v := obj.(type) {
				case *corev1.Pod:
					if !responsibleForPod(v, schedulerName, mySchedulerPodName, c) {
						if len(v.Spec.NodeName) == 0 {
							return false
						}
						if !responsibleForNode(v.Spec.NodeName, mySchedulerPodName, c) {
							return false
						}
					}
					return true
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sc.AddPod,
				UpdateFunc: sc.UpdatePod,
				DeleteFunc: sc.DeletePod,
			},
		})

	// TODO: the following should be un-comment after the cmd/scheduler is finished
	// if options.ServerOpts.EnablePriorityClass {
	// 	sc.pcInformer = informerFactory.Scheduling().V1().PriorityClasses()
	// 	sc.pcInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
	// 		AddFunc:    sc.AddPriorityClass,
	// 		UpdateFunc: sc.UpdatePriorityClass,
	// 		DeleteFunc: sc.DeletePriorityClass,
	// 	})
	// }
	// TODO: temporarily replacement
	sc.pcInformer = informerFactory.Scheduling().V1().PriorityClasses()
	sc.pcInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.AddPriorityClass,
		UpdateFunc: sc.UpdatePriorityClass,
		DeleteFunc: sc.DeletePriorityClass,
	})

	sc.quotaInformer = informerFactory.Core().V1().ResourceQuotas()
	sc.quotaInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.AddResourceQuota,
		UpdateFunc: sc.UpdateResourceQuota,
		DeleteFunc: sc.DeleteResourceQuota,
	})

	vcInformers := vcinformer.NewSharedInformerFactory(sc.vcClient, 0)
	sc.vcInformerFactory = vcInformers

	// create informer for PodGroup information
	sc.pgInformer = vcInformers.Scheduling().V1alpha1().PodGroups()
	sc.pgInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch v := obj.(type) {
				case *schedulingv1alpha1.PodGroup:
					return responsibleForPodGroup(v, mySchedulerPodName, c)
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sc.AddPodGroupV1alpha1,
				UpdateFunc: sc.UpdatePodGroupV1alpha1,
				DeleteFunc: sc.DeletePodGroupV1alpha1,
			},
		})

	// create informer for Queue information
	sc.queueInformer = vcInformers.Scheduling().V1alpha1().Queues()
	sc.queueInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.AddQueueV1alpha1,
		UpdateFunc: sc.UpdateQueueV1alpha1,
		DeleteFunc: sc.DeleteQueueV1alpha1,
	})

	// create informer for NUMA topology information
	sc.cpuInformer = vcInformers.Nodeinfo().V1alpha1().Numatopologies()
	sc.cpuInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.AddNumaInfoV1alpha1,
		UpdateFunc: sc.UpdateNumaInfoV1alpha1,
		DeleteFunc: sc.DeleteNumaInfoV1alpha1,
	})

	return sc
}

// Snapshot returns the complete snapshot of the cluster from cache.
func (sc *SchedulerCache) Snapshot() *apis.ClusterInfo {
	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	snapshot := &apis.ClusterInfo{
		Nodes:          map[string]*apis.NodeInfo{},
		Jobs:           map[apis.JobID]*apis.JobInfo{},
		Queues:         map[apis.QueueID]*apis.QueueInfo{},
		NamespaceInfo:  map[apis.NamespaceName]*apis.NamespaceInfo{},
		RevocableNodes: map[string]*apis.NodeInfo{},
		NodeList:       make([]string, len(sc.NodeList)),
	}

	// get current node list
	copy(snapshot.NodeList, sc.NodeList)
	for _, value := range sc.Nodes {
		value.RefreshNumaSchedulerInfoByCrd()
	}

	// get current nodes info
	for _, value := range sc.Nodes {
		if !value.Ready() {
			continue
		}
		snapshot.Nodes[value.Name] = value.Clone()
		if value.RevocableZone != "" {
			snapshot.RevocableNodes[value.Name] = snapshot.Nodes[value.Name]
		}
	}

	// gte current queues
	for _, value := range sc.Queues {
		snapshot.Queues[value.UID] = value.Clone()
	}

	// get current namespace collection
	for _, value := range sc.NamespaceCollection {
		info := value.Snapshot()
		snapshot.NamespaceInfo[info.Name] = info
		klog.V(4).Infof("Namespace %s has weight %v",
			value.Name, info.GetWeight())
	}

	// get current jobs
	var cloneJobLock sync.Mutex
	var wg sync.WaitGroup

	// update of snapshot.Jobs should be in lock
	cloneJob := func(value *apis.JobInfo) {
		defer wg.Done()
		if value.PodGroup != nil {
			value.Priority = sc.defaultPriority

			priName := value.PodGroup.Spec.PriorityClassName
			if priorityClass, found := sc.PriorityClasses[priName]; found {
				value.Priority = priorityClass.Value
			}

			klog.V(4).Infof("The priority of job <%s/%s> is <%s/%d>",
				value.Namespace, value.Name, priName, value.Priority)
		}

		clonedJob := value.Clone()

		cloneJobLock.Lock()
		snapshot.Jobs[value.UID] = clonedJob
		cloneJobLock.Unlock()
	}

	for _, value := range sc.Jobs {
		// If no scheduling spec, does not handle it.
		if value.PodGroup == nil {
			klog.V(4).Infof("The scheduling spec of Job <%v:%s/%s> is nil, ignore it.",
				value.UID, value.Namespace, value.Name)

			continue
		}

		if _, found := snapshot.Queues[value.Queue]; !found {
			klog.V(3).Infof("The Queue <%v> of Job <%v/%v> does not exist, ignore it.",
				value.Queue, value.Namespace, value.Name)
			continue
		}

		wg.Add(1)
		go cloneJob(value)
	}
	wg.Wait()

	klog.V(3).Infof("There are <%d> Jobs, <%d> Queues and <%d> Nodes in total for scheduling.",
		len(snapshot.Jobs), len(snapshot.Queues), len(snapshot.Nodes))

	return snapshot
}

// String returns information about the cache in a string format
func (sc *SchedulerCache) String() string {
	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	str := "Cache:\n"

	if len(sc.Nodes) != 0 {
		str += "Nodes:\n"
		for _, n := range sc.Nodes {
			str += fmt.Sprintf("\t %s: idle(%v) used(%v) allocatable(%v) pods(%d)\n",
				n.Name, n.Idle, n.Used, n.Allocatable, len(n.Tasks))

			i := 0
			for _, p := range n.Tasks {
				str += fmt.Sprintf("\t\t %d: %v\n", i, p)
				i++
			}
		}
	}

	if len(sc.Jobs) != 0 {
		str += "Jobs:\n"
		for _, job := range sc.Jobs {
			str += fmt.Sprintf("\t %s\n", job)
		}
	}

	if len(sc.NamespaceCollection) != 0 {
		str += "Namespaces:\n"
		for _, ns := range sc.NamespaceCollection {
			info := ns.Snapshot()
			str += fmt.Sprintf("\t Namespace(%s) Weight(%v)\n",
				info.Name, info.Weight)
		}
	}

	if len(sc.NodeList) != 0 {
		str += fmt.Sprintf("NodeList: %v\n", sc.NodeList)
	}

	return str
}

// WaitForCacheSync sync the cache with the api server.
func (sc *SchedulerCache) WaitForCacheSync(stopCh <-chan struct{}) {
	sc.kubeInformerFactory.WaitForCacheSync(stopCh)
	sc.vcInformerFactory.WaitForCacheSync(stopCh)
}

// Run  starts the schedulerCache
func (sc *SchedulerCache) Run(stopCh <-chan struct{}) {
	sc.kubeInformerFactory.Start(stopCh)
	sc.vcInformerFactory.Start(stopCh)

	// Re-sync error tasks.
	go wait.Until(sc.processReSyncTask, 0, stopCh)

	// Cleanup jobs.
	go wait.Until(sc.processCleanupJob, 0, stopCh)

	// Bind volumes and hosts for tasks.
	go wait.Until(sc.processBindTask, time.Millisecond*20, stopCh)
}

// Evict will evict the pod.
//
// If error occurs both task and job are guaranteed to be in the original state.
func (sc *SchedulerCache) Evict(taskInfo *apis.TaskInfo, reason string) error {
	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	job, task, err := sc.findJobAndTask(taskInfo)
	if err != nil {
		return err
	}

	node, found := sc.Nodes[task.NodeName]
	if !found {
		return fmt.Errorf("failed to bind Task %v to host %v, host does not exist",
			task.UID, task.NodeName)
	}

	originalStatus := task.Status
	if err := job.UpdateTaskStatus(task, apis.Releasing); err != nil {
		return err
	}

	// Add new task to node.
	if err := node.UpdateTask(task); err != nil {
		// After failing to update task to a node we need to revert task status from Releasing,
		// otherwise task might be stuck in the Releasing state indefinitely.
		if err := job.UpdateTaskStatus(task, originalStatus); err != nil {
			klog.Errorf("Task <%s/%s> will be resynchronized after failing to revert status "+
				"from %s to %s after failing to update Task on Node <%s>: %v",
				task.Namespace, task.Name, task.Status, originalStatus, node.Name, err)
			sc.reSyncTask(task)
		}
		return err
	}

	p := task.Pod

	go func() {
		err := sc.Evictor.Evict(p, reason)
		if err != nil {
			sc.reSyncTask(task)
		}
	}()

	podgroup := &schedulingv1alpha1.PodGroup{}
	if err := schedulingscheme.Scheme.Convert(&job.PodGroup.PodGroup, podgroup, nil); err != nil {
		klog.Errorf("Error while converting PodGroup to v1alpha1.PodGroup with error: %v", err)
		return err
	}
	sc.Recorder.Eventf(podgroup, corev1.EventTypeNormal, "Evict", reason)
	return nil
}

// RecordJobStatusEvent records related events according to job status.
func (sc *SchedulerCache) RecordJobStatusEvent(job *apis.JobInfo) {
	pgUnschedulable := job.PodGroup != nil &&
		(job.PodGroup.Status.Phase == scheduling.PodGroupUnknown ||
			job.PodGroup.Status.Phase == scheduling.PodGroupPending ||
			job.PodGroup.Status.Phase == scheduling.PodGroupInqueue)

	// If pending or unschedulable, record unschedulable event.
	if pgUnschedulable {
		msg := fmt.Sprintf("%v/%v tasks in gang unschedulable: %v",
			len(job.TaskStatusIndex[apis.Pending]),
			len(job.Tasks),
			job.FitError())
		sc.recordPodGroupEvent(job.PodGroup, corev1.EventTypeWarning, string(scheduling.PodGroupUnschedulableType), msg)
	} else {
		sc.recordPodGroupEvent(job.PodGroup, corev1.EventTypeNormal, string(scheduling.PodGroupScheduled), string(scheduling.PodGroupReady))
	}

	baseErrorMessage := job.JobFitErrors
	if baseErrorMessage == "" {
		baseErrorMessage = apis.AllNodeUnavailableMsg
	}
	// Update podCondition for tasks Allocated and Pending before job discarded
	for _, status := range []apis.TaskStatus{apis.Allocated, apis.Pending, apis.Pipelined} {
		for _, taskInfo := range job.TaskStatusIndex[status] {
			reason, msg := job.TaskSchedulingReason(taskInfo.UID)
			if len(msg) == 0 {
				msg = baseErrorMessage
			}
			if err := sc.taskUnschedulable(taskInfo, reason, msg); err != nil {
				klog.Errorf("Failed to update unschedulable task status <%s/%s>: %v",
					taskInfo.Namespace, taskInfo.Name, err)
			}
		}
	}
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

// processCleanupJob retrieves a job from deletedJobs and delete it (if permitted).
func (sc *SchedulerCache) processCleanupJob() {
	obj, shutdown := sc.deletedJobs.Get()
	if shutdown {
		return
	}

	defer sc.deletedJobs.Done(obj)

	job, found := obj.(*apis.JobInfo)
	if !found {
		klog.Errorf("Failed to convert <%v> to *apis.JobInfo", obj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	if apis.JobTerminated(job) {
		delete(sc.Jobs, job.UID)
		klog.V(3).Infof("Job <%v:%v/%v> was deleted.", job.UID, job.Namespace, job.Name)
	} else {
		// Retry
		sc.deleteJob(job)
	}
}

func (sc *SchedulerCache) Client() kubernetes.Interface {
	return sc.kubeClient
}

func (sc *SchedulerCache) SharedInformerFactory() informers.SharedInformerFactory {
	return sc.kubeInformerFactory
}

func (sc *SchedulerCache) UpdateSchedulerNumaInfo(AllocatedSets map[string]apis.ResNumaSets) error {
	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	for nodeName, sets := range AllocatedSets {
		if _, found := sc.Nodes[nodeName]; !found {
			continue
		}

		numaInfo := sc.Nodes[nodeName].NumaSchedulerInfo
		if numaInfo == nil {
			continue
		}

		numaInfo.Allocate(sets)
	}
	return nil
}

func (sc *SchedulerCache) recordPodGroupEvent(podgroup *apis.PodGroup, eventType, reason, msg string) {
	if podgroup == nil {
		return
	}

	pg := &schedulingv1alpha1.PodGroup{}
	if err := schedulingscheme.Scheme.Convert(&podgroup.PodGroup, pg, nil); err != nil {
		klog.Errorf("Error while converting PodGroup to schedulingv1alpha1.PodGroup with error: %v", err)
		return
	}
	sc.Recorder.Event(pg, eventType, reason, msg)
}

func (sc *SchedulerCache) UpdateJobStatus(job *apis.JobInfo, updatePg bool) (*apis.JobInfo, error) {
	if updatePg {
		pg, err := sc.StatusUpdater.UpdatePodGroup(job.PodGroup)
		if err != nil {
			return nil, err
		}
		job.PodGroup = pg
	}

	sc.RecordJobStatusEvent(job)

	return job, nil
}

func (sc *SchedulerCache) Bind(tasks []*apis.TaskInfo) error {
	go func(taskArr []*apis.TaskInfo) {
		tmp := time.Now()
		err, errTasks := sc.Binder.Bind(sc.kubeClient, taskArr)
		if err != nil {
			klog.V(3).Infof("bind ok, latency %v", time.Since(tmp))
			for _, task := range tasks {
				sc.Recorder.Eventf(task.Pod, corev1.EventTypeNormal, "Scheduled",
					"Successfully assigned %v/%v to %v", task.Namespace, task.Name, task.NodeName)
			}
		} else {
			for _, task := range errTasks {
				klog.V(2).Infof("reSyncTask task %s", task.Name)
				sc.reSyncTask(task)
			}
		}
	}(tasks)

	return nil
}

// BindPodGroup binds job to silo cluster.
func (sc *SchedulerCache) BindPodGroup(job *apis.JobInfo, cluster string) error {
	if _, err := sc.PodGroupBinder.Bind(job, cluster); err != nil {
		klog.Errorf("Bind job <%s> to cluster <%s> failed: %v", job.Name, cluster, err)
		return err
	}
	return nil
}

func (sc *SchedulerCache) BindVolumes(task *apis.TaskInfo, podVolumes *volumebinding.PodVolumes) error {
	return sc.VolumeBinder.BindVolumes(task, podVolumes)
}

func (sc *SchedulerCache) AllocateVolumes(task *apis.TaskInfo, hostname string, podVolumes *volumebinding.PodVolumes) error {
	return sc.VolumeBinder.AllocateVolumes(task, hostname, podVolumes)
}

func (sc *SchedulerCache) GetPodVolumes(task *apis.TaskInfo, node *corev1.Node) (*volumebinding.PodVolumes, error) {
	return sc.VolumeBinder.GetPodVolumes(task, node)
}

func (sc *SchedulerCache) AddBindTask(taskInfo *apis.TaskInfo) error {
	klog.V(5).Infof("add bind task %v/%v", taskInfo.Namespace, taskInfo.Name)

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	job, task, err := sc.findJobAndTask(taskInfo)
	if err != nil {
		return err
	}

	node, found := sc.Nodes[taskInfo.NodeName]
	if !found {
		return fmt.Errorf("failed to bind Task %v to host %v, host does not exist",
			task.UID, taskInfo.NodeName)
	}

	originalStatus := task.Status
	if err := job.UpdateTaskStatus(task, apis.Binding); err != nil {
		return err
	}

	// Add task to the node.
	if err := node.AddTask(task); err != nil {
		// After failing to update task to a node we need to revert task status from Releasing,
		// otherwise task might be stuck in the Releasing state indefinitely.
		if err := job.UpdateTaskStatus(task, originalStatus); err != nil {
			klog.Errorf("Task <%s/%s> will be resynchronized after failing to revert status "+
				"from %s to %s after failing to update Task on Node <%s>: %v",
				task.Namespace, task.Name, task.Status, originalStatus, node.Name, err)
			sc.reSyncTask(task)
		}
		return err
	}

	sc.BindFlowChannel <- taskInfo

	return nil
}

func (sc *SchedulerCache) BindTask() {
	klog.V(5).Infof("batch bind task count %d", len(sc.bindCache))

	// bind volumes
	for _, task := range sc.bindCache {
		if err := sc.BindVolumes(task, task.PodVolumes); err != nil {
			klog.Errorf("task %s/%s bind Volumes failed: %#v", task.Namespace, task.Name, err)
			sc.reSyncTask(task)
			return
		}
	}

	// bind task to target node
	bindTasks := make([]*apis.TaskInfo, len(sc.bindCache))
	copy(bindTasks, sc.bindCache)
	if err := sc.Bind(bindTasks); err != nil {
		return
	}

	// update metrics
	for _, task := range sc.bindCache {
		metrics.UpdateTaskScheduleDuration(metrics.Duration(task.Pod.CreationTimestamp.Time))
	}

	// remove all bounded tasks
	sc.bindCache = sc.bindCache[0:0]
	return
}

func (sc *SchedulerCache) processBindTask() {
	for {
		select {
		case taskInfo, ok := <-sc.BindFlowChannel:
			if !ok {
				return
			}

			sc.bindCache = append(sc.bindCache, taskInfo)
			if len(sc.bindCache) == sc.batchNum {
				sc.BindTask()
			}
		}

		if len(sc.BindFlowChannel) == 0 {
			break
		}
	}

	if len(sc.bindCache) == 0 {
		return
	}

	sc.BindTask()
}

// findJobAndTask returns job and the task info
func (sc *SchedulerCache) findJobAndTask(taskInfo *apis.TaskInfo) (*apis.JobInfo, *apis.TaskInfo, error) {
	job, found := sc.Jobs[taskInfo.Job]
	if !found {
		return nil, nil, fmt.Errorf("failed to find Job %v for Task %v",
			taskInfo.Job, taskInfo.UID)
	}

	task, found := job.Tasks[taskInfo.UID]
	if !found {
		return nil, nil, fmt.Errorf("failed to find task in status %v by id %v",
			taskInfo.Status, taskInfo.UID)
	}

	return job, task, nil
}