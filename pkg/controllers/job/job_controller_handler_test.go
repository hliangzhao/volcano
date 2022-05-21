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

// TODO: just copied. Not checked.
// Passed.

import (
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/apis/helpers"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	volcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned"
	"github.com/hliangzhao/volcano/pkg/controllers/framework"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"testing"
)

func newController() *jobController {
	kubeClientSet := kubernetes.NewForConfigOrDie(&rest.Config{
		Host: "",
		ContentConfig: rest.ContentConfig{
			GroupVersion: &corev1.SchemeGroupVersion,
		},
	})

	vcClient := volcanoclient.NewForConfigOrDie(&rest.Config{
		Host: "",
		ContentConfig: rest.ContentConfig{
			GroupVersion: &batchv1alpha1.SchemeGroupVersion,
		},
	})

	sharedInformers := informers.NewSharedInformerFactory(kubeClientSet, 0)

	controller := &jobController{}
	opt := &framework.ControllerOption{
		VolcanoClient:         vcClient,
		KubeClient:            kubeClientSet,
		SharedInformerFactory: sharedInformers,
		WorkerNum:             3,
	}

	_ = controller.Initialize(opt)

	return controller
}

func buildPod(namespace, name string, p corev1.PodPhase, labels map[string]string) *corev1.Pod {
	boolValue := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:             types.UID(fmt.Sprintf("%v-%v", namespace, name)),
			Name:            name,
			Namespace:       namespace,
			Labels:          labels,
			ResourceVersion: string(uuid.NewUUID()),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: helpers.JobKind.GroupVersion().String(),
					Kind:       helpers.JobKind.Kind,
					Controller: &boolValue,
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: p,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "nginx",
					Image: "nginx:latest",
				},
			},
		},
	}
}

func addPodAnnotation(pod *corev1.Pod, annotations map[string]string) *corev1.Pod {
	podWithAnnotation := pod
	for key, value := range annotations {
		if podWithAnnotation.Annotations == nil {
			podWithAnnotation.Annotations = make(map[string]string)
		}
		podWithAnnotation.Annotations[key] = value
	}
	return podWithAnnotation
}

func TestAddCommandFunc(t *testing.T) {

	namespace := "test"

	testCases := []struct {
		Name        string
		command     interface{}
		ExpectValue int
	}{
		{
			Name: "AddCommand Success Case",
			command: &busv1alpha1.Command{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "Valid Command",
					Namespace: namespace,
				},
			},
			ExpectValue: 1,
		},
		{
			Name:        "AddCommand Failure Case",
			command:     "Command",
			ExpectValue: 0,
		},
	}

	for i, testcase := range testCases {
		t.Run(testcase.Name, func(t *testing.T) {
			controller := newController()
			controller.addCommand(testcase.command)
			length := controller.cmdQueue.Len()
			if testcase.ExpectValue != length {
				t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, length)
			}
		})
	}
}

func TestJobAddFunc(t *testing.T) {
	namespace := "test"

	testCases := []struct {
		Name        string
		job         *batchv1alpha1.Job
		ExpectValue int
	}{
		{
			Name: "AddJob Success",
			job: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "Job1",
					Namespace: namespace,
				},
			},
			ExpectValue: 1,
		},
	}
	for i, testcase := range testCases {
		t.Run(testcase.Name, func(t *testing.T) {
			controller := newController()
			controller.addJob(testcase.job)
			key := fmt.Sprintf("%s/%s", testcase.job.Namespace, testcase.job.Name)
			job, err := controller.cache.Get(key)
			if job == nil || err != nil {
				t.Errorf("Error while Adding Job in case %d with error %s", i, err)
			}
			queue := controller.getWorkerQueue(key)
			length := queue.Len()
			if testcase.ExpectValue != length {
				t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, length)
			}
		})
	}
}

func TestUpdateJobFunc(t *testing.T) {
	namespace := "test"

	testcases := []struct {
		Name   string
		oldJob *batchv1alpha1.Job
		newJob *batchv1alpha1.Job
	}{
		{
			Name: "Job Update Success Case",
			oldJob: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "job1",
					Namespace:       namespace,
					ResourceVersion: "54467984",
				},
				Spec: batchv1alpha1.JobSpec{
					SchedulerName: "volcano",
					MinAvailable:  5,
				},
				Status: batchv1alpha1.JobStatus{
					State: batchv1alpha1.JobState{
						Phase: batchv1alpha1.Pending,
					},
				},
			},
			newJob: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "job1",
					Namespace:       namespace,
					ResourceVersion: "54469999",
				},
				Spec: batchv1alpha1.JobSpec{
					SchedulerName: "volcano",
					MinAvailable:  5,
				},
				Status: batchv1alpha1.JobStatus{
					State: batchv1alpha1.JobState{
						Phase: batchv1alpha1.Running,
					},
				},
			},
		},
		{
			Name: "Job Update Failure Case",
			oldJob: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "job1",
					Namespace:       namespace,
					ResourceVersion: "54469999",
				},
				Spec: batchv1alpha1.JobSpec{
					SchedulerName: "volcano",
					MinAvailable:  5,
				},
				Status: batchv1alpha1.JobStatus{
					State: batchv1alpha1.JobState{
						Phase: batchv1alpha1.Pending,
					},
				},
			},
			newJob: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "job1",
					Namespace:       namespace,
					ResourceVersion: "54469999",
				},
				Spec: batchv1alpha1.JobSpec{
					SchedulerName: "volcano",
					MinAvailable:  5,
				},
				Status: batchv1alpha1.JobStatus{
					State: batchv1alpha1.JobState{
						Phase: batchv1alpha1.Pending,
					},
				},
			},
		},
	}

	for i, testcase := range testcases {
		t.Run(testcase.Name, func(t *testing.T) {
			controller := newController()
			controller.addJob(testcase.oldJob)
			controller.updateJob(testcase.oldJob, testcase.newJob)
			key := fmt.Sprintf("%s/%s", testcase.newJob.Namespace, testcase.newJob.Name)
			job, err := controller.cache.Get(key)

			if job == nil || job.Job == nil || err != nil {
				t.Errorf("Error while Updating Job in case %d with error %s", i, err)
			}

			if job.Job.Status.State.Phase != testcase.newJob.Status.State.Phase {
				t.Errorf("Error while Updating Job in case %d with error %s", i, err)
			}
		})
	}
}

func TestAddPodFunc(t *testing.T) {
	namespace := "test"

	testcases := []struct {
		Name          string
		Job           *batchv1alpha1.Job
		pods          []*corev1.Pod
		Annotation    map[string]string
		ExpectedValue int
	}{
		{
			Name: "AddPod Success case",
			Job: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job1",
					Namespace: namespace,
				},
			},
			pods: []*corev1.Pod{
				buildPod(namespace, "pod1", corev1.PodPending, nil),
			},
			Annotation: map[string]string{
				batchv1alpha1.JobNameKey:  "job1",
				batchv1alpha1.JobVersion:  "0",
				batchv1alpha1.TaskSpecKey: "task1",
			},
			ExpectedValue: 1,
		},
		{
			Name: "AddPod Duplicate Pod case",
			Job: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job1",
					Namespace: namespace,
				},
			},
			pods: []*corev1.Pod{
				buildPod(namespace, "pod1", corev1.PodPending, nil),
				buildPod(namespace, "pod1", corev1.PodPending, nil),
			},
			Annotation: map[string]string{
				batchv1alpha1.JobNameKey:  "job1",
				batchv1alpha1.JobVersion:  "0",
				batchv1alpha1.TaskSpecKey: "task1",
			},
			ExpectedValue: 1,
		},
	}

	for i, testcase := range testcases {

		t.Run(testcase.Name, func(t *testing.T) {
			controller := newController()
			controller.addJob(testcase.Job)
			for _, pod := range testcase.pods {
				addPodAnnotation(pod, testcase.Annotation)
				controller.addPod(pod)
			}

			key := fmt.Sprintf("%s/%s", testcase.Job.Namespace, testcase.Job.Name)
			job, err := controller.cache.Get(key)

			if job == nil || job.Pods == nil || err != nil {
				t.Errorf("Error while Getting Job in case %d with error %s", i, err)
			}

			var totalPods int
			for _, task := range job.Pods {
				totalPods = len(task)
			}
			if totalPods != testcase.ExpectedValue {
				t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectedValue, totalPods)
			}
		})
	}
}

func TestUpdatePodFunc(t *testing.T) {
	namespace := "test"

	testcases := []struct {
		Name          string
		Job           *batchv1alpha1.Job
		oldPod        *corev1.Pod
		newPod        *corev1.Pod
		Annotation    map[string]string
		ExpectedValue corev1.PodPhase
	}{
		{
			Name: "UpdatePod Success case",
			Job: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job1",
					Namespace: namespace,
				},
			},
			oldPod: buildPod(namespace, "pod1", corev1.PodPending, nil),
			newPod: buildPod(namespace, "pod1", corev1.PodRunning, nil),
			Annotation: map[string]string{
				batchv1alpha1.JobNameKey:  "job1",
				batchv1alpha1.JobVersion:  "0",
				batchv1alpha1.TaskSpecKey: "task1",
			},
			ExpectedValue: corev1.PodRunning,
		},
		{
			Name: "UpdatePod Failed case",
			Job: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job1",
					Namespace: namespace,
				},
			},
			oldPod: buildPod(namespace, "pod1", corev1.PodPending, nil),
			newPod: buildPod(namespace, "pod1", corev1.PodFailed, nil),
			Annotation: map[string]string{
				batchv1alpha1.JobNameKey:  "job1",
				batchv1alpha1.JobVersion:  "0",
				batchv1alpha1.TaskSpecKey: "task1",
			},
			ExpectedValue: corev1.PodFailed,
		},
	}

	for i, testcase := range testcases {
		t.Run(testcase.Name, func(t *testing.T) {
			controller := newController()
			controller.addJob(testcase.Job)
			addPodAnnotation(testcase.oldPod, testcase.Annotation)
			addPodAnnotation(testcase.newPod, testcase.Annotation)
			controller.addPod(testcase.oldPod)
			controller.updatePod(testcase.oldPod, testcase.newPod)

			key := fmt.Sprintf("%s/%s", testcase.Job.Namespace, testcase.Job.Name)
			job, err := controller.cache.Get(key)

			if job == nil || job.Pods == nil || err != nil {
				t.Errorf("Error while Getting Job in case %d with error %s", i, err)
			}

			pod := job.Pods[testcase.Annotation[batchv1alpha1.TaskSpecKey]][testcase.oldPod.Name]

			if pod.Status.Phase != testcase.ExpectedValue {
				t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectedValue, pod.Status.Phase)
			}
		})
	}
}

func TestDeletePodFunc(t *testing.T) {
	namespace := "test"

	testcases := []struct {
		Name          string
		Job           *batchv1alpha1.Job
		availablePods []*corev1.Pod
		deletePod     *corev1.Pod
		Annotation    map[string]string
		ExpectedValue int
	}{
		{
			Name: "DeletePod success case",
			Job: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job1",
					Namespace: namespace,
				},
			},
			availablePods: []*corev1.Pod{
				buildPod(namespace, "pod1", corev1.PodRunning, nil),
				buildPod(namespace, "pod2", corev1.PodRunning, nil),
			},
			deletePod: buildPod(namespace, "pod2", corev1.PodRunning, nil),
			Annotation: map[string]string{
				batchv1alpha1.JobNameKey:  "job1",
				batchv1alpha1.JobVersion:  "0",
				batchv1alpha1.TaskSpecKey: "task1",
			},
			ExpectedValue: 1,
		},
		{
			Name: "DeletePod Pod NotAvailable case",
			Job: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job1",
					Namespace: namespace,
				},
			},
			availablePods: []*corev1.Pod{
				buildPod(namespace, "pod1", corev1.PodRunning, nil),
				buildPod(namespace, "pod2", corev1.PodRunning, nil),
			},
			deletePod: buildPod(namespace, "pod3", corev1.PodRunning, nil),
			Annotation: map[string]string{
				batchv1alpha1.JobNameKey:  "job1",
				batchv1alpha1.JobVersion:  "0",
				batchv1alpha1.TaskSpecKey: "task1",
			},
			ExpectedValue: 2,
		},
	}

	for i, testcase := range testcases {
		t.Run(testcase.Name, func(t *testing.T) {
			controller := newController()
			controller.addJob(testcase.Job)
			for _, pod := range testcase.availablePods {
				addPodAnnotation(pod, testcase.Annotation)
				controller.addPod(pod)
			}

			addPodAnnotation(testcase.deletePod, testcase.Annotation)
			controller.deletePod(testcase.deletePod)
			key := fmt.Sprintf("%s/%s", testcase.Job.Namespace, testcase.Job.Name)
			job, err := controller.cache.Get(key)

			if job == nil || job.Pods == nil || err != nil {
				t.Errorf("Error while Getting Job in case %d with error %s", i, err)
			}

			var totalPods int
			for _, task := range job.Pods {
				totalPods = len(task)
			}

			if totalPods != testcase.ExpectedValue {
				t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectedValue, totalPods)
			}
		})
	}
}

func TestUpdatePodGroupFunc(t *testing.T) {

	namespace := "test"

	testCases := []struct {
		Name        string
		oldPodGroup *schedulingv1alpha1.PodGroup
		newPodGroup *schedulingv1alpha1.PodGroup
		ExpectValue int
	}{
		{
			Name: "AddCommand Success Case",
			oldPodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1alpha1.PodGroupSpec{
					MinMember: 3,
				},
				Status: schedulingv1alpha1.PodGroupStatus{
					Phase: schedulingv1alpha1.PodGroupPending,
				},
			},
			newPodGroup: &schedulingv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1alpha1.PodGroupSpec{
					MinMember: 3,
				},
				Status: schedulingv1alpha1.PodGroupStatus{
					Phase: schedulingv1alpha1.PodGroupRunning,
				},
			},
			ExpectValue: 1,
		},
	}

	for i, testcase := range testCases {

		t.Run(testcase.Name, func(t *testing.T) {
			controller := newController()
			controller.updatePodGroup(testcase.oldPodGroup, testcase.newPodGroup)
			key := fmt.Sprintf("%s/%s", testcase.oldPodGroup.Namespace, testcase.oldPodGroup.Name)
			queue := controller.getWorkerQueue(key)
			length := queue.Len()
			if testcase.ExpectValue != length {
				t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, length)
			}
		})
	}
}
