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

package apis

// TODO: just copied. Not checked.
// Passed.

import (
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"testing"
)

func TestAddPod(t *testing.T) {
	namespace := "test"
	name := "pod1"

	testCases := []struct {
		Name        string
		jobInfo     JobInfo
		pod         *v1.Pod
		ExpectValue bool
		ExpectErr   string
	}{
		{
			Name: "AddPod",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID:       types.UID(fmt.Sprintf("%v-%v", namespace, name)),
					Name:      name,
					Namespace: namespace,
					Labels:    nil,
					Annotations: map[string]string{batchv1alpha1.JobNameKey: "job1",
						batchv1alpha1.JobVersion:  "0",
						batchv1alpha1.TaskSpecKey: "task1"},
				},
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "nginx",
							Image: "nginx:latest",
						},
					},
				},
			},
			jobInfo: JobInfo{
				Pods: make(map[string]map[string]*v1.Pod),
			},
			ExpectValue: true,
			ExpectErr:   "duplicated pod",
		},
	}

	for i, testcase := range testCases {
		err := testcase.jobInfo.AddPod(testcase.pod)
		if err != nil {
			t.Fatalf("AddPod() error: %v", err)
		}

		if _, ok := testcase.jobInfo.Pods["task1"][testcase.pod.Name]; ok != testcase.ExpectValue {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, ok)
		}

		err = testcase.jobInfo.AddPod(testcase.pod)

		if err == nil {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectErr, nil)
		}
	}

}

func TestDeletePod(t *testing.T) {
	namespace := "test"
	name := "pod1"

	testCases := []struct {
		Name        string
		jobInfo     JobInfo
		pod         *v1.Pod
		ExpectValue bool
	}{
		{
			Name: "DeletePod",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID:       types.UID(fmt.Sprintf("%v-%v", namespace, name)),
					Name:      name,
					Namespace: namespace,
					Labels:    nil,
					Annotations: map[string]string{batchv1alpha1.JobNameKey: "job1",
						batchv1alpha1.JobVersion:  "0",
						batchv1alpha1.TaskSpecKey: "task1"},
				},
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "nginx",
							Image: "nginx:latest",
						},
					},
				},
			},
			jobInfo: JobInfo{
				Pods: make(map[string]map[string]*v1.Pod),
			},
			ExpectValue: false,
		},
	}

	for i, testcase := range testCases {

		testcase.jobInfo.Pods["task1"] = make(map[string]*v1.Pod)
		testcase.jobInfo.Pods["task1"][testcase.pod.Name] = testcase.pod

		err := testcase.jobInfo.DeletePod(testcase.pod)
		if err != nil {
			t.Fatalf("DeletePod() error: %v", err)
		}
		if _, ok := testcase.jobInfo.Pods["task1"][testcase.pod.Name]; ok != testcase.ExpectValue {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, ok)
		}
	}
}

func TestUpdatePod(t *testing.T) {
	namespace := "test"
	name := "pod1"

	testCases := []struct {
		Name        string
		jobInfo     JobInfo
		oldPod      *v1.Pod
		newPod      *v1.Pod
		ExpectValue v1.PodPhase
	}{
		{
			Name: "UpdatePod",
			oldPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID:       types.UID(fmt.Sprintf("%v-%v", namespace, name)),
					Name:      name,
					Namespace: namespace,
					Labels:    nil,
					Annotations: map[string]string{batchv1alpha1.JobNameKey: "job1",
						batchv1alpha1.JobVersion:  "0",
						batchv1alpha1.TaskSpecKey: "task1"},
				},
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "nginx",
							Image: "nginx:latest",
						},
					},
				},
			},
			newPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID:       types.UID(fmt.Sprintf("%v-%v", namespace, name)),
					Name:      name,
					Namespace: namespace,
					Labels:    nil,
					Annotations: map[string]string{batchv1alpha1.JobNameKey: "job1",
						batchv1alpha1.JobVersion:  "0",
						batchv1alpha1.TaskSpecKey: "task1"},
				},
				Status: v1.PodStatus{
					Phase: v1.PodSucceeded,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "nginx",
							Image: "nginx:latest",
						},
					},
				},
			},
			jobInfo: JobInfo{
				Pods: make(map[string]map[string]*v1.Pod),
			},
			ExpectValue: v1.PodSucceeded,
		},
	}

	for i, testcase := range testCases {

		testcase.jobInfo.Pods["task1"] = make(map[string]*v1.Pod)
		testcase.jobInfo.Pods["task1"][testcase.oldPod.Name] = testcase.oldPod

		err := testcase.jobInfo.UpdatePod(testcase.newPod)
		if err != nil {
			t.Fatalf("UpdatePod() error: %v", err)
		}
		if val, ok := testcase.jobInfo.Pods["task1"][testcase.newPod.Name]; ok != true {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, true, ok)
		} else if val.Status.Phase != v1.PodSucceeded {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, val.Status.Phase)
		}
	}
}

func TestClone(t *testing.T) {
	testCases := []struct {
		Name    string
		jobInfo JobInfo

		ExpectValue v1.PodPhase
	}{
		{
			Name: "Clone",
			jobInfo: JobInfo{
				Name: "testjobInfo",
				Pods: make(map[string]map[string]*v1.Pod),
			},
		},
	}

	for i, testcase := range testCases {
		newJobInfo := testcase.jobInfo.Clone()

		if newJobInfo.Name != testcase.jobInfo.Name {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.jobInfo.Name, newJobInfo.Name)
		}
	}
}

func TestSetJob(t *testing.T) {
	testCases := []struct {
		Name    string
		job     batchv1alpha1.Job
		jobInfo JobInfo

		ExpectValue v1.PodPhase
	}{
		{
			Name: "Clone",
			jobInfo: JobInfo{
				Name: "testjobinfo",
				Pods: make(map[string]map[string]*v1.Pod),
			},
			job: batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testjob",
				},
			},
		},
	}

	for i, testcase := range testCases {
		testcase.jobInfo.SetJob(&testcase.job)

		if testcase.jobInfo.Job.Name != testcase.jobInfo.Name {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.job.Name, testcase.jobInfo.Job.Name)
		}
	}
}

func TestRequest_String(t *testing.T) {
	testCases := []struct {
		Name          string
		req           Request
		ExpectedValue string
	}{
		{
			Name: "RequestToString",
			req: Request{
				Namespace:  "testnamespace",
				JobName:    "testjobname",
				QueueName:  "testqueuename",
				TaskName:   "testtaskname",
				Event:      busv1alpha1.AnyEvent,
				ExitCode:   0,
				Action:     busv1alpha1.SyncJobAction,
				JobVersion: 0,
			},
			ExpectedValue: "Queue: testqueuename, Job: testnamespace/testjobname, Task:testtaskname, Event:*, ExitCode:0, Action:SyncJob, JobVersion: 0",
		},
	}

	for i, testcase := range testCases {
		reqString := testcase.req.String()

		if reqString != testcase.ExpectedValue {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectedValue, reqString)
		}
	}

}
