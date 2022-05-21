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

package helpers

// TODO: just copied. Not checked.
// Passed.

import (
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
)

func TestCompareTask(t *testing.T) {
	createTime := time.Now()
	items := []struct {
		lv     *apis.TaskInfo
		rv     *apis.TaskInfo
		expect bool
	}{
		{
			generateTaskInfo("prod-worker-21", createTime),
			generateTaskInfo("prod-worker-1", createTime),
			false,
		},
		{
			generateTaskInfo("prod-worker-0", createTime),
			generateTaskInfo("prod-worker-3", createTime),
			true,
		},
		{
			generateTaskInfo("prod-worker", createTime),
			generateTaskInfo("prod-worker-3", createTime.Add(time.Hour)),
			true,
		},
		{
			generateTaskInfo("prod-worker", createTime),
			generateTaskInfo("prod-worker-3", createTime.Add(-time.Hour)),
			false,
		},
	}

	for i, item := range items {
		if value := CompareTask(item.lv, item.rv); value != item.expect {
			t.Errorf("case %d: expected: %v, got %v", i, item.expect, value)
		}
	}
}

func generateTaskInfo(name string, createTime time.Time) *apis.TaskInfo {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.Time{Time: createTime},
		},
	}
	return &apis.TaskInfo{
		Name: name,
		Pod:  pod,
	}
}

func TestGetTaskIndexUnderJobFunc(t *testing.T) {
	namespace := "test"
	testCases := []struct {
		Name     string
		TaskName string
		Job      *batchv1alpha1.Job
		Expect   int
	}{
		{
			Name:     "GetTaskIndexUnderJob1",
			TaskName: "task1",
			Job: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job1",
					Namespace: namespace,
				},
				Spec: batchv1alpha1.JobSpec{
					Tasks: []batchv1alpha1.TaskSpec{
						{
							Name:     "task1",
							Replicas: 2,
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "pods",
									Namespace: namespace,
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name: "Containers",
										},
									},
								},
							},
						},
						{
							Name:     "task2",
							Replicas: 2,
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "pods",
									Namespace: namespace,
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name: "Containers",
										},
									},
								},
							},
						},
					},
				},
			},
			Expect: 0,
		},
		{
			Name:     "GetTaskIndexUnderJob2",
			TaskName: "task2",
			Job: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job1",
					Namespace: namespace,
				},
				Spec: batchv1alpha1.JobSpec{
					Tasks: []batchv1alpha1.TaskSpec{
						{
							Name:     "task1",
							Replicas: 2,
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "pods",
									Namespace: namespace,
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name: "Containers",
										},
									},
								},
							},
						},
						{
							Name:     "task2",
							Replicas: 2,
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "pods",
									Namespace: namespace,
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name: "Containers",
										},
									},
								},
							},
						},
					},
				},
			},
			Expect: 1,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			index := GetTaskIndexUnderJob(testCase.TaskName, testCase.Job)
			if index != testCase.Expect {
				t.Errorf("GetTasklndexUnderJobFunc(%s) = %d, expect %d", testCase.TaskName, index, testCase.Expect)
			}
		})
	}
}

func TestGetPodsNameUnderTaskFunc(t *testing.T) {
	namespace := "test"
	testCases := []struct {
		Name     string
		TaskName string
		Job      *batchv1alpha1.Job
		Expect   []string
	}{
		{
			Name:     "GetTaskIndexUnderJob1",
			TaskName: "task1",
			Job: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job1",
					Namespace: namespace,
				},
				Spec: batchv1alpha1.JobSpec{
					Tasks: []batchv1alpha1.TaskSpec{
						{
							Name:     "task1",
							Replicas: 2,
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "pods1",
									Namespace: namespace,
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name: "Containers",
										},
									},
								},
							},
						},
						{
							Name:     "task2",
							Replicas: 2,
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "pods2",
									Namespace: namespace,
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name: "Containers",
										},
									},
								},
							},
						},
					},
				},
			},
			Expect: []string{"job1-task1-0", "job1-task1-1"},
		},
		{
			Name:     "GetTaskIndexUnderJob2",
			TaskName: "task2",
			Job: &batchv1alpha1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "job1",
					Namespace: namespace,
				},
				Spec: batchv1alpha1.JobSpec{
					Tasks: []batchv1alpha1.TaskSpec{
						{
							Name:     "task1",
							Replicas: 2,
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "pods1",
									Namespace: namespace,
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name: "Containers",
										},
									},
								},
							},
						},
						{
							Name:     "task2",
							Replicas: 2,
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "pods2",
									Namespace: namespace,
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name: "Containers",
										},
									},
								},
							},
						},
					},
				},
			},
			Expect: []string{"job1-task2-0", "job1-task2-1"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			pods := GetPodsNameUnderTask(testCase.TaskName, testCase.Job)
			for _, pod := range pods {
				if !contains(testCase.Expect, pod) {
					t.Errorf("Test case failed: %s, expect: %v, got: %v", testCase.Name, testCase.Expect, pods)
				}
			}
		})
	}
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
