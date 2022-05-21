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

package validate

// TODO: just copied.
//  Passed.

import (
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	"reflect"
	"testing"
)

func TestTopoSort(t *testing.T) {
	testCases := []struct {
		name        string
		job         *batchv1alpha1.Job
		sortedTasks []string
		isDag       bool
	}{
		{
			name: "test-1",
			job: &batchv1alpha1.Job{
				Spec: batchv1alpha1.JobSpec{
					Tasks: []batchv1alpha1.TaskSpec{
						{
							Name: "t1",
							DependsOn: &batchv1alpha1.DependsOn{
								Name: []string{"t2", "t3"},
							},
						},
						{
							Name: "t2",
							DependsOn: &batchv1alpha1.DependsOn{
								Name: []string{"t3"},
							},
						},
						{
							Name: "t3",
							DependsOn: &batchv1alpha1.DependsOn{
								Name: []string{},
							},
						},
					},
				},
			},
			sortedTasks: []string{"t3", "t2", "t1"},
			isDag:       true,
		},
		{
			name: "test-2",
			job: &batchv1alpha1.Job{
				Spec: batchv1alpha1.JobSpec{
					Tasks: []batchv1alpha1.TaskSpec{
						{
							Name: "t1",
							DependsOn: &batchv1alpha1.DependsOn{
								Name: []string{"t2"},
							},
						},
						{
							Name: "t2",
							DependsOn: &batchv1alpha1.DependsOn{
								Name: []string{"t1"},
							},
						},
						{
							Name:      "t3",
							DependsOn: nil,
						},
					},
				},
			},
			sortedTasks: nil,
			isDag:       false,
		},
		{
			name: "test-3",
			job: &batchv1alpha1.Job{
				Spec: batchv1alpha1.JobSpec{
					Tasks: []batchv1alpha1.TaskSpec{
						{
							Name: "t1",
							DependsOn: &batchv1alpha1.DependsOn{
								Name: []string{"t2", "t3"},
							},
						},
						{
							Name: "t2",
							DependsOn: &batchv1alpha1.DependsOn{
								Name: []string{"t2"},
							},
						},
						{
							Name:      "t3",
							DependsOn: nil,
						},
					},
				},
			},
			sortedTasks: nil,
			isDag:       false,
		},
		{
			name: "test-4",
			job: &batchv1alpha1.Job{
				Spec: batchv1alpha1.JobSpec{
					Tasks: []batchv1alpha1.TaskSpec{
						{
							Name: "t1",
							DependsOn: &batchv1alpha1.DependsOn{
								Name: []string{"t2", "t3"},
							},
						},
						{
							Name:      "t2",
							DependsOn: nil,
						},
						{
							Name: "t3",
							DependsOn: &batchv1alpha1.DependsOn{
								Name: []string{"t2"},
							},
						},
					},
				},
			},
			sortedTasks: []string{"t2", "t3", "t1"},
			isDag:       true,
		},
	}

	for _, testcase := range testCases {
		tasks, isDag := topoSort(testcase.job)
		if isDag != testcase.isDag || !reflect.DeepEqual(tasks, testcase.sortedTasks) {
			t.Errorf("%s failed, expect sortedTasks: %v, got: %v, expected isDag: %v, got: %v",
				testcase.name, testcase.sortedTasks, tasks, testcase.isDag, isDag)
		}
	}
}
