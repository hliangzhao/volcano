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

// fully checked and understood

import (
	"context"
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/cli/utils"
	volcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned"
	"github.com/spf13/cobra"
	"io/ioutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
	"strings"
)

type runFlags struct {
	commonFlags

	// explanations of these flags can be found at the InitRunFlags() function

	Name      string
	Namespace string
	Image     string

	MinAvailable  int
	Replicas      int
	Requests      string
	Limits        string
	SchedulerName string
	FileName      string
}

var launchJobFlags = &runFlags{}

// InitRunFlags init the run flags.
func InitRunFlags(cmd *cobra.Command) {
	initFlags(cmd, &launchJobFlags.commonFlags)

	cmd.Flags().StringVarP(&launchJobFlags.Image, "image", "i", "busybox", "the container image of job")
	cmd.Flags().StringVarP(&launchJobFlags.Namespace, "namespace", "n", "default", "the namespace of job")
	cmd.Flags().StringVarP(&launchJobFlags.Name, "name", "N", "", "the name of job")
	cmd.Flags().IntVarP(&launchJobFlags.MinAvailable, "min", "m", 1, "the minimal available tasks of job")
	cmd.Flags().IntVarP(&launchJobFlags.Replicas, "replicas", "r", 1, "the total tasks of job")
	cmd.Flags().StringVarP(&launchJobFlags.Requests, "requests", "R", "cpu=1000m,memory=100Mi", "the resource request of the task")
	cmd.Flags().StringVarP(&launchJobFlags.Limits, "limits", "L", "cpu=1000m,memory=100Mi", "the resource limit of the task")
	cmd.Flags().StringVarP(&launchJobFlags.SchedulerName, "scheduler", "S", "volcano", "the scheduler for this job")
	cmd.Flags().StringVarP(&launchJobFlags.FileName, "filename", "f", "", "the yaml file of job")
}

var jobName = "job.volcano.sh"

// RunJob creates the job.
func RunJob() error {
	// create the kubeconfig
	config, err := utils.BuildConfig(launchJobFlags.Master, launchJobFlags.Kubeconfig)
	if err != nil {
		return err
	}

	// check legal or not
	if launchJobFlags.Name == "" && launchJobFlags.FileName == "" {
		err = fmt.Errorf("job name cannot be left blank")
		return err
	}

	// transform req and limit flags into ResourceList
	req, err := utils.PopulateResourceListV1(launchJobFlags.Requests)
	if err != nil {
		return err
	}
	limit, err := utils.PopulateResourceListV1(launchJobFlags.Limits)
	if err != nil {
		return err
	}

	// create volcano job instance from yaml file or command flags
	job, err := readFile(launchJobFlags.FileName)
	if err != nil {
		return err
	}
	if job == nil {
		job = constructLaunchJobFlagsJob(launchJobFlags, req, limit)
	}

	// create the volcano job resource in cluster with the volcano job instance
	jobClient := volcanoclient.NewForConfigOrDie(config)
	newJob, err := jobClient.BatchV1alpha1().Jobs(launchJobFlags.Namespace).Create(context.TODO(), job, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	// set the queue that this job belongs to
	if newJob.Spec.Queue == "" {
		newJob.Spec.Queue = "default"
	}

	fmt.Printf("run job %v successfully\n", newJob.Name)

	return nil
}

// readFile reads yaml file and unmarshal it to a volcano job.
func readFile(filename string) (*batchv1alpha1.Job, error) {
	if filename == "" {
		return nil, nil
	}

	if !strings.Contains(filename, ".yaml") && !strings.Contains(filename, ".yml") {
		return nil, fmt.Errorf("only support yaml file")
	}

	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file, err: %v", err)
	}

	var job batchv1alpha1.Job
	if err := yaml.Unmarshal(file, &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal file, err:  %v", err)
	}

	return &job, nil
}

// constructLaunchJobFlagsJob constructs a volcano job with the input flags.
func constructLaunchJobFlagsJob(launchJobFlags *runFlags, req, limit corev1.ResourceList) *batchv1alpha1.Job {
	return &batchv1alpha1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      launchJobFlags.Name,
			Namespace: launchJobFlags.Namespace,
		},
		Spec: batchv1alpha1.JobSpec{
			MinAvailable:  int32(launchJobFlags.MinAvailable),
			SchedulerName: launchJobFlags.SchedulerName,
			Tasks: []batchv1alpha1.TaskSpec{
				{
					Replicas: int32(launchJobFlags.Replicas),

					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name:   launchJobFlags.Name,
							Labels: map[string]string{jobName: launchJobFlags.Name},
						},
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{
								{
									Image:           launchJobFlags.Image,
									Name:            launchJobFlags.Name,
									ImagePullPolicy: corev1.PullIfNotPresent,
									Resources: corev1.ResourceRequirements{
										Limits:   limit,
										Requests: req,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
