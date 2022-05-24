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
	"io"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"strings"
)

type listFlags struct {
	commonFlags

	Namespace     string
	SchedulerName string
	allNamespace  bool
	selector      string
}

// TODO: there are two segments not printed
const (
	Name        string = "Name"
	Creation    string = "Creation"
	Phase       string = "Phase"
	Replicas    string = "Replicas"
	Min         string = "Min"
	Scheduler   string = "Scheduler"
	Pending     string = "Pending"
	Running     string = "Running"
	Succeeded   string = "Succeeded"
	Terminating string = "Terminating"
	Version     string = "Version"
	Failed      string = "Failed"
	Unknown     string = "Unknown"
	RetryCount  string = "RetryCount"
	Type        string = "JobType"
	Namespace   string = "Namespace"
)

var listJobFlags = &listFlags{}

// InitListFlags init list command flags.
func InitListFlags(cmd *cobra.Command) {
	initFlags(cmd, &listJobFlags.commonFlags)

	cmd.Flags().StringVarP(&listJobFlags.Namespace, "namespace", "n", "default", "the namespace of job")
	cmd.Flags().StringVarP(&listJobFlags.SchedulerName, "scheduler", "S", "", "list job with specified scheduler name")
	cmd.Flags().BoolVarP(&listJobFlags.allNamespace, "all-namespaces", "", false, "list jobs in all namespaces")
	cmd.Flags().StringVarP(&listJobFlags.selector, "selector", "", "", "fuzzy matching jobName")
}

// ListJobs lists all jobs details.
func ListJobs() error {
	config, err := utils.BuildConfig(listJobFlags.Master, listJobFlags.Kubeconfig)
	if err != nil {
		return err
	}
	if listJobFlags.allNamespace {
		listJobFlags.Namespace = ""
	}
	jobClient := volcanoclient.NewForConfigOrDie(config)
	jobs, err := jobClient.BatchV1alpha1().Jobs(listJobFlags.Namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	if len(jobs.Items) == 0 {
		fmt.Printf("No resources found\n")
		return nil
	}
	PrintJobs(jobs, os.Stdout)

	return nil
}

// PrintJobs prints all jobs details.
func PrintJobs(jobs *batchv1alpha1.JobList, writer io.Writer) {
	maxLenInfo := getMaxLen(jobs)

	titleFormat := "%%-%ds%%-15s%%-12s%%-12s%%-12s%%-6s%%-10s%%-10s%%-12s%%-10s%%-12s%%-10s\n"
	contentFormat := "%%-%ds%%-15s%%-12s%%-12s%%-12d%%-6d%%-10d%%-10d%%-12d%%-10d%%-12d%%-10d\n"

	var err error
	if listJobFlags.allNamespace {
		_, err = fmt.Fprintf(writer, fmt.Sprintf("%%-%ds"+titleFormat, maxLenInfo[1], maxLenInfo[0]),
			Namespace, Name, Creation, Phase, Type, Replicas, Min, Pending, Running, Succeeded, Failed, Unknown, RetryCount)
	} else {
		_, err = fmt.Fprintf(writer, fmt.Sprintf(titleFormat, maxLenInfo[0]),
			Name, Creation, Phase, Type, Replicas, Min, Pending, Running, Succeeded, Failed, Unknown, RetryCount)
	}
	if err != nil {
		fmt.Printf("Failed to print list command result: %s.\n", err)
	}

	for _, job := range jobs.Items {
		// if not the target job to print, skip it
		if listJobFlags.SchedulerName != "" && listJobFlags.SchedulerName != job.Spec.SchedulerName {
			continue
		}
		if !strings.Contains(job.Name, listJobFlags.selector) {
			continue
		}
		replicas := int32(0)
		for _, ts := range job.Spec.Tasks {
			replicas += ts.Replicas
		}
		jobType := job.ObjectMeta.Labels[batchv1alpha1.JobTypeKey]
		if jobType == "" {
			jobType = "Batch"
		}

		if listJobFlags.allNamespace {
			_, err = fmt.Fprintf(writer, fmt.Sprintf("%%-%ds"+contentFormat, maxLenInfo[1], maxLenInfo[0]),
				job.Namespace, job.Name, job.CreationTimestamp.Format("2006-01-02"), job.Status.State.Phase, jobType, replicas,
				job.Status.MinAvailable, job.Status.Pending, job.Status.Running, job.Status.Succeeded, job.Status.Failed, job.Status.Unknown, job.Status.RetryCount)
		} else {
			_, err = fmt.Fprintf(writer, fmt.Sprintf(contentFormat, maxLenInfo[0]),
				job.Name, job.CreationTimestamp.Format("2006-01-02"), job.Status.State.Phase, jobType, replicas,
				job.Status.MinAvailable, job.Status.Pending, job.Status.Running, job.Status.Succeeded, job.Status.Failed, job.Status.Unknown, job.Status.RetryCount)
		}
		if err != nil {
			fmt.Printf("Failed to print list command result: %s.\n", err)
		}
	}
}

// getMaxLen is used for pretty print.
func getMaxLen(jobs *batchv1alpha1.JobList) []int {
	maxNameLen := len(Name)
	maxNamespaceLen := len(Namespace)
	for _, job := range jobs.Items {
		if len(job.Name) > maxNameLen {
			maxNameLen = len(job.Name)
		}
		if len(job.Namespace) > maxNamespaceLen {
			maxNamespaceLen = len(job.Namespace)
		}
	}

	return []int{maxNameLen + 3, maxNamespaceLen + 3}
}
