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

package main

import (
	"github.com/hliangzhao/volcano/pkg/cli/job"
	"github.com/spf13/cobra"
)

func buildJobCmd() *cobra.Command {
	jobCmd := &cobra.Command{
		Use:   "job",
		Short: "vcctl is the command line tool to operate job",
	}

	jobRunCmd := &cobra.Command{
		Use:   "run",
		Short: "run job by parameters from the command line",
		Run: func(cmd *cobra.Command, args []string) {
			checkError(cmd, job.RunJob())
		},
	}
	job.InitRunFlags(jobRunCmd)
	jobCmd.AddCommand(jobRunCmd)

	jobListCmd := &cobra.Command{
		Use:   "list",
		Short: "list job information",
		Run: func(cmd *cobra.Command, args []string) {
			checkError(cmd, job.ListJobs())
		},
	}
	job.InitListFlags(jobListCmd)
	jobCmd.AddCommand(jobListCmd)

	jobViewCmd := &cobra.Command{
		Use:   "view",
		Short: "show job information",
		Run: func(cmd *cobra.Command, args []string) {
			checkError(cmd, job.ViewJob())
		},
	}
	job.InitViewFlags(jobViewCmd)
	jobCmd.AddCommand(jobViewCmd)

	jobSuspendCmd := &cobra.Command{
		Use:   "suspend",
		Short: "abort a job",
		Run: func(cmd *cobra.Command, args []string) {
			checkError(cmd, job.SuspendJob())
		},
	}
	job.InitSuspendFlags(jobSuspendCmd)
	jobCmd.AddCommand(jobSuspendCmd)

	jobResumeCmd := &cobra.Command{
		Use:   "resume",
		Short: "resume a job",
		Run: func(cmd *cobra.Command, args []string) {
			checkError(cmd, job.ResumeJob())
		},
	}
	job.InitResumeFlags(jobResumeCmd)
	jobCmd.AddCommand(jobResumeCmd)

	jobDelCmd := &cobra.Command{
		Use:   "delete",
		Short: "delete a job",
		Run: func(cmd *cobra.Command, args []string) {
			checkError(cmd, job.DeleteJob())
		},
	}
	job.InitDeleteFlags(jobDelCmd)
	jobCmd.AddCommand(jobDelCmd)

	return jobCmd
}
