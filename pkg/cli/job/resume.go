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
	"fmt"
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/cli/utils"
	"github.com/spf13/cobra"
)

type resumeFlags struct {
	commonFlags

	Namespace string
	JobName   string
}

var resumeJobFlags = &resumeFlags{}

// InitResumeFlags init resume command flags.
func InitResumeFlags(cmd *cobra.Command) {
	initFlags(cmd, &resumeJobFlags.commonFlags)

	cmd.Flags().StringVarP(&resumeJobFlags.Namespace, "namespace", "n", "default", "the namespace of job")
	cmd.Flags().StringVarP(&resumeJobFlags.JobName, "name", "N", "", "the name of job")
}

// ResumeJob resumes the job.
// Other than `run`, `list`, and `view`, `resume` cannot be implemented with simple CRUD through the clientset.
// It is implemented by the `Command` CRD.
func ResumeJob() error {
	config, err := utils.BuildConfig(resumeJobFlags.Master, resumeJobFlags.Kubeconfig)
	if err != nil {
		return err
	}
	if resumeJobFlags.JobName == "" {
		err := fmt.Errorf("job name is mandatory to resume a particular job")
		return err
	}

	return utils.CreateJobCommand(config, resumeJobFlags.Namespace, resumeJobFlags.JobName, busv1alpha1.ResumeJobAction)
}
