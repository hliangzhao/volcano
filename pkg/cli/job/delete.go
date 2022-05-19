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

package job

import (
	"context"
	"fmt"
	`github.com/hliangzhao/volcano/pkg/cli/utils`
	volcanoclient `github.com/hliangzhao/volcano/pkg/client/clientset/versioned`
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type deleteFlags struct {
	commonFlags

	Namespace string
	JobName   string
}

var deleteJobFlags = &deleteFlags{}

// InitDeleteFlags init the delete command flags.
func InitDeleteFlags(cmd *cobra.Command) {
	initFlags(cmd, &deleteJobFlags.commonFlags)

	cmd.Flags().StringVarP(&deleteJobFlags.Namespace, "namespace", "n", "default", "the namespace of job")
	cmd.Flags().StringVarP(&deleteJobFlags.JobName, "name", "N", "", "the name of job")
}

// DeleteJob delete the job.
func DeleteJob() error {
	config, err := utils.BuildConfig(deleteJobFlags.Master, deleteJobFlags.Kubeconfig)
	if err != nil {
		return err
	}

	if deleteJobFlags.JobName == "" {
		err := fmt.Errorf("job name is mandatory to delete a particular job")
		return err
	}

	jobClient := volcanoclient.NewForConfigOrDie(config)
	err = jobClient.BatchV1alpha1().Jobs(deleteJobFlags.Namespace).Delete(context.TODO(), deleteJobFlags.JobName, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	fmt.Printf("delete job %v successfully\n", deleteJobFlags.JobName)
	return nil
}
