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

package vcancel

import (
	"context"
	"fmt"
	"github.com/hliangzhao/volcano/pkg/cli/utils"
	volcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type cancelFlags struct {
	utils.CommonFlags

	Namespace string
	JobName   string
}

var cancelJobFlags = &cancelFlags{}

// InitCancelFlags init the cancel command flags.
func InitCancelFlags(cmd *cobra.Command) {
	utils.InitFlags(cmd, &cancelJobFlags.CommonFlags)

	cmd.Flags().StringVarP(&cancelJobFlags.Namespace, "namespace", "N", "default", "the namespace of job")
	cmd.Flags().StringVarP(&cancelJobFlags.JobName, "name", "n", "", "the name of job")
}

// CancelJob cancel the job.
func CancelJob() error {
	config, err := utils.BuildConfig(cancelJobFlags.Master, cancelJobFlags.Kubeconfig)
	if err != nil {
		return err
	}

	if cancelJobFlags.JobName == "" {
		err := fmt.Errorf("job name is mandatory to cancel a particular job")
		return err
	}

	jobClient := volcanoclient.NewForConfigOrDie(config)
	err = jobClient.BatchV1alpha1().Jobs(cancelJobFlags.Namespace).Delete(context.TODO(), cancelJobFlags.JobName, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	fmt.Printf("cancel job %v successfully\n", cancelJobFlags.JobName)
	return nil
}
