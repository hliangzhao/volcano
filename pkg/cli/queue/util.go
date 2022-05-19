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

package queue

import (
	"context"
	"fmt"
	busv1alpha1 `github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1`
	`github.com/hliangzhao/volcano/pkg/apis/helpers`
	volcanoclient `github.com/hliangzhao/volcano/pkg/client/clientset/versioned`
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"strings"

	// Initialize client auth plugin.
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func buildConfig(master, kubeconfig string) (*rest.Config, error) {
	return clientcmd.BuildConfigFromFlags(master, kubeconfig)
}

func createQueueCommand(config *rest.Config, action busv1alpha1.Action) error {
	queueClient := volcanoclient.NewForConfigOrDie(config)
	queue, err := queueClient.SchedulingV1alpha1().Queues().Get(context.TODO(), operateQueueFlags.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	ctrlRef := metav1.NewControllerRef(queue, helpers.QueueKind)
	cmd := &busv1alpha1.Command{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-%s-",
				queue.Name, strings.ToLower(string(action))),
			OwnerReferences: []metav1.OwnerReference{
				*ctrlRef,
			},
		},
		TargetObject: ctrlRef,
		Action:       string(action),
	}

	if _, err := queueClient.BusV1alpha1().Commands("default").Create(context.TODO(), cmd, metav1.CreateOptions{}); err != nil {
		return err
	}

	return nil
}
