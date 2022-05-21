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

import (
	"context"
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	busv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/bus/v1alpha1"
	schedulingv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/scheduling/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"
)

// Create GVKs

var JobKind = batchv1alpha1.SchemeGroupVersion.WithKind("Job")
var CommandKind = busv1alpha1.SchemeGroupVersion.WithKind("Command")
var QueueKind = schedulingv1alpha1.SchemeGroupVersion.WithKind("Queue")

// CreateOrUpdateConfigMap creates or updates the ConfigMap with data for job.
func CreateOrUpdateConfigMap(job *batchv1alpha1.Job, kubeClient kubernetes.Interface, data map[string]string, cmName string) error {
	// The code in this func is standard!
	// kubernetes.Interface is the client that we used for the CRUD of k8s resources
	foundCm, err := kubeClient.CoreV1().ConfigMaps(job.Namespace).Get(context.TODO(), cmName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.V(3).Infof("Failed to get ConfigMap for Job <%s/%s>: %v", job.Namespace, job.Name, err)
			return err
		}

		// not found, create it
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: job.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(job, JobKind),
				},
			},
			Data: data, // default string data
		}
		if _, err = kubeClient.CoreV1().ConfigMaps(job.Namespace).Create(context.TODO(), cm, metav1.CreateOptions{}); err != nil {
			klog.V(3).Infof("Failed to create ConfigMap for Job <%s/%s>: %v", job.Namespace, job.Name, err)
			return err
		}
		return nil
	}

	// found, but no changes needed
	if reflect.DeepEqual(foundCm.Data, data) {
		return nil
	}

	// found, update required
	foundCm.Data = data
	if _, err = kubeClient.CoreV1().ConfigMaps(job.Namespace).Update(context.TODO(), foundCm, metav1.UpdateOptions{}); err != nil {
		klog.V(3).Infof("Failed to update ConfigMap for Job <%s/%s>: %v", job.Namespace, job.Name, err)
		return err
	}
	return nil
}

// DeleteConfigMap deletes the ConfigMap with name cmName for job.
func DeleteConfigMap(job *batchv1alpha1.Job, kubeClient kubernetes.Interface, cmName string) error {
	if err := kubeClient.CoreV1().ConfigMaps(job.Namespace).Delete(context.TODO(), cmName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to delete ConfigMap of Job <%s/%s>: %v", job.Namespace, job.Name, err)
		return err
	}
	return nil
}

// CreateOrUpdateSecret creates or updates the Secret with the given data.
func CreateOrUpdateSecret(job *batchv1alpha1.Job, kubeClient kubernetes.Interface, data map[string][]byte, secretName string) error {
	foundSc, err := kubeClient.CoreV1().Secrets(job.Namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.V(3).Infof("Failed to get Secret for Job <%s/%s>: %v", job.Namespace, job.Name, err)
			return err
		}

		// not found, create it
		sc := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: job.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(job, JobKind),
				},
			},
			Data: data, // default binary data
		}
		if _, err = kubeClient.CoreV1().Secrets(job.Namespace).Create(context.TODO(), sc, metav1.CreateOptions{}); err != nil {
			klog.V(3).Infof("Failed to create ConfigMap for Job <%s/%s>: %v", job.Namespace, job.Name, err)
			return err
		}
		return nil
	}

	// found, but no changes needed
	sshConfig := "config"
	if reflect.DeepEqual(foundSc.Data[sshConfig], data[sshConfig]) {
		return nil
	}

	// found, update required
	foundSc.Data = data
	if _, err = kubeClient.CoreV1().Secrets(job.Namespace).Update(context.TODO(), foundSc, metav1.UpdateOptions{}); err != nil {
		klog.V(3).Infof("Failed to update Secret for Job <%s/%s>: %v", job.Namespace, job.Name, err)
		return err
	}
	return nil
}

// DeleteSecret deletes the Secret with name secretName for job.
func DeleteSecret(job *batchv1alpha1.Job, kubeClient kubernetes.Interface, secretName string) error {
	if err := kubeClient.CoreV1().Secrets(job.Namespace).Delete(context.TODO(), secretName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to delete Secret of Job <%s/%s>: %v", job.Namespace, job.Name, err)
		return err
	}
	return nil
}

// GeneratePodGroupName returns the PodGroup name as "podgroup-{ownerRef.UID}" or "podgroup-{pod.UID}".
func GeneratePodGroupName(pod *corev1.Pod) string {
	pgName := batchv1alpha1.PodgroupNamePrefix
	if len(pod.OwnerReferences) != 0 {
		// at most one owner reference has controller set
		for _, ownerRef := range pod.OwnerReferences {
			if ownerRef.Controller != nil && *ownerRef.Controller {
				pgName += string(ownerRef.UID)
				return pgName
			}
		}
	}
	pgName += string(pod.UID)
	return pgName
}

// StartHealthz starts a server on an address for handling health check.
func StartHealthz(healthzBindAddress, name string) error {
	listener, err := net.Listen("tcp", healthzBindAddress)
	if err != nil {
		return fmt.Errorf("failed to create listener: %v", err)
	}

	pathRecordMux := mux.NewPathRecorderMux(name)
	healthz.InstallHandler(pathRecordMux)

	server := &http.Server{
		Addr:           listener.Addr().String(),
		Handler:        pathRecordMux,
		MaxHeaderBytes: 1 << 20,
	}

	return runServer(server, listener)
}

func runServer(server *http.Server, ln net.Listener) error {
	if ln == nil || server == nil {
		return fmt.Errorf("listener and server must not be nil")
	}

	// start a coroutine to receive os signal (TERM and INT), then close server
	stopCh := make(chan os.Signal, 2)
	signal.Notify(stopCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-stopCh
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		_ = server.Shutdown(ctx)
		cancel()
	}()

	// start a coroutine to run server
	go func() {
		defer runtime.HandleCrash()

		listener := tcpKeepAliveListener{ln.(*net.TCPListener)}
		err := server.Serve(listener)

		msg := fmt.Sprintf("Stopped listening on %s", listener.Addr().String())
		select {
		case <-stopCh:
			klog.Info(msg) // when logging, server.Shutdown() (in the coroutine defined above) is also executed
		default:
			klog.Fatalf("%s due to error: %v", msg, err)
		}
	}()

	return nil
}

type tcpKeepAliveListener struct {
	*net.TCPListener
}

// Accept waits for and returns the next connection to the listener.
// This func will be called internally in `server.Serve(listener)`.
func (listener tcpKeepAliveListener) Accept() (net.Conn, error) {
	// only accept TCP connections
	tc, err := listener.AcceptTCP()
	if err != nil {
		return nil, err
	}

	// keep alive within 3 minutes since last confirmed time
	_ = tc.SetKeepAlive(true)
	_ = tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}
