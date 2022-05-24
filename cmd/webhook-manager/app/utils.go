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

package app

// fully checked and understood

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/hliangzhao/volcano/cmd/webhook-manager/app/options"
	volcanoclient "github.com/hliangzhao/volcano/pkg/client/clientset/versioned"
	"github.com/hliangzhao/volcano/pkg/webhooks/router"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"regexp"
	"strings"
)

// registerWebhookConfig registers mutating and validating webhooks in cluster.
func registerWebhookConfig(kubeClient *kubernetes.Clientset, config *options.Config, service *router.AdmissionService, caBundle []byte) {
	sideEffect := admissionregistrationv1.SideEffectClassNoneOnDryRun
	reviewVersions := []string{"v1"}
	clientConfig := admissionregistrationv1.WebhookClientConfig{
		CABundle: caBundle,
	}

	if config.WebhookURL != "" {
		url := config.WebhookURL + service.Path
		clientConfig.URL = &url
		klog.Infof("The URL of webhook manager is <%s>.", url)
	}

	if config.WebhookName != "" && config.WebhookNamespace != "" {
		clientConfig.Service = &admissionregistrationv1.ServiceReference{
			Name:      config.WebhookName,
			Namespace: config.WebhookNamespace,
			Path:      &service.Path,
		}
		klog.Infof("The service of webhook manager is <%s/%s/%s>.",
			config.WebhookName, config.WebhookNamespace, service.Path)
	}

	if service.MutatingConfig != nil {
		for i := range service.MutatingConfig.Webhooks {
			service.MutatingConfig.Webhooks[i].SideEffects = &sideEffect
			service.MutatingConfig.Webhooks[i].AdmissionReviewVersions = reviewVersions
			service.MutatingConfig.Webhooks[i].ClientConfig = clientConfig
		}

		service.MutatingConfig.ObjectMeta.Name = webhookConfigName(config.WebhookName, service.Path)

		if err := registerMutateWebhook(kubeClient, service.MutatingConfig); err != nil {
			klog.Errorf("Failed to register mutating admission webhook (%s): %v", service.Path, err)
		} else {
			klog.V(3).Infof("Registered mutating webhook for path <%s>.", service.Path)
		}
	}

	if service.ValidatingConfig != nil {
		for i := range service.ValidatingConfig.Webhooks {
			service.ValidatingConfig.Webhooks[i].SideEffects = &sideEffect
			service.ValidatingConfig.Webhooks[i].AdmissionReviewVersions = reviewVersions
			service.ValidatingConfig.Webhooks[i].ClientConfig = clientConfig
		}

		service.ValidatingConfig.ObjectMeta.Name = webhookConfigName(config.WebhookName, service.Path)

		if err := registerValidateWebhook(kubeClient, service.ValidatingConfig); err != nil {
			klog.Errorf("Failed to register validating admission webhook (%s): %v",
				service.Path, err)
		} else {
			klog.V(3).Infof("Registered validating webhook for path <%s>.", service.Path)
		}
	}
}

// getKubeClient gets a clientset with restConfig.
func getKubeClient(restConfig *rest.Config) *kubernetes.Clientset {
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		klog.Fatal(err)
	}
	return clientset
}

// GetVolcanoClient gets a clientset for volcano.
func getVolcanoClient(restConfig *rest.Config) *volcanoclient.Clientset {
	clientset, err := volcanoclient.NewForConfig(restConfig)
	if err != nil {
		klog.Fatal(err)
	}
	return clientset
}

// configTLS is a helper function that generate tls certificates from directly defined tls config or kubeconfig.
// These are passed in as command line for cluster certification. If tls config is passed in, we use the directly
// defined tls config, else use that defined in kubeconfig.
func configTLS(config *options.Config, restConfig *rest.Config) *tls.Config {
	if len(config.CertData) != 0 && len(config.KeyData) != 0 {
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(config.CaCertData)

		sCert, err := tls.X509KeyPair(config.CertData, config.KeyData)
		if err != nil {
			klog.Fatal(err)
		}

		return &tls.Config{
			Certificates: []tls.Certificate{sCert},
			RootCAs:      certPool,
			MinVersion:   tls.VersionTLS12,
			ClientAuth:   tls.VerifyClientCertIfGiven,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			},
		}
	}

	if len(restConfig.CertData) != 0 && len(restConfig.KeyData) != 0 {
		sCert, err := tls.X509KeyPair(restConfig.CertData, restConfig.KeyData)
		if err != nil {
			klog.Fatal(err)
		}

		return &tls.Config{
			Certificates: []tls.Certificate{sCert},
		}
	}

	klog.Fatal("tls: failed to find any tls config data")
	return &tls.Config{}
}

// registerMutateWebhook updates or creates the mutating webhook in cluster with the input hook.
func registerMutateWebhook(clientset *kubernetes.Clientset, hook *admissionregistrationv1.MutatingWebhookConfiguration) error {
	client := clientset.AdmissionregistrationV1().MutatingWebhookConfigurations()
	existing, err := client.Get(context.TODO(), hook.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil && existing != nil {
		klog.V(4).Infof("Updating MutatingWebhookConfiguration %v", hook)
		existing.Webhooks = hook.Webhooks
		if _, err := client.Update(context.TODO(), existing, metav1.UpdateOptions{}); err != nil {
			return err
		}
	} else {
		klog.V(4).Infof("Creating MutatingWebhookConfiguration %v", hook)
		if _, err := client.Create(context.TODO(), hook, metav1.CreateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

// registerValidateWebhook updates or creates the validating webhook in cluster with the input hook.
func registerValidateWebhook(clientset *kubernetes.Clientset, hook *admissionregistrationv1.ValidatingWebhookConfiguration) error {
	client := clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations()
	existing, err := client.Get(context.TODO(), hook.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil && existing != nil {
		existing.Webhooks = hook.Webhooks
		klog.V(4).Infof("Updating ValidatingWebhookConfiguration %v", hook)
		if _, err := client.Update(context.TODO(), existing, metav1.UpdateOptions{}); err != nil {
			return err
		}
	} else {
		klog.V(4).Infof("Creating ValidatingWebhookConfiguration %v", hook)
		if _, err := client.Create(context.TODO(), hook, metav1.CreateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

// webhookConfigName creates the webhook config name with the given webhook name.
func webhookConfigName(name, path string) string {
	if name == "" {
		name = "webhook"
	}
	re := regexp.MustCompile(`-+`)
	raw := strings.Join([]string{name, strings.ReplaceAll(path, "/", "-")}, "-")
	return re.ReplaceAllString(raw, "-")
}
