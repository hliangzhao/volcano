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

package app

import (
	"fmt"
	"github.com/hliangzhao/volcano/cmd/webhook-manager/app/options"
	"github.com/hliangzhao/volcano/pkg/apis/scheduling/scheme"
	"github.com/hliangzhao/volcano/pkg/kube"
	"github.com/hliangzhao/volcano/pkg/version"
	webhooksconfig "github.com/hliangzhao/volcano/pkg/webhooks/config"
	"github.com/hliangzhao/volcano/pkg/webhooks/router"
	corev1 "k8s.io/api/core/v1"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

// Run start the service of admission controller.
func Run(config *options.Config) error {
	if config.PrintVersion {
		version.PrintVersionAndExit()
		return nil
	}

	if config.WebhookURL == "" && config.WebhookNamespace == "" && config.WebhookName == "" {
		return fmt.Errorf("failed to start webhooks as both 'url' and 'namespace/name' of webhook are empty")
	}

	restConfig, err := kube.BuildConfig(config.KubeClientOptions)
	if err != nil {
		return fmt.Errorf("unable to build k8s config: %v", err)
	}

	admissionConf := webhooksconfig.LoadAdmissionConf(config.ConfigPath)
	if admissionConf == nil {
		klog.Errorf("loadAdmissionConf failed.")
	} else {
		klog.V(2).Infof("loadAdmissionConf:%v", admissionConf.ResGroupsConfig)
	}

	vClient := getVolcanoClient(restConfig)
	kubeClient := getKubeClient(restConfig)

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: config.SchedulerName})
	router.ForEachAdmission(config, func(service *router.AdmissionService) {
		if service.Config != nil {
			service.Config.VolcanoClient = vClient
			service.Config.KubeClient = kubeClient
			service.Config.SchedulerName = config.SchedulerName
			service.Config.Recorder = recorder
			service.Config.ConfigData = admissionConf
		}

		klog.V(3).Infof("Registered '%s' as webhook.", service.Path)
		http.HandleFunc(service.Path, service.Handler)

		klog.V(3).Infof("Registered configuration for webhook <%s>", service.Path)
		registerWebhookConfig(kubeClient, config, service, config.CaCertData)
	})

	webhookServeError := make(chan struct{})
	stopChannel := make(chan os.Signal, 1)
	signal.Notify(stopChannel, syscall.SIGTERM, syscall.SIGINT)

	server := &http.Server{
		Addr:      config.ListenAddress + ":" + strconv.Itoa(config.Port),
		TLSConfig: configTLS(config, restConfig),
	}
	go func() {
		err = server.ListenAndServeTLS("", "")
		if err != nil && err != http.ErrServerClosed {
			klog.Fatalf("ListenAndServeTLS for admission webhook failed: %v", err)
			close(webhookServeError)
		}

		klog.Info("Volcano Webhook manager started.")
	}()

	if config.ConfigPath != "" {
		go webhooksconfig.WatchAdmissionConf(config.ConfigPath, stopChannel)
	}

	select {
	case <-stopChannel:
		if err := server.Close(); err != nil {
			return fmt.Errorf("close admission server failed: %v", err)
		}
		return nil
	case <-webhookServeError:
		return fmt.Errorf("unknown webhook server error")
	}
}
