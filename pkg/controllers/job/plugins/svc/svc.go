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

package svc

import (
	"context"
	"flag"
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/apis/helpers"
	jobhelpers "github.com/hliangzhao/volcano/pkg/controllers/job/helpers"
	plugininterface "github.com/hliangzhao/volcano/pkg/controllers/job/plugins/interface"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog/v2"
	"strconv"
	"strings"
)

/*
This plugin will create DNS name for the given pod, so that it could be searched and visited through the DNS name.
*/

type svcPlugin struct {
	arguments            []string
	client               plugininterface.PluginClient
	publishNotReadyAddrs bool
	disableNetworkPolicy bool
}

func New(client plugininterface.PluginClient, arguments []string) plugininterface.PluginInterface {
	sp := svcPlugin{arguments: arguments, client: client}
	sp.addFlags()
	return &sp
}

func (sp *svcPlugin) addFlags() {
	flagSet := flag.NewFlagSet(sp.Name(), flag.ContinueOnError)
	flagSet.BoolVar(&sp.publishNotReadyAddrs, "publish-not-ready-addresses", sp.publishNotReadyAddrs,
		"set publishNotReadyAddresses of svc to true")
	flagSet.BoolVar(&sp.disableNetworkPolicy, "disable-network-policy", sp.disableNetworkPolicy,
		"set disableNetworkPolicy of svc to true")

	if err := flagSet.Parse(sp.arguments); err != nil {
		klog.Errorf("plugin %s flagset parse failed, err: %v", sp.Name(), err)
	}
}

func (sp *svcPlugin) Name() string {
	return "svc"
}

// OnPodCreate adds `hostname` and `subdomain` for pod, and mount service config for pod.
func (sp *svcPlugin) OnPodCreate(pod *corev1.Pod, job *batchv1alpha1.Job) error {
	// Add `hostname` and `subdomain` for pod, mount service config for pod.
	// A pod with `hostname` and `subdomain` will have the fully qualified domain name (FQDN)
	// `hostname.subdomain.namespace.svc.cluster-domain.example`.
	// If there exists a headless service in the same namespace as the pod and with the
	// same name as the `subdomain`, the cluster's KubeDNS Server will return an A record for
	// the Pod's fully qualified hostname, pointing to the Pod’s IP.
	// `hostname.subdomain` will be used as address of the pod.
	// By default, a client Pod’s DNS search list will include the Pod’s own namespace and
	// the cluster’s default domain, so the pod can be accessed by pods in the same namespace
	// through the address of pod.
	// More info: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service
	if len(pod.Spec.Hostname) == 0 {
		pod.Spec.Hostname = pod.Name
	}
	if len(pod.Spec.Subdomain) == 0 {
		pod.Spec.Subdomain = job.Name
	}

	var hostEnv []corev1.EnvVar
	var envNames []string

	for _, task := range job.Spec.Tasks {
		// TODO: The splitter and the prefix of env should be configurable
		formatEnvKey := strings.Replace(task.Name, "-", "_", -1)
		envNames = append(envNames, fmt.Sprintf(EnvTaskHostFmt, strings.ToUpper(formatEnvKey)))
		envNames = append(envNames, fmt.Sprintf(EnvHostNumFmt, strings.ToUpper(formatEnvKey)))
	}

	// set env variables
	for _, name := range envNames {
		hostEnv = append(hostEnv, corev1.EnvVar{
			Name: name,
			ValueFrom: &corev1.EnvVarSource{
				ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: sp.cmName(job)},
					Key:                  name,
				},
			},
		})
	}

	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, hostEnv...)
	}

	for i := range pod.Spec.InitContainers {
		pod.Spec.InitContainers[i].Env = append(pod.Spec.InitContainers[i].Env, hostEnv...)
	}

	sp.mountConfigMap(pod, job)
	return nil
}

func (sp *svcPlugin) OnJobAdd(job *batchv1alpha1.Job) error {
	if job.Status.ControlledResources["plugin-"+sp.Name()] == sp.Name() {
		return nil
	}

	// create configmap, service, and network
	cmData := GenerateHosts(job)
	if err := helpers.CreateOrUpdateConfigMap(job, sp.client.KubeClient, cmData, sp.cmName(job)); err != nil {
		return err
	}
	if err := sp.createServiceIfNotExist(job); err != nil {
		return err
	}
	if !sp.disableNetworkPolicy {
		if err := sp.createNetworkPolicyIfNotExist(job); err != nil {
			return err
		}
	}

	job.Status.ControlledResources["plugin-"+sp.Name()] = sp.Name()
	return nil
}

func (sp *svcPlugin) OnJobDelete(job *batchv1alpha1.Job) error {
	if job.Status.ControlledResources["plugin-"+sp.Name()] != sp.Name() {
		return nil
	}

	// delete configmap, service, and network
	if err := helpers.DeleteConfigMap(job, sp.client.KubeClient, sp.cmName(job)); err != nil {
		return err
	}
	if err := sp.client.KubeClient.CoreV1().Services(job.Namespace).Delete(context.TODO(), job.Name, metav1.DeleteOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to delete Service of Job %v/%v: %v", job.Namespace, job.Name, err)
			return err
		}
	}
	delete(job.Status.ControlledResources, "plugin-"+sp.Name())
	if !sp.disableNetworkPolicy {
		if err := sp.client.KubeClient.NetworkingV1().NetworkPolicies(job.Namespace).Delete(context.TODO(), job.Name, metav1.DeleteOptions{}); err != nil {
			if !apierrors.IsNotFound(err) {
				klog.Errorf("Failed to delete Network policy of Job %v/%v: %v", job.Namespace, job.Name, err)
				return err
			}
		}
	}
	return nil
}

func (sp *svcPlugin) OnJobUpdate(job *batchv1alpha1.Job) error {
	cmData := GenerateHosts(job)
	return helpers.CreateOrUpdateConfigMap(job, sp.client.KubeClient, cmData, sp.cmName(job))
}

func (sp *svcPlugin) mountConfigMap(pod *corev1.Pod, job *batchv1alpha1.Job) {
	// create cm volume
	cmName := sp.cmName(job)
	cmVolume := corev1.Volume{
		Name: cmName,
	}
	cmVolume.ConfigMap = &corev1.ConfigMapVolumeSource{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: cmName,
		},
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, cmVolume)

	// mount cm volume to containers
	vm := corev1.VolumeMount{
		MountPath: ConfigMapMountPath,
		Name:      cmName,
	}
	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].VolumeMounts = append(pod.Spec.Containers[i].VolumeMounts, vm)
	}
	for i := range pod.Spec.InitContainers {
		pod.Spec.InitContainers[i].VolumeMounts = append(pod.Spec.InitContainers[i].VolumeMounts, vm)
	}
}

func (sp *svcPlugin) createServiceIfNotExist(job *batchv1alpha1.Job) error {
	// If Service does not exist, create one for Job.
	if _, err := sp.client.KubeClient.CoreV1().Services(job.Namespace).Get(context.TODO(), job.Name, metav1.GetOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			klog.V(3).Infof("Failed to get Service for Job <%s/%s>: %v",
				job.Namespace, job.Name, err)
			return err
		}

		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: job.Namespace,
				Name:      job.Name,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(job, helpers.JobKind),
				},
			},
			Spec: corev1.ServiceSpec{
				ClusterIP: "None",
				Selector: map[string]string{
					batchv1alpha1.JobNameKey:      job.Name,
					batchv1alpha1.JobNamespaceKey: job.Namespace,
				},
				PublishNotReadyAddresses: sp.publishNotReadyAddrs,
				Ports: []corev1.ServicePort{
					{
						Name:       "placeholder-volcano",
						Port:       1,
						Protocol:   corev1.ProtocolTCP,
						TargetPort: intstr.FromInt(1),
					},
				},
			},
		}

		if _, e := sp.client.KubeClient.CoreV1().Services(job.Namespace).Create(context.TODO(), svc, metav1.CreateOptions{}); e != nil {
			klog.V(3).Infof("Failed to create Service for Job <%s/%s>: %v", job.Namespace, job.Name, e)
			return e
		}
		job.Status.ControlledResources["plugin-"+sp.Name()] = sp.Name()
	}

	return nil
}

// createNetworkPolicyIfNotExist limits that pods can only be accessible by pods belong to the job.
func (sp *svcPlugin) createNetworkPolicyIfNotExist(job *batchv1alpha1.Job) error {
	// If network policy does not exist, create one for Job.
	if _, err := sp.client.KubeClient.NetworkingV1().NetworkPolicies(job.Namespace).Get(context.TODO(), job.Name, metav1.GetOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			klog.V(3).Infof("Failed to get NetworkPolicy for Job <%s/%s>: %v",
				job.Namespace, job.Name, err)
			return err
		}

		np := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: job.Namespace,
				Name:      job.Name,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(job, helpers.JobKind),
				},
			},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{
					MatchLabels: map[string]string{
						batchv1alpha1.JobNameKey:      job.Name,
						batchv1alpha1.JobNamespaceKey: job.Namespace,
					},
				},
				Ingress: []networkingv1.NetworkPolicyIngressRule{{
					From: []networkingv1.NetworkPolicyPeer{{
						PodSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								batchv1alpha1.JobNameKey:      job.Name,
								batchv1alpha1.JobNamespaceKey: job.Namespace,
							},
						},
					}},
				}},
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			},
		}

		if _, err := sp.client.KubeClient.NetworkingV1().NetworkPolicies(job.Namespace).Create(context.TODO(), np, metav1.CreateOptions{}); err != nil {
			klog.V(3).Infof("Failed to create NetworkPolicy for Job <%s/%s>: %v", job.Namespace, job.Name, err)
			return err
		}
		job.Status.ControlledResources["plugin-"+sp.Name()] = sp.Name()
	}

	return nil
}

func (sp *svcPlugin) cmName(job *batchv1alpha1.Job) string {
	return fmt.Sprintf("%s-%s", job.Name, sp.Name())
}

func GenerateHosts(job *batchv1alpha1.Job) map[string]string {
	cmData := make(map[string]string, len(job.Spec.Tasks))

	for _, task := range job.Spec.Tasks {
		hosts := make([]string, 0, task.Replicas)
		for i := 0; i < int(task.Replicas); i++ {
			hostName := task.Template.Spec.Hostname
			subdomain := task.Template.Spec.Subdomain
			if len(hostName) == 0 {
				hostName = jobhelpers.MakePodName(job.Name, task.Name, i)
			}
			if len(subdomain) == 0 {
				subdomain = job.Name
			}
			hosts = append(hosts, hostName+"."+subdomain)
			if len(task.Template.Spec.Hostname) != 0 {
				break
			}
		}

		formatEnvKey := strings.Replace(task.Name, "-", "_", -1)
		key := fmt.Sprintf(ConfigMapTaskHostFmt, formatEnvKey)
		cmData[key] = strings.Join(hosts, "\n")

		// TODO: The splitter and the prefix of env should be configurable.
		// export hosts as environment
		key = fmt.Sprintf(EnvTaskHostFmt, strings.ToUpper(formatEnvKey))
		cmData[key] = strings.Join(hosts, ",")
		// export host number as environment
		key = fmt.Sprintf(EnvHostNumFmt, strings.ToUpper(formatEnvKey))
		cmData[key] = strconv.Itoa(len(hosts))
	}
	return cmData
}
