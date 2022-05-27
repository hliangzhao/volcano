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

package ssh

// fully checked and understood

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	apishelpers "github.com/hliangzhao/volcano/pkg/apis/helpers"
	"github.com/hliangzhao/volcano/pkg/controllers/job/helpers"
	plugininterface "github.com/hliangzhao/volcano/pkg/controllers/job/plugins/interface"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

/* This plugin will mount the secrets of SSH info to the given job pod. */

// sshPlugin implements the PluginInterface interface.
type sshPlugin struct {
	arguments      []string
	client         plugininterface.PluginClient
	sshKeyFilePath string
	sshPrivateKey  string
	sshPublicKey   string
}

func (sp *sshPlugin) Name() string {
	return "ssh"
}

func (sp *sshPlugin) OnPodCreate(pod *corev1.Pod, job *batchv1alpha1.Job) error {
	sp.mountRSAKey(pod, job)
	return nil
}

func (sp *sshPlugin) OnJobAdd(job *batchv1alpha1.Job) error {
	// already have the secret, nothing to do
	if job.Status.ControlledResources["plugin-"+sp.Name()] == sp.Name() {
		return nil
	}

	// get key pair data (user-provided or generated)
	var data map[string][]byte
	var err error
	if len(sp.sshPrivateKey) > 0 {
		data, err = withUserProvidedRSAKey(job, sp.sshPrivateKey, sp.sshPublicKey)
	} else {
		data, err = generateRSAKey(job)
	}
	if err != nil {
		return err
	}

	// create the secret resource in cluster and set the owner reference
	if err = apishelpers.CreateOrUpdateSecret(job, sp.client.KubeClient, data, sp.secretName(job)); err != nil {
		return fmt.Errorf("create secret for job <%s/%s> with ssh plugin failed for %v",
			job.Namespace, job.Name, err)
	}

	// add to controlled resources
	job.Status.ControlledResources["plugin-"+sp.Name()] = sp.Name()
	return nil
}

// OnJobDelete deletes the secret resource in cluster and remove the controlled segment from job.
func (sp *sshPlugin) OnJobDelete(job *batchv1alpha1.Job) error {
	if job.Status.ControlledResources["plugin-"+sp.Name()] != sp.Name() {
		return nil
	}
	if err := apishelpers.DeleteSecret(job, sp.client.KubeClient, sp.secretName(job)); err != nil {
		return err
	}
	delete(job.Status.ControlledResources, "plugin-"+sp.Name())

	return nil
}

func (sp *sshPlugin) OnJobUpdate(job *batchv1alpha1.Job) error {
	// TODO: currently a container using a Secret as a subPath volume mount will not receive Secret updates.
	//  we may not update the job secret due to the above reason now.
	//  related issue: https://github.com/volcano-sh/volcano/issues/1420
	return nil
}

// mountRSAKey creates the RSA secret volume and mounts it to each container of the given pod.
func (sp *sshPlugin) mountRSAKey(pod *corev1.Pod, job *batchv1alpha1.Job) {
	// create the secret (a volume of course)
	secretName := sp.secretName(job)
	sshVolume := corev1.Volume{Name: secretName}

	// set the k-v of the secret
	var mode int32 = 0600
	sshVolume.Secret = &corev1.SecretVolumeSource{
		SecretName: secretName,
		Items: []corev1.KeyToPath{
			{Key: PrivateKey, Path: RelativePath + "/" + PrivateKey},
			{Key: PublicKey, Path: RelativePath + "/" + PublicKey},
			{Key: AuthorizedKeys, Path: RelativePath + "/" + AuthorizedKeys},
			{Key: Config, Path: RelativePath + "/" + Config},
		},
		DefaultMode: &mode,
	}

	// set no-root mode if the provided path is not absolute path
	if sp.sshKeyFilePath != AbsolutePath {
		var noRootMode int32 = 0600
		sshVolume.Secret.DefaultMode = &noRootMode
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, sshVolume)

	// mount the secret to each container in this pod
	for i, c := range pod.Spec.Containers {
		vm := corev1.VolumeMount{
			MountPath: sp.sshKeyFilePath,
			SubPath:   RelativePath,
			Name:      secretName,
		}
		// TODO: pay attention to the following error!
		// c.VolumeMounts = append(c.VolumeMounts, vm)
		pod.Spec.Containers[i].VolumeMounts = append(c.VolumeMounts, vm)
	}
	// mount the secret to each init container in this pod
	for i, c := range pod.Spec.InitContainers {
		vm := corev1.VolumeMount{
			MountPath: sp.sshKeyFilePath,
			SubPath:   RelativePath,
			Name:      secretName,
		}
		c.VolumeMounts = append(c.VolumeMounts, vm)
		pod.Spec.InitContainers[i].VolumeMounts = append(c.VolumeMounts, vm)
	}
}

// generateRSAKey implements the procedure of private and public key pair generations.
func generateRSAKey(job *batchv1alpha1.Job) (map[string][]byte, error) {
	bitSize := 2048

	// generate the RSA keypair with the input bit size
	rsaKeyPair, err := rsa.GenerateKey(rand.Reader, bitSize)
	if err != nil {
		klog.Errorf("rsa generateKey err: %v", err)
		return nil, err
	}

	// generate private key
	privateBlock := pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(rsaKeyPair),
	}
	privateKeyBytes := pem.EncodeToMemory(&privateBlock)

	// generate public key
	publicKey, err := ssh.NewPublicKey(&rsaKeyPair.PublicKey)
	if err != nil {
		klog.Errorf("ssh newPublicKey err: %v", err)
		return nil, err
	}
	publicKeyBytes := ssh.MarshalAuthorizedKey(publicKey)

	// save the key pair
	data := make(map[string][]byte)
	data[PrivateKey] = privateKeyBytes
	data[PublicKey] = publicKeyBytes
	data[AuthorizedKeys] = publicKeyBytes
	data[Config] = []byte(generateSSHConfig(job))

	return data, nil
}

// secretName generates the name of the ssh plugin secret.
func (sp *sshPlugin) secretName(job *batchv1alpha1.Job) string {
	return fmt.Sprintf("%s-%s", job.Name, sp.Name())
}

// withUserProvidedRSAKey sets data by the user provided info directly.
// No generation is required.
func withUserProvidedRSAKey(job *batchv1alpha1.Job, sshPrivateKey, sshPublicKey string) (map[string][]byte, error) {
	data := make(map[string][]byte)
	data[PrivateKey] = []byte(sshPrivateKey)
	data[PublicKey] = []byte(sshPublicKey)
	data[AuthorizedKeys] = []byte(sshPublicKey)
	data[Config] = []byte(generateSSHConfig(job))
	return data, nil
}

func (sp *sshPlugin) addFlags() {
	flagset := flag.NewFlagSet(sp.Name(), flag.ContinueOnError)
	flagset.StringVar(&sp.sshKeyFilePath, "ssh-key-file-path", sp.sshKeyFilePath, "The path used to store "+
		"ssh private and public keys, it is `/root/.ssh` by default.")
	flagset.StringVar(&sp.sshPrivateKey, "ssh-private-key", sp.sshPrivateKey, "The input string of the private key")
	flagset.StringVar(&sp.sshPublicKey, "ssh-public-key", sp.sshPublicKey, "The input string of the public key")

	if err := flagset.Parse(sp.arguments); err != nil {
		klog.Errorf("plugin %s flagset parse failed, err: %v", sp.Name(), err)
	}
}

// generateSSHConfig generates the config string for the given job.
// The generated config is used for establishing ssh connection.
func generateSSHConfig(job *batchv1alpha1.Job) string {
	/*
		e.g.,
		StrictHostKeyChecking no
		UserKnownHostsFile /dev/null
		Host ubuntu-23EDer8DQ21
		  HostName ubuntu-23EDsqR8DQ21.mnist-train-123HSDee6Q
	*/
	config := "StrictHostKeyChecking no\nUserKnownHostsFile /dev/null\n"

	for _, task := range job.Spec.Tasks {
		hostName := task.Template.Spec.Hostname
		subdomain := task.Template.Spec.Subdomain
		for i := 0; i < int(task.Replicas); i++ {
			// NOTE hostName is the name of the container as a VM machine
			if len(hostName) == 0 {
				hostName = helpers.MakePodName(job.Name, task.Name, i)
			}
			if len(subdomain) == 0 {
				subdomain = job.Name
			}

			config += "Host " + hostName + "\n"
			config += "  HostName " + hostName + "." + subdomain + "\n"
			if len(task.Template.Spec.Hostname) != 0 {
				break
			}
		}
	}

	return config
}

func New(client plugininterface.PluginClient, arguments []string) plugininterface.PluginInterface {
	sp := sshPlugin{
		arguments:      arguments,
		client:         client,
		sshKeyFilePath: AbsolutePath,
	}
	sp.addFlags()
	return &sp
}
