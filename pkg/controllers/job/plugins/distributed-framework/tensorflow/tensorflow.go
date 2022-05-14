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

package tensorflow

import (
	"flag"
	"fmt"
	batchv1alpha1 "github.com/hliangzhao/volcano/pkg/apis/batch/v1alpha1"
	"github.com/hliangzhao/volcano/pkg/controllers/job/helpers"
	plugininterface "github.com/hliangzhao/volcano/pkg/controllers/job/plugins/interface"
	"gopkg.in/square/go-jose.v2/json"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"strconv"
)

const (
	DefaultPort = 2222
	TFConfig    = "TF_CONFIG"
)

type tensorflowPlugin struct {
	arguments     []string
	client        plugininterface.PluginClient
	psName        string
	workerName    string
	chiefName     string
	evaluatorName string
	port          int
}

func (tp *tensorflowPlugin) Name() string {
	return "tensorflow"
}

func (tp *tensorflowPlugin) OnPodCreate(pod *corev1.Pod, job *batchv1alpha1.Job) error {
	// No need to generate TF_CONFIG for stand-alone tensorflow job
	if len(job.Spec.Tasks) == 1 && job.Spec.Tasks[0].Replicas == 1 {
		return nil
	}

	// Generate TF_CONFIG value
	spec, err := tp.generateTFClusterSpec(pod, job)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	// Add TF_CONFIG to container's environment variables
	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, corev1.EnvVar{
			Name:  TFConfig,
			Value: string(raw),
		})
	}
	return nil
}

func (tp *tensorflowPlugin) OnJobAdd(job *batchv1alpha1.Job) error {
	if job.Status.ControlledResources["plugin-"+tp.Name()] == tp.Name() {
		return nil
	}
	job.Status.ControlledResources["plugin-"+tp.Name()] = tp.Name()
	return nil
}

func (tp *tensorflowPlugin) OnJobDelete(job *batchv1alpha1.Job) error {
	if job.Status.ControlledResources["plugin-"+tp.Name()] != tp.Name() {
		return nil
	}
	delete(job.Status.ControlledResources, "plugin-"+tp.Name())
	return nil
}

func (tp *tensorflowPlugin) OnJobUpdate(job *batchv1alpha1.Job) error {
	return nil
}

func (tp *tensorflowPlugin) addFlags() {
	flagSet := flag.NewFlagSet(tp.Name(), flag.ContinueOnError)
	flagSet.StringVar(&tp.psName, "ps", "ps", "name of ps role task")
	flagSet.StringVar(&tp.workerName, "worker", "worker", "name of worker role task")
	flagSet.StringVar(&tp.chiefName, "chief", "chief", "name of chief role task")
	flagSet.StringVar(&tp.evaluatorName, "evaluator", "evaluator", "name of evaluator role task")
	flagSet.IntVar(&tp.port, "port", DefaultPort, "service port")
	if err := flagSet.Parse(tp.arguments); err != nil {
		klog.Errorf("plugin %s flagset parse failed, err: %v", tp.Name(), err)
	}
}

func New(client plugininterface.PluginClient, arguments []string) plugininterface.PluginInterface {
	tp := tensorflowPlugin{
		arguments: arguments,
		client:    client,
	}
	tp.addFlags()
	return &tp
}

type tfTaskType string

const (
	tfWorker    tfTaskType = "worker"
	tfChief     tfTaskType = "chief"
	tfPS        tfTaskType = "ps"
	tfEvaluator tfTaskType = "evaluator"
)

type taskInfo struct {
	Type  tfTaskType `json:"type"`
	Index int        `json:"index"`
}

type clusterInfo struct {
	PS        []string `json:"ps,omitempty"`
	Worker    []string `json:"worker,omitempty"`
	Chief     []string `json:"chief,omitempty"`
	Evaluator []string `json:"evaluator,omitempty"`
}

// tfClusterSpec is the spec of a tensorflow cluster.
// It will be injected into container's environment variables, and be used by tensorflow framework.
// e.g.
// {
//    "cluster": {
//      "worker": ["worker-0:2222", "worker-1:2222"],
//      "ps": ["ps-0:2222"]
//    },
//    "task": {
//      "type": "worker",
//      "index": 0
//    }
// }
type tfClusterSpec struct {
	Cluster clusterInfo `json:"cluster"`
	Task    taskInfo    `json:"task"`
}

func (tp *tensorflowPlugin) generateTFClusterSpec(pod *corev1.Pod, job *batchv1alpha1.Job) (tfClusterSpec, error) {
	index, err := strconv.Atoi(helpers.GetPodIndexUnderTask(pod))
	if err != nil {
		return tfClusterSpec{}, nil
	}

	c := tfClusterSpec{
		Task: taskInfo{
			Type:  tp.getTaskType(helpers.GetTaskKey(pod)),
			Index: index,
		},
	}

	for _, task := range job.Spec.Tasks {
		var hosts []string
		for i := 0; i < int(task.Replicas); i++ {
			hosts = append(hosts, fmt.Sprintf("%s:%d", helpers.MakeDomainName(task, job, i), tp.port))
		}
		switch task.Name {
		case tp.psName:
			c.Cluster.PS = hosts
		case tp.workerName:
			c.Cluster.Worker = hosts
		case tp.chiefName:
			c.Cluster.Chief = hosts
		case tp.evaluatorName:
			c.Cluster.Evaluator = hosts
		}
	}

	return c, nil
}

func (tp *tensorflowPlugin) getTaskType(taskKey string) tfTaskType {
	switch taskKey {
	case tp.chiefName:
		return tfChief
	case tp.workerName:
		return tfWorker
	case tp.psName:
		return tfPS
	case tp.evaluatorName:
		return tfEvaluator
	}
	return tfTaskType(taskKey)
}
