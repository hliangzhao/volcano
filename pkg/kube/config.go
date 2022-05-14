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

package kube

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

/* This module defines volcano config. */

type ClientOptions struct {
	Master     string
	KubeConfig string
	QPS        float32
	Burst      int
}

// BuildConfig builds a config from ClientOptions.
func BuildConfig(opt ClientOptions) (*rest.Config, error) {
	cfg, err := clientcmd.BuildConfigFromFlags(opt.Master, opt.KubeConfig)
	if err != nil {
		return nil, err
	}
	cfg.QPS = opt.QPS
	cfg.Burst = opt.Burst
	return cfg, nil
}
