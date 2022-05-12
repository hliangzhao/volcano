/*
Copyright 2021 hliangzhao.

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

package plugins

import (
	"github.com/hliangzhao/volcano/pkg/controllers/job/plugins/distributed-framework/tensorflow"
	"github.com/hliangzhao/volcano/pkg/controllers/job/plugins/env"
	plugininterface "github.com/hliangzhao/volcano/pkg/controllers/job/plugins/interface"
	"github.com/hliangzhao/volcano/pkg/controllers/job/plugins/ssh"
	"github.com/hliangzhao/volcano/pkg/controllers/job/plugins/svc"
	"sync"
)

func init() {
	RegisterPluginBuilder("ssh", ssh.New)
	RegisterPluginBuilder("env", env.New)
	RegisterPluginBuilder("svc", svc.New)
	RegisterPluginBuilder("tensorflow", tensorflow.New)
}

var pluginMutex sync.Mutex

type PluginBuilder func(plugininterface.PluginClient, []string) plugininterface.PluginInterface

var PluginBuilders = map[string]PluginBuilder{}

func RegisterPluginBuilder(name string, pb PluginBuilder) {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()
	PluginBuilders[name] = pb
}

func GetPluginBuilder(name string) (PluginBuilder, bool) {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	pb, found := PluginBuilders[name]
	return pb, found
}