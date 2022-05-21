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

package framework

import (
	"fmt"
	"k8s.io/klog/v2"
	"path/filepath"
	"plugin"
	"strings"
	"sync"
)

/* This file provides functions to register plugins and actions. */

/* Plugins related */

var pluginMutex sync.Mutex

type PluginBuilder = func(Arguments) Plugin

var pluginBuilders = map[string]PluginBuilder{}

func RegisterPluginBuilder(name string, pb PluginBuilder) {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	pluginBuilders[name] = pb
}

func CleanupPluginBuilders() {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	pluginBuilders = map[string]PluginBuilder{}
}

func GetPluginBuilder(name string) (PluginBuilder, bool) {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	pb, found := pluginBuilders[name]
	return pb, found
}

func getPluginName(pluginPath string) string {
	return strings.TrimSuffix(filepath.Base(pluginPath), filepath.Ext(pluginPath))
}

func loadPluginBuilder(pluginPath string) (PluginBuilder, error) {
	plug, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, err
	}
	symBuilder, err := plug.Lookup("New")
	if err != nil {
		return nil, err
	}

	builder, ok := symBuilder.(PluginBuilder)
	if !ok {
		return nil, fmt.Errorf("unexpected plugin: %s, failed to convert PluginBuilder `New`", pluginPath)
	}
	return builder, nil
}

// LoadCustomPlugins loads custom scheduling plugins from the given pluginsDir.
func LoadCustomPlugins(pluginsDir string) error {
	pluginPaths, _ := filepath.Glob(fmt.Sprintf("%s/*.so", pluginsDir))
	for _, path := range pluginPaths {
		pluginBuilder, err := loadPluginBuilder(path)
		if err != nil {
			return err
		}
		pluginName := getPluginName(path)
		RegisterPluginBuilder(pluginName, pluginBuilder)
		klog.V(4).Infof("Custom plugin %s loaded", pluginName)
	}
	return nil
}

/* Actions related */

var actionMap = map[string]Action{}

func RegisterAction(act Action) {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	actionMap[act.Name()] = act
}

func GetAction(name string) (Action, bool) {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	act, found := actionMap[name]
	return act, found
}
