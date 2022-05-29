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

// fully checked and understood

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/conf"
	"k8s.io/klog/v2"
)

type Arguments map[string]interface{}

// GetArgOfActionFromConf returns argument of action reading from configuration of schedule.
func GetArgOfActionFromConf(configs []conf.Configuration, actionName string) Arguments {
	for _, c := range configs {
		if c.Name == actionName {
			return c.Arguments
		}
	}
	return nil
}

/* The following functions parse input pointer and get corresponding param of different types. */

func (a Arguments) GetInt(ptr *int, key string) {
	if ptr == nil {
		return
	}

	argv, ok := a[key]
	if !ok || argv == "" {
		return
	}

	value, ok := argv.(int)
	if !ok {
		klog.Warningf("Could not parse argument: %s for key %s to int", argv, key)
		return
	}

	*ptr = value
}

func (a Arguments) GetFloat64(ptr *float64, key string) {
	if ptr == nil {
		return
	}

	argv, ok := a[key]
	if !ok {
		return
	}

	value, ok := argv.(float64)
	if !ok {
		klog.Warningf("Could not parse argument: %s for key %s to float64", argv, key)
		return
	}

	*ptr = value
}

func (a Arguments) GetBool(ptr *bool, key string) {
	if ptr == nil {
		return
	}

	argv, ok := a[key]
	if !ok {
		return
	}

	value, ok := argv.(bool)
	if !ok {
		klog.Warningf("Could not parse argument: %s for key %s to bool", argv, key)
		return
	}

	*ptr = value
}
