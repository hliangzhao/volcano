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

package rescheduling

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"k8s.io/klog/v2"
	"time"
)

const (
	PluginName      = "rescheduling"
	DefaultInterval = 5 * time.Minute
	DefaultStrategy = "lowNodeUtilization"
)

var (
	Session                   *framework.Session
	RegisteredStrategies      []string
	RegisteredStrategyConfigs map[string]interface{}
	VictimFns                 map[string]apis.VictimTasksFn
	Interval                  string
)

func init() {
	RegisteredStrategies = make([]string, 0)
	RegisteredStrategyConfigs = map[string]interface{}{}
	VictimFns = map[string]apis.VictimTasksFn{}
	Interval = "5m"

	VictimFns["lowNodeUtilization"] = victimsFnForLnu
}

type reschedulingPlugin struct {
	args framework.Arguments
}

func New(args framework.Arguments) framework.Plugin {
	return &reschedulingPlugin{
		args: args,
	}
}

func (rp *reschedulingPlugin) Name() string {
	return PluginName
}

func (rp *reschedulingPlugin) OnSessionOpen(sess *framework.Session) {
	klog.V(4).Infof("Enter rescheduling plugin ...")
	defer klog.V(4).Infof("Leaving rescheduling plugin.")

	Session = sess
	configs := NewResheduleConfigs()
	for _, tier := range sess.Tiers {
		for _, plgOpt := range tier.Plugins {
			if plgOpt.Name == PluginName {
				configs.parseArgs(plgOpt.Arguments)
				break
			}
		}
	}

	// Judge whether it is time to execute rescheduling now
	if !timeToRun("reschedulingFns", configs.interval) {
		klog.V(4).Infof("It is not the time to execute rescheduling strategies.")
		return
	}

	// Get all strategies and register the VictimTasksFromCandidatesFns
	victimFns := make([]apis.VictimTasksFn, 0)
	for _, strategy := range configs.strategies {
		victimFns = append(victimFns, VictimFns[strategy.Name])
	}
	sess.AddVictimTasksFns(rp.Name(), victimFns)
}

func (rp *reschedulingPlugin) OnSessionClose(sess *framework.Session) {
	Session = nil
	RegisteredStrategies = RegisteredStrategies[0:0]
	for k := range RegisteredStrategyConfigs {
		delete(RegisteredStrategyConfigs, k)
	}
	VictimFns = nil
}

type Strategy struct {
	Name       string
	Parameters map[string]interface{}
}

type RescheduleConfigs struct {
	interval   time.Duration
	strategies []Strategy
}

func NewResheduleConfigs() *RescheduleConfigs {
	configs := &RescheduleConfigs{
		interval: DefaultInterval,
		strategies: []Strategy{
			{
				Name:       DefaultStrategy,
				Parameters: DefaultLowNodeConf,
			},
		},
	}
	RegisteredStrategies = append(RegisteredStrategies, DefaultStrategy)
	RegisteredStrategyConfigs[DefaultStrategy] = DefaultLowNodeConf
	return configs
}

func (rc *RescheduleConfigs) parseArgs(args framework.Arguments) {
	// parse interval
	var intervalStr string
	var err error
	if intervalArg, ok := args["interval"]; ok {
		intervalStr = intervalArg.(string)
	}
	rc.interval, err = time.ParseDuration(intervalStr)
	if err != nil {
		klog.V(4).Infof("Parse rescheduling interval failed. Reset the interval to 5m by default.")
		rc.interval = DefaultInterval
	} else {
		Interval = intervalStr
	}

	// parse strategies
	strategiesArg, ok := args["strategies"]
	if ok {
		rc.strategies = strategiesArg.([]Strategy)
		RegisteredStrategies = RegisteredStrategies[0:0]
		for k := range RegisteredStrategyConfigs {
			delete(RegisteredStrategyConfigs, k)
		}
		for _, strategy := range rc.strategies {
			RegisteredStrategies = append(RegisteredStrategies, strategy.Name)
			RegisteredStrategyConfigs[strategy.Name] = strategy.Parameters
		}
	}
}
