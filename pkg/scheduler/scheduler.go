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

package scheduler

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/hliangzhao/volcano/pkg/filewatcher"
	"github.com/hliangzhao/volcano/pkg/scheduler/cache"
	"github.com/hliangzhao/volcano/pkg/scheduler/conf"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/scheduler/metrics"
	"github.com/hliangzhao/volcano/pkg/scheduler/plugins"
	"io/ioutil"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var defaultSchedulerConf = `
actions: "enqueue, allocate, backfill"
tiers:
- plugins:
  - name: priority
  - name: gang
  - name: conformance
- plugins:
  - name: overcommit
  - name: drf
  - name: predicates
  - name: proportion
  - name: nodeorder
`

// Scheduler watches for new unscheduled pods for volcano. It attempts to find
// nodes that they fit on and writes bindings back to the api server.
type Scheduler struct {
	cache          cache.Cache
	schedulerConf  string                  // the path of scheduler's config
	fileWatcher    filewatcher.FileWatcher // file watcher on scheduler's config
	schedulePeriod time.Duration
	once           sync.Once

	mutex          sync.Mutex
	actions        []framework.Action
	plugins        []conf.Tier
	configurations []conf.Configuration
	metricsConf    map[string]string
}

func NewScheduler(config *rest.Config, schedulerName string, schedulerConf string,
	period time.Duration, defaultQueue string, nodeSelectors []string) (*Scheduler, error) {

	// watch the scheduler config dir
	var watcher filewatcher.FileWatcher
	if schedulerConf != "" {
		var err error
		path := filepath.Dir(schedulerConf)
		watcher, err = filewatcher.NewFileWatcher(path)
		if err != nil {
			return nil, fmt.Errorf("failed creating filewatcher for %s: %v", schedulerConf, err)
		}
	}

	sched := &Scheduler{
		schedulerConf:  schedulerConf,
		fileWatcher:    watcher,
		cache:          cache.New(config, schedulerName, defaultQueue, nodeSelectors),
		schedulePeriod: period,
	}

	return sched, nil
}

func (s *Scheduler) Run(stopCh <-chan struct{}) {
	s.loadSchedulerConf()
	go s.watchSchedulerConf(stopCh)

	// start cache for policy
	s.cache.SetMetricsConf(s.metricsConf)
	go s.cache.Run(stopCh)
	s.cache.WaitForCacheSync(stopCh)

	klog.V(2).Infof("scheduler completes Initialization and start to run")
	go wait.Until(s.runOnce, s.schedulePeriod, stopCh)
}

func (s *Scheduler) runOnce() {
	klog.V(4).Infof("Start volcano scheduling ...")
	scheduleStartTime := time.Now()
	defer klog.V(4).Infof("End volcano scheduling ...")

	// get vc scheduler config for running
	s.mutex.Lock()
	actions := s.actions
	plgs := s.plugins
	configurations := s.configurations
	s.mutex.Unlock()

	sess := framework.OpenSession(s.cache, plgs, configurations)
	defer framework.CloseSession(sess)

	for _, act := range actions {
		actStartTime := time.Now()
		act.Execute(sess)
		metrics.UpdateActionDuration(act.Name(), metrics.Duration(actStartTime))
	}
	metrics.UpdateE2eDuration(metrics.Duration(scheduleStartTime))
}

func readSchedulerConf(confPath string) (string, error) {
	dat, err := ioutil.ReadFile(confPath)
	if err != nil {
		return "", err
	}
	return string(dat), nil
}

// unmarshalSchedulerConf unmarshal the vc scheduler config string to schedulerConf.
func unmarshalSchedulerConf(configStr string) ([]framework.Action, []conf.Tier, []conf.Configuration, map[string]string, error) {
	var actions []framework.Action
	schedulerConf := &conf.SchedulerConfiguration{}

	if err := yaml.Unmarshal([]byte(configStr), schedulerConf); err != nil {
		return nil, nil, nil, nil, err
	}

	// Set default settings for each plugin if not set
	for i, tier := range schedulerConf.Tiers {
		// drf with hierarchy enabled
		hdrf := false
		// proportion enabled
		proportion := false
		for j := range tier.Plugins {
			if tier.Plugins[j].Name == "drf" &&
				tier.Plugins[j].EnabledHierarchy != nil && *tier.Plugins[j].EnabledHierarchy {
				hdrf = true
			}
			if tier.Plugins[j].Name == "proportion" {
				proportion = true
			}
			plugins.ApplyPluginConfDefaults(&schedulerConf.Tiers[i].Plugins[j])
		}
		if hdrf && proportion {
			return nil, nil, nil, nil, fmt.Errorf("proportion and drf with hierarchy enabled conflicts")
		}
	}

	actionNames := strings.Split(schedulerConf.Actions, ",")
	for _, actionName := range actionNames {
		if action, found := framework.GetAction(strings.TrimSpace(actionName)); found {
			actions = append(actions, action)
		} else {
			return nil, nil, nil, nil, fmt.Errorf("failed to find Action %s, ignore it", actionName)
		}
	}

	return actions, schedulerConf.Tiers, schedulerConf.Configurations, schedulerConf.MetricsConfiguration, nil
}

func (s *Scheduler) loadSchedulerConf() {
	var err error

	// set default vc scheduler config only once
	s.once.Do(func() {
		s.actions, s.plugins, s.configurations, s.metricsConf, err = unmarshalSchedulerConf(defaultSchedulerConf)
		if err != nil {
			klog.Errorf("unmarshal scheduler config %s failed: %v", defaultSchedulerConf, err)
			panic("invalid default configuration")
		}
	})

	// update vc scheduler config from file
	var configStr string
	if len(s.schedulerConf) != 0 {
		if configStr, err = readSchedulerConf(s.schedulerConf); err != nil {
			klog.Errorf("Failed to read scheduler configuration '%s', using previous configuration: %v",
				s.schedulerConf, err)
			return
		}
	}

	actions, plgs, configurations, metricsConf, err := unmarshalSchedulerConf(configStr)
	if err != nil {
		klog.Errorf("scheduler config %s is invalid: %v", configStr, err)
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	// If it is valid, use the new configuration
	s.actions = actions
	s.plugins = plgs
	s.configurations = configurations
	s.metricsConf = metricsConf
}

func (s *Scheduler) watchSchedulerConf(stopCh <-chan struct{}) {
	if s.fileWatcher == nil {
		return
	}
	eventCh := s.fileWatcher.Events()
	errCh := s.fileWatcher.Errors()
	for {
		select {
		case event, ok := <-eventCh:
			if !ok {
				return
			}
			klog.V(4).Infof("watch %s event: %v", s.schedulerConf, event)
			// update vc scheduler conf if config dir changes
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				s.loadSchedulerConf()
			}
		case err, ok := <-errCh:
			if !ok {
				return
			}
			klog.Infof("watch %s error: %v", s.schedulerConf, err)
		case <-stopCh:
			return
		}
	}
}
