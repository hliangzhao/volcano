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

package app

import (
	"context"
	"fmt"
	"github.com/hliangzhao/volcano/cmd/scheduler/app/options"
	"github.com/hliangzhao/volcano/pkg/apis/helpers"
	"github.com/hliangzhao/volcano/pkg/kube"
	"github.com/hliangzhao/volcano/pkg/scheduler"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
	"github.com/hliangzhao/volcano/pkg/signals"
	"github.com/hliangzhao/volcano/pkg/version"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"os"
	"time"
)

const (
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 5 * time.Second
)

func Run(opt *options.ServerOption) error {
	if opt.PrintVersion {
		version.PrintVersionAndExit()
	}

	config, err := kube.BuildConfig(opt.KubeClientOptions)
	if err != nil {
		return err
	}

	if opt.PluginsDir != "" {
		err := framework.LoadCustomPlugins(opt.PluginsDir)
		if err != nil {
			klog.Errorf("Failed to load custom plugins: %v", err)
			return err
		}
	}

	sched, err := scheduler.NewScheduler(
		config,
		opt.SchedulerName,
		opt.SchedulerConf,
		opt.SchedulePeriod,
		opt.DefaultQueue,
		opt.NodeSelector,
	)
	if err != nil {
		return err
	}

	if opt.EnableHealthz {
		if err := helpers.StartHealthz(opt.HealthzBindAddress, "volcano-scheduler"); err != nil {
			return err
		}
	}

	ctx := signals.SetupSignalContext()
	run := func(ctx context.Context) {
		sched.Run(ctx.Done())
		<-ctx.Done()
	}

	if !opt.EnableLeaderElection {
		run(ctx)
		return fmt.Errorf("finished without leader elect")
	}

	leaderElectionClient, err := kubernetes.NewForConfig(rest.AddUserAgent(config, "leader-election"))
	if err != nil {
		return err
	}

	// Prepare event clients.
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: leaderElectionClient.CoreV1().Events(opt.LockObjectNamespace)})
	eventRecorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: opt.SchedulerName})

	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("unable to get hostname: %v", err)
	}
	// add an uniquer so that two processes on the same host don't accidentally both become active
	id := hostname + "_" + string(uuid.NewUUID())

	rl, err := resourcelock.New(
		resourcelock.ConfigMapsResourceLock,
		opt.LockObjectNamespace,
		opt.SchedulerName,
		leaderElectionClient.CoreV1(),
		leaderElectionClient.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: eventRecorder,
		},
	)
	if err != nil {
		return fmt.Errorf("couldn't create resource lock: %v", err)
	}

	// leader volcano scheduler instance will run till die, non-leader will do nothing
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: leaseDuration,
		RenewDeadline: renewDeadline,
		RetryPeriod:   retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: run,
			OnStoppedLeading: func() {
				klog.Fatalf("leader election lost")
			},
		},
	})
	return fmt.Errorf("lose lease")
}
