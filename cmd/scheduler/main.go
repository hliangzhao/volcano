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

package main

// fully checked and understood

import (
	"fmt"
	"github.com/hliangzhao/volcano/cmd/scheduler/app"
	"github.com/hliangzhao/volcano/cmd/scheduler/app/options"
	"github.com/spf13/pflag"
	_ "go.uber.org/automaxprocs"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"

	// init pprof server
	_ "net/http/pprof"

	"os"
	"runtime"
	"time"

	// Import default actions/plugins.
	_ "github.com/hliangzhao/volcano/pkg/scheduler/actions"
	_ "github.com/hliangzhao/volcano/pkg/scheduler/plugins"

	// init assert
	_ "github.com/hliangzhao/volcano/pkg/scheduler/utils/assert"
)

var logFlushFreq = pflag.Duration("log-flush-frequency", 5*time.Second, "Maximum number of seconds between log flushes")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	klog.InitFlags(nil)

	schedOpt := options.NewServerOption()
	schedOpt.AddFlags(pflag.CommandLine)
	schedOpt.RegisterOptions()

	flag.InitFlags()
	if err := schedOpt.CheckOptionOrDie(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	go wait.Until(klog.Flush, *logFlushFreq, wait.NeverStop)
	defer klog.Flush()

	if err := app.Run(schedOpt); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
