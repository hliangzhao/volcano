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

package assert

import (
	"fmt"
	"k8s.io/klog/v2"
	"os"
	"runtime/debug"
)

const (
	// EnvPanicOnError is the env name to determine panic on assertion failed or not
	EnvPanicOnError = "PANIC_ON_ERROR"
)

var (
	panicOnError = true
)

func init() {
	env := os.Getenv(EnvPanicOnError)
	if env == "false" {
		panicOnError = false
	}
}

// Assert checks condition, if condition is false, print message by log or panic.
func Assert(condition bool, msg string) {
	if condition {
		return
	}
	if panicOnError {
		panic(msg)
	}
	klog.Errorf("%s, %s", msg, debug.Stack())
}

// Assertf check condition, if condition is false, print message (args with defined format) using Assert.
func Assertf(condition bool, format string, args ...interface{}) {
	if condition {
		return
	}
	Assert(condition, fmt.Sprintf(format, args...))
}
