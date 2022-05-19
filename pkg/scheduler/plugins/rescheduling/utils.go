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

import "time"

var FnsLastExecTime map[string]time.Time

func init() {
	FnsLastExecTime = map[string]time.Time{}
}

func timeToRun(fnName string, interval time.Duration) bool {
	now := time.Now()
	lastExecTime, ok := FnsLastExecTime[fnName]
	if !ok || lastExecTime.Add(interval).Before(now) {
		FnsLastExecTime[fnName] = now
		return true
	}
	return false
}
