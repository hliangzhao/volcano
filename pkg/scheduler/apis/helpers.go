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

package apis

import (
	"fmt"
)

// TODO: fully checked

// MergeErrors is used to merge multiple errors into a single error.
func MergeErrors(errs ...error) error {
	msg := "errors: "

	foundErr := false
	i := 1

	for _, e := range errs {
		if e != nil {
			if foundErr {
				msg = fmt.Sprintf("%s, %d: ", msg, i)
			} else {
				msg = fmt.Sprintf("%s %d: ", msg, i)
			}
			msg = fmt.Sprintf("%s%v", msg, e)
			foundErr = true
			i++
		}
	}

	// the returned string is "errors: 1: xxx, 2: xxx, 3: xxx"
	if foundErr {
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// JobTerminated checks whether job is terminated.
func JobTerminated(job *JobInfo) bool {
	return job.PodGroup == nil && len(job.Tasks) == 0
}
