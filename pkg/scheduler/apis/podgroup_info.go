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

package apis

import "github.com/hliangzhao/volcano/pkg/apis/scheduling"

// No more wrapper is required for podgroup.

// PodGroupPhase is the phase of a podgroup at current time.
type PodGroupPhase string

// These are the valid phase of podGroups.
const (
	// PodGroupVersionV1Alpha1 represents PodGroupVersion of v1alpha1
	PodGroupVersionV1Alpha1 string = "v1alpha1"
)

type PodGroup struct {
	scheduling.PodGroup
	Version string
}

func (pg *PodGroup) Clone() *PodGroup {
	return &PodGroup{
		PodGroup: *pg.PodGroup.DeepCopy(),
		Version:  pg.Version,
	}
}
