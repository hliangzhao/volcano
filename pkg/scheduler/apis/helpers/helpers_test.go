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

package helpers

// TODO: just copied. Not checked.
// Passed.

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	corev1 "k8s.io/api/core/v1"
	"reflect"
	"testing"
)

func TestMax(t *testing.T) {
	l := &apis.Resource{
		MilliCPU: 1,
		Memory:   1024,
		ScalarResources: map[corev1.ResourceName]float64{
			"gpu":    1,
			"common": 4,
		},
	}
	r := &apis.Resource{
		MilliCPU: 2,
		Memory:   1024,
		ScalarResources: map[corev1.ResourceName]float64{
			"npu":    2,
			"common": 5,
		},
	}
	expected := &apis.Resource{
		MilliCPU: 2,
		Memory:   1024,
		ScalarResources: map[corev1.ResourceName]float64{
			"gpu":    1,
			"npu":    2,
			"common": 5,
		},
	}
	re := Max(l, r)
	if !reflect.DeepEqual(expected, re) {
		t.Errorf("expected: %#v, got: %#v", expected, re)
	}
}
