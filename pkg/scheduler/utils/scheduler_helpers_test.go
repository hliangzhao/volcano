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

package utils

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"reflect"
	"testing"
)

func TestSelectBestNode(t *testing.T) {
	cases := []struct {
		NodeScores map[float64][]*apis.NodeInfo
		// Expected node is one of ExpectedNodes
		ExpectedNodes []*apis.NodeInfo
	}{
		{
			NodeScores: map[float64][]*apis.NodeInfo{
				1.0: {&apis.NodeInfo{Name: "node1"}, &apis.NodeInfo{Name: "node2"}},
				2.0: {&apis.NodeInfo{Name: "node3"}, &apis.NodeInfo{Name: "node4"}},
			},
			ExpectedNodes: []*apis.NodeInfo{{Name: "node3"}, {Name: "node4"}},
		},
		{
			NodeScores: map[float64][]*apis.NodeInfo{
				1.0: {&apis.NodeInfo{Name: "node1"}, &apis.NodeInfo{Name: "node2"}},
				3.0: {&apis.NodeInfo{Name: "node3"}},
				2.0: {&apis.NodeInfo{Name: "node4"}, &apis.NodeInfo{Name: "node5"}},
			},
			ExpectedNodes: []*apis.NodeInfo{{Name: "node3"}},
		},
		{
			NodeScores:    map[float64][]*apis.NodeInfo{},
			ExpectedNodes: []*apis.NodeInfo{nil},
		},
	}

	oneOf := func(node *apis.NodeInfo, nodes []*apis.NodeInfo) bool {
		for _, v := range nodes {
			if reflect.DeepEqual(node, v) {
				return true
			}
		}
		return false
	}
	for i, test := range cases {
		result := SelectBestNode(test.NodeScores)
		if !oneOf(result, test.ExpectedNodes) {
			t.Errorf("Failed test case #%d, expected: %#v, got %#v", i, test.ExpectedNodes, result)
		}
	}
}

func TestGetMinInt(t *testing.T) {
	cases := []struct {
		vals   []int
		result int
	}{
		{
			vals:   []int{1, 2, 3},
			result: 1,
		},
		{
			vals:   []int{10, 9, 8},
			result: 8,
		},
		{
			vals:   []int{10, 0, 8},
			result: 0,
		},
		{
			vals:   []int{},
			result: 0,
		},
		{
			vals:   []int{0, -1, 1},
			result: -1,
		},
	}
	for i, test := range cases {
		result := GetMinInt(test.vals...)
		if result != test.result {
			t.Errorf("Failed test case #%d, expected: %#v, got %#v", i, test.result, result)
		}
	}
}
