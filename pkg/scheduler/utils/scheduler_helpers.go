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

package utils

import "github.com/hliangzhao/volcano/pkg/scheduler/apis"

// GetNodeList returns values of the map 'nodes'
func GetNodeList(nodes map[string]*apis.NodeInfo, nodeList []string) []*apis.NodeInfo {
	result := make([]*apis.NodeInfo, 0, len(nodeList))
	for _, name := range nodeList {
		if ni, ok := nodes[name]; ok {
			result = append(result, ni)
		}
	}
	return result
}
