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

package actions

import (
	"github.com/hliangzhao/volcano/pkg/scheduler/actions/allocate"
	"github.com/hliangzhao/volcano/pkg/scheduler/actions/backfill"
	"github.com/hliangzhao/volcano/pkg/scheduler/actions/elect"
	"github.com/hliangzhao/volcano/pkg/scheduler/actions/enqueue"
	"github.com/hliangzhao/volcano/pkg/scheduler/actions/preempt"
	"github.com/hliangzhao/volcano/pkg/scheduler/actions/reclaim"
	"github.com/hliangzhao/volcano/pkg/scheduler/actions/reserve"
	"github.com/hliangzhao/volcano/pkg/scheduler/actions/shuffle"
	"github.com/hliangzhao/volcano/pkg/scheduler/framework"
)

// init registers all the implemented actions
func init() {
	framework.RegisterAction(allocate.New())
	framework.RegisterAction(backfill.New())
	framework.RegisterAction(elect.New())
	framework.RegisterAction(enqueue.New())
	framework.RegisterAction(preempt.New())
	framework.RegisterAction(reclaim.New())
	framework.RegisterAction(reserve.New())
	framework.RegisterAction(shuffle.New())
}
