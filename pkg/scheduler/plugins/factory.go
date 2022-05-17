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

package plugins

import (
	`github.com/hliangzhao/volcano/pkg/scheduler/framework`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/binpack`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/conformance`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/drf`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/extender`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/gang`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/nodeorder`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/numaaware`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/overcommit`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/predicates`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/priority`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/proportion`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/reservation`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/sla`
	tasktopology `github.com/hliangzhao/volcano/pkg/scheduler/plugins/task-topology`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/tdm`
	`k8s.io/apiserver/pkg/admission/plugin/resourcequota`
)

func init() {
	// Plugins for Jobs
	framework.RegisterPluginBuilder(drf.PluginName, drf.New)
	framework.RegisterPluginBuilder(gang.PluginName, gang.New)
	framework.RegisterPluginBuilder(predicates.PluginName, predicates.New)
	framework.RegisterPluginBuilder(priority.PluginName, priority.New)
	framework.RegisterPluginBuilder(nodeorder.PluginName, nodeorder.New)
	framework.RegisterPluginBuilder(conformance.PluginName, conformance.New)
	framework.RegisterPluginBuilder(binpack.PluginName, binpack.New)
	framework.RegisterPluginBuilder(reservation.PluginName, reservation.New)
	framework.RegisterPluginBuilder(tdm.PluginName, tdm.New)
	framework.RegisterPluginBuilder(overcommit.PluginName, overcommit.New)
	framework.RegisterPluginBuilder(sla.PluginName, sla.New)
	framework.RegisterPluginBuilder(tasktopology.PluginName, tasktopology.New)
	framework.RegisterPluginBuilder(numaaware.PluginName, numaaware.New)
	framework.RegisterPluginBuilder(rescheduling.PluginName, rescheduling.New)
	framework.RegisterPluginBuilder(usage.PluginName, usage.New)

	// Plugins for Queues
	framework.RegisterPluginBuilder(proportion.PluginName, proportion.New)

	// Plugins for Extender
	framework.RegisterPluginBuilder(extender.PluginName, extender.New)

	// Plugins for ResourceQuota
	framework.RegisterPluginBuilder(resourcequota.PluginName, resourcequota.New)
}
