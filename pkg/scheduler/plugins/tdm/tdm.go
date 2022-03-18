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

package tdm

import (
	`fmt`
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	`github.com/hliangzhao/volcano/pkg/scheduler/framework`
	`github.com/hliangzhao/volcano/pkg/scheduler/utils`
	`k8s.io/apimachinery/pkg/util/intstr`
	`k8s.io/klog/v2`
	`strings`
	`time`
)

const (
	PluginName = "tdm"

	// revocableZoneLayout revocable zone layout
	revocableZoneLayout      = "15:04"
	revocableZoneLabelPrefix = "tdm.revocable-zone."
	evictPeriodLabel         = "tdm.evict.period"
	defaultPodEvictNum       = 1
)

var lastEvictAt time.Time

/*
   actions: "enqueue, reclaim, allocate, preempt"
   tiers:
   - plugins:
     - name: tdm
       arguments:
         tdm.revocable-zone.rz1: 10:00-21:00
         tdm.revocable-zone.rz2: 12:00-14:00
         tdm.evict.period: 1m
*/

type tdmPlugin struct {
	revocableZone map[string]string
	evictPeriod   time.Duration
}

func New(arguments framework.Arguments) framework.Plugin {
	revocableZone := make(map[string]string)
	evictPeriod := time.Minute

	for k, v := range arguments {
		if strings.Contains(k, revocableZoneLabelPrefix) {
			revocableZone[strings.Replace(k, revocableZoneLabelPrefix, "", 1)] = v
		}
	}

	if period, ok := arguments[evictPeriodLabel]; ok {
		if d, err := time.ParseDuration(period); err == nil {
			evictPeriod = d
		}
	}

	return &tdmPlugin{
		revocableZone: revocableZone,
		evictPeriod:   evictPeriod,
	}
}

func (tp *tdmPlugin) Name() string {
	return PluginName
}

func (tp *tdmPlugin) OnSessionOpen(sess *framework.Session) {
	// TODO
}

func (tp *tdmPlugin) OnSessionClose(sess *framework.Session) {}

func parseRevocableZone(rz string) (start, end time.Time, err error) {
	rzValues := strings.Split(strings.TrimSpace(rz), "-")
	if len(rzValues) != 2 {
		err = fmt.Errorf("revocable zone %v format error", rzRaw)
		return
	}

	t1, err := time.Parse(revocableZoneLayout, rzValues[0])
	if err != nil {
		return
	}
	t2, err := time.Parse(revocableZoneLayout, rzValues[1])
	if err != nil {
		return
	}

	now := time.Now()
	start = time.Date(now.Year(), now.Month(), now.Day(), t1.Hour(), t1.Minute(), 0, 0, now.Location())
	if t1.After(t2) || t1.Equal(t2) {
		start = time.Date(now.Year(), now.Month(), now.Day()+1, t2.Hour(), t2.Minute(), 0, 0, now.Location())
	} else {
		start = time.Date(now.Year(), now.Month(), now.Day(), t2.Hour(), t2.Minute(), 0, 0, now.Location())
	}

	return
}

func (tp *tdmPlugin) availableRevocableZone(rz string) error {
	rzRaw, ok := tp.revocableZone[rz]
	if !ok {
		return fmt.Errorf("revocable zone %v not support", rz)
	}

	start, end, err := parseRevocableZone(rzRaw)
	if err != nil {
		return err
	}

	now := time.Now()
	if now.Unix() < start.Unix() || now.Unix() > end.Unix() {
		return fmt.Errorf("current time beyond revocable zone %v:%v", rz, rzRaw)
	}

	return nil
}

func (tp *tdmPlugin) maxVictims(job *apis.JobInfo, victims []*apis.TaskInfo) []*apis.TaskInfo {
	maxPodEvictNum := tp.getMaxPodVictimNum(job)
	targetNum := utils.GetMinInt(maxPodEvictNum, len(victims))

	klog.V(3).Infof("Job <%s/%s> max evict: %v, potential victims number:%v, max victims number:%v",
		job.Namespace, job.Name, maxPodEvictNum, len(victims), targetNum)
	return victims[:targetNum]
}

func (tp *tdmPlugin) getMaxPodVictimNum(job *apis.JobInfo) int {
	jobRunningTaskNum := len(job.TaskStatusIndex[apis.Running])

	if job.Budget.MaxUnavailable != "" {
		maxUnavailable := tp.parseIntStr(job.Budget.MaxUnavailable, len(job.Tasks))
		finalTaskNum := len(job.TaskStatusIndex[apis.Succeeded]) + len(job.TaskStatusIndex[apis.Failed])
		realUnavailable := len(job.Tasks) - finalTaskNum - jobRunningTaskNum
		if realUnavailable >= maxUnavailable {
			return 0
		}
		return maxUnavailable - realUnavailable
	}

	if job.Budget.MinAvailable != "" {
		minAvailable := tp.parseIntStr(job.Budget.MinAvailable, len(job.Tasks))
		if jobRunningTaskNum >= -minAvailable {
			return jobRunningTaskNum - minAvailable
		}
	}

	return defaultPodEvictNum
}

func (tp *tdmPlugin) parseIntStr(input string, taskNum int) int {
	resVal := 0
	tmp := intstr.Parse(input)
	switch tmp.Type {
	case intstr.Int:
		resVal = tmp.IntValue()
	case intstr.String:
		if v, err := intstr.GetScaledValueFromIntOrPercent(&tmp, taskNum, true); err == nil {
			resVal = v
		} else {
			klog.Warningf("TDM get percent value err: %v", err)
		}
	}
	return resVal
}

// revocableNodePreemptableTask returns the running tasks that could be preempted.
func (tp *tdmPlugin) revocableNodePreemptableTask(rz string, sess *framework.Session) map[apis.JobID][]*apis.TaskInfo {
	tasksMap := make(map[apis.JobID][]*apis.TaskInfo)
	for _, node := range sess.RevocableNodes {
		if node.RevocableZone != rz {
			continue
		}
		for _, task := range node.Tasks {
			if task.Preemptable {
				if task.Status == apis.Running {
					tasksMap[task.Job] = append(tasksMap[task.Job], task)
				}
			}
		}
	}
	return tasksMap
}
