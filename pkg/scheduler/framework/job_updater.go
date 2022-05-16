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

package framework

import (
	"context"
	"github.com/hliangzhao/volcano/pkg/apis/scheduling"
	"github.com/hliangzhao/volcano/pkg/scheduler/apis"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"math/rand"
	"reflect"
	"time"
)

const (
	jobUpdaterWorker             = 16
	jobConditionUpdateTime       = time.Minute
	jobConditionUpdateTimeJitter = 30 * time.Second
)

type jobUpdater struct {
	sess     *Session
	jobQueue []*apis.JobInfo
}

func newJobUpdater(sess *Session) *jobUpdater {
	queue := make([]*apis.JobInfo, 0, len(sess.Jobs))
	for _, job := range sess.Jobs {
		queue = append(queue, job)
	}

	ju := &jobUpdater{
		sess:     sess,
		jobQueue: queue,
	}
	return ju
}

// UpdateAll updates all jobs.
func (ju *jobUpdater) UpdateAll() {
	workqueue.ParallelizeUntil(context.TODO(), jobUpdaterWorker, len(ju.jobQueue), ju.updateJob)
}

// updateJob updates a specified job.
func (ju *jobUpdater) updateJob(index int) {
	job := ju.jobQueue[index]
	sess := ju.sess

	job.PodGroup.Status = jobStatus(sess, job)
	oldStatus, found := sess.podGroupStatus[job.UID]
	updatePG := !found || isPodGroupStatusUpdated(job.PodGroup.Status, oldStatus)
	if _, err := sess.cache.UpdateJobStatus(job, updatePG); err != nil {
		klog.Errorf("Failed to update job <%s/%s>: %v",
			job.Namespace, job.Name, err)
	}
}

// isPodGroupStatusUpdated checks that if newStatus is different odlStatus. If yes, return true.
func isPodGroupStatusUpdated(newStatus, oldStatus scheduling.PodGroupStatus) bool {
	newCondition := newStatus.Conditions
	newStatus.Conditions = nil
	oldCondition := oldStatus.Conditions
	oldStatus.Conditions = nil

	return !reflect.DeepEqual(newStatus, oldStatus) || isPodGroupConditionsUpdated(newCondition, oldCondition)
}

// isPodGroupConditionsUpdated checks that if newCondition is different oldCondition. If yes, return true.
func isPodGroupConditionsUpdated(newCondition, oldCondition []scheduling.PodGroupCondition) bool {
	if len(newCondition) != len(oldCondition) {
		return true
	}

	for index, newCond := range newCondition {
		oldCond := oldCondition[index]

		newTime := newCond.LastTransitionTime
		oldTime := oldCond.LastTransitionTime
		if TimeJitterAfter(
			newTime.Time,
			oldTime.Time,
			jobConditionUpdateTime,
			jobConditionUpdateTimeJitter,
		) {
			return true
		}

		// if newCond is not new enough, we treat it the same as the old one
		newCond.LastTransitionTime = oldTime

		// comparing should ignore the TransitionID
		newTransitionID := newCond.TransitionID
		newCond.TransitionID = oldCond.TransitionID

		shouldUpdate := !reflect.DeepEqual(&newCond, &oldCond)

		newCond.LastTransitionTime = newTime
		newCond.TransitionID = newTransitionID
		if shouldUpdate {
			return true
		}
	}

	return false
}

// TimeJitterAfter returns a new time: old time + duration + jitter time.
func TimeJitterAfter(new, old time.Time, duration, maxJitter time.Duration) bool {
	var jitter int64
	if maxJitter > 0 {
		jitter = rand.Int63n(int64(maxJitter))
	}
	return new.After(old.Add(duration + time.Duration(jitter)))
}
