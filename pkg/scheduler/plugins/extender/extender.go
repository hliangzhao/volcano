/*
Copyright 2021-2022 The Volcano Authors.

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

package extender

// fully checked and understood

// TODO: this plugin delegates the executing of predicate, taskOrder, nodeOrder, etc. to an out-side server.
//  My RL-based agent could be implemented as an server for handling these requests.
//  Check whether official server-side code exist!

import (
	`bytes`
	"errors"
	`fmt`
	`github.com/hliangzhao/volcano/pkg/scheduler/apis`
	`github.com/hliangzhao/volcano/pkg/scheduler/framework`
	`github.com/hliangzhao/volcano/pkg/scheduler/plugins/utils`
	"gopkg.in/square/go-jose.v2/json"
	`k8s.io/klog/v2`
	`net/http`
	`strings`
	`time`
)

const (
	PluginName = "extender"

	URLPrefix          = "extender.urlPrefix"
	HTTPTimeout        = "extender.httpTimeout"
	OnSessionOpenVerb  = "extender.onSessionOpenVerb"
	OnSessionCloseVerb = "extender.onSessionCloseVerb"
	PredicateVerb      = "extender.predicateVerb"
	PrioritizeVerb     = "extender.prioritizeVerb"
	PreemptableVerb    = "extender.preemptableVerb"
	ReclaimableVerb    = "extender.reclaimableVerb"
	QueueOverusedVerb  = "extender.queueOverusedVerb"
	JobEnqueuableVerb  = "extender.jobEnqueuableVerb"
	Ignorable          = "extender.ignorable"
)

type extenderConfig struct {
	urlPrefix          string
	httpTimeout        time.Duration
	onSessionOpenVerb  string
	onSessionCloseVerb string
	predicateVerb      string
	prioritizeVerb     string
	preemptableVerb    string
	reclaimableVerb    string
	queueOverusedVerb  string
	jobEnqueuableVerb  string
	ignorable          bool
}

type extenderPlugin struct {
	client http.Client
	config *extenderConfig
}

// parseExtenderConfig parses the config of extenderPlugin.
// The arguments of the plugin looks like:
// actions: "reclaim, allocate, backfill, preempt"
// tiers:
// - plugins:
//  - name: priority
//  - name: gang
//  - name: conformance
// - plugins:
//  - name: drf
//  - name: predicates
//  - name: extender
//    arguments:
// 	   extender.urlPrefix: http://127.0.0.1
// 	   extender.httpTimeout: 100ms
// 	   extender.onSessionOpenVerb: onSessionOpen
// 	   extender.onSessionCloseVerb: onSessionClose
// 	   extender.predicateVerb: predicate
// 	   extender.prioritizeVerb: prioritize
// 	   extender.preemptableVerb: preemptable
// 	   extender.reclaimableVerb: reclaimable
// 	   extender.queueOverusedVerb: queueOverused
// 	   extender.jobEnqueuableVerb: jobEnqueueable
// 	   extender.ignorable: true
//  - name: proportion
//  - name: nodeorder
func parseExtenderConfig(args framework.Arguments) *extenderConfig {
	ec := &extenderConfig{}
	ec.urlPrefix, _ = args[URLPrefix].(string)
	ec.onSessionOpenVerb, _ = args[OnSessionOpenVerb].(string)
	ec.onSessionCloseVerb, _ = args[OnSessionCloseVerb].(string)
	ec.predicateVerb, _ = args[PredicateVerb].(string)
	ec.prioritizeVerb, _ = args[PrioritizeVerb].(string)
	ec.preemptableVerb, _ = args[PreemptableVerb].(string)
	ec.reclaimableVerb, _ = args[ReclaimableVerb].(string)
	ec.queueOverusedVerb, _ = args[QueueOverusedVerb].(string)
	ec.jobEnqueuableVerb, _ = args[JobEnqueuableVerb].(string)
	args.GetBool(&ec.ignorable, Ignorable)

	ec.httpTimeout = time.Second
	if httpTimeout, _ := args[HTTPTimeout].(string); httpTimeout != "" {
		if timeoutDuration, err := time.ParseDuration(httpTimeout); err != nil {
			ec.httpTimeout = timeoutDuration
		}
	}

	return ec
}

func New(args framework.Arguments) framework.Plugin {
	cfg := parseExtenderConfig(args)
	klog.V(4).Infof("Initialize extender plugin with endpoint address %s", cfg.urlPrefix)
	return &extenderPlugin{
		client: http.Client{Timeout: cfg.httpTimeout},
		config: cfg,
	}
}

func (ep *extenderPlugin) Name() string {
	return PluginName
}

func (ep *extenderPlugin) OnSessionOpen(sess *framework.Session) {
	// firstly, send the session open POST to the extender server with the jobs, nodes, queues of current session
	if ep.config.onSessionOpenVerb != "" {
		err := ep.send(ep.config.onSessionOpenVerb, &OnSessionOpenRequest{
			Jobs:           sess.Jobs,
			Nodes:          sess.Nodes,
			Queues:         sess.Queues,
			NamespaceInfo:  sess.NamespaceInfo,
			RevocableNodes: sess.RevocableNodes,
		}, nil)
		if err != nil {
			klog.Warningf("OnSessionOpen failed with error %v", err)
		}
		if err != nil && !ep.config.ignorable {
			return
		}
	}

	if ep.config.predicateVerb != "" {
		// the predicate function will check whether the node is suitable for the task or not
		// the check is delegated to the server outside
		// all the following functions are the same
		sess.AddPredicateFn(ep.Name(), func(task *apis.TaskInfo, node *apis.NodeInfo) error {
			resp := &PredicateResponse{}
			err := ep.send(ep.config.predicateVerb, &PredicateRequest{Task: task, Node: node}, resp)
			if err != nil {
				klog.Warningf("Predicate failed with error %v", err)

				if ep.config.ignorable {
					return nil
				}
				return err
			}

			if resp.ErrorMessage == "" {
				return nil
			}
			return errors.New(resp.ErrorMessage)
		})
	}

	if ep.config.prioritizeVerb != "" {
		sess.AddBatchNodeOrderFn(ep.Name(), func(task *apis.TaskInfo, nodes []*apis.NodeInfo) (map[string]float64, error) {
			resp := &PrioritizeResponse{}
			err := ep.send(ep.config.prioritizeVerb, &PrioritizeRequest{Task: task, Nodes: nodes}, resp)
			if err != nil {
				klog.Warningf("Prioritize failed with error %v", err)

				if ep.config.ignorable {
					return nil, nil
				}
				return nil, err
			}

			if resp.ErrorMessage == "" && resp.NodeScore != nil {
				return resp.NodeScore, nil
			}
			return nil, errors.New(resp.ErrorMessage)
		})
	}

	if ep.config.preemptableVerb != "" {
		sess.AddPreemptableFn(ep.Name(), func(evictor *apis.TaskInfo, evictees []*apis.TaskInfo) ([]*apis.TaskInfo, int) {
			resp := &PreemptableResponse{}
			err := ep.send(ep.config.preemptableVerb, &PreemptableRequest{Evictor: evictor, Evictees: evictees}, resp)
			if err != nil {
				klog.Warningf("Preemptable failed with error %v", err)

				if ep.config.ignorable {
					return nil, utils.Permit
				}
				return nil, utils.Reject
			}

			return resp.Victims, resp.Status
		})
	}

	if ep.config.reclaimableVerb != "" {
		sess.AddReclaimableFn(ep.Name(), func(evictor *apis.TaskInfo, evictees []*apis.TaskInfo) ([]*apis.TaskInfo, int) {
			resp := &ReclaimableResponse{}
			err := ep.send(ep.config.reclaimableVerb, &ReclaimableRequest{Evictor: evictor, Evictees: evictees}, resp)
			if err != nil {
				klog.Warningf("Reclaimable failed with error %v", err)

				if ep.config.ignorable {
					return nil, utils.Permit
				}
				return nil, utils.Reject
			}

			return resp.Victims, resp.Status
		})
	}

	if ep.config.jobEnqueuableVerb != "" {
		sess.AddJobEnqueuableFn(ep.Name(), func(obj interface{}) int {
			job := obj.(*apis.JobInfo)
			resp := &JobEnqueueableResponse{}
			err := ep.send(ep.config.reclaimableVerb, &JobEnqueueableRequest{Job: job}, resp)
			if err != nil {
				klog.Warningf("JobEnqueueable failed with error %v", err)

				if ep.config.ignorable {
					return utils.Permit
				}
				return utils.Reject
			}

			return resp.Status
		})
	}

	if ep.config.queueOverusedVerb != "" {
		sess.AddOverusedFn(ep.Name(), func(obj interface{}) bool {
			queue := obj.(*apis.QueueInfo)
			resp := &QueueOverusedResponse{}
			err := ep.send(ep.config.reclaimableVerb, &QueueOverusedRequest{Queue: queue}, resp)
			if err != nil {
				klog.Warningf("QueueOverused failed with error %v", err)

				return !ep.config.ignorable
			}

			return resp.Overused
		})
	}
}

// OnSessionClose of extenderPlugin will send a http POST (represent close the session) to the url.
func (ep *extenderPlugin) OnSessionClose(sess *framework.Session) {
	if ep.config.onSessionCloseVerb != "" {
		if err := ep.send(ep.config.onSessionCloseVerb, &OnSessionCloseRequest{}, nil); err != nil {
			klog.Warningf("OnSessionClose failed with error %v", err)
		}
	}
}

// send will send a http POST request to the configurated url and get the result of it.
func (ep *extenderPlugin) send(act string, args interface{}, result interface{}) error {
	// send a http POST request to the configurated url
	out, err := json.Marshal(args)
	if err != nil {
		return err
	}
	url := strings.TrimRight(ep.config.urlPrefix, "/") + "/" + act
	req, err := http.NewRequest("POST", url, bytes.NewReader(out))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	// we hope the response status is ok
	response, err := ep.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("failed %v with extender at URL %v, code %v", act, url, response.StatusCode)
	}

	// decode the return result of the http POST
	if result != nil {
		return json.NewDecoder(response.Body).Decode(result)
	}
	return nil
}
