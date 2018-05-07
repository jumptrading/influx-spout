// Copyright 2017 Jump Trading
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package filter configures and sets up a filter node
package filter

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/probes"
	"github.com/jumptrading/influx-spout/stats"
	"github.com/nats-io/go-nats"
)

// Name for supported stats
const (
	statPassed            = "passed"
	statProcessed         = "processed"
	statRejected          = "rejected"
	statInvalidTime       = "invalid_time"
	statFailedNATSPublish = "failed_nats_publish"
	statNATSDropped       = "nats_dropped"
)

// StartFilter creates a Filter instance, sets up its rules based on
// the configuration give and sets up a subscription for the incoming
// NATS topic.
func StartFilter(conf *config.Config) (_ *Filter, err error) {
	f := &Filter{
		c:      conf,
		stop:   make(chan struct{}),
		wg:     new(sync.WaitGroup),
		probes: probes.Listen(conf.ProbePort),
	}
	defer func() {
		if err != nil {
			f.Stop()
		}
	}()

	rules, err := RuleSetFromConfig(conf)
	if err != nil {
		return nil, err
	}

	stats := initStats(rules)

	f.nc, err = f.natsConnect()
	if err != nil {
		return nil, err
	}

	jobs := make(chan []byte, 1024)
	for i := 0; i < f.c.Workers; i++ {
		w, err := newWorker(
			f.c.MaxTimeDeltaSecs,
			rules,
			stats,
			f.c.Debug,
			f.natsConnect,
			f.c.NATSSubjectJunkyard,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to start worker: %v", err)
		}
		f.wg.Add(1)
		go w.run(jobs, f.stop, f.wg)
	}

	// A fixed queue name is used to avoid a potential source of
	// misconfiguration. Queue groups are tied to the subject being
	// subscribed to and it's unlikely we'll want different queue
	// groups for a single NATS subject.
	f.sub, err = f.nc.QueueSubscribe(f.c.NATSSubject[0], "filter", func(msg *nats.Msg) {
		if conf.Debug {
			log.Printf("filter received %d bytes", len(msg.Data))
		}
		jobs <- msg.Data
	})
	if err != nil {
		return nil, fmt.Errorf("NATS: failed to subscribe: %v", err)
	}
	if err := f.sub.SetPendingLimits(-1, conf.NATSPendingMaxMB*1024*1024); err != nil {
		return nil, fmt.Errorf("NATS: failed to set pending limits: %v", err)
	}

	f.wg.Add(1)
	go f.startStatistician(stats, rules)

	log.Printf("filter subscribed to [%s] at %s with %d rules\n",
		f.c.NATSSubject[0], f.c.NATSAddress, rules.Count())

	f.probes.SetReady(true)
	return f, nil
}

func (f *Filter) natsConnect() (natsConn, error) {
	nc, err := nats.Connect(f.c.NATSAddress, nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("NATS: failed to connect: %v", err)
	}
	return nc, nil
}

func initStats(rules *RuleSet) *stats.Stats {
	// Initialise
	statNames := []string{
		statPassed,
		statProcessed,
		statRejected,
		statInvalidTime,
		statFailedNATSPublish,
		statNATSDropped,
	}
	for i := 0; i < rules.Count(); i++ {
		statNames = append(statNames, ruleToStatsName(i))
	}
	return stats.New(statNames...)
}

// natsConn allows a mock nats.Conn to be substituted in during tests.
type natsConn interface {
	Publish(string, []byte) error
	QueueSubscribe(string, string, nats.MsgHandler) (*nats.Subscription, error)
	Close()
}

// Filter is a struct that contains the configuration we are running with
// and the NATS bus connection
type Filter struct {
	c      *config.Config
	nc     natsConn
	sub    *nats.Subscription
	wg     *sync.WaitGroup
	probes probes.Probes
	stop   chan struct{}
}

// Stop shuts down goroutines and closes resources related to the filter.
func (f *Filter) Stop() {
	f.probes.SetReady(false)
	f.probes.SetAlive(false)

	// Stop receiving lines to filter.
	f.sub.Unsubscribe()

	// Shut down goroutines.
	close(f.stop)
	f.wg.Wait()

	// Close the connection to NATS.
	if f.nc != nil {
		f.nc.Close()
	}

	f.probes.Close()
}

// startStatistician defines a goroutine that is responsible for
// regularly sending the filter's statistics to the monitoring
// backend.
func (f *Filter) startStatistician(st *stats.Stats, rules *RuleSet) {
	defer f.wg.Done()

	generalLabels := map[string]string{
		"component": "filter",
		"name":      f.c.Name,
	}

	for {
		f.updateNATSDropped(st)

		now := time.Now()
		snap, ruleCounts := splitSnapshot(st.Snapshot())

		// publish the general stats
		lines := stats.SnapshotToPrometheus(snap, now, generalLabels)
		f.nc.Publish(f.c.NATSSubjectMonitor, lines)

		// publish the per rule stats
		for i, subject := range rules.Subjects() {
			f.nc.Publish(f.c.NATSSubjectMonitor, stats.CounterToPrometheus(
				"triggered",
				ruleCounts[i],
				now,
				map[string]string{
					"component": "filter",
					"name":      f.c.Name,
					"rule":      subject,
				},
			))
		}

		select {
		case <-time.After(3 * time.Second):
		case <-f.stop:
			return
		}
	}
}

func (f *Filter) updateNATSDropped(st *stats.Stats) {
	dropped, err := f.sub.Dropped()
	if err != nil {
		log.Printf("NATS: failed to read subscription drops: %v", err)
		return
	}
	st.Max(statNATSDropped, dropped)
}

const rulePrefix = "rule-"

// ruleToStatsName converts a rule index to a name to a key for use
// with a stats.Stats instance.
func ruleToStatsName(i int) string {
	return fmt.Sprintf("%s%06d", rulePrefix, i)
}

// splitSnapshot takes a Snapshot and splits out the rule counters
// from the others. The rule counters are returned in an ordered slice
// while the other counters are returned as a new (smaller) Snapshot.
func splitSnapshot(snap stats.Snapshot) (stats.Snapshot, []int) {
	var genSnap stats.Snapshot
	var ruleSnap stats.Snapshot

	// Split up rule counters from the others.
	for _, counter := range snap {
		if strings.HasPrefix(counter.Name, rulePrefix) {
			ruleSnap = append(ruleSnap, counter)
		} else {
			genSnap = append(genSnap, counter)
		}
	}

	// Sort the rule counters by name and extract just the counts.
	sort.Slice(ruleSnap, func(i, j int) bool {
		return ruleSnap[i].Name < ruleSnap[j].Name
	})
	ruleCounts := make([]int, len(ruleSnap))
	for i, counter := range ruleSnap {
		ruleCounts[i] = counter.Value
	}

	return genSnap, ruleCounts
}
