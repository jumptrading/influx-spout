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
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/jumptrading/influx-spout/v2/config"
	"github.com/jumptrading/influx-spout/v2/probes"
	"github.com/jumptrading/influx-spout/v2/stats"
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
// NATS subject.
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

	st := initStats()
	ruleSt := stats.NewAnon(rules.Count())

	f.nc, err = f.natsConnect()
	if err != nil {
		return nil, err
	}

	jobs := make(chan []byte, 1024)
	for i := 0; i < f.c.Workers; i++ {
		w, err := newWorker(
			f.c.MaxTimeDelta.Duration,
			rules,
			st,
			ruleSt,
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
	if err := f.sub.SetPendingLimits(-1, int(conf.NATSMaxPendingSize.Bytes())); err != nil {
		return nil, fmt.Errorf("NATS: failed to set pending limits: %v", err)
	}

	f.wg.Add(1)
	go f.startStatistician(st, ruleSt, rules)

	log.Printf("filter subscribed to [%s] at %s with %d rules\n",
		f.c.NATSSubject[0], f.c.NATSAddress, rules.Count())

	f.probes.SetReady(true)
	return f, nil
}

func (f *Filter) natsConnect() (natsConn, error) {
	nc, err := nats.Connect(f.c.NATSAddress, nats.MaxReconnects(-1), nats.Name(f.c.Name))
	if err != nil {
		return nil, fmt.Errorf("NATS: failed to connect: %v", err)
	}
	return nc, nil
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
func (f *Filter) startStatistician(st *stats.Stats, ruleSt *stats.AnonStats, rules *RuleSet) {
	defer f.wg.Done()

	labels := stats.NewLabels("filter", f.c.Name)
	for {
		f.updateNATSDropped(st)

		now := time.Now()
		snap := st.Snapshot()
		ruleCounts := ruleSt.Snapshot()

		// publish the general stats
		lines := stats.SnapshotToPrometheus(snap, now, labels)
		f.nc.Publish(f.c.NATSSubjectMonitor, lines)

		// publish the per rule stats
		for i, subject := range rules.Subjects() {
			f.nc.Publish(f.c.NATSSubjectMonitor, stats.CounterToPrometheus(
				"triggered",
				int(ruleCounts[i]),
				now,
				labels.With("rule", subject),
			))
		}

		select {
		case <-time.After(f.c.StatsInterval.Duration):
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
	st.Max(statNATSDropped, uint64(dropped))
}

func initStats() *stats.Stats {
	return stats.New(
		statPassed,
		statProcessed,
		statRejected,
		statInvalidTime,
		statFailedNATSPublish,
		statNATSDropped,
	)
}
