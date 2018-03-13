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
	"strconv"
	"sync"
	"time"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/lineformatter"
	"github.com/jumptrading/influx-spout/stats"
	"github.com/nats-io/go-nats"
)

// Name for supported stats
const (
	linesPassed      = "passed"
	linesProcessed   = "processed"
	linesRejected    = "rejected"
	linesInvalidTime = "invalid-time"
)

// StartFilter creates a Filter instance, sets up its rules based on
// the configuration give and sets up a subscription for the incoming
// NATS topic.
func StartFilter(conf *config.Config) (_ *Filter, err error) {
	f := &Filter{
		c:    conf,
		stop: make(chan struct{}),
		wg:   new(sync.WaitGroup),
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

	f.sub, err = f.nc.Subscribe(f.c.NATSSubject[0], func(msg *nats.Msg) {
		if conf.Debug {
			log.Printf("filter received %d bytes", len(msg.Data))
		}
		jobs <- msg.Data
	})
	if err != nil {
		return nil, fmt.Errorf("NATS: failed to subscribe: %v", err)
	}

	f.wg.Add(1)
	go f.startStatistician(stats, rules)

	log.Printf("filter subscribed to [%s] at %s with %d rules\n",
		f.c.NATSSubject[0], f.c.NATSAddress, rules.Count())
	return f, nil
}

func (f *Filter) natsConnect() (natsConn, error) {
	nc, err := nats.Connect(f.c.NATSAddress)
	if err != nil {
		return nil, fmt.Errorf("NATS: failed to connect: %v", err)
	}
	return nc, nil
}

func initStats(rules *RuleSet) *stats.Stats {
	// Initialise
	statNames := []string{
		linesPassed,
		linesProcessed,
		linesRejected,
		linesInvalidTime,
	}
	for i := 0; i < rules.Count(); i++ {
		statNames = append(statNames, ruleToStatsName(i))
	}
	return stats.New(statNames...)
}

// natsConn allows a mock nats.Conn to be substituted in during tests.
type natsConn interface {
	Publish(string, []byte) error
	Subscribe(string, nats.MsgHandler) (*nats.Subscription, error)
	Close()
}

// Filter is a struct that contains the configuration we are running with
// and the NATS bus connection
type Filter struct {
	c    *config.Config
	nc   natsConn
	sub  *nats.Subscription
	wg   *sync.WaitGroup
	stop chan struct{}
}

// Stop shuts down goroutines and closes resources related to the filter.
func (f *Filter) Stop() {
	// Stop receiving lines to filter.
	f.sub.Unsubscribe()

	// Shut down goroutines.
	close(f.stop)
	f.wg.Wait()

	// Close the connection to NATS.
	if f.nc != nil {
		f.nc.Close()
	}
}

// startStatistician defines a goroutine that is responsible for
// regularly sending the filter's statistics to the monitoring
// backend.
func (f *Filter) startStatistician(stats *stats.Stats, rules *RuleSet) {
	defer f.wg.Done()

	totalLine := lineformatter.New(
		"spout_stat_filter",
		[]string{"filter"},
		linesPassed, linesProcessed, linesRejected, linesInvalidTime,
	)
	ruleLine := lineformatter.New(
		"spout_stat_filter_rule",
		[]string{"filter", "rule"},
		"triggered",
	)

	for {
		st := stats.Clone()

		// publish the grand stats
		f.nc.Publish(f.c.NATSSubjectMonitor, totalLine.Format(
			[]string{f.c.Name},
			st.Get(linesPassed),
			st.Get(linesProcessed),
			st.Get(linesRejected),
			st.Get(linesInvalidTime),
		))

		// publish the per rule stats
		for i, subject := range rules.Subjects() {
			f.nc.Publish(f.c.NATSSubjectMonitor,
				ruleLine.Format(
					[]string{f.c.Name, subject},
					st.Get(ruleToStatsName(i)),
				),
			)
		}

		select {
		case <-time.After(3 * time.Second):
		case <-f.stop:
			return
		}
	}
}

// ruleToStatsName converts a rule index to a name to a key for use
// with a stats.Stats instance.
func ruleToStatsName(i int) string {
	return "rule" + strconv.Itoa(i)
}
