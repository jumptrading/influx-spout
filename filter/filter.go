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
	"bytes"
	"fmt"
	"hash/fnv"
	"log"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/go-nats"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/lineformatter"
	"github.com/jumptrading/influx-spout/stats"
)

// Name for supported stats
const (
	linesPassed    = "lines-passed"
	linesProcessed = "lines-processed"
	linesRejected  = "lines-rejected"
)

// StartFilter creates a Filter instance, sets up its rules based on
// the configuration give and sets up a subscription for the incoming
// NATS topic.
func StartFilter(conf *config.Config) (*Filter, error) {
	var err error

	// Create rules from the config.
	f := &Filter{
		c:    conf,
		stop: make(chan struct{}),
	}
	for _, r := range conf.Rule {
		switch r.Rtype {
		case "basic":
			f.appendRule(CreateBasicRule(r.Match, r.Subject))
		case "regex":
			f.appendRule(CreateRegexRule(r.Match, r.Subject))
		case "negregex":
			f.appendRule(CreateNegativeRegexRule(r.Match, r.Subject))
		default:
			return nil, fmt.Errorf("Unsupported rule type: [%v]", r)
		}
	}

	f.initRuleBatches()

	// Connect to the NATS server.
	f.nc, err = nats.Connect(f.c.NATSAddress)
	if err != nil {
		return nil, fmt.Errorf("NATS: failed to connect: %v", err)
	}

	// Subscribe to the NATS subject.
	f.sub, err = f.nc.Subscribe(f.c.NATSSubject[0], func(msg *nats.Msg) {
		f.processBatch(msg.Data)
	})
	if err != nil {
		return nil, fmt.Errorf("NATS: failed to subscribe: %v", err)
	}

	f.wg.Add(1)
	go f.startStatistician()

	log.Printf("Filter listening on [%s] with %d rules\n", f.c.NATSSubject, len(f.rules))
	return f, nil
}

// Rule encapsulates a matching function and the NATS topic to
// send lines to if the rule matches.
type Rule struct {
	// Function used to check if the rule matches
	match func([]byte) bool

	// escaped is true if the match function needs the original,
	// escaped version of the line. The unescaped version of the line
	// is passed otherwise.
	escaped bool

	// if the rule matches, the measurement is sent to this NATS subject
	subject string
}

type filtering interface {
	GetRules() []Rule
}

type subjectBuffer struct {
	sync.Mutex
	b       *bytes.Buffer
	subject string
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
	c  *config.Config
	nc natsConn

	ruleBatches []subjectBuffer
	junkBatch   subjectBuffer
	rules       []Rule
	stats       *stats.Stats

	sub  *nats.Subscription
	wg   sync.WaitGroup
	stop chan struct{}
}

func (f *Filter) initRuleBatches() {
	statNames := []string{
		linesPassed,
		linesProcessed,
		linesRejected,
	}

	// set up the buffers for batching
	f.ruleBatches = make([]subjectBuffer, len(f.rules))
	f.junkBatch.b = new(bytes.Buffer)
	for i, rule := range f.rules {
		f.ruleBatches[i].b = new(bytes.Buffer)
		f.ruleBatches[i].b.Grow(16384)
		f.ruleBatches[i].subject = rule.subject

		statNames = append(statNames, ruleToStatsName(i))
	}

	f.stats = stats.New(statNames...)
}

// Stop shuts down goroutines and closes resources related to the filter.
func (f *Filter) Stop() {
	// Stop receiving lines to filter.
	f.sub.Unsubscribe()

	// Shut down goroutines (just the statistician at this stage).
	close(f.stop)
	f.wg.Wait()

	// Close the connection to NATS.
	if f.nc != nil {
		f.nc.Close()
	}
}

// GetRules returns all rules associated with the filter.
func (f *Filter) GetRules() []Rule {
	return f.rules
}

// CreateBasicRule creates a simple rule that publishes measurements
// with the name @measurement to the NATS @subject.
func CreateBasicRule(measurement string, subject string) Rule {
	hh := hashMeasurement([]byte(measurement))

	return Rule{
		match: func(line []byte) bool {
			name := influxUnescape(measurementName(line))
			return hh == hashMeasurement(name)
		},
		escaped: true,
		subject: subject,
	}
}

// measurementName takes an *escaped* line protocol line and returns
// the *escaped* measurement from it.
func measurementName(s []byte) []byte {
	// Handle the unlikely case of a single character line.
	if len(s) == 1 {
		switch s[0] {
		case ' ', ',':
			return s[:0]
		default:
			return s
		}
	}

	i := 0
	for {
		i++
		if i >= len(s) {
			return s
		}

		if s[i-1] == '\\' {
			// Skip character (it's escaped).
			continue
		}

		if s[i] == ',' || s[i] == ' ' {
			return s[:i]
		}
	}
}

// CreateRegexRule creates a rule that publishes measurements which
// match the given @regexString to the NATS @subject.
func CreateRegexRule(regexString, subject string) Rule {
	reg := regexp.MustCompile(regexString)
	return Rule{
		match: func(line []byte) bool {
			return reg.Match(line)
		},
		subject: subject,
	}
}

// CreateNegativeRegexRule creates a rule that publishes measurements
// which *don't* match the given @regexString to the NATS @subject.
func CreateNegativeRegexRule(regexString, subject string) Rule {
	reg := regexp.MustCompile(regexString)
	return Rule{
		match: func(line []byte) bool {
			return !reg.Match(line)
		},
		subject: subject,
	}
}

func hashMeasurement(measurement []byte) uint32 {
	hh := fnv.New32()
	hh.Write(measurement)
	return hh.Sum32()
}

// appendRule appends a rule to a filter node. once the filter
// starts receving messages, the rules are processed in the order they
// were added.
func (f *Filter) appendRule(rule Rule) {
	f.rules = append(f.rules, rule)
}

// LookupLine takes a raw line and returns the index of the rule from
// the `filtering` provided that matches. Returns -1 if there was no
// match.
func LookupLine(f filtering, escapedLine []byte) int {
	line := influxUnescape(escapedLine)
	for i, rule := range f.GetRules() {
		matchLine := line
		if rule.escaped {
			matchLine = escapedLine
		}
		if rule.match(matchLine) {
			return i
		}
	}
	return -1
}

func (f *Filter) processLine(line []byte) {
	f.stats.Inc(linesProcessed)

	id := LookupLine(f, line)
	if id == -1 {
		// no rule for this => junkyard
		f.stats.Inc(linesRejected)

		// batch up
		f.junkBatch.b.Write(line)
		return
	}

	if f.c.Debug {
		log.Printf("forwarded [%s] to subject nats:[%s]\n", line, f.rules[id].subject)
	}

	// write to the corresponding batch buffer
	f.ruleBatches[id].b.Write(line)

	f.stats.Inc(linesPassed)
	f.stats.Inc(ruleToStatsName(id))
}

func (f *Filter) sendOff() {
	for _, b := range f.ruleBatches {
		if b.b.Len() > 0 {
			f.nc.Publish(b.subject, b.b.Bytes())
			b.b.Reset()
		}
	}

	// send the junk batch
	if f.junkBatch.b.Len() > 0 {
		f.nc.Publish(f.c.NATSSubjectJunkyard, f.junkBatch.b.Bytes())
		f.junkBatch.b.Reset()
	}
}

func (f *Filter) processBatch(batch []byte) {
	for _, line := range bytes.SplitAfter(batch, []byte("\n")) {
		if len(line) > 0 {
			f.processLine(line)
		}
	}

	// batches have been processed, empty the buffers onto NATS
	f.sendOff()
}

// startStatistician defines a goroutine that is responsible for
// regularly sending the filter's statistics to the monitoring
// backend.
func (f *Filter) startStatistician() {
	defer f.wg.Done()

	totalLine := lineformatter.New("spout_stat_filter", nil,
		"passed", "processed", "rejected")
	ruleLine := lineformatter.New("spout_stat_filter_rule",
		[]string{"rule"}, "triggered")

	for {
		st := f.stats.Clone()

		// publish the grand stats
		f.nc.Publish(f.c.NATSSubjectMonitor, totalLine.Format(nil,
			st.Get(linesPassed),
			st.Get(linesProcessed),
			st.Get(linesRejected),
		))

		// publish the per rule stats
		for i, b := range f.ruleBatches {
			f.nc.Publish(f.c.NATSSubjectMonitor,
				ruleLine.Format([]string{b.subject}, st.Get(ruleToStatsName(i))),
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
