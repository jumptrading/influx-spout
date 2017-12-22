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
	"hash/fnv"
	"log"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/go-nats"

	"github.com/jump-opensource/influxdb-relay-nova/config"
	"github.com/jump-opensource/influxdb-relay-nova/lineformatter"
	"github.com/jump-opensource/influxdb-relay-nova/stats"
)

type Filtering interface {
	GetRules() []FilterRule
}

type ChannelBuffer struct {
	sync.Mutex
	b       *bytes.Buffer
	channel string
}

type FilterRule struct {
	// Function used to check if the rule matches
	match func([]byte) bool

	// escaped is true if the match function needs the original,
	// escaped version of the line. The unescaped version of the line
	// is passed otherwise.
	escaped bool

	// if it matches, the measurement is sent to this NATS channel
	natsChan string
}

// Name for supported stats
const (
	linesPassed    = "lines-passed"
	linesProcessed = "lines-processed"
	linesRejected  = "lines-rejected"
)

// natsConn allows a mock nats.Conn to be substituted in during tests.
type natsConn interface {
	Publish(string, []byte) error
	Subscribe(string, nats.MsgHandler) (*nats.Subscription, error)
}

// filter is a struct that contains the configuration we are running with
// and the NATS bus connection
type filter struct {
	c  *config.Config
	nc natsConn

	ruleBatches []ChannelBuffer
	junkBatch   ChannelBuffer
	rules       []FilterRule
	stats       *stats.Stats
}

func (f *filter) GetRules() []FilterRule {
	return f.rules
}

// CreateBasicRule creates a simple rule that publishes measurements
// with the name @measurements to the NATS channel @channel
func CreateBasicRule(measurement string, channel string) FilterRule {
	hh := hashMeasurement([]byte(measurement))

	return FilterRule{
		match: func(line []byte) bool {
			name := influxUnescape(measurementName(line))
			return hh == hashMeasurement(name)
		},
		escaped:  true,
		natsChan: channel,
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
// match the given @regexString to the NATS channel @channel.
func CreateRegexRule(regexString, channel string) FilterRule {
	reg := regexp.MustCompile(regexString)
	return FilterRule{
		match: func(line []byte) bool {
			return reg.Match(line)
		},
		natsChan: channel,
	}
}

func CreateNegativeRegexRule(regexString, channel string) FilterRule {
	reg := regexp.MustCompile(regexString)
	return FilterRule{
		match: func(line []byte) bool {
			return !reg.Match(line)
		},
		natsChan: channel,
	}
}

func hashMeasurement(measurement []byte) uint32 {
	hh := fnv.New32()
	hh.Write(measurement)
	return hh.Sum32()
}

// AppendFilterRule appends a rule to a filter node. once the filter
// starts receving messages, the rules are processed in the order they
// were added.
func (f *filter) AppendFilterRule(rule FilterRule) {
	f.rules = append(f.rules, rule)
}

func LookupLine(f Filtering, escapedLine []byte) int {
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

func (f *filter) ProcessLine(line []byte) {
	f.stats.Inc(linesProcessed)

	id := LookupLine(f, line)
	if id == -1 {
		// no rule for this => junkyard
		f.stats.Inc(linesRejected)

		// batch up
		f.junkBatch.b.Write(line)
		return
	}

	if f.c.IsTesting {
		log.Printf("forwarded [%s] to channel nats:[%s]\n", line, f.rules[id].natsChan)
	}

	// write to the corresponding batch buffer
	f.ruleBatches[id].b.Write(line)

	f.stats.Inc(linesPassed)
	f.stats.Inc(ruleToStatsName(id))
}

func (f *filter) sendOff() {
	for _, b := range f.ruleBatches {
		if b.b.Len() > 0 {
			f.nc.Publish(b.channel, b.b.Bytes())
			b.b.Reset()
		}
	}

	// send the junk batch
	if f.junkBatch.b.Len() > 0 {
		f.nc.Publish(f.c.NATSTopicJunkyard, f.junkBatch.b.Bytes())
		f.junkBatch.b.Reset()
	}
}

func (f *filter) ProcessBatch(batch []byte) {
	for _, line := range bytes.SplitAfter(batch, []byte("\n")) {
		if len(line) > 0 {
			f.ProcessLine(line)
		}
	}

	// batches have been processed, empty the buffers onto NATS
	f.sendOff()
}

func (f *filter) startStatistician() {
	// This goroutine is responsible for monitoring the statistics and
	// sending it to the monitoring backend.

	totalLine := lineformatter.New("relay_stat_filter", nil,
		"passed", "processed", "rejected")
	ruleLine := lineformatter.New("relay_stat_filter_rule",
		[]string{"rule"}, "triggered")

	for {
		st := f.stats.Clone()

		// publish the grand stats
		f.nc.Publish(f.c.NATSTopicMonitor, totalLine.Format(nil,
			st.Get(linesPassed),
			st.Get(linesProcessed),
			st.Get(linesRejected),
		))

		// publish the per rule stats
		for i, b := range f.ruleBatches {
			f.nc.Publish(f.c.NATSTopicMonitor,
				ruleLine.Format([]string{b.channel}, st.Get(ruleToStatsName(i))),
			)
		}

		time.Sleep(3 * time.Second)
	}
}

func (f *filter) SetupFilter() {
	statNames := []string{
		linesPassed,
		linesProcessed,
		linesRejected,
	}

	// set up the buffers for batching
	f.ruleBatches = make([]ChannelBuffer, len(f.rules))
	f.junkBatch.b = new(bytes.Buffer)
	for i, rule := range f.rules {
		f.ruleBatches[i].b = new(bytes.Buffer)
		f.ruleBatches[i].b.Grow(16384)
		f.ruleBatches[i].channel = rule.natsChan

		statNames = append(statNames, ruleToStatsName(i))
	}

	f.stats = stats.New(statNames...)
}

func StartFilter(conf *config.Config) {
	// create the filter instance
	var f *filter = &filter{c: conf}
	var err error

	// create our rules from the config rules
	for _, r := range conf.Rule {
		switch r.Rtype {
		case "basic":
			f.AppendFilterRule(CreateBasicRule(r.Match, r.Channel))
		case "regex":
			f.AppendFilterRule(CreateRegexRule(r.Match, r.Channel))
		case "negregex":
			f.AppendFilterRule(CreateNegativeRegexRule(r.Match, r.Channel))
		default:
			log.Fatalf("Unsupported rule type: [%v]", r)
		}
	}

	if f.c.IsTesting {
		f.AppendFilterRule(CreateBasicRule("hello", "hello-chan"))
	}

	f.SetupFilter()

	// connect to the NATS channel
	f.nc, err = nats.Connect(f.c.NATSAddress)
	if err != nil {
		log.Fatalf("NATS: failed to connect: %v\n", err)
	}

	// subscribe to the NATS channel
	f.nc.Subscribe(f.c.NATSTopic[0], func(msg *nats.Msg) {
		f.ProcessBatch(msg.Data)
	})

	go f.startStatistician()

	log.Printf("Filter listening on [%s] with %d rules\n", f.c.NATSTopic, len(f.rules))
	runtime.Goexit()
}

// ruleToStatsName converts a rule index to a name to a key for use
// with a stats.Stats instance.
func ruleToStatsName(i int) string {
	return "rule" + strconv.Itoa(i)
}
