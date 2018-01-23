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

// +build medium

package filter

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/spouttest"
)

const natsPort = 44446

var conf = config.Config{
	NATSAddress:         fmt.Sprintf("nats://127.0.0.1:%d", natsPort),
	NATSSubject:         []string{"filter-test"},
	NATSSubjectMonitor:  "filter-test-monitor",
	NATSSubjectJunkyard: "filter-junkyard",
	Rule: []config.Rule{{
		Rtype:   "basic",
		Match:   "hello",
		Subject: "hello-subject",
	}},
}

func TestFilterWorker(t *testing.T) {
	gnatsd := spouttest.RunGnatsd(natsPort)
	defer gnatsd.Shutdown()

	filter, err := StartFilter(&conf)
	require.NoError(t, err)
	defer filter.Stop()

	nc, err := nats.Connect(conf.NATSAddress)
	require.NoError(t, err)
	defer nc.Close()

	// Subscribe to filter output
	helloCh := make(chan string, 1)
	_, err = nc.Subscribe(conf.Rule[0].Subject, func(msg *nats.Msg) {
		helloCh <- string(msg.Data)
	})
	require.NoError(t, err)

	// Subscribe to junkyard output
	junkCh := make(chan string, 1)
	_, err = nc.Subscribe(conf.NATSSubjectJunkyard, func(msg *nats.Msg) {
		junkCh <- string(msg.Data)
	})
	require.NoError(t, err)

	// Subscribe to stats output
	statsCh := make(chan string, 10)
	_, err = nc.Subscribe(conf.NATSSubjectMonitor, func(msg *nats.Msg) {
		statsCh <- string(msg.Data)
	})
	require.NoError(t, err)

	// Publish some lines.
	lines := `
hello,host=gopher01
goodbye,host=gopher01
hello,host=gopher01
`[1:]
	err = nc.Publish(conf.NATSSubject[0], []byte(lines))
	require.NoError(t, err)

	// Receive filter output
	assertReceived(t, helloCh, "data", `
hello,host=gopher01
hello,host=gopher01
`)

	// Receive junkyard output
	assertReceived(t, junkCh, "junkyard data", `
goodbye,host=gopher01
`)

	// Receive total stats
	assertReceived(t, statsCh, "stats", `
spout_stat_filter passed=2,processed=3,rejected=1
`)

	// Receive rule specific stats
	assertReceived(t, statsCh, "rule stats", `
spout_stat_filter_rule,rule=hello-subject triggered=2
`)
}

func assertReceived(t *testing.T, ch <-chan string, label, expected string) {
	expected = expected[1:]
	select {
	case received := <-ch:
		assert.Equal(t, expected, received)
	case <-time.After(spouttest.LongWait):
		t.Fatal("timed out waiting for " + label)
	}
}
