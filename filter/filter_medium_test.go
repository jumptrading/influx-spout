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
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/nats-io/go-nats"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/spouttest"
)

const natsPort = 44100
const probePort = 44101

func testConfig() *config.Config {
	return &config.Config{
		Name:                "particle",
		NATSAddress:         fmt.Sprintf("nats://127.0.0.1:%d", natsPort),
		NATSSubject:         []string{"filter-test"},
		NATSSubjectMonitor:  "filter-test-monitor",
		NATSSubjectJunkyard: "filter-junkyard",
		NATSMaxPendingSize:  32 * datasize.MB,
		Workers:             1,
		MaxTimeDeltaSecs:    600,
		Rule: []config.Rule{{
			Rtype:   "basic",
			Match:   "hello",
			Subject: "hello-subject",
		}},
		ProbePort: probePort,
	}
}

func TestFilterWorker(t *testing.T) {
	gnatsd := spouttest.RunGnatsd(natsPort)
	defer gnatsd.Shutdown()

	conf := testConfig()

	filter := startFilter(t, conf)
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

	// Subscribe to monitor output
	monitorCh := make(chan string, 10)
	_, err = nc.Subscribe(conf.NATSSubjectMonitor, func(msg *nats.Msg) {
		monitorCh <- string(msg.Data)
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
	spouttest.AssertRecv(t, helloCh, "data", `
hello,host=gopher01
hello,host=gopher01
`)

	// Receive junkyard output
	spouttest.AssertRecv(t, junkCh, "junkyard data", `
goodbye,host=gopher01
`)

	// Receive monitor metrics
	spouttest.AssertMonitor(t, monitorCh, []string{
		`passed{component="filter",name="particle"} 2`,
		`processed{component="filter",name="particle"} 3`,
		`rejected{component="filter",name="particle"} 1`,
		`invalid_time{component="filter",name="particle"} 0`,
		`failed_nats_publish{component="filter",name="particle"} 0`,
		`nats_dropped{component="filter",name="particle"} 0`,
		`triggered{component="filter",name="particle",rule="hello-subject"} 2`,
	})
}

func TestInvalidTimeStamps(t *testing.T) {
	gnatsd := spouttest.RunGnatsd(natsPort)
	defer gnatsd.Shutdown()

	conf := testConfig()
	conf.MaxTimeDeltaSecs = 10

	filter := startFilter(t, conf)
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

	// Subscribe to monitor output
	monitorCh := make(chan string, 10)
	_, err = nc.Subscribe(conf.NATSSubjectMonitor, func(msg *nats.Msg) {
		monitorCh <- string(msg.Data)
	})
	require.NoError(t, err)

	// Publish 3 lines.
	// The first should be rejected because it is too old.
	// The second should be rejected because it is too new.
	// The third should make it through because it is current.
	// The fourth should make it through because it has no timestamp.
	now := time.Now()
	lines := []string{
		fmt.Sprintf("hello,instance=0 foo=0 %d", now.Add(-time.Second*11).UnixNano()),
		fmt.Sprintf("hello,instance=1 foo=0 %d", now.Add(time.Second*11).UnixNano()),
		fmt.Sprintf("hello,instance=2 foo=1 %d", now.UnixNano()),
		"hello,instance=2 foo=3",
	}
	err = nc.Publish(conf.NATSSubject[0], []byte(strings.Join(lines, "\n")))
	require.NoError(t, err)

	// Expect to see the 3rd & 4th lines.
	spouttest.AssertRecv(t, helloCh, "helloCh", strings.Join(lines[2:], "\n"))

	// Receive monitor metrics.
	spouttest.AssertMonitor(t, monitorCh, []string{
		`passed{component="filter",name="particle"} 2`,
		`processed{component="filter",name="particle"} 4`,
		`rejected{component="filter",name="particle"} 0`,
		`invalid_time{component="filter",name="particle"} 2`,
		`failed_nats_publish{component="filter",name="particle"} 0`,
		`nats_dropped{component="filter",name="particle"} 0`,
		`triggered{component="filter",name="particle",rule="hello-subject"} 2`,
	})
}

func startFilter(t *testing.T, conf *config.Config) *Filter {
	filter, err := StartFilter(conf)
	require.NoError(t, err)
	if !spouttest.CheckReadyProbe(conf.ProbePort) {
		filter.Stop()
		t.Fatal("filter not ready")
	}
	return filter
}
