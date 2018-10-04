// Copyright 2018 Jump Trading
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

package downsampler

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/nats-io/go-nats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/influx"
	"github.com/jumptrading/influx-spout/prometheus"
	"github.com/jumptrading/influx-spout/spouttest"
	"github.com/jumptrading/influx-spout/stats"
)

const natsPort = 44500
const probePort = 44501

func testConfig() *config.Config {
	return &config.Config{
		Mode:               "downsampler",
		Name:               "mpc-60",
		NATSAddress:        fmt.Sprintf("nats://127.0.0.1:%d", natsPort),
		NATSSubject:        []string{"subject0", "subject1"},
		NATSSubjectMonitor: "downsampler-test-monitor",
		DownsamplePeriod:   config.NewDuration(1 * time.Second),
		DownsampleSuffix:   "-arch",
		ProbePort:          probePort,
		NATSMaxPendingSize: 32 * datasize.MB,
		Debug:              true,

		// Make the statistician report more often during tests. This
		// makes the tests run faster.
		StatsInterval: config.NewDuration(250 * time.Millisecond),
	}
}

func TestDownsampler(t *testing.T) {
	stats.SetHostname("h")

	gnatsd := spouttest.RunGnatsd(natsPort)
	defer gnatsd.Shutdown()

	conf := testConfig()

	ds := startDownsampler(t, conf)
	defer ds.Stop()

	nc, err := nats.Connect(conf.NATSAddress)
	require.NoError(t, err)
	defer nc.Close()

	// Subscribe to outputs
	output0 := subscribeOutput(t, nc, "subject0-arch")
	output1 := subscribeOutput(t, nc, "subject1-arch")
	monitorCh := subscribeOutput(t, nc, conf.NATSSubjectMonitor)

	// Send lines to each subject to be archived.
	err = nc.Publish("subject0", []byte(`
string,host=host0 value="foo bar"
nums,host=host0 x=111i,y=1.5
string,host=host0 value="bar foo"
bool,host=0 b=true
string,host=host0 value="foo bar"
bool,host=1 b=false
nums,host=host0 x=333i,y=2.5
bool,host=0 b=false
bool,host=1 b=true
too_old_timestamp 123
`[1:]))
	require.NoError(t, err)

	err = nc.Publish("subject1", []byte(`
string,host=host0 value="one"
int,host=host0 xyz=111i
string,host=host0 value="two"
string,host=host0 value="three"
int,host=host0 xyz=333i
too_new_timestamp 9223372036854775807
invalid x
`[1:]))
	require.NoError(t, err)

	// Check archived lines.
	assertDownsamplerRecv(t, output0, "subject0-arch", `
string,host=host0 value="foo bar"
nums,host=host0 x=222i,y=2
bool,host=0 b=false
bool,host=1 b=true
`)
	assertDownsamplerRecv(t, output1, "subject1-arch", `
string,host=host0 value="three"
int,host=host0 xyz=222i
`)

	// Now wait a little and ensure that nothing else is emitted.
	time.Sleep(time.Second)

	spouttest.AssertNoMore(t, output0)
	spouttest.AssertNoMore(t, output1)

	// // Check the monitor output.

	baseLabels := prometheus.Labels{
		prometheus.NewLabel("component", "downsampler"),
		prometheus.NewLabel("host", "h"),
		prometheus.NewLabel("name", "mpc-60"),
	}
	baseLabelStr := string(baseLabels.ToBytes())
	extendLabels := func(name, value string) string {
		out := append(baseLabels, prometheus.NewLabel(name, value))
		return string(out.ToBytes())
	}
	spouttest.AssertMonitor(t, monitorCh, []string{
		`received` + baseLabelStr + ` 2`,
		`sent` + baseLabelStr + ` 2`,
		`invalid_lines` + baseLabelStr + ` 1`,
		`invalid_timestamps` + baseLabelStr + ` 2`,
		`failed_nats_publish` + baseLabelStr + ` 0`,
		`nats_dropped` + extendLabels("subject", "subject0") + ` 0`,
		`nats_dropped` + extendLabels("subject", "subject1") + ` 0`,
	})
}

func startDownsampler(t *testing.T, conf *config.Config) *Downsampler {
	ds, err := StartDownsampler(conf)
	require.NoError(t, err)
	if !spouttest.CheckReadyProbe(conf.ProbePort) {
		ds.Stop()
		t.Fatal("downsampler not ready")
	}
	return ds
}

func subscribeOutput(t *testing.T, nc *nats.Conn, chanName string) chan string {
	output := make(chan string, 10)
	_, err := nc.Subscribe(chanName, func(msg *nats.Msg) {
		output <- string(msg.Data)
	})
	require.NoError(t, err)
	return output
}

func assertDownsamplerRecv(t *testing.T, ch <-chan string, label, expected string) {
	expectedLines := splitLines(expected)
	select {
	case received := <-ch:
		lines := splitLines(received)
		lines = assertTimestamps(t, lines)
		assert.ElementsMatch(t, expectedLines, lines)
	case <-time.After(spouttest.LongWait):
		t.Fatalf("timed out waiting for %s", label)
	}
}

// assertTimestamps checks that the timestamps on the input lines are
// recent and the same. Returns the input lines with the timestamps
// stripped.
func assertTimestamps(t *testing.T, lines []string) []string {
	outLines := make([]string, 0, len(lines))
	seenTs := make(map[int64]bool)
	for _, line := range lines {
		ts, offset := influx.ExtractTimestamp([]byte(line))
		require.True(t, offset >= 0) // Must have timestamp
		outLines = append(outLines, line[:offset-1])
		seenTs[ts] = true
	}

	assert.Len(t, seenTs, 1, "expected one timestamp")
	for ts := range seenTs {
		assert.True(t, time.Duration(time.Now().UnixNano()-ts) < (10*time.Second))
	}

	return outLines
}

func splitLines(raw string) []string {
	rawLines := strings.Split(raw, "\n")
	out := make([]string, 0, len(rawLines))
	for _, line := range rawLines {
		if len(line) > 0 {
			out = append(out, line)
		}
	}
	return out
}
