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

// +build large

package spouttest_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/cmd"
	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/spouttest"
)

const (
	natsPort    = 44600
	influxdPort = 44601

	listenerPort      = 44610
	listenerProbePort = 44611

	httpListenerPort      = 44620
	httpListenerProbePort = 44621

	filterProbePort = 44631

	writerProbePort = 44641

	monitorPort      = 44650
	monitorProbePort = 44651

	influxDBName = "test"
	sendCount    = 10
)

func TestEndToEnd(t *testing.T) {
	// Start gnatsd.
	gnatsd := spouttest.RunGnatsd(natsPort)
	defer gnatsd.Shutdown()

	// Start influxd & set up test database.
	influxd := spouttest.RunFakeInfluxd(influxdPort)
	defer influxd.Stop()

	// Use a fake filesystem (for config files).
	fs := afero.NewMemMapFs()
	config.Fs = fs

	// Start spout components.
	listener := startListener(t, fs)
	defer listener.Stop()
	spouttest.AssertReadyProbe(t, listenerProbePort)

	httpListener := startHTTPListener(t, fs)
	defer httpListener.Stop()
	spouttest.AssertReadyProbe(t, httpListenerProbePort)

	filter := startFilter(t, fs)
	defer filter.Stop()
	spouttest.AssertReadyProbe(t, filterProbePort)

	writer := startWriter(t, fs)
	defer writer.Stop()
	spouttest.AssertReadyProbe(t, writerProbePort)

	monitor := startMonitor(t, fs)
	defer monitor.Stop()
	spouttest.AssertReadyProbe(t, monitorProbePort)

	// Connect to the listener.
	addr := net.JoinHostPort("localhost", strconv.Itoa(listenerPort))
	conn, err := net.Dial("udp", addr)
	require.NoError(t, err)
	defer conn.Close()

	// Do 5 UDP metric sends each containing 2 lines.
	for i := 0; i < sendCount/2; i++ {
		_, err := conn.Write(makeTestLines().Bytes())
		require.NoError(t, err)

		// Generous sleep between sends to avoid UDP drops.
		time.Sleep(100 * time.Millisecond)
	}

	// Do 5 HTTP metric sends, the same as the UDP sends above.
	url := fmt.Sprintf("http://localhost:%d/write", httpListenerPort)
	for i := 0; i < sendCount/2; i++ {
		_, err := http.Post(url, "text/plain", makeTestLines())
		require.NoError(t, err)
	}

	// Check "database".
	maxWaitTime := time.Now().Add(spouttest.LongWait)
	for {
		lines := influxd.Lines()
		recvCount := len(lines[influxDBName])
		if recvCount == sendCount {
			// Expected number of lines received...
			// Now check they are correct.
			for _, line := range lines[influxDBName] {
				if !strings.HasPrefix(line, cpuLine) {
					t.Fatalf("unexpected line received: %s", line)
				}
			}

			// No writes to other databases are expected.
			assert.Len(t, lines, 1)

			break // Success
		}
		if time.Now().After(maxWaitTime) {
			t.Fatalf("failed to see expected database records. Saw %d records.", recvCount)
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Check metrics published by monitor component.
	expectedMetrics := regexp.MustCompile(`
failed_nats_publish{component="filter",name="filter"} 0
failed_nats_publish{component="listener",name="listener"} 0
failed_writes{component="writer",influxdb_address="localhost",influxdb_dbname="test",influxdb_port="44601",name="writer"} 0
invalid_time{component="filter",name="filter"} 0
max_pending{component="writer",influxdb_address="localhost",influxdb_dbname="test",influxdb_port="44601",name="writer"} \d+
nats_dropped{component="filter",name="filter"} 0
nats_dropped{component="writer",influxdb_address="localhost",influxdb_dbname="test",influxdb_port="44601",name="writer",subject="system"} 0
passed{component="filter",name="filter"} 10
processed{component="filter",name="filter"} 20
read_errors{component="listener",name="listener"} 0
received{component="listener",name="listener"} 5
received{component="writer",influxdb_address="localhost",influxdb_dbname="test",influxdb_port="44601",name="writer"} 2
rejected{component="filter",name="filter"} 10
sent{component="listener",name="listener"} 1
triggered{component="filter",name="filter",rule="system"} 10
write_requests{component="writer",influxdb_address="localhost",influxdb_dbname="test",influxdb_port="44601",name="writer"} 2
$`[1:])
	var lines string
	for try := 0; try < 20; try++ {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", monitorPort))
		require.NoError(t, err)

		raw, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)

		lines = spouttest.StripTimestamps(t, string(raw))
		if expectedMetrics.MatchString(lines) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("Failed to see expected metrics. Last saw:\n%s", lines)
}

const cpuLine = "cpu,env=prod,cls=server user=13.33,usage_system=0.16,usage_idle=86.53"

func makeTestLines() *bytes.Buffer {
	now := time.Now().UnixNano()
	out := new(bytes.Buffer)

	// Only the 2nd line should make it through the filter.
	fmt.Fprintf(out, `
foo,env=dev bar=99 %d
%s %d
`[1:], now, cpuLine, now)

	return out
}

func startListener(t *testing.T, fs afero.Fs) cmd.Stoppable {
	return startComponent(t, fs, "listener", fmt.Sprintf(`
mode = "listener"
port = %d
nats_address = "nats://localhost:%d"
batch = 5
debug = true
nats_subject_monitor = "monitor"
probe_port = %d
`, listenerPort, natsPort, listenerProbePort))
}

func startHTTPListener(t *testing.T, fs afero.Fs) cmd.Stoppable {
	return startComponent(t, fs, "listener", fmt.Sprintf(`
mode = "listener_http"
port = %d
nats_address = "nats://localhost:%d"
batch = 5
debug = true
nats_subject_monitor = "monitor"
probe_port = %d
`, httpListenerPort, natsPort, httpListenerProbePort))
}

func startFilter(t *testing.T, fs afero.Fs) cmd.Stoppable {
	return startComponent(t, fs, "filter", fmt.Sprintf(`
mode = "filter"
nats_address = "nats://localhost:%d"
debug = true
nats_subject_monitor = "monitor"
probe_port = %d

[[rule]]
type = "basic"
match = "cpu"
subject = "system"
`, natsPort, filterProbePort))
}

func startWriter(t *testing.T, fs afero.Fs) cmd.Stoppable {
	return startComponent(t, fs, "writer", fmt.Sprintf(`
mode = "writer"
nats_address = "nats://localhost:%d"
nats_subject = ["system"]
influxdb_port = %d
influxdb_dbname = "%s"
batch = 1
workers = 4
debug = true
nats_subject_monitor = "monitor"
probe_port = %d
`, natsPort, influxdPort, influxDBName, writerProbePort))
}

func startMonitor(t *testing.T, fs afero.Fs) cmd.Stoppable {
	return startComponent(t, fs, "monitor", fmt.Sprintf(`
mode = "monitor"
nats_address = "nats://localhost:%d"
nats_subject_monitor = "monitor"
port = %d
probe_port = %d
`, natsPort, monitorPort, monitorProbePort))
}

func startComponent(t *testing.T, fs afero.Fs, name, config string) cmd.Stoppable {
	configFilename := name + ".toml"
	err := afero.WriteFile(fs, configFilename, []byte(config), 0600)
	require.NoError(t, err)
	s, err := cmd.Run(configFilename)
	require.NoError(t, err)
	return s
}
