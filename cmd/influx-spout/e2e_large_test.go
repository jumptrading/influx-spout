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

package main

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

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/spouttest"
	"github.com/jumptrading/influx-spout/stats"
)

const (
	natsPort    = 44600
	influxdPort = 44601

	listenerPort      = 44610
	listenerProbePort = 44611

	httpListenerPort      = 44620
	httpListenerProbePort = 44621

	filterProbePort = 44631

	downsamplerProbePort = 44641

	writerProbePort        = 44651
	archiveWriterProbePort = 44652

	monitorPort      = 44660
	monitorProbePort = 44661

	dbName        = "test"
	archiveDBName = "test-archive"
	sendCount     = 10
)

func TestEndToEnd(t *testing.T) {
	stats.SetHostname("h")

	// Start gnatsd.
	gnatsd := spouttest.RunGnatsd(natsPort)
	defer gnatsd.Shutdown()

	// Start influxd & set up test database.
	influxd := spouttest.RunFakeInfluxd(influxdPort)
	defer influxd.Stop()

	// Use a fake filesystem (for config files).
	config.Fs = afero.NewMemMapFs()

	// Start spout components.
	listener := startListener(t)
	defer listener.Stop()
	spouttest.AssertReadyProbe(t, listenerProbePort)

	httpListener := startHTTPListener(t)
	defer httpListener.Stop()
	spouttest.AssertReadyProbe(t, httpListenerProbePort)

	filter := startFilter(t)
	defer filter.Stop()
	spouttest.AssertReadyProbe(t, filterProbePort)

	downsampler := startDownsampler(t)
	defer downsampler.Stop()
	spouttest.AssertReadyProbe(t, downsamplerProbePort)

	writer := startWriter(t)
	defer writer.Stop()
	spouttest.AssertReadyProbe(t, writerProbePort)

	archiveWriter := startArchiveWriter(t)
	defer archiveWriter.Stop()
	spouttest.AssertReadyProbe(t, archiveWriterProbePort)

	monitor := startMonitor(t)
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

	// Check "databases".
	checkDatabase(t, influxd, dbName, sendCount, isCPULine)
	checkDatabase(t, influxd, archiveDBName, 1, isLikeCPULine)
	assert.Equal(t, 2, influxd.DatabaseCount()) // primary + archive

	// Check metrics published by monitor component.
	expectedMetrics := regexp.MustCompile(`
failed_nats_publish{component="downsampler",host="h",name="downsampler"} 0
failed_nats_publish{component="filter",host="h",name="filter"} 0
failed_nats_publish{component="listener",host="h",name="listener"} 0
failed_writes{component="writer",host="h",influxdb_address="localhost",influxdb_dbname="test",influxdb_port="44601",name="writer"} 0
failed_writes{component="writer",host="h",influxdb_address="localhost",influxdb_dbname="test-archive",influxdb_port="44601",name="archive-writer"} 0
invalid_lines{component="downsampler",host="h",name="downsampler"} 0
invalid_timestamps{component="downsampler",host="h",name="downsampler"} 0
invalid_time{component="filter",host="h",name="filter"} 0
max_pending{component="writer",host="h",influxdb_address="localhost",influxdb_dbname="test",influxdb_port="44601",name="writer"} \d+
max_pending{component="writer",host="h",influxdb_address="localhost",influxdb_dbname="test-archive",influxdb_port="44601",name="archive-writer"} \d+
nats_dropped{component="downsampler",host="h",name="downsampler",subject="system"} 0
nats_dropped{component="filter",host="h",name="filter"} 0
nats_dropped{component="writer",host="h",influxdb_address="localhost",influxdb_dbname="test",influxdb_port="44601",name="writer",subject="system"} 0
nats_dropped{component="writer",host="h",influxdb_address="localhost",influxdb_dbname="test-archive",influxdb_port="44601",name="archive-writer",subject="system-archive"} 0
passed{component="filter",host="h",name="filter"} 10
processed{component="filter",host="h",name="filter"} 20
read_errors{component="listener",host="h",name="listener"} 0
received{component="downsampler",host="h",name="downsampler"} 2
received{component="listener",host="h",name="listener"} 5
received{component="writer",host="h",influxdb_address="localhost",influxdb_dbname="test",influxdb_port="44601",name="writer"} 2
received{component="writer",host="h",influxdb_address="localhost",influxdb_dbname="test-archive",influxdb_port="44601",name="archive-writer"} 1
rejected{component="filter",host="h",name="filter"} 10
sent{component="downsampler",host="h",name="downsampler"} 1
sent{component="listener",host="h",name="listener"} 1
triggered{component="filter",host="h",name="filter",rule="system"} 10
write_requests{component="writer",host="h",influxdb_address="localhost",influxdb_dbname="test",influxdb_port="44601",name="writer"} 2
write_requests{component="writer",host="h",influxdb_address="localhost",influxdb_dbname="test-archive",influxdb_port="44601",name="archive-writer"} 1
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

const cpuLineHeader = "cpu,cls=server,env=prod "
const cpuLine = cpuLineHeader + "user=13.33,usage_system=0.16,usage_idle=86.53"

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

func startListener(t *testing.T) stoppable {
	return startComponent(t, "listener", fmt.Sprintf(`
mode = "listener"
port = %d
nats_address = "nats://localhost:%d"
batch_max_count = 5
debug = true
nats_subject_monitor = "monitor"
probe_port = %d
`, listenerPort, natsPort, listenerProbePort))
}

func startHTTPListener(t *testing.T) stoppable {
	return startComponent(t, "listener", fmt.Sprintf(`
mode = "listener_http"
port = %d
nats_address = "nats://localhost:%d"
batch_max_count = 5
debug = true
nats_subject_monitor = "monitor"
probe_port = %d
`, httpListenerPort, natsPort, httpListenerProbePort))
}

func startFilter(t *testing.T) stoppable {
	return startComponent(t, "filter", fmt.Sprintf(`
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

func startDownsampler(t *testing.T) stoppable {
	return startComponent(t, "downsampler", fmt.Sprintf(`
mode = "downsampler"
nats_address = "nats://localhost:%d"
debug = true
nats_subject_monitor = "monitor"
probe_port = %d

nats_subject = ["system"]
downsample_period = "3s"
`, natsPort, downsamplerProbePort))
}

func startWriter(t *testing.T) stoppable {
	return baseStartWriter(t, "writer", "system", dbName, writerProbePort)
}

func startArchiveWriter(t *testing.T) stoppable {
	return baseStartWriter(t, "archive-writer", "system-archive", archiveDBName, archiveWriterProbePort)
}

func baseStartWriter(t *testing.T, name, subject, dbName string, probePort int) stoppable {
	return startComponent(t, name, fmt.Sprintf(`
mode = "writer"
name = "%s"
nats_address = "nats://localhost:%d"
nats_subject = ["%s"]
influxdb_port = %d
influxdb_dbname = "%s"
batch_max_count = 1
workers = 4
debug = true
nats_subject_monitor = "monitor"
probe_port = %d
`, name, natsPort, subject, influxdPort, dbName, probePort))
}

func startMonitor(t *testing.T) stoppable {
	return startComponent(t, "monitor", fmt.Sprintf(`
mode = "monitor"
nats_address = "nats://localhost:%d"
nats_subject_monitor = "monitor"
port = %d
probe_port = %d
`, natsPort, monitorPort, monitorProbePort))
}

func startComponent(t *testing.T, name, configText string) stoppable {
	configFilename := name + ".toml"
	err := afero.WriteFile(config.Fs, configFilename, []byte(configText), 0600)
	require.NoError(t, err)
	s, err := runComponent(configFilename)
	require.NoError(t, err)
	return s
}

func checkDatabase(t *testing.T, db *spouttest.FakeInfluxDB, dbName string, expectedCount int, checkLine func(string) bool) {
	maxWaitTime := time.Now().Add(spouttest.LongWait)
	for {
		lines := db.Lines(dbName)
		recvCount := len(lines)
		if recvCount == expectedCount {
			// Expected number of lines received. Now check they are correct.
			for _, line := range lines {
				if !checkLine(line) {
					t.Fatalf("unexpected line received: %s", line)
				}
			}
			break // Success
		}
		if time.Now().After(maxWaitTime) {
			t.Fatalf("failed to see expected database records. Saw %d records.", recvCount)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func isCPULine(line string) bool {
	return strings.HasPrefix(line, cpuLine)
}

func isLikeCPULine(line string) bool {
	return strings.HasPrefix(line, cpuLineHeader)
}
