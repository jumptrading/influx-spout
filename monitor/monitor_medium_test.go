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

package monitor_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/v2/config"
	"github.com/jumptrading/influx-spout/v2/monitor"
	"github.com/jumptrading/influx-spout/v2/prometheus"
	"github.com/jumptrading/influx-spout/v2/spouttest"
)

const natsPort = 44300
const httpPort = 44301
const probePort = 44302

var natsAddress = fmt.Sprintf("nats://127.0.0.1:%d", natsPort)

func testConfig() *config.Config {
	return &config.Config{
		Name:               "nats.server",
		NATSAddress:        natsAddress,
		NATSSubjectMonitor: "monitor-test-monitor",
		Port:               httpPort,
		ProbePort:          probePort,
	}
}

func TestMonitor(t *testing.T) {
	nc, stopNats := runGnatsd(t)
	defer stopNats()

	conf := testConfig()

	mon, err := monitor.Start(conf)
	require.NoError(t, err)
	defer mon.Stop()
	spouttest.AssertReadyProbe(t, conf.ProbePort)

	publish := func(data []byte) {
		err := nc.Publish(conf.NATSSubjectMonitor, data)
		require.NoError(t, err)
	}

	expected := prometheus.NewMetricSet()

	// Send a metric to the monitor and see it included at the
	// monitor's metric endpoint.
	m0 := &prometheus.Metric{
		Name: []byte("foo"),
		Labels: prometheus.Labels{
			{
				Name:  []byte("host"),
				Value: []byte("nyc01"),
			},
			{
				Name:  []byte("land"),
				Value: []byte("ho"),
			},
		},
		Value:        42,
		Milliseconds: 11111111,
	}
	publish(m0.ToBytes())
	expected.Update(m0)

	assertMetrics(t, expected)

	// Send another update with 2 metrics to the monitor and see them
	// included.
	nextUpdate := prometheus.NewMetricSet()
	nextUpdate.Update(&prometheus.Metric{
		Name: []byte("foo"),
		Labels: prometheus.Labels{
			{
				Name:  []byte("host"),
				Value: []byte("nyc01"),
			},
			{
				Name:  []byte("land"),
				Value: []byte("ho"),
			},
		},
		Value:        99,
		Milliseconds: 22222222,
	})
	nextUpdate.Update(&prometheus.Metric{
		Name: []byte("bar"),
		Labels: prometheus.Labels{
			{
				Name:  []byte("host"),
				Value: []byte("nyc02"),
			},
		},
		Value:        1024,
		Milliseconds: 33333333,
	})
	publish(nextUpdate.ToBytes())
	expected.UpdateFromSet(nextUpdate)

	assertMetrics(t, expected)
}

func runGnatsd(t *testing.T) (*nats.Conn, func()) {
	gnatsd := spouttest.RunGnatsd(natsPort)

	nc, err := nats.Connect(natsAddress, nats.Name("listenerTest"))
	if err != nil {
		gnatsd.Shutdown()
		t.Fatalf("NATS connect failed: %v", err)
	}

	return nc, func() {
		nc.Close()
		gnatsd.Shutdown()
	}
}

func assertMetrics(t *testing.T, expected *prometheus.MetricSet) {
	var actual *prometheus.MetricSet

	for try := 0; try < 10; try++ {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", httpPort))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, resp.StatusCode, 200)
		require.Equal(t, "text/plain", resp.Header.Get("Content-Type"))

		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)

		actual, err = prometheus.ParseMetrics(body)
		require.NoError(t, err)

		if string(expected.ToBytes()) == string(actual.ToBytes()) {
			return // Success
		}

		// Metrics may not have been processed yet - sleep and try again.
		time.Sleep(250 * time.Millisecond)
	}

	t.Fatalf("Failed to see expected metrics. Wanted:\n%s\nLast saw:\n%s", expected.ToBytes(), actual.ToBytes())
}
