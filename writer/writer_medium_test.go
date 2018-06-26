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

package writer

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/nats-io/go-nats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/spouttest"
	"github.com/jumptrading/influx-spout/stats"
)

const natsPort = 44200
const influxPort = 44201
const probePort = 44202

var natsAddress = fmt.Sprintf("nats://127.0.0.1:%d", natsPort)

func testConfig() *config.Config {
	return &config.Config{
		Mode:               "writer",
		Name:               "foo",
		NATSAddress:        natsAddress,
		NATSSubject:        []string{"writer-test"},
		NATSSubjectMonitor: "writer-test-monitor",
		InfluxDBAddress:    "localhost",
		InfluxDBPort:       influxPort,
		DBName:             "metrics",
		BatchMaxCount:      1,
		BatchMaxSize:       10 * datasize.MB,
		BatchMaxAge:        config.Duration{5 * time.Minute},
		Port:               influxPort,
		Workers:            96,
		NATSMaxPendingSize: 32 * datasize.MB,
		ProbePort:          probePort,
	}
}

func TestBasicWriter(t *testing.T) {
	stats.SetHostname("h")

	nc, closeNATS := runGnatsd(t)
	defer closeNATS()

	influxd := runTestInfluxd()
	defer influxd.Stop()

	// No filter rules.
	conf := testConfig()
	w := startWriter(t, conf)
	defer w.Stop()

	// Subscribe to stats output.
	monitorCh := make(chan string, 10)
	_, err := nc.Subscribe(conf.NATSSubjectMonitor, func(msg *nats.Msg) {
		monitorCh <- string(msg.Data)
	})
	require.NoError(t, err)

	// Publish 5 messages to the bus.
	subject := conf.NATSSubject[0]
	publish(t, nc, subject, "To be, or not to be: that is the question:")
	publish(t, nc, subject, "Whether ’tis nobler in the mind to suffer")
	publish(t, nc, subject, "The slings and arrows of outrageous fortune,")
	publish(t, nc, subject, "Or to take arms against a sea of troubles,")
	publish(t, nc, subject, "And by opposing end them. To die: to sleep;")

	// Wait for confirmation that they were written.
	timeout := time.After(spouttest.LongWait)
	for i := 0; i < 5; i++ {
		select {
		case <-influxd.Writes:
		case <-timeout:
			t.Fatal("timed out waiting for messages")
		}
	}

	// Check the monitor output.
	labels := "{" + strings.Join([]string{
		`component="writer"`,
		`host="h"`,
		`influxdb_address="localhost"`,
		`influxdb_dbname="metrics"`,
		fmt.Sprintf(`influxdb_port="%d"`, influxPort),
		`name="foo"`,
	}, ",") + "}"
	spouttest.AssertMonitor(t, monitorCh, []string{
		`received` + labels + ` 5`,
		`write_requests` + labels + ` 5`,
		`failed_writes` + labels + ` 0`,
	})
}

func TestBatchMBLimit(t *testing.T) {
	nc, closeNATS := runGnatsd(t)
	defer closeNATS()

	influxd := runTestInfluxd()
	defer influxd.Stop()

	// No filter rules.
	conf := testConfig()
	conf.Workers = 1
	conf.BatchMaxCount = 9999
	conf.BatchMaxSize = 1 * datasize.MB
	w := startWriter(t, conf)
	defer w.Stop()

	// Send 4 large chunks which will exactly hit BatchMaxMB.
	const totalSize = 1024 * 1024 // 1 MB
	const chunks = 4
	large := make([]byte, totalSize/chunks)
	for i := range large {
		large[i] = byte('x')
	}
	for i := 0; i < chunks; i++ {
		publish(t, nc, conf.NATSSubject[0], string(large))
	}

	// the messages should come through (in one batch) because
	// BatchMaxMB is exceed
	select {
	case msg := <-influxd.Writes:
		assert.Len(t, msg, totalSize)
	case <-time.After(spouttest.LongWait):
		t.Fatal("timed out waiting for messages")
	}
	influxd.AssertNoWrite(t)
}

func TestBatchTimeLimit(t *testing.T) {
	nc, closeNATS := runGnatsd(t)
	defer closeNATS()

	influxd := runTestInfluxd()
	defer influxd.Stop()

	// No filter rules.
	conf := testConfig()
	conf.Workers = 1
	conf.BatchMaxCount = 9999
	conf.BatchMaxAge = config.Duration{time.Second}
	w := startWriter(t, conf)
	defer w.Stop()

	// Send one small message. It should still come through because of
	// BatchMaxAge.
	publish(t, nc, conf.NATSSubject[0], "foo")

	influxd.AssertWrite(t, "foo")
	influxd.AssertNoWrite(t)
}

func TestBasicFilterRule(t *testing.T) {
	nc, closeNATS := runGnatsd(t)
	defer closeNATS()

	influxd := runTestInfluxd()
	defer influxd.Stop()

	conf := testConfig()
	conf.Rule = []config.Rule{{
		Rtype: "basic",
		Match: "foo",
	}}
	w := startWriter(t, conf)
	defer w.Stop()

	// Send 2 messages, the first of which should be dropped.
	publish(t, nc, conf.NATSSubject[0], "should be dropped")
	publish(t, nc, conf.NATSSubject[0], "foo bar")

	influxd.AssertWrite(t, "foo bar")
	influxd.AssertNoWrite(t)
}

func TestBatchedInput(t *testing.T) {
	nc, closeNATS := runGnatsd(t)
	defer closeNATS()

	influxd := runTestInfluxd()
	defer influxd.Stop()

	conf := testConfig()
	conf.Rule = []config.Rule{{
		Rtype: "basic",
		Match: "foo",
	}}
	w := startWriter(t, conf)
	defer w.Stop()

	// Send 2 messages together, the first of which should be dropped.
	publish(t, nc, conf.NATSSubject[0], "should be dropped\nfoo bar")

	influxd.AssertWrite(t, "foo bar")
	influxd.AssertNoWrite(t)
}

func TestRegexFilterRule(t *testing.T) {
	nc, closeNATS := runGnatsd(t)
	defer closeNATS()

	influxd := runTestInfluxd()
	defer influxd.Stop()

	conf := testConfig()
	conf.Rule = []config.Rule{{
		Rtype: "regex",
		Match: "bar$",
	}}
	w := startWriter(t, conf)
	defer w.Stop()

	// Send 2 messages, the first of which should be dropped.
	publish(t, nc, conf.NATSSubject[0], "should be dropped")
	publish(t, nc, conf.NATSSubject[0], "foo bar")

	influxd.AssertWrite(t, "foo bar")
	influxd.AssertNoWrite(t)
}

func TestNegativeRegexFilterRule(t *testing.T) {
	nc, closeNATS := runGnatsd(t)
	defer closeNATS()

	influxd := runTestInfluxd()
	defer influxd.Stop()

	conf := testConfig()
	conf.Rule = []config.Rule{{
		Rtype: "negregex",
		Match: "dropped$",
	}}
	w := startWriter(t, conf)
	defer w.Stop()

	// Send 2 messages, the first of which should be dropped.
	publish(t, nc, conf.NATSSubject[0], "should be dropped")
	publish(t, nc, conf.NATSSubject[0], "foo bar")

	influxd.AssertWrite(t, "foo bar")
	influxd.AssertNoWrite(t)
}

func BenchmarkWriterLatency(b *testing.B) {
	spouttest.SuppressLogs()
	defer spouttest.RestoreLogs()

	nc, closeNATS := runGnatsd(b)
	defer closeNATS()

	influxd := runTestInfluxd()
	defer influxd.Stop()

	conf := testConfig()
	w := startWriter(b, conf)
	defer w.Stop()

	byteArr := []byte("Microsoft: \"You've got questions. We've got dancing paperclips.\" ")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nc.Publish(conf.NATSSubject[0], byteArr)
		<-influxd.Writes
	}
}

type FatalTestingT interface {
	Fatalf(string, ...interface{})
}

func runGnatsd(t FatalTestingT) (*nats.Conn, func()) {
	gnatsd := spouttest.RunGnatsd(natsPort)

	nc, err := nats.Connect(natsAddress)
	if err != nil {
		gnatsd.Shutdown()
		t.Fatalf("NATS connect failed: %v", err)
	}

	return nc, func() {
		nc.Close()
		gnatsd.Shutdown()
	}
}

func startWriter(t require.TestingT, conf *config.Config) *Writer {
	w, err := StartWriter(conf)
	require.NoError(t, err)
	if !spouttest.CheckReadyProbe(conf.ProbePort) {
		w.Stop()
		t.Errorf("writer not ready")
		t.FailNow()
	}
	return w
}

func publish(t require.TestingT, nc *nats.Conn, subject, msg string) {
	err := nc.Publish(subject, []byte(msg))
	require.NoError(t, err)
}

type testInfluxd struct {
	server *http.Server
	wg     sync.WaitGroup
	Writes chan string
	ready  chan struct{}
}

func runTestInfluxd() *testInfluxd {
	s := &testInfluxd{
		server: &http.Server{
			Addr: fmt.Sprintf(":%d", influxPort),
		},
		Writes: make(chan string, 99),
		ready:  make(chan struct{}),
	}

	s.wg.Add(1)
	go s.run()
	select {
	case <-s.ready:
	case <-time.After(spouttest.LongWait):
		panic("testInfluxd failed to start")
	}

	return s
}

func (s *testInfluxd) Stop() {
	s.server.Close()
	s.wg.Wait()
}

func (s *testInfluxd) AssertWrite(t *testing.T, expected string) {
	select {
	case msg := <-s.Writes:
		assert.Equal(t, msg, expected)
	case <-time.After(spouttest.LongWait):
		t.Fatal("timed out waiting for message")
	}
}

func (s *testInfluxd) AssertNoWrite(t *testing.T) {
	select {
	case msg := <-s.Writes:
		t.Fatalf("saw unexpected write: %q", msg)
	case <-time.After(spouttest.ShortWait):
	}
}

func (s *testInfluxd) run() {
	defer s.wg.Done()

	mux := http.NewServeMux()
	mux.HandleFunc("/write", s.handleWrite)
	s.server.Handler = mux
	close(s.ready)
	s.server.ListenAndServe()
}

func (s *testInfluxd) handleWrite(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(fmt.Sprintf("Body read: %s", err))
	}
	w.WriteHeader(http.StatusNoContent)

	s.Writes <- string(body)
}
