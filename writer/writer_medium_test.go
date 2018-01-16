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
	"os"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/relaytest"
)

var httpWrites = make(chan string, 10)
var nc *nats.Conn

const natsPort = 44443
const influxPort = 44445

var natsAddress = fmt.Sprintf("nats://127.0.0.1:%d", natsPort)

func testConfig() *config.Config {
	return &config.Config{
		NATSAddress:        natsAddress,
		NATSSubject:        []string{"writer-test"},
		NATSSubjectMonitor: "writer-test-monitor",
		InfluxDBAddress:    "localhost",
		InfluxDBPort:       influxPort,
		BatchMessages:      1,
		BatchMaxMB:         10,
		BatchMaxSecs:       300,
		Port:               influxPort,
		Mode:               "writer",
		WriterWorkers:      96,
		NATSPendingMaxMB:   32,
	}
}

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	var err error

	// Start gnatsd.
	s := relaytest.RunGnatsd(natsPort)
	defer s.Shutdown()

	// Set up a dummy HTTP server to write to.
	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("Body read: %v\n", err)
		}
		httpWrites <- string(body)
		w.WriteHeader(http.StatusNoContent)
	})
	go http.ListenAndServe(fmt.Sprintf(":%d", influxPort), nil)

	// connect to the NATS instance
	nc, err = nats.Connect(natsAddress)
	if err != nil {
		fmt.Printf("Error while setup: %v\n", err)
		return 1
	}
	defer nc.Close()

	return m.Run()
}

func TestBasicWriter(t *testing.T) {
	// No filter rules.
	conf := testConfig()
	w := startWriter(t, conf)
	defer w.Stop()

	// publish 5 messages to the bus
	subject := conf.NATSSubject[0]
	publish(t, subject, "To be, or not to be: that is the question:")
	publish(t, subject, "Whether ’tis nobler in the mind to suffer")
	publish(t, subject, "The slings and arrows of outrageous fortune,")
	publish(t, subject, "Or to take arms against a sea of troubles,")
	publish(t, subject, "And by opposing end them. To die: to sleep;")

	// wait for confirmation that they were written
	timeout := time.After(relaytest.LongWait)
	for i := 0; i < 5; i++ {
		select {
		case <-httpWrites:
		case <-timeout:
			t.Fatal("timed out waiting for messages")
		}
	}
}

func TestBatchMBLimit(t *testing.T) {
	// No filter rules.
	conf := testConfig()
	conf.WriterWorkers = 1
	conf.BatchMessages = 9999
	conf.BatchMaxMB = 1
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
		publish(t, conf.NATSSubject[0], string(large))
	}

	// the messages should come through (in one batch) because
	// BatchMaxMB is exceed
	select {
	case msg := <-httpWrites:
		assert.Len(t, msg, totalSize)
	case <-time.After(relaytest.LongWait):
		t.Fatal("timed out waiting for messages")
	}
	assertNoWrite(t)
}

func TestBasicFilterRule(t *testing.T) {
	conf := testConfig()
	conf.Rule = []config.Rule{{
		Rtype: "basic",
		Match: "foo",
	}}
	w := startWriter(t, conf)
	defer w.Stop()

	// Send 2 messages, the first of which should be dropped.
	publish(t, conf.NATSSubject[0], "should be dropped")
	publish(t, conf.NATSSubject[0], "foo bar")

	assertWrite(t, "foo bar")
	assertNoWrite(t)
}

func TestRegexFilterRule(t *testing.T) {
	conf := testConfig()
	conf.Rule = []config.Rule{{
		Rtype: "regex",
		Match: "bar$",
	}}
	w := startWriter(t, conf)
	defer w.Stop()

	// Send 2 messages, the first of which should be dropped.
	publish(t, conf.NATSSubject[0], "should be dropped")
	publish(t, conf.NATSSubject[0], "foo bar")

	assertWrite(t, "foo bar")
	assertNoWrite(t)
}

func TestNegativeRegexFilterRule(t *testing.T) {
	conf := testConfig()
	conf.Rule = []config.Rule{{
		Rtype: "negregex",
		Match: "dropped$",
	}}
	w := startWriter(t, conf)
	defer w.Stop()

	// Send 2 messages, the first of which should be dropped.
	publish(t, conf.NATSSubject[0], "should be dropped")
	publish(t, conf.NATSSubject[0], "foo bar")

	assertWrite(t, "foo bar")
	assertNoWrite(t)
}

func BenchmarkWriterLatency(b *testing.B) {
	conf := testConfig()
	w := startWriter(b, conf)
	defer w.Stop()

	byteArr := []byte("Microsoft: \"You've got questions. We've got dancing paperclips.\" ")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nc.Publish(conf.NATSSubject[0], byteArr)
		<-httpWrites
	}
}

func startWriter(t require.TestingT, conf *config.Config) *Writer {
	w, err := StartWriter(conf)
	require.NoError(t, err)
	return w
}

func publish(t require.TestingT, subject, msg string) {
	err := nc.Publish(subject, []byte(msg))
	require.NoError(t, err)
}

func assertWrite(t *testing.T, expected string) {
	select {
	case msg := <-httpWrites:
		assert.Equal(t, msg, expected)
	case <-time.After(relaytest.LongWait):
		t.Fatal("timed out waiting for message")
	}
}

func assertNoWrite(t *testing.T) {
	select {
	case msg := <-httpWrites:
		t.Fatalf("saw unexpected write: %q", msg)
	case <-time.After(relaytest.ShortWait):
	}
}
