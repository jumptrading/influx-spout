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

package listener

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/spouttest"
)

const (
	natsPort           = 44000
	listenPort         = 44001
	probePort          = 44002
	natsSubject        = "listener-test"
	natsMonitorSubject = natsSubject + "-monitor"
)

var (
	natsAddress = fmt.Sprintf("nats://127.0.0.1:%d", natsPort)

	poetry = []string{
		"Midnight Song of the Seasons: Autumn Song\n",
		"The autumn wind enters through the window,\n",
		"The gauze curtain starts to flutter and fly.\n",
		"I raise my head and look at the bright moon,\n",
		"And send my feelings a thousand miles in its light.\n",
	}
	numLines = len(poetry)
)

func init() {
	// Make the statistician report more often during tests (default
	// is 3s). This makes the tests run faster.
	statsInterval = 500 * time.Millisecond
}

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()
	return m.Run()
}

func testConfig() *config.Config {
	return &config.Config{
		Mode:               "listener",
		Name:               "testlistener",
		NATSAddress:        natsAddress,
		NATSSubject:        []string{natsSubject},
		NATSSubjectMonitor: natsMonitorSubject,
		BatchMessages:      1,
		ReadBufferBytes:    4 * 1024 * 1024,
		ListenerBatchBytes: 1024 * 1024,
		Port:               listenPort,
		ProbePort:          probePort,
	}
}

func TestBatching(t *testing.T) {
	conf := testConfig()
	conf.BatchMessages = numLines // batch messages into one packet

	listener := startListener(t, conf)
	defer listener.Stop()

	listenerCh, unsubListener := subListener(t)
	defer unsubListener()

	monitorCh, unsubMonitor := subMonitor(t)
	defer unsubMonitor()

	// Send some lines to the listener.
	conn := dialListener(t)
	defer conn.Close()
	for _, line := range poetry {
		_, err := conn.Write([]byte(line))
		require.NoError(t, err)
	}

	// Should receive a single batch.
	assertBatch(t, listenerCh, strings.Join(poetry, ""))
	assertNoMore(t, listenerCh)

	assertMonitor(t, monitorCh, numLines, 1)
}

func TestWhatComesAroundGoesAround(t *testing.T) {
	listener := startListener(t, testConfig())
	defer listener.Stop()

	listenerCh, unsubListener := subListener(t)
	defer unsubListener()

	monitorCh, unsubMonitor := subMonitor(t)
	defer unsubMonitor()

	// Send some lines to the listener.
	conn := dialListener(t)
	defer conn.Close()
	for _, line := range poetry {
		_, err := conn.Write([]byte(line))
		require.NoError(t, err)
	}

	for i := 0; i < numLines; i++ {
		assertBatch(t, listenerCh, poetry[i])
	}
	assertNoMore(t, listenerCh)

	assertMonitor(t, monitorCh, numLines, numLines)
}

func TestBatchBufferFull(t *testing.T) {
	conf := testConfig()
	// Set batch size high so that the batch will only send due to the
	// batch buffer filling up.
	conf.BatchMessages = 99999

	listener := startListener(t, conf)
	defer listener.Stop()

	listenerCh, unsubListener := subListener(t)
	defer unsubListener()

	// Keep sending to the listener until it emits a batch.
	conn := dialListener(t)
	defer conn.Close()
	msg := make([]byte, 100)
	timeout := time.After(spouttest.LongWait)
	writeCount := 0
loop:
	for {
		_, err := conn.Write(msg)
		require.NoError(t, err)
		writeCount++

		select {
		case <-listenerCh:
			break loop
		case <-time.After(time.Microsecond):
			// Send again
		case <-timeout:
			t.Fatal("no message seen")
		}
	}

	assertNoMore(t, listenerCh)

	// Ensure that batch was output because batch size limit was
	// reached, not the message count.
	assert.True(t, writeCount < conf.BatchMessages,
		fmt.Sprintf("writeCount = %d", writeCount))
}

func TestHTTPListener(t *testing.T) {
	conf := testConfig()
	listener, err := StartHTTPListener(conf)
	require.NoError(t, err)
	spouttest.AssertReadyProbe(t, conf.ProbePort)
	defer listener.Stop()

	listenerCh, unsubListener := subListener(t)
	defer unsubListener()

	monitorCh, unsubMonitor := subMonitor(t)
	defer unsubMonitor()

	// Send some lines to the listener.
	url := fmt.Sprintf("http://localhost:%d/write", listenPort)
	for _, line := range poetry {
		_, err := http.Post(url, "text/plain", bytes.NewBufferString(line))
		require.NoError(t, err)
	}

	for i := 0; i < numLines; i++ {
		assertBatch(t, listenerCh, poetry[i])
	}
	assertNoMore(t, listenerCh)

	assertMonitor(t, monitorCh, numLines, numLines)
}

func TestHTTPListenerBigPOST(t *testing.T) {
	conf := testConfig()
	conf.ListenerBatchBytes = 1024
	// Use a batch size > 1. Even though a single write will be made,
	// the batch should still get sent because the buffer size limit
	// is exceeded.
	conf.BatchMessages = 10

	listener, err := StartHTTPListener(conf)
	require.NoError(t, err)
	spouttest.AssertReadyProbe(t, conf.ProbePort)
	defer listener.Stop()

	listenerCh, unsubListener := subListener(t)
	defer unsubListener()

	monitorCh, unsubMonitor := subMonitor(t)
	defer unsubMonitor()

	// Send a post that's bigger than the configured batch size. This
	// will force the batch buffer to grow.
	buf := make([]byte, conf.ListenerBatchBytes+200)

	url := fmt.Sprintf("http://localhost:%d/write", listenPort)
	_, err = http.Post(url, "text/plain", bytes.NewBuffer(buf))
	require.NoError(t, err)

	assertBatch(t, listenerCh, string(buf))
	assertNoMore(t, listenerCh)

	assertMonitor(t, monitorCh, 1, 1)
}

func BenchmarkListenerLatency(b *testing.B) {
	listener := startListener(b, testConfig())
	defer listener.Stop()

	listenerCh, unsubscribe := subListener(b)
	defer unsubscribe()

	conn := dialListener(b)
	defer conn.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Write([]byte("git - the stupid content tracker"))
		<-listenerCh
	}
	b.StopTimer()
}

func startListener(t require.TestingT, conf *config.Config) *Listener {
	listener, err := StartListener(conf)
	require.NoError(t, err)
	if !spouttest.CheckReadyProbe(conf.ProbePort) {
		listener.Stop()
		t.Errorf("listener not ready")
		t.FailNow()
	}
	return listener
}

// dialListener creates a UDP connection to the listener's inbound port.
func dialListener(t require.TestingT) *net.UDPConn {
	saddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("0.0.0.0:%d", listenPort))
	require.NoError(t, err)
	laddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	require.NoError(t, err)
	conn, err := net.DialUDP("udp", laddr, saddr)
	require.NoError(t, err)
	return conn
}

func subListener(t require.TestingT) (chan string, func()) {
	return subscribe(t, natsSubject)
}

func subMonitor(t require.TestingT) (chan string, func()) {
	return subscribe(t, natsMonitorSubject)
}

func subscribe(t require.TestingT, subject string) (chan string, func()) {
	nc, err := nats.Connect(natsAddress)
	require.NoError(t, err)

	msgCh := make(chan string, 10)
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		msgCh <- string(msg.Data)
	})
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	return msgCh, func() {
		sub.Unsubscribe()
		nc.Close()
	}
}

func assertBatch(t *testing.T, ch chan string, expected string) {
	select {
	case received := <-ch:
		assert.Equal(t, expected, received)
	case <-time.After(spouttest.LongWait):
		t.Fatal("failed to see message")
	}
}

func assertNoMore(t *testing.T, ch chan string) {
	select {
	case <-ch:
		t.Fatal("unexpectedly saw message")
	case <-time.After(spouttest.ShortWait):
	}
}

func assertMonitor(t *testing.T, monitorCh chan string, received, sent int) {
	expected := []string{
		fmt.Sprintf(`received{component="listener",name="testlistener"} %d`, received),
		fmt.Sprintf(`sent{component="listener",name="testlistener"} %d`, sent),
		`read_errors{component="listener",name="testlistener"} 0`,
		`failed_nats_publish{component="listener",name="testlistener"} 0`,
	}
	spouttest.AssertMonitor(t, monitorCh, expected)
}
