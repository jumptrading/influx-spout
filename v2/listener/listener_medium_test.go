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
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/v2/config"
	"github.com/jumptrading/influx-spout/v2/spouttest"
	"github.com/jumptrading/influx-spout/v2/stats"
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

func testConfig() *config.Config {
	return &config.Config{
		Mode:               "listener",
		Name:               "testlistener",
		NATSAddress:        natsAddress,
		NATSSubject:        []string{natsSubject},
		NATSSubjectMonitor: natsMonitorSubject,
		BatchMaxCount:      1,
		BatchMaxSize:       1 * datasize.MB,
		BatchMaxAge:        config.NewDuration(60 * time.Second),
		ReadBufferSize:     4 * datasize.MB,

		Port:      listenPort,
		ProbePort: probePort,

		// Make the statistician report more often during tests. This
		// makes the tests run faster.
		StatsInterval: config.NewDuration(250 * time.Millisecond),
	}
}

func TestBatching(t *testing.T) {
	stats.SetHostname("h")

	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

	conf := testConfig()
	conf.BatchMaxCount = numLines // batch messages into one packet

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
	spouttest.AssertNoMore(t, listenerCh)

	assertMonitor(t, monitorCh, numLines, 1)
}

func TestWhatComesAroundGoesAround(t *testing.T) {
	stats.SetHostname("h")

	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

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
	spouttest.AssertNoMore(t, listenerCh)

	assertMonitor(t, monitorCh, numLines, numLines)
}

func TestBatchBufferFull(t *testing.T) {
	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

	conf := testConfig()
	// Set batch size high so that the batch will only send due to the
	// batch buffer filling up.
	conf.BatchMaxCount = 99999

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

	spouttest.AssertNoMore(t, listenerCh)

	// Ensure that batch was output because batch size limit was
	// reached, not the message count.
	assert.True(t, writeCount < conf.BatchMaxCount,
		fmt.Sprintf("writeCount = %d", writeCount))
}

func TestBatchAge(t *testing.T) {
	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

	// Set config so that a small write will only come through due to
	// batch age expiry.
	conf := testConfig()
	conf.BatchMaxCount = 9999
	conf.BatchMaxAge = config.NewDuration(time.Second)

	listener := startListener(t, conf)
	defer listener.Stop()

	listenerCh, unsubListener := subListener(t)
	defer unsubListener()

	// Send a single line to the listener.
	conn := dialListener(t)
	defer conn.Close()
	line := poetry[0]
	_, err := conn.Write([]byte(line))
	require.NoError(t, err)

	// Line should be emitted after 1 sec.
	assertBatch(t, listenerCh, line)
	spouttest.AssertNoMore(t, listenerCh)
}

func TestMissingNewline(t *testing.T) {
	stats.SetHostname("h")

	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

	lines := []string{
		"one\ntwo",
		"three",
		"four\n",
		"five",
	}
	conf := testConfig()
	conf.BatchMaxCount = len(lines)
	listener := startListener(t, conf)
	defer listener.Stop()

	listenerCh, unsubListener := subListener(t)
	defer unsubListener()

	// Send lines, some without a newline at the end.
	conn := dialListener(t)
	defer conn.Close()
	for _, line := range lines {
		_, err := conn.Write([]byte(line))
		require.NoError(t, err)
	}

	assertBatch(t, listenerCh, "one\ntwo\nthree\nfour\nfive\n")
	spouttest.AssertNoMore(t, listenerCh)
}

func TestHTTPListener(t *testing.T) {
	stats.SetHostname("h")

	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

	conf := testConfig()
	listener := startHTTPListener(t, conf)
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
	spouttest.AssertNoMore(t, listenerCh)

	assertMonitor(t, monitorCh, numLines, numLines)
}

func TestHTTPListenerBigPOST(t *testing.T) {
	stats.SetHostname("h")

	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

	conf := testConfig()
	conf.BatchMaxSize = 1024 * datasize.B
	// Use a batch size > 1. Even though a single write will be made,
	// the batch should still get sent because the buffer size limit
	// is exceeded.
	conf.BatchMaxCount = 10

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
	buf := make([]byte, conf.BatchMaxSize.Bytes()+200)

	url := fmt.Sprintf("http://localhost:%d/write", listenPort)
	_, err = http.Post(url, "text/plain", bytes.NewBuffer(buf))
	require.NoError(t, err)

	assertBatch(t, listenerCh, string(buf))
	spouttest.AssertNoMore(t, listenerCh)

	assertMonitor(t, monitorCh, 1, 1)
}

func TestHTTPListenerConcurrency(t *testing.T) {
	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

	conf := testConfig()
	listener := startHTTPListener(t, conf)
	defer listener.Stop()

	listenerCh, unsubListener := subListener(t)
	defer unsubListener()

	// Send the same line many times from multiple goroutines.
	const senders = 10
	const sendCount = 100
	const totalLines = senders * sendCount
	sendLine := fmt.Sprintf("cpu load=0.69 foo=bar %d\n", time.Now().UnixNano())

	url := fmt.Sprintf("http://localhost:%d/write", listenPort)
	errs := make(chan error, senders)
	for sender := 0; sender < senders; sender++ {
		go func() {
			client := new(http.Client)
			for i := 0; i < sendCount; i++ {
				_, err := client.Post(url, "text/plain", bytes.NewBufferString(sendLine))
				if err != nil {
					errs <- err
				}
			}
			errs <- nil
		}()
	}

	// Wait for the senders to be done sending, and all the lines to
	// be returned.
	sendersDone := 0
	received := 0
	timeout := time.After(spouttest.LongWait)
	for received < totalLines || sendersDone < senders {
		select {
		case lines := <-listenerCh:
			for _, line := range strings.SplitAfter(lines, "\n") {
				if len(line) > 0 {
					require.Equal(t, sendLine, line)
					received++
				}
			}
		case err := <-errs:
			require.NoError(t, err)
			sendersDone++
		case <-timeout:
			t.Fatal("timed out waiting for lines")
		}
	}

	spouttest.AssertNoMore(t, listenerCh)
}

func TestHTTPListenerWithPrecision(t *testing.T) {
	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

	conf := testConfig()
	listener := startHTTPListener(t, conf)
	defer listener.Stop()

	listenerCh, unsubListener := subListener(t)
	defer unsubListener()

	// Construct lines with timestamps. Seconds based timestamps will
	// be sent into the listener which it should convert to nanosecond
	// based timestamps on the way out.
	in := bytes.NewBuffer(nil)
	out := bytes.NewBuffer(nil)
	for i, line := range poetry {
		if i == 0 {
			// No timestamp on the first line.
			in.WriteString(line)
			out.WriteString(line)
		} else {
			line = strings.TrimRight(line, "\n")
			secs := int64(1500000000 + i)
			nanos := secs * int64(time.Second)
			in.WriteString(line + fmt.Sprintf(" %d\n", secs))
			out.WriteString(line + fmt.Sprintf(" %d\n", nanos))
		}
	}

	// Send the input lines.
	url := fmt.Sprintf("http://localhost:%d/write?precision=s", listenPort)
	_, err := http.Post(url, "text/plain", in)
	require.NoError(t, err)

	// Check for the expected output.
	assertBatch(t, listenerCh, out.String())
	spouttest.AssertNoMore(t, listenerCh)
}

func TestBatchAgeHTTPListener(t *testing.T) {
	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

	// Set config so that a small write will only come through due to
	// batch age expiry.
	conf := testConfig()
	conf.BatchMaxCount = 9999
	conf.BatchMaxAge = config.NewDuration(time.Second)
	listener := startHTTPListener(t, conf)
	defer listener.Stop()

	listenerCh, unsubListener := subListener(t)
	defer unsubListener()

	// Send a single line.
	line := poetry[0]
	url := fmt.Sprintf("http://localhost:%d/write", listenPort)
	_, err := http.Post(url, "text/plain", bytes.NewReader([]byte(line)))
	require.NoError(t, err)

	// Line should be emitted after 1 sec.
	assertBatch(t, listenerCh, line)
	spouttest.AssertNoMore(t, listenerCh)
}

func BenchmarkListenerLatency(b *testing.B) {
	spouttest.SuppressLogs()
	defer spouttest.RestoreLogs()

	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

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

func BenchmarkHTTPListener(b *testing.B) {
	spouttest.SuppressLogs()
	defer spouttest.RestoreLogs()

	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

	listener := startHTTPListener(b, testConfig())
	defer listener.Stop()

	listenerCh, unsubscribe := subListener(b)
	defer unsubscribe()

	url := fmt.Sprintf("http://localhost:%d/write", listenPort)
	line := []byte(fmt.Sprintf("foo bar=2 %d", time.Now().UnixNano()))
	reader := bytes.NewReader(line)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		http.Post(url, "text/plain", reader)
		reader.Seek(0, 0)
		<-listenerCh
	}
	b.StopTimer()
}

func BenchmarkHTTPListenerWithPrecision(b *testing.B) {
	spouttest.SuppressLogs()
	defer spouttest.RestoreLogs()

	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()

	listener := startHTTPListener(b, testConfig())
	defer listener.Stop()

	listenerCh, unsubscribe := subListener(b)
	defer unsubscribe()

	url := fmt.Sprintf("http://localhost:%d/write?precision=s", listenPort)
	line := []byte(fmt.Sprintf("foo bar=2 %d", time.Now().UnixNano()/int64(time.Second)))
	reader := bytes.NewReader(line)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		http.Post(url, "text/plain", reader)
		reader.Seek(0, 0)
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

func startHTTPListener(t require.TestingT, conf *config.Config) *Listener {
	listener, err := StartHTTPListener(conf)
	require.NoError(t, err)
	if !spouttest.CheckReadyProbe(conf.ProbePort) {
		listener.Stop()
		t.Errorf("HTTP listener not ready")
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

func assertMonitor(t *testing.T, monitorCh chan string, received, sent int) {
	expected := []string{
		fmt.Sprintf(`received{component="listener",host="h",name="testlistener"} %d`, received),
		fmt.Sprintf(`sent{component="listener",host="h",name="testlistener"} %d`, sent),
		`read_errors{component="listener",host="h",name="testlistener"} 0`,
		`failed_nats_publish{component="listener",host="h",name="testlistener"} 0`,
	}
	spouttest.AssertMonitor(t, monitorCh, expected)
}
