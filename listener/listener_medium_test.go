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
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/spouttest"
)

const (
	natsPort    = 44444
	listenPort  = 44445
	natsSubject = "listener-test"
)

var (
	natsAddress = fmt.Sprintf("nats://127.0.0.1:%d", natsPort)
)

func testConfig() *config.Config {
	return &config.Config{
		Mode:               "listener",
		NATSAddress:        natsAddress,
		NATSSubject:        []string{natsSubject},
		NATSSubjectMonitor: natsSubject + "-monitor",
		BatchMessages:      1,
		Port:               listenPort,
	}
}

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	s := spouttest.RunGnatsd(natsPort)
	defer s.Shutdown()
	return m.Run()
}

func TestBatching(t *testing.T) {
	conf := testConfig()
	conf.BatchMessages = 5 // batch 5 messages into one packet
	stop := startTestListener(t, conf)
	defer stop()

	newMsg, unsubscribe := subscribe(t)
	defer unsubscribe()

	go func() {
		conn := dialListener(t)
		defer conn.Close()

		_, err := conn.Write([]byte(`Midnight Song of the Seasons: Autumn Song`))
		require.NoError(t, err)
		_, err = conn.Write([]byte(`The autumn wind enters through the window,`))
		require.NoError(t, err)
		_, err = conn.Write([]byte(`The gauze curtain starts to flutter and fly.`))
		require.NoError(t, err)
		_, err = conn.Write([]byte(`I raise my head and look at the bright moon,`))
		require.NoError(t, err)
		_, err = conn.Write([]byte(`And send my feelings a thousand miles in its light.`))
		require.NoError(t, err)
	}()

	// we should receive a message
	select {
	case <-newMsg:
		break
	case <-time.After(spouttest.LongWait):
		t.Fatal("failed to see message")
	}

	// and, we should only receive one!
	select {
	case <-newMsg:
		t.Fatal("unexpectedly saw message")
	case <-time.After(spouttest.ShortWait):
	}
}

func TestWhatComesAroundGoesAround(t *testing.T) {
	stop := startTestListener(t, testConfig())
	defer stop()

	newMsg, unsubscribe := subscribe(t)
	defer unsubscribe()

	go func() {
		conn := dialListener(t)
		defer conn.Close()

		// send 5 messages
		_, err := conn.Write([]byte("Beatrice. I am stuffed, cousin, I cannot smell.\n"))
		require.NoError(t, err)
		_, err = conn.Write([]byte("Margaret. A maid, and stuffed! There's goodly catching of cold.\n"))
		require.NoError(t, err)
		_, err = conn.Write([]byte("Hast thou not dragged Diana from her car, \n"))
		require.NoError(t, err)
		_, err = conn.Write([]byte("And driven the hamadryad from the wood \n"))
		require.NoError(t, err)
		_, err = conn.Write([]byte("To seek a shelter in some happier star?\n"))
		require.NoError(t, err)
	}()

	// check that 5 messages came through
	for i := 0; i < 5; i++ {
		<-newMsg
	}
}

func TestBatchBufferFull(t *testing.T) {
	conf := testConfig()
	// Set batch size high so that the batch will only send due to the
	// batch buffer filling up.
	conf.BatchMessages = 99999
	stop := startTestListener(t, conf)
	defer stop()

	newMsg, unsubscribe := subscribe(t)
	defer unsubscribe()

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
		case <-newMsg:
			break loop
		case <-time.After(time.Microsecond):
			// Send again
		case <-timeout:
			t.Fatal("no message seen")
		}
	}

	// Ensure that batch was output because batch size limit was
	// reached, not the message count.
	assert.True(t, writeCount < conf.BatchMessages,
		fmt.Sprintf("writeCount = %d", writeCount))

	// and, we should only receive one!
	select {
	case <-newMsg:
		t.Fatal("message unexpectedly seen")
	case <-time.After(spouttest.ShortWait):
		return
	}
}

func TestStatistician(t *testing.T) {
	// Create a listener with some stats already set up.
	conf := testConfig()
	listener := newListener(conf)
	listener.stats.Inc(linesReceived)
	listener.stats.Inc(linesReceived)
	listener.stats.Inc(linesReceived)
	listener.stats.Inc(batchesSent)
	listener.stats.Inc(batchesSent)
	listener.stats.Inc(readErrors)

	// Subscribe to the statistician output.
	nc, err := nats.Connect(natsAddress)
	require.NoError(t, err)

	statsCh := make(chan string)
	sub, err := nc.Subscribe(conf.NATSSubjectMonitor, func(msg *nats.Msg) {
		statsCh <- string(msg.Data)
	})
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	// Start statistician
	stop := make(chan struct{})
	go listener.startStatistician(stop)

	// Look for expected stats
	select {
	case received := <-statsCh:
		assert.Equal(t, "spout_stat_listener received=3,sent=2,read_errors=1\n", received)
	case <-time.After(spouttest.LongWait):
		t.Fatal("no message seen")
	}

	// Stop the statistician
	sub.Unsubscribe()
	close(stop)
}

func BenchmarkListenerLatency(b *testing.B) {
	stop := startTestListener(b, testConfig())
	defer stop()

	newMsg, unsubscribe := subscribe(b)
	defer unsubscribe()

	conn := dialListener(b)
	defer conn.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Write([]byte("git - the stupid content tracker"))
		<-newMsg
	}
}

// startTestListener starts a listener that can be stopped. When the
// returned value is called the listener will be stoppped.
func startTestListener(t require.TestingT, conf *config.Config) func() {
	listener := newListener(conf)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			listener.readUDP()
		}
	}()

	return func() {
		close(stop)

		// The listener needs to read something in order to see that
		// the stop channel was closed.
		conn := dialListener(t)
		conn.Write([]byte("die"))
		conn.Close()

		wg.Wait()
		listener.close()
	}
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

// subscribe sets up a subject callback for natsSubject. A channel is
// returned which reports when messages are received. A function to
// cancel the subcription is also returned.
func subscribe(t require.TestingT) (chan struct{}, func() error) {
	nc, err := nats.Connect(natsAddress)
	require.NoError(t, err)

	msgCh := make(chan struct{}, 10)
	sub, err := nc.Subscribe(natsSubject, func(msg *nats.Msg) {
		msgCh <- struct{}{}
	})
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	return msgCh, sub.Unsubscribe
}
