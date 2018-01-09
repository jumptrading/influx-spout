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

// Package listener defines the functions for the publisher of
// messages to the bus.
package listener

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/nats-io/go-nats"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/lineformatter"
	"github.com/jumptrading/influx-spout/stats"
)

// Listener stats counters
const (
	linesReceived = "lines-received"
	batchesSent   = "batches-sent"
	readErrors    = "read-errors"
)

var allStats = []string{linesReceived, batchesSent, readErrors}

// StartListener initialises a listener, starts its statistician
// goroutine and runs it's main loop. It never returns.
//
// The listener reads incoming UDP packets, batches them up and send
// batches onwards to a NATS subject.
func StartListener(c *config.Config) {
	listener := newListener(c)
	go listener.startStatistician(nil)
	for {
		listener.readUDP()
	}
}

// StartHTTPListener initialises listener configured to accept lines
// from HTTP request bodies instead of via UDP. It starts the listener
// and its statistician and never returns.
func StartHTTPListener(c *config.Config) {
	listener := newHTTPListener(c)
	go listener.startStatistician(nil)

	err := http.ListenAndServe(fmt.Sprintf(":%d", c.Port), nil)
	log.Fatal(err)
}

type listener struct {
	c     *config.Config
	nc    *nats.Conn
	sc    *net.UDPConn
	stats *stats.Stats

	buf           []byte
	batchSize     int
	sendThreshold int
}

// newListener creates and initialises a listener, setting up its UDP
// listener port and connection to NATS.
func newListener(c *config.Config) *listener {
	var err error

	l := &listener{
		c:     c,
		stats: stats.New(allStats...),
	}

	l.connectNATS()

	bufSize := l.setupBuffers()

	// setup the UDP listener socket
	ServerAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", c.Port))
	if err != nil {
		log.Fatalf("Error: failed to create UDP socket: %v\n", err)
	}

	l.sc, err = net.ListenUDP("udp", ServerAddr)
	if err != nil {
		log.Fatalf("Error: failed to bind UDP socket: %v\n", err)
	}
	log.Printf("Listener bound to UDP socket: %v\n", l.sc.LocalAddr().String())

	if err = l.sc.SetReadBuffer(bufSize); err != nil {
		log.Fatal(err)
	}

	l.notifyState("ready")

	return l
}

func newHTTPListener(c *config.Config) *listener {
	l := &listener{
		c:     c,
		stats: stats.New(allStats...),
	}

	l.connectNATS()

	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		sz, err := r.Body.Read(l.buf[l.batchSize:])
		if err != nil {
			l.stats.Inc(readErrors)
		}
		// Attempt to process the read even on error has Read may
		// still have read some bytes successfully.
		l.processRead(sz)
	})

	l.setupBuffers()

	l.notifyState("ready")

	return l
}

func (l *listener) setupBuffers() int {
	// create a buffer to read incoming UDP packets into,
	// make it at least a page to get optimal performance
	bufSize := 32 * os.Getpagesize()

	l.buf = make([]byte, bufSize)
	l.sendThreshold = bufSize - 2048

	return bufSize
}

var notifyLine = lineformatter.New("relay_mon", nil, "type", "state", "pid")

func (l *listener) notifyState(state string) {
	line := notifyLine.Format(nil, "listener", state, os.Getpid())
	if err := l.nc.Publish(l.c.NATSSubjectMonitor, line); err != nil {
		l.handleNatsError(err)
		return
	}
	if err := l.nc.Flush(); err != nil {
		l.handleNatsError(err)
	}
}

func (l *listener) connectNATS() {
	var err error

	// connect to the NATS instance running on this machine
	l.nc, err = nats.Connect(l.c.NATSAddress)
	if err != nil {
		log.Fatal(err)
	}

	// If we disconnect, we want to try reconnecting as many times as
	// we can.
	l.nc.Opts.MaxReconnect = -1

	l.notifyState("boot")
}

func (l *listener) readUDP() {
	sz, _, err := l.sc.ReadFromUDP(l.buf[l.batchSize:])
	if err != nil {
		l.stats.Inc(readErrors)
	}
	// Attempt to process the read even on error has Read may
	// still have read some bytes successfully.
	l.processRead(sz)
}

func (l *listener) processRead(sz int) {
	if sz < 1 {
		return // Empty read
	}

	linesReceived := l.stats.Inc(linesReceived)
	l.batchSize += sz

	if l.c.Debug {
		log.Printf("Info: Listener read %d bytes\n", sz)
	}

	// Send when sufficient lines are batched or the batch buffer is almost full.
	if linesReceived%l.c.BatchMessages == 0 || l.batchSize >= l.sendThreshold {
		l.stats.Inc(batchesSent)
		if err := l.nc.Publish(l.c.NATSSubject[0], l.buf[:l.batchSize]); err != nil {
			l.handleNatsError(err)
		}
		l.batchSize = 0
	}
}

func (l *listener) handleNatsError(err error) {
	log.Printf("NATS Error: %v\n", err)
}

func (l *listener) startStatistician(stop <-chan struct{}) {
	statsLine := lineformatter.New(
		"relay_stat_listener", nil,
		"received",
		"sent",
		"read_errors",
	)
	for {
		stats := l.stats.Clone() // Sample counts
		l.nc.Publish(l.c.NATSSubjectMonitor, statsLine.Format(nil,
			stats.Get(linesReceived),
			stats.Get(batchesSent),
			stats.Get(readErrors),
		))
		select {
		case <-time.After(3 * time.Second):
		case <-stop:
			return
		}
	}
}

func (l *listener) close() {
	l.nc.Close()
	l.sc.Close()
}
