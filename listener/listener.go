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
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
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

var statsInterval = 3 * time.Second

// StartListener initialises a listener, starts its statistician
// goroutine and runs it's main loop. It never returns.
//
// The listener reads incoming UDP packets, batches them up and send
// batches onwards to a NATS subject.
func StartListener(c *config.Config) (*Listener, error) {
	listener, err := newListener(c)
	if err != nil {
		return nil, err
	}
	sc, err := listener.setupUDP()
	if err != nil {
		return nil, err
	}

	listener.wg.Add(2)
	go listener.startStatistician()
	go listener.listenUDP(sc)
	listener.notifyState("ready")

	return listener, nil
}

// StartHTTPListener initialises listener configured to accept lines
// from HTTP request bodies instead of via UDP. It starts the listener
// and its statistician and never returns.
func StartHTTPListener(c *config.Config) (*Listener, error) {
	listener, err := newListener(c)
	if err != nil {
		return nil, err
	}
	server := listener.setupHTTP()

	listener.wg.Add(2)
	go listener.startStatistician()
	go listener.listenHTTP(server)
	listener.notifyState("ready")

	return listener, nil
}

type Listener struct {
	c     *config.Config
	nc    *nats.Conn
	stats *stats.Stats

	bufSize       int
	buf           []byte
	batchSize     int
	sendThreshold int

	wg   sync.WaitGroup
	stop chan struct{}
}

func (l *Listener) Stop() {
	close(l.stop)
	l.wg.Wait()
	l.nc.Close()
}

func newListener(c *config.Config) (*Listener, error) {
	l := &Listener{
		c:     c,
		stop:  make(chan struct{}),
		stats: stats.New(allStats...),

		// create a buffer to read incoming UDP packets into,
		// make it at least a page to get optimal performance
		bufSize: 32 * os.Getpagesize(),
	}
	if err := l.connectNATS(); err != nil {
		return nil, err
	}
	l.setupBuffers()
	return l, nil
}

func (l *Listener) setupBuffers() {
	l.buf = make([]byte, l.bufSize)
	l.sendThreshold = l.bufSize - 2048
}

func (l *Listener) connectNATS() error {
	var err error

	// connect to the NATS instance running on this machine
	l.nc, err = nats.Connect(l.c.NATSAddress)
	if err != nil {
		return err
	}

	// If we disconnect, we want to try reconnecting as many times as
	// we can.
	l.nc.Opts.MaxReconnect = -1

	l.notifyState("boot")

	return nil
}

func (l *Listener) setupUDP() (*net.UDPConn, error) {
	serverAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", l.c.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to create UDP socket: %v", err)
	}
	sc, err := net.ListenUDP("udp", serverAddr)
	if err != nil {
		return nil, err
	}
	if err := sc.SetReadBuffer(l.bufSize); err != nil {
		return nil, err
	}
	log.Printf("Listener bound to UDP socket: %v\n", sc.LocalAddr().String())
	return sc, nil
}

func (l *Listener) listenUDP(sc *net.UDPConn) {
	defer l.wg.Done()
	defer sc.Close()
	for {
		sc.SetReadDeadline(time.Now().Add(time.Second))
		sz, _, err := sc.ReadFromUDP(l.buf[l.batchSize:])
		if err != nil && !isTimeout(err) {
			l.stats.Inc(readErrors)
		}

		// Attempt to process the read even on error as Read may
		// still have read some bytes successfully.
		l.processRead(sz)

		select {
		case <-l.stop:
			return
		default:
		}
	}
}

func (l *Listener) setupHTTP() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		for {
			sz, err := r.Body.Read(l.buf[l.batchSize:])

			// Attempt to process the read even on error has Read may
			// still have read some bytes successfully.
			l.processRead(sz)

			if err != nil {
				if err != io.EOF {
					l.stats.Inc(readErrors)
				}
				break
			}
		}
	})
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", l.c.Port),
		Handler: mux,
	}
}

func (l *Listener) listenHTTP(server *http.Server) {
	defer l.wg.Done()

	go func() {
		err := server.ListenAndServe()
		if err == nil || err == http.ErrServerClosed {
			return
		}
		log.Fatal(err)
	}()

	// Close the server if the stop channel is closed.
	<-l.stop
	server.Close()
}

func (l *Listener) processRead(sz int) {
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

func (l *Listener) handleNatsError(err error) {
	log.Printf("NATS Error: %v\n", err)
}

func (l *Listener) startStatistician() {
	defer l.wg.Done()

	statsLine := lineformatter.New(
		"spout_stat_listener", nil,
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
		case <-time.After(statsInterval):
		case <-l.stop:
			return
		}
	}
}

var notifyLine = lineformatter.New("spout_mon", nil, "type", "state", "pid")

func (l *Listener) notifyState(state string) {
	line := notifyLine.Format(nil, "listener", state, os.Getpid())
	if err := l.nc.Publish(l.c.NATSSubjectMonitor, line); err != nil {
		l.handleNatsError(err)
		return
	}
	if err := l.nc.Flush(); err != nil {
		l.handleNatsError(err)
	}
}

type timeouter interface {
	Timeout() bool
}

func isTimeout(err error) bool {
	if timeoutErr, ok := err.(timeouter); ok {
		return timeoutErr.Timeout()
	}
	return false
}
