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
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jumptrading/influx-spout/batch"
	"github.com/jumptrading/influx-spout/batchsplitter"
	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/influx"
	"github.com/jumptrading/influx-spout/probes"
	"github.com/jumptrading/influx-spout/stats"
	nats "github.com/nats-io/go-nats"
)

const (
	// Listener stats counters
	statReceived          = "received"
	statSent              = "sent"
	statReadErrors        = "read_errors"
	statFailedNATSPublish = "failed_nats_publish"

	// The maximum possible UDP read size.
	maxUDPDatagramSize = 65536
)

// StartListener initialises a listener, starts its statistician
// goroutine and runs it's main loop. It never returns.
//
// The listener reads incoming UDP packets, batches them up and sends
// them onwards to a NATS subject.
func StartListener(c *config.Config) (_ *Listener, err error) {
	listener, err := newListener(c)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			listener.Stop()
		}
	}()

	sc, err := listener.setupUDP(int(c.ReadBufferSize.Bytes()))
	if err != nil {
		return nil, err
	}

	listener.wg.Add(2)
	go listener.startStatistician()
	go listener.listenUDP(sc)

	log.Printf("UDP listener publishing to [%s] at %s", c.NATSSubject[0], c.NATSAddress)

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

	log.Printf("HTTP listener publishing to [%s] at %s", c.NATSSubject[0], c.NATSAddress)

	return listener, nil
}

// Listener accepts measurements in InfluxDB Line Protocol format via
// UDP or HTTP, batches them and then publishes them to a NATS
// subject.
type Listener struct {
	c      *config.Config
	nc     *nats.Conn
	stats  *stats.Stats
	probes probes.Probes

	batch *batch.Batch

	wg   sync.WaitGroup
	stop chan struct{}
	mu   sync.Mutex // only used for HTTP listener
}

// Stop shuts down a running listener. It should be called exactly
// once for every Listener instance.
func (l *Listener) Stop() {
	l.probes.SetReady(false)
	l.probes.SetAlive(false)

	close(l.stop)
	l.wg.Wait()
	l.nc.Close()
	l.probes.Close()
}

func newListener(c *config.Config) (*Listener, error) {
	l := &Listener{
		c:    c,
		stop: make(chan struct{}),
		stats: stats.New(
			statReceived,
			statSent,
			statReadErrors,
			statFailedNATSPublish,
		),
		probes: probes.Listen(c.ProbePort),
		batch:  batch.New(int(c.BatchMaxSize.Bytes())),
	}

	nc, err := nats.Connect(l.c.NATSAddress, nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	l.nc = nc

	return l, nil
}

func (l *Listener) setupUDP(configBufSize int) (*net.UDPConn, error) {
	serverAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", l.c.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to create UDP socket: %v", err)
	}
	sc, err := net.ListenUDP("udp", serverAddr)
	if err != nil {
		return nil, err
	}

	bufSize := roundUpToPageSize(configBufSize)
	if bufSize != configBufSize {
		log.Printf("rounding up receive buffer to nearest page size (now %d bytes)", bufSize)
	}
	if err := sc.SetReadBuffer(bufSize); err != nil {
		return nil, err
	}

	log.Printf("listener bound to UDP socket: %v\n", sc.LocalAddr().String())
	return sc, nil
}

func roundUpToPageSize(n int) int {
	pageSize := os.Getpagesize()
	if n <= 0 {
		return pageSize
	}
	return (n + pageSize - 1) / pageSize * pageSize
}

func (l *Listener) listenUDP(sc *net.UDPConn) {
	defer func() {
		sc.Close()
		l.wg.Done()
	}()

	l.probes.SetReady(true)
	for {
		// Read deadline is used so that the stop channel can be
		// periodically checked.
		sc.SetReadDeadline(time.Now().Add(time.Second))
		bytesRead, err := l.batch.ReadOnceFrom(sc)
		if err != nil && !isTimeout(err) {
			l.stats.Inc(statReadErrors)
		}
		if bytesRead > 0 {
			if l.c.Debug {
				log.Printf("listener read %d bytes", bytesRead)
			}
			l.stats.Inc(statReceived)
		}

		l.maybeSendBatch()

		select {
		case <-l.stop:
			return
		default:
		}
	}
}

func (l *Listener) setupHTTP() *http.Server {
	l.wg.Add(1)
	go l.oldBatchSender()

	mux := http.NewServeMux()
	mux.HandleFunc("/write", l.handleHTTPWrite)
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", l.c.Port),
		Handler: mux,
	}
}

// oldBatchSender is a goroutine which sends the batch when it reached
// the configured maximum age. It is only used with the HTTP listener
// because the UDP listener does batch age handling in-line.
func (l *Listener) oldBatchSender() {
	defer l.wg.Done()
	for {
		l.mu.Lock()
		waitTime := l.c.BatchMaxAge.Duration - l.batch.Age()
		l.mu.Unlock()

		select {
		case <-time.After(waitTime):
			l.mu.Lock()
			if l.batch.Age() >= l.c.BatchMaxAge.Duration {
				l.sendBatch()
			}
			l.mu.Unlock()
		case <-l.stop:
			return
		}
	}
}

func (l *Listener) handleHTTPWrite(w http.ResponseWriter, r *http.Request) {
	bytesRead, err := l.readHTTPBody(r)
	if bytesRead > 0 {
		if l.c.Debug {
			log.Printf("HTTP listener read %d bytes", bytesRead)
		}
		l.stats.Inc(statReceived)

		l.mu.Lock()
		l.maybeSendBatch()
		l.mu.Unlock()
	}
	if err != nil {
		l.stats.Inc(statReadErrors)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (l *Listener) readHTTPBody(r *http.Request) (int64, error) {
	precision := r.URL.Query().Get("precision")

	if precision == "" || precision == "ns" {
		// Fast-path when timestamps are already in nanoseconds - no
		// need for conversion.
		return l.readHTTPBodyNanos(r)
	}

	// Non-nanosecond precison specified. Read lines individually and
	// convert timestamps to nanoseconds.
	count, err := l.readHTTPBodyWithPrecision(r, precision)
	return int64(count), err
}

func (l *Listener) readHTTPBodyNanos(r *http.Request) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.batch.ReadFrom(r.Body)
}

func (l *Listener) readHTTPBodyWithPrecision(r *http.Request, precision string) (int, error) {
	scanner := bufio.NewScanner(r.Body)

	// scanLines is like bufio.ScanLines but the returned lines
	// includes the trailing newlines. Leaving the newline on the line
	// is useful for incoming lines that don't contain a timestamp and
	// therefore should pass through unchanged.
	scanner.Split(scanLines)

	bytesRead := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		bytesRead += len(line)
		if len(line) <= 1 {
			continue
		}

		newLine := applyTimestampPrecision(line, precision)
		l.mu.Lock()
		l.batch.Append(newLine)
		l.mu.Unlock()
	}
	return bytesRead, scanner.Err()
}

func scanLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, data[0 : i+1], nil

	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil

	}
	// Request more data.
	return 0, nil, nil
}

func applyTimestampPrecision(line []byte, precision string) []byte {
	ts, offset := influx.ExtractTimestamp(line)
	if offset == -1 {
		return line
	}

	newTs, err := influx.SafeCalcTime(ts, precision)
	if err != nil {
		return line
	}

	newLine := make([]byte, offset, offset+influx.MaxTsLen+1)
	copy(newLine, line[:offset])
	newLine = strconv.AppendInt(newLine, newTs, 10)
	return append(newLine, '\n')
}

func (l *Listener) listenHTTP(server *http.Server) {
	defer l.wg.Done()

	go func() {
		l.probes.SetReady(true)
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

func (l *Listener) maybeSendBatch() {
	if l.shouldSend() {
		l.sendBatch()
	}
}

func (l *Listener) shouldSend() bool {
	if l.batch.Writes() >= l.c.BatchMaxCount {
		return true
	}

	if l.batch.Age() >= l.c.BatchMaxAge.Duration {
		return true
	}

	// If the batch size is within a (maximum) UDP datagram of the
	// configured target batch size, then force a send to avoid
	// growing the batch unnecessarily (allocations hurt performance).
	if int(l.c.BatchMaxSize.Bytes())-l.batch.Size() <= maxUDPDatagramSize {
		return true
	}

	return false
}

func (l *Listener) sendBatch() {
	if l.batch.Size() < 1 {
		return // Nothing to do
	}

	l.stats.Inc(statSent)

	// The goal is for the batch size to never be bigger than what
	// NATS will accept but there is a small chance that a series of
	// large incoming chunks could cause the batch to grow beyond the
	// intended limit. For these cases, use the batchsplitter just in
	// case. batchsplitter has very low overhead when no splitting is
	// required.
	splitter := batchsplitter.New(l.batch.Bytes(), config.MaxNATSMsgSize)
	for splitter.Next() {
		if err := l.nc.Publish(l.c.NATSSubject[0], splitter.Chunk()); err != nil {
			l.stats.Inc(statFailedNATSPublish)
			l.handleNatsError(err)
		}
	}
	l.batch.Reset()
}

func (l *Listener) handleNatsError(err error) {
	log.Printf("NATS Error: %v\n", err)
}

func (l *Listener) startStatistician() {
	defer l.wg.Done()
	labels := stats.NewLabels("listener", l.c.Name)
	for {
		lines := stats.SnapshotToPrometheus(l.stats.Snapshot(), time.Now(), labels)
		l.nc.Publish(l.c.NATSSubjectMonitor, lines)
		select {
		case <-time.After(l.c.StatsInterval.Duration):
		case <-l.stop:
			return
		}
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
