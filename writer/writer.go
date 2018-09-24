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

// Package writer configures and instantiates the subscribers to the
// NATS bus, in turn POST'ing data to InfluxDB.
package writer

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/go-nats"

	"github.com/jumptrading/influx-spout/batch"
	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/filter"
	"github.com/jumptrading/influx-spout/probes"
	"github.com/jumptrading/influx-spout/stats"
)

// Writer stats counters
const (
	statReceived      = "received"
	statWriteRequests = "write_requests"
	statFailedWrites  = "failed_writes"
	statMaxPending    = "max_pending"
	statNATSDropped   = "nats_dropped"
)

// Writer consumes lines from one or more NATS subjects, optionally
// applies filtering, batches them up and then writes them to a
// InfluxDB endpoint.
type Writer struct {
	c      *config.Config
	url    string
	nc     *nats.Conn
	rules  *filter.RuleSet
	stats  *stats.Stats
	wg     sync.WaitGroup
	probes probes.Probes
	stop   chan struct{}
}

// StartWriter creates and configures a Writer.
func StartWriter(c *config.Config) (_ *Writer, err error) {
	w := &Writer{
		c:      c,
		url:    fmt.Sprintf("http://%s:%d/write?db=%s", c.InfluxDBAddress, c.InfluxDBPort, c.DBName),
		stats:  stats.New(statReceived, statWriteRequests, statFailedWrites, statMaxPending),
		probes: probes.Listen(c.ProbePort),
		stop:   make(chan struct{}),
	}
	defer func() {
		if err != nil {
			w.Stop()
		}
	}()

	w.rules, err = filter.RuleSetFromConfig(c)
	if err != nil {
		return nil, err
	}

	w.nc, err = nats.Connect(c.NATSAddress, nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("NATS Error: can't connect: %v", err)
	}

	jobs := make(chan *nats.Msg, 1024)
	w.wg.Add(w.c.Workers)
	for wk := 0; wk < w.c.Workers; wk++ {
		go w.worker(jobs)
	}

	// Subscribe the writer to the configured NATS subjects.
	subs := make([]*nats.Subscription, 0, len(c.NATSSubject))
	for _, subject := range c.NATSSubject {
		sub, err := w.nc.Subscribe(subject, func(msg *nats.Msg) {
			jobs <- msg
		})
		if err != nil {
			return nil, fmt.Errorf("NATS: subscription for %q failed: %v", subject, err)
		}
		if err := sub.SetPendingLimits(-1, int(c.NATSMaxPendingSize.Bytes())); err != nil {
			return nil, fmt.Errorf("NATS: failed to set pending limits: %v", err)
		}
		subs = append(subs, sub)
	}

	// Subscriptions don't seem to be reliable without flushing after
	// subscribing.
	if err := w.nc.Flush(); err != nil {
		return nil, fmt.Errorf("NATS flush error: %v", err)
	}

	w.wg.Add(1)
	go w.startStatistician(subs)

	log.Printf("writer subscribed to [%v] at %s with %d workers",
		c.NATSSubject, c.NATSAddress, c.Workers)
	log.Printf("POST timeout: %s", c.WriteTimeout)
	log.Printf("maximum NATS subject size: %s", c.NATSMaxPendingSize)

	w.probes.SetReady(true)

	return w, nil
}

// Stop aborts all goroutines belonging to the Writer and closes its
// connection to NATS. It will be block until all Writer goroutines
// have stopped.
func (w *Writer) Stop() {
	w.probes.SetReady(false)
	w.probes.SetAlive(false)

	close(w.stop)
	w.wg.Wait()
	if w.nc != nil {
		w.nc.Close()
	}

	w.probes.Close()
}

func (w *Writer) worker(jobs <-chan *nats.Msg) {
	defer w.wg.Done()

	tr := &http.Transport{
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 1,
		MaxConnsPerHost:     2,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   w.c.WriteTimeout.Duration,
	}

	batch := batch.New(32 * os.Getpagesize())
	batchAppend := w.getBatchWriteFunc(batch)
	for {
		select {
		case j := <-jobs:
			w.stats.Inc(statReceived)
			batchAppend(j.Data)
		case <-time.After(time.Second):
			// Wake up regularly to check batch age
		case <-w.stop:
			return
		}

		if w.shouldSend(batch) {
			w.stats.Inc(statWriteRequests)

			if err := w.sendBatch(batch, client); err != nil {
				w.stats.Inc(statFailedWrites)
				log.Printf("Error: %v", err)
			}

			// Reset buffer on success or error; batch will not be sent again.
			batch.Reset()
		}
	}
}

func (w *Writer) getBatchWriteFunc(batch *batch.Batch) func([]byte) {
	if w.rules.Count() == 0 {
		// No rules - just append the received data straight onto the
		// batch buffer.
		return batch.Append
	}

	return func(data []byte) {
		// Rules exist - split the received data into lines and apply
		// filters.
		for _, line := range bytes.SplitAfter(data, []byte("\n")) {
			if w.filterLine(line) {
				batch.Append(line)
			}
		}
	}
}

func (w *Writer) filterLine(line []byte) bool {
	if len(line) == 0 {
		return false
	}
	return w.rules.Lookup(line) != -1
}

func (w *Writer) shouldSend(batch *batch.Batch) bool {
	return batch.Writes() >= w.c.BatchMaxCount ||
		uint64(batch.Size()) >= w.c.BatchMaxSize.Bytes() ||
		batch.Age() >= w.c.BatchMaxAge.Duration
}

// sendBatch sends the accumulated batch via HTTP to InfluxDB.
func (w *Writer) sendBatch(batch *batch.Batch, client *http.Client) error {
	req, err := http.NewRequest("POST", w.url, bytes.NewReader(batch.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json; charset=UTF-8")
	if w.c.InfluxDBUser != "" {
		req.SetBasicAuth(w.c.InfluxDBUser, w.c.InfluxDBPass)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %v\n", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode > 300 {
		errText := fmt.Sprintf("received HTTP %v from %v", resp.Status, w.url)
		if w.c.Debug {
			body, err := ioutil.ReadAll(resp.Body)
			if err == nil {
				errText += fmt.Sprintf("\nresponse body: %s\n", body)
			}
		}
		return errors.New(errText)
	}

	return nil
}

// This goroutine is responsible for monitoring the statistics and
// sending it to the monitoring backend.
func (w *Writer) startStatistician(subs []*nats.Subscription) {
	defer w.wg.Done()

	labels := stats.NewLabels("writer", w.c.Name).
		With("influxdb_address", w.c.InfluxDBAddress).
		With("influxdb_port", strconv.Itoa(w.c.InfluxDBPort)).
		With("influxdb_dbname", w.c.DBName)

	for {
		now := time.Now()

		// Publish general stats.
		lines := stats.SnapshotToPrometheus(w.stats.Snapshot(), now, labels)
		w.nc.Publish(w.c.NATSSubjectMonitor, lines)

		// Publish per-subscription NATS drop counters.
		for _, sub := range subs {
			dropped, err := sub.Dropped()
			if err != nil {
				log.Printf("NATS: failed to get dropped count: %v", err)
				continue
			}
			line := stats.CounterToPrometheus(
				statNATSDropped,
				dropped,
				now,
				labels.With("subject", sub.Subject))
			w.nc.Publish(w.c.NATSSubjectMonitor, line)
		}

		w.nc.Flush()

		select {
		case <-time.After(w.c.StatsInterval.Duration):
		case <-w.stop:
			return
		}
	}
}
