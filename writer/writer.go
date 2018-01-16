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
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	// for profiling a nasty memleak
	"net/http"
	_ "net/http/pprof"

	// This would be nice, but it's too unstable for now
	// revisit eventually
	//"github.com/valyala/fasthttp"

	"github.com/nats-io/go-nats"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/filter"
	"github.com/jumptrading/influx-spout/lineformatter"
	"github.com/jumptrading/influx-spout/stats"
)

// Writer stats counters
const (
	batchesReceived = "batches-received"
	writeRequests   = "write-requests"
	failedWrites    = "failed-writes"
)

type Writer struct {
	c     *config.Config
	nc    *nats.Conn
	rules []filter.FilterRule
	stats *stats.Stats
	wg    sync.WaitGroup
	stop  chan struct{}
}

// StartWriter is the heavylifter, subscribes to the subject where
// listeners publish the messages and writes it the InfluxDB endpoint.
func StartWriter(c *config.Config) (_ *Writer, err error) {
	w := &Writer{
		c:     c,
		stats: stats.New(batchesReceived, writeRequests, failedWrites),
		stop:  make(chan struct{}),
	}
	defer func() {
		if err != nil {
			w.Stop()
		}
	}()

	// for pprof profiling
	go http.ListenAndServe(":8080", nil)

	// create our rules from the config rules
	for _, r := range c.Rule {
		switch r.Rtype {
		case "basic":
			w.rules = append(w.rules, filter.CreateBasicRule(r.Match, r.Subject))
		case "regex":
			w.rules = append(w.rules, filter.CreateRegexRule(r.Match, r.Subject))
		case "negregex":
			w.rules = append(w.rules, filter.CreateNegativeRegexRule(r.Match, r.Subject))
		default:
			return nil, fmt.Errorf("Unsupported rule: [%v]", r)
		}
	}

	w.nc, err = nats.Connect(c.NATSAddress)
	if err != nil {
		return nil, fmt.Errorf("NATS Error: can't connect: %v\n", err)
	}

	// if we disconnect, we want to try reconnecting as many times as
	// we can
	w.nc.Opts.MaxReconnect = -1

	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

	w.notifyState("boot") // notify the monitor that we have finished booting and soon are ready
	jobs := make(chan *nats.Msg, 1024)
	w.wg.Add(w.c.WriterWorkers)
	for wk := 0; wk < w.c.WriterWorkers; wk++ {
		go w.worker(jobs)
	}

	// subscribe this writer to the NATS subject.
	maxPendingBytes := c.NATSPendingMaxMB * 1024 * 1024
	for _, subject := range c.NATSSubject {
		sub, err := w.nc.Subscribe(subject, func(msg *nats.Msg) {
			jobs <- msg
		})
		if err != nil {
			return nil, fmt.Errorf("subscription for %q failed: %v", subject, err)
		}
		if err := sub.SetPendingLimits(-1, maxPendingBytes); err != nil {
			return nil, fmt.Errorf("failed to set pending limits: %v", err)
		}

		w.wg.Add(1)
		go w.monitorSub(sub)
	}

	w.wg.Add(1)
	go w.startStatistician()

	if err = w.nc.LastError(); err != nil {
		w.notifyState("crashed")
		return nil, err
	}

	// notify the monitor that we are ready to receive messages and transmit to influxdb
	w.notifyState("ready")

	log.Printf("listening on [%v] with %d workers\n", c.NATSSubject, c.WriterWorkers)
	log.Printf("POST timeout: %ds", c.WriteTimeoutSecs)
	log.Printf("maximum NATS subject size: %dMB", c.NATSPendingMaxMB)

	return w, nil
}

// Stop aborts all goroutines belonging to the Writer and closes its
// connection to NATS. It will be block until all Writer goroutines
// have stopped.
func (w *Writer) Stop() {
	close(w.stop)
	w.wg.Wait()
	if w.nc != nil {
		w.nc.Close()
	}
}

// GetRules implements filter.Filtering.
func (w *Writer) GetRules() []filter.FilterRule {
	return w.rules
}

func (w *Writer) worker(jobs <-chan *nats.Msg) {
	defer w.wg.Done()

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   time.Duration(w.c.WriteTimeoutSecs) * time.Second,
	}

	url := fmt.Sprintf("http://%s:%d/write?db=%s", w.c.InfluxDBAddress, w.c.InfluxDBPort, w.c.DBName)
	var num int
	var msgBuffer bytes.Buffer
	msgBuffer.Grow(32 * os.Getpagesize())

	noRules := len(w.rules) == 0
	for {
		select {
		case j := <-jobs:
			w.stats.Inc(batchesReceived)
			if noRules {
				w.processMsg(&msgBuffer, num, url, client, j.Data)
				num++
			} else {
				// do some filtering
				for _, line := range bytes.SplitAfter(j.Data, []byte("\n")) {
					if len(line) == 0 {
						continue
					}
					if w.filterLine(line) {
						w.processMsg(&msgBuffer, num, url, client, line)
					}
					num++
				}
			}
		case <-w.stop:
			return
		}
	}
}

func (w *Writer) filterLine(line []byte) bool {
	return filter.LookupLine(w, line) != -1
}

func (w *Writer) processMsg(msgBuffer *bytes.Buffer, msgsRecv int, url string, client *http.Client, data []byte) {
	// put the message in the queue and process it later
	if n, err := msgBuffer.Write(data); err != nil {
		log.Printf("Error: failed to write to message buffer (wrote %d): %v\n", n, err)
	}

	// if we have queued enough messages, then we can go ahead and submit them
	if ((msgsRecv%w.c.BatchMessages) == 0 && msgBuffer.Len() > 0) || msgBuffer.Len() > 10*1024*1024 {
		// send the messages via HTTP to influxdb
		w.stats.Inc(writeRequests)
		resp, err := client.Post(url, "application/json; charset=UTF-8", msgBuffer)

		// Reset buffer on success or error; batch will not be sent again.
		msgBuffer.Reset()

		if err != nil {
			log.Printf("Error: failed to send HTTP request: %v\n", err)
			w.stats.Inc(failedWrites)
			return
		}

		defer resp.Body.Close()

		if resp.StatusCode > 300 {
			log.Printf("Error: received HTTP %v from %v\n", resp.Status, url)
			if w.c.Debug {
				body, err := ioutil.ReadAll(resp.Body)
				if err == nil {
					log.Printf("response body: %s\n", body)
				}
			}
			w.stats.Inc(failedWrites)
		}
	}
}

var dropLine = lineformatter.New("writer_drop", nil, "total", "diff")

func (w *Writer) signalDrop(drop, last int) {
	// uh, this writer is overloaded and had to drop a packet
	log.Printf("Warning: dropped %d (now %d dropped in total)\n", drop-last, drop)

	// publish to the monitor subject, so grafana can pick it up and report failures
	w.nc.Publish(w.c.NATSSubjectMonitor, dropLine.FormatT(time.Now(), nil, drop, drop-last))

	// the fact the we dropped a packet MUST reach the server
	// immediately so we can investigate
	w.nc.Flush()
}

func (w *Writer) monitorSub(sub *nats.Subscription) {
	defer w.wg.Done()

	last, err := sub.Dropped()
	if err != nil {
		log.Printf("NATS Warning: Failed to get the number of dropped message from NATS: %v\n", err)
	}
	drop := last

	for {
		drop, err = sub.Dropped()
		if err != nil {
			log.Printf("NATS Warning: Failed to get the number of dropped message from NATS: %v\n", err)
		}

		if drop != last {
			w.signalDrop(drop, last)
		}
		last = drop

		select {
		case <-time.After(time.Second):
		case <-w.stop:
			sub.Unsubscribe()
			return
		}
	}
}

func (w *Writer) startStatistician() {
	defer w.wg.Done()

	// This goroutine is responsible for monitoring the statistics and
	// sending it to the monitoring backend.
	statsLine := lineformatter.New(
		"relay_stat_writer", nil,
		"received",
		"write_requests",
		"failed_writes",
	)
	for {
		stats := w.stats.Clone()
		w.nc.Publish(w.c.NATSSubjectMonitor, statsLine.Format(nil,
			stats.Get(batchesReceived),
			stats.Get(writeRequests),
			stats.Get(failedWrites),
		))

		select {
		case <-time.After(3 * time.Second):
		case <-w.stop:
			return
		}
	}
}

var notifyLine = lineformatter.New("relay_mon", nil, "type", "state", "pid")

func (w *Writer) notifyState(state string) {
	line := notifyLine.Format(nil, "writer", state, os.Getpid())
	if err := w.nc.Publish(w.c.NATSSubjectMonitor, line); err != nil {
		log.Printf("NATS Error: %v\n", err)
		return
	}
	if err := w.nc.Flush(); err != nil {
		log.Printf("NATS Error: %v\n", err)
	}
}
