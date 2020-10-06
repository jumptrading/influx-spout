// Copyright 2018 Jump Trading
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

// Package monitor defines the influx-spount monitor component.
package monitor

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/jumptrading/influx-spout/v2/config"
	"github.com/jumptrading/influx-spout/v2/probes"
	"github.com/jumptrading/influx-spout/v2/prometheus"
)

// Start initialises, starts and returns a new Monitor instance based
// on the configuration supplies.
func Start(conf *config.Config) (_ *Monitor, err error) {
	m := &Monitor{
		c:       conf,
		stop:    make(chan struct{}),
		metrics: prometheus.NewMetricSet(),
		probes:  probes.Listen(conf.ProbePort),
	}
	defer func() {
		if err != nil {
			m.Stop()
		}
	}()

	m.nc, err = m.natsConnect()
	if err != nil {
		return nil, err
	}

	m.sub, err = m.nc.Subscribe(m.c.NATSSubjectMonitor, m.receiveMetrics)
	if err != nil {
		return nil, fmt.Errorf("NATS: failed to subscribe: %v", err)
	}
	if err := m.nc.Flush(); err != nil {
		return nil, fmt.Errorf("NATS: failed to flush: %v", err)
	}

	m.wg.Add(1)
	go m.serveHTTP()

	log.Printf("monitor subscribed to [%s] at %s - serving HTTP on port %d",
		m.c.NATSSubjectMonitor, m.c.NATSAddress, m.c.Port)
	return m, nil
}

// Monitor defines an influx-spout component which accumulates
// runtime statistics from the other influx-spout components and
// makes them available via a HTTP endpoint in Prometheus format.
type Monitor struct {
	c      *config.Config
	nc     *nats.Conn
	sub    *nats.Subscription
	wg     sync.WaitGroup
	stop   chan struct{}
	probes probes.Probes

	mu      sync.Mutex
	metrics *prometheus.MetricSet
}

// Stop shuts down goroutines and closes resources related to the filter.
func (m *Monitor) Stop() {
	m.probes.SetReady(false)
	m.probes.SetAlive(false)

	// Stop receiving lines from NATS.
	m.sub.Unsubscribe()

	// Shut down goroutines.
	close(m.stop)
	m.wg.Wait()

	// Close the connection to NATS.
	if m.nc != nil {
		m.nc.Close()
	}

	m.probes.Close()
}

func (m *Monitor) natsConnect() (*nats.Conn, error) {
	nc, err := nats.Connect(m.c.NATSAddress, nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("NATS: failed to connect: %v", err)
	}
	return nc, nil
}

func (m *Monitor) serveHTTP() {
	defer m.wg.Done()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		r.Body.Close()
		w.Header().Set("Content-Type", "text/plain")

		defer m.mu.Unlock()
		m.mu.Lock()
		w.Write(m.metrics.ToBytes())
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", m.c.Port),
		Handler: mux,
	}

	go func() {
		m.probes.SetReady(true)
		err := server.ListenAndServe()
		if err == nil || err == http.ErrServerClosed {
			return
		}
		log.Fatal(err)
	}()

	// Close the server if the stop channel is closed.
	<-m.stop
	server.Close()
}

func (m *Monitor) receiveMetrics(msg *nats.Msg) {
	newMetrics, err := prometheus.ParseMetrics(msg.Data)
	if err != nil {
		log.Printf("invalid metrics received: %v", err)
		return
	}

	defer m.mu.Unlock()
	m.mu.Lock()
	m.metrics.UpdateFromSet(newMetrics)
}
