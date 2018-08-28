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

package downsampler

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/go-nats"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/probes"
	"github.com/jumptrading/influx-spout/stats"
)

const maxNATSMsgSize = 1024 * 1024

const (
	statReceived          = "received"
	statSent              = "sent"
	statInvalidLines      = "invalid_lines"
	statFailedNATSPublish = "failed_nats_publish"
	statNATSDropped       = "nats_dropped"
)

// Downsampler consumes lines from one or more NATS subjects, and
// applies downsampling to the InfluxDB measurements received from
// them.
type Downsampler struct {
	c                *config.Config
	nc               *nats.Conn
	wg               sync.WaitGroup
	probes           probes.Probes
	stop             chan struct{}
	stats            *stats.Stats
	statsNATSDropped *stats.AnonStats
	subscriptions    []*nats.Subscription
}

// StartDownsampler creates and configures a Downsampler.
func StartDownsampler(c *config.Config) (_ *Downsampler, err error) {
	ds := &Downsampler{
		c:                c,
		probes:           probes.Listen(c.ProbePort),
		stop:             make(chan struct{}),
		stats:            stats.New(statReceived, statSent, statInvalidLines, statFailedNATSPublish),
		statsNATSDropped: stats.NewAnon(len(c.NATSSubject)),
	}
	defer func() {
		if err != nil {
			ds.Stop()
		}
	}()

	ds.nc, err = nats.Connect(c.NATSAddress, nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("NATS Error: can't connect: %v", err)
	}

	maxPendingSize := int(c.NATSMaxPendingSize.Bytes())
	for _, subject := range c.NATSSubject {
		inputCh := make(chan []byte, 1024)
		ds.wg.Add(1)
		go ds.worker(subject, inputCh)
		subscription, err := ds.nc.Subscribe(subject, func(msg *nats.Msg) {
			inputCh <- msg.Data
		})
		if err != nil {
			return nil, fmt.Errorf("NATS: subscription for %q failed: %v", subject, err)
		}
		if err := subscription.SetPendingLimits(-1, maxPendingSize); err != nil {
			return nil, fmt.Errorf("NATS: failed to set pending limits: %v", err)
		}
		ds.subscriptions = append(ds.subscriptions, subscription)
	}

	// Subscriptions don't seem to be reliable without flushing after
	// subscribing.
	if err := ds.nc.Flush(); err != nil {
		return nil, fmt.Errorf("NATS flush error: %v", err)
	}

	ds.wg.Add(1)
	go ds.startStatistician()

	log.Printf("downsampler subscribed to %v at %s", c.NATSSubject, c.NATSAddress)
	log.Printf("downsampler period: %s", c.DownsamplePeriod.Duration)
	log.Printf("downsampler output suffix: %s", c.DownsampleSuffix)
	log.Printf("maximum NATS subject size: %s", c.NATSMaxPendingSize)

	ds.probes.SetReady(true)

	return ds, nil
}

// Stop aborts all goroutines belonging to the Downsampler and closes its
// connection to NATS. It will be block until all Downsampler goroutines
// have stopped.
func (ds *Downsampler) Stop() {
	// Indicate that the downsampler is shutting down.
	ds.probes.SetReady(false)
	ds.probes.SetAlive(false)

	// Signal for goroutines to stop and wait for them.
	close(ds.stop)
	ds.wg.Wait()

	// Clean up NATS subscriptions and close NATS connection.
	for _, subscription := range ds.subscriptions {
		subscription.Unsubscribe()
	}
	if ds.nc != nil {
		ds.nc.Close()
	}

	// Shut down probes listener.
	ds.probes.Close()
}

func (ds *Downsampler) worker(subject string, inputCh <-chan []byte) {
	defer ds.wg.Done()

	outSubject := subject + ds.c.DownsampleSuffix
	assigner := newAssigner(ds.c.DownsamplePeriod.Duration, newAvgBucket, new(realClock))
	for {
		select {
		case lines := <-inputCh:
			ds.stats.Inc(statReceived)
			errs := assigner.Update(lines)
			for _, err := range errs {
				log.Println(err)
				ds.stats.Inc(statInvalidLines)
			}
		case <-time.After(assigner.UntilNext()):
		case <-ds.stop:
			return
		}

		buf := assigner.Bytes()
		if len(buf) > 0 {
			if ds.c.Debug {
				log.Printf("publishing to %s (%d bytes)", outSubject, len(buf))
			}

			splitter := newBatchSplitter(buf, maxNATSMsgSize)
			for splitter.Next() {
				if err := ds.nc.Publish(outSubject, splitter.Chunk()); err != nil {
					log.Printf("publish error for %s: %v", outSubject, err)
					ds.stats.Inc(statFailedNATSPublish)
				}
			}
			ds.stats.Inc(statSent)
		}
	}
}

// startStatistician defines a goroutine that is responsible for
// regularly sending the downsamplers's statistics to the monitoring
// backend.
func (ds *Downsampler) startStatistician() {
	defer ds.wg.Done()

	labels := stats.NewLabels("downsampler", ds.c.Name)
	for {
		now := time.Now()
		snap := ds.stats.Snapshot()
		natsDrops := ds.updateNATSDropped()

		// Publish general stats.
		lines := stats.SnapshotToPrometheus(snap, now, labels)
		ds.nc.Publish(ds.c.NATSSubjectMonitor, lines)

		// Publish the per subject nats_dropped metric.
		for i, subject := range ds.c.NATSSubject {
			ds.nc.Publish(ds.c.NATSSubjectMonitor, stats.CounterToPrometheus(
				statNATSDropped,
				int(natsDrops[i]),
				now,
				labels.With("subject", subject),
			))
		}

		select {
		case <-time.After(ds.c.StatsInterval.Duration):
		case <-ds.stop:
			return
		}
	}
}

func (ds *Downsampler) updateNATSDropped() []uint64 {
	for i, sub := range ds.subscriptions {
		dropped, err := sub.Dropped()
		if err != nil {
			log.Printf("NATS: failed to read drops for %q subscription: %v", sub.Subject, err)
		} else {
			ds.statsNATSDropped.Max(i, uint64(dropped))
		}
	}
	return ds.statsNATSDropped.Snapshot()
}
