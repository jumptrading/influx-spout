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
)

const maxNATSMsgSize = 1024 * 1024

// Downsampler consumes lines from one or more NATS subjects, and
// applies downsampling to the InfluxDB measurements received from
// them.
type Downsampler struct {
	c      *config.Config
	nc     *nats.Conn
	wg     sync.WaitGroup
	probes probes.Probes
	stop   chan struct{}
}

// StartDownsampler creates and configures a Downsampler.
func StartDownsampler(c *config.Config) (_ *Downsampler, err error) {
	w := &Downsampler{
		c:      c,
		probes: probes.Listen(c.ProbePort),
		stop:   make(chan struct{}),
	}
	defer func() {
		if err != nil {
			w.Stop()
		}
	}()

	w.nc, err = nats.Connect(c.NATSAddress, nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("NATS Error: can't connect: %v", err)
	}

	for _, subject := range c.NATSSubject {
		inputCh := make(chan []byte, 1024)
		w.wg.Add(1)
		go w.worker(subject, inputCh)
		sub, err := w.nc.Subscribe(subject, func(msg *nats.Msg) {
			inputCh <- msg.Data
		})
		if err != nil {
			return nil, fmt.Errorf("NATS: subscription for %q failed: %v", subject, err)
		}
		if err := sub.SetPendingLimits(-1, int(c.NATSMaxPendingSize.Bytes())); err != nil {
			return nil, fmt.Errorf("NATS: failed to set pending limits: %v", err)
		}
	}

	// Subscriptions don't seem to be reliable without flushing after
	// subscribing.
	if err := w.nc.Flush(); err != nil {
		return nil, fmt.Errorf("NATS flush error: %v", err)
	}

	log.Printf("downsampler subscribed to %v at %s", c.NATSSubject, c.NATSAddress)
	log.Printf("downsampler period: %s", c.DownsamplePeriod.Duration)
	log.Printf("downsampler output suffix: %s", c.DownsampleSuffix)
	log.Printf("maximum NATS subject size: %s", c.NATSMaxPendingSize)

	w.probes.SetReady(true)

	return w, nil
}

// Stop aborts all goroutines belonging to the Downsampler and closes its
// connection to NATS. It will be block until all Downsampler goroutines
// have stopped.
func (ds *Downsampler) Stop() {
	ds.probes.SetReady(false)
	ds.probes.SetAlive(false)

	close(ds.stop)
	ds.wg.Wait()
	if ds.nc != nil {
		ds.nc.Close()
	}

	ds.probes.Close()
}

func (ds *Downsampler) worker(subject string, inputCh <-chan []byte) {
	defer ds.wg.Done()

	outSubject := subject + ds.c.DownsampleSuffix
	nextEmitTime := ds.nextTime(time.Now())
	batch := newSamplingBatch(nextEmitTime)
	for {
		select {
		case lines := <-inputCh:
			batch.Append(lines)
		case <-time.After(time.Until(nextEmitTime)):
		case <-ds.stop:
			return
		}

		if !time.Now().Before(nextEmitTime) {
			if ds.c.Debug {
				log.Printf("total unique fields for %s: %d", subject, batch.FieldCount())
			}
			buf := batch.Bytes()
			if len(buf) > 0 {
				if ds.c.Debug {
					log.Printf("publishing to %s (%d bytes)", outSubject, len(buf))
				}

				splitter := newBatchSplitter(buf, maxNATSMsgSize)
				for splitter.Next() {
					if err := ds.nc.Publish(outSubject, splitter.Chunk()); err != nil {
						log.Printf("publish error for %s: %v", outSubject, err)
						// XXX increment counter
					}
				}

			}

			nextEmitTime = ds.nextTime(nextEmitTime)
			batch = newSamplingBatch(nextEmitTime)
		}
	}
}

func (ds *Downsampler) nextTime(t time.Time) time.Time {
	period := ds.c.DownsamplePeriod.Duration
	return t.Add(period).Truncate(period)
}
