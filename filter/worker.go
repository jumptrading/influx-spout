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

package filter

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/jumptrading/influx-spout/stats"
)

type worker struct {
	rules       *RuleSet
	stats       *stats.Stats
	nc          natsConn
	junkSubject string
	batches     []*bytes.Buffer
	junkBatch   *bytes.Buffer
}

func newWorker(
	rules *RuleSet,
	stats *stats.Stats,
	natsConnect func() (natsConn, error),
	junkSubject string,
) (*worker, error) {
	nc, err := natsConnect()
	if err != nil {
		return nil, fmt.Errorf("NATS: failed to connect: %v", err)
	}

	batches := make([]*bytes.Buffer, rules.Count())
	for i := 0; i < rules.Count(); i++ {
		batches[i] = new(bytes.Buffer)
		batches[i].Grow(65536)
	}

	return &worker{
		rules:       rules,
		stats:       stats,
		nc:          nc,
		batches:     batches,
		junkBatch:   new(bytes.Buffer),
		junkSubject: junkSubject,
	}, nil
}

func (w *worker) run(jobs <-chan []byte, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer func() {
		w.nc.Close()
		wg.Done()
	}()

	for {
		select {
		case data := <-jobs:
			w.processBatch(data)
		case <-stop:
			return
		}
	}
}

func (w *worker) processBatch(batch []byte) {
	for _, line := range bytes.SplitAfter(batch, []byte("\n")) {
		if len(line) > 0 {
			w.processLine(line)
		}
	}

	// batches have been processed, empty the buffers onto NATS
	w.sendOff()
}

func (w *worker) processLine(line []byte) {
	w.stats.Inc(linesProcessed)

	idx := w.rules.Lookup(line)
	if idx == -1 {
		// no rule for this => junkyard
		w.stats.Inc(linesRejected)
		w.junkBatch.Write(line)
		return
	}

	// write to the corresponding batch buffer
	w.batches[idx].Write(line)

	w.stats.Inc(linesPassed)
	w.stats.Inc(ruleToStatsName(idx))
}

func (w *worker) sendOff() {
	for i, subject := range w.rules.Subjects() {
		batch := w.batches[i]
		if batch.Len() > 0 {
			w.nc.Publish(subject, batch.Bytes())
			batch.Reset()
		}
	}

	// send the junk batch
	if w.junkBatch.Len() > 0 {
		w.nc.Publish(w.junkSubject, w.junkBatch.Bytes())
		w.junkBatch.Reset()
	}
}
