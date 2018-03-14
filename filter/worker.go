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
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jumptrading/influx-spout/stats"
)

type worker struct {
	maxTsDeltaNs int64
	rules        *RuleSet
	stats        *stats.Stats
	debug        bool
	nc           natsConn
	junkSubject  string
	batches      []*bytes.Buffer
	junkBatch    *bytes.Buffer
}

func newWorker(
	maxTsDeltaSecs int,
	rules *RuleSet,
	stats *stats.Stats,
	debug bool,
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
		maxTsDeltaNs: int64(maxTsDeltaSecs) * 1e9,
		rules:        rules,
		stats:        stats,
		nc:           nc,
		batches:      batches,
		junkBatch:    new(bytes.Buffer),
		junkSubject:  junkSubject,
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
	now := time.Now().UnixNano()
	minTs := now - w.maxTsDeltaNs
	maxTs := now + w.maxTsDeltaNs

	for _, line := range bytes.SplitAfter(batch, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		w.stats.Inc(linesProcessed)

		ts := extractTimestamp(line, now)
		if minTs < ts && ts < maxTs {
			w.processLine(line)
		} else {
			w.stats.Inc(linesInvalidTime)
			if w.debug {
				log.Printf("invalid line timestamp: %q", string(line))
			}
		}
	}

	// batches have been processed, empty the buffers onto NATS
	w.sendOff()
}

func (w *worker) processLine(line []byte) {
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

// Any realistic timestamp will be 18 or 19 characters long.
const minTsLen = 18
const maxTsLen = 19

func extractTimestamp(line []byte, defaultTs int64) int64 {
	length := len(line)

	// Reject lines that are too short to have a timestamp.
	if length <= minTsLen+6 {
		return defaultTs
	}

	// Remove trailing newline.
	if line[length-1] == '\n' {
		length--
		line = line[:length]
	}

	// Expect a space just before the timestamp.
	for i := length - maxTsLen - 1; i < length-minTsLen; i++ {
		if line[i] == ' ' {
			out, err := fastParseInt(line[i+1:])
			if err != nil {
				return defaultTs
			}
			return out
		}
	}
	return defaultTs
}

const int64Max = (1 << 63) - 1

// fastParseInt is a simpler, faster version of strconv.ParseInt().
// Differences to ParseInt:
// - input is []byte instead of a string (no type conversion required
//   by caller)
// - only supports base 10 input
// - only handles positive values
func fastParseInt(s []byte) (int64, error) {
	if len(s) == 0 {
		return 0, errors.New("empty")
	}

	var n uint64
	for _, c := range s {
		if '0' <= c && c <= '9' {
			c -= '0'
		} else {
			return 0, errors.New("invalid char")
		}
		n = n*10 + uint64(c)
	}

	if n > int64Max {
		return 0, errors.New("overflow")
	}
	return int64(n), nil
}
