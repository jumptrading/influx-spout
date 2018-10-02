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

package writer

import (
	"container/list"
	"sync"
	"time"

	"github.com/jumptrading/influx-spout/config"
)

type dbWriter interface {
	Write([]byte) error
}

func newRetryWorker(
	inputCh <-chan []byte,
	dbWriter dbWriter,
	c *config.Config,
) *retryWorker {
	w := &retryWorker{
		inputCh:       inputCh,
		dbWriter:      dbWriter,
		retryInterval: c.WriterRetryInterval.Duration,
		queue: &retryQueue{
			maxBatches: c.WriterRetryBatches,
			maxTTL:     c.WriterRetryTimeout.Duration,
		},
		writeErrors: make(chan error, 16),
		stop:        make(chan struct{}),
	}
	w.wg.Add(1)
	go w.loop()
	return w
}

// retryWorker implements a goroutine which maintains a queue of
// batches which are to be written to an InfluxDB instance.
//
// When there one or more batches to send it will attempt to write
// *one* batch to InfluxDB at every retry interval. The limits the
// load that retries may cause on InfluxDB.

// To limit the maximum amount of memory that can be consumed, the
// retryWorker will only allow a limited number of batches to be
// retried at any one time. Old batches will be discarded if necessary
// to enforce this constraint.
//
// Failures to write to InfluxDB are reported to the channel returned
// by WriteErrors().  This channel should be consumed regularly.
type retryWorker struct {
	inputCh       <-chan []byte
	dbWriter      dbWriter
	retryInterval time.Duration
	writeErrors   chan error
	queue         *retryQueue
	stop          chan struct{}
	wg            sync.WaitGroup
}

func (w *retryWorker) Stop() {
	close(w.stop)
	w.wg.Wait()
}

func (w *retryWorker) WriteErrors() <-chan error {
	return w.writeErrors
}

func (w *retryWorker) loop() {
	defer w.wg.Done()

	var nextSend <-chan time.Time
	for {
		if nextSend == nil && w.queue.Len() > 0 {
			nextSend = time.After(w.retryInterval)
		}
		select {
		case <-w.stop:
			return
		case buf := <-w.inputCh:
			w.queue.Add(buf)
		case <-nextSend:
			nextSend = nil

			buf := w.queue.Front()
			if buf != nil {
				err := w.dbWriter.Write(buf)

				if err != nil {
					if isPermanentError(err) {
						w.queue.DropFront()
					} else {
						w.queue.CycleFront()
					}

					w.reportError(err)
				} else {
					w.queue.DropFront()
				}
			}
		}
	}
}

func (w *retryWorker) reportError(err error) {
	select {
	case w.writeErrors <- err:
	default:
	}
}

// retryQueue maintains a list of batches that should be retried. Each
// batch has a maximum time-to-live (TTL) associated with it, set when
// a batch is added to the queue, calculated using the queues maxTTL
// attribute.
//
// Batches are automatically removed from the queue when their TTL is
// exceeded.
//
// The queue also limits the number batches that may be stored at
// once. If the limit is exceeded, the oldest batch in the queue will
// be dropped.
//
// The user of the queue is expected to primarily interact with the
// batch at the front of the queue. Front, DropFront and CycleFront
// all interact with the first batch in the queue.
type retryQueue struct {
	maxTTL     time.Duration
	maxBatches int
	batches    list.List
}

func (q *retryQueue) Len() int {
	return q.batches.Len()
}

// Add a new batch to the end of the queue. If the queue's batch limit
// is exceeded, the oldest batch in the queue will be dropped.
func (q *retryQueue) Add(batch []byte) {
	if q.batches.Len() >= q.maxBatches {
		q.removeOldest()
	}
	q.batches.PushBack(retryBatch{
		buf: batch,
		ttl: time.Now().Add(q.maxTTL),
	})
}

// Front returns the buffer at the start of the queue. Nil is returned
// if the queue is empty.
func (q *retryQueue) Front() []byte {
	now := time.Now()
	for {
		el := q.batches.Front()
		if el == nil {
			return nil
		}

		batch := el.Value.(retryBatch)
		if now.After(batch.ttl) {
			q.batches.Remove(el)
		} else {
			return batch.buf
		}
	}
}

// DropFront removes the batch at the front of the queue.
func (q *retryQueue) DropFront() {
	el := q.batches.Front()
	if el == nil {
		return
	}
	q.batches.Remove(el)
}

// CycleFront moves the batch at the front of the queue to the back.
func (q *retryQueue) CycleFront() {
	el := q.batches.Front()
	if el == nil {
		return
	}
	q.batches.MoveToBack(el)
}

func (q *retryQueue) removeOldest() {
	var removeE *list.Element
	var minTTL time.Time
	for e := q.batches.Front(); e != nil; e = e.Next() {
		b := e.Value.(retryBatch)
		if removeE == nil || b.ttl.Before(minTTL) {
			removeE = e
			minTTL = b.ttl
		}
	}
	if removeE != nil {
		q.batches.Remove(removeE)
	}
}

type retryBatch struct {
	buf []byte
	ttl time.Time
}
