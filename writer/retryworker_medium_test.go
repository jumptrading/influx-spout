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

// +build medium

package writer

import (
	"errors"
	"testing"
	"time"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/spouttest"
	"github.com/stretchr/testify/assert"
)

func TestRetryWorkerWrites(t *testing.T) {
	c := &config.Config{
		WriterRetryBatches:  1,
		WriterRetryInterval: config.NewDuration(100 * time.Millisecond),
		WriterRetryTimeout:  config.NewDuration(30 * time.Second),
	}

	batchCh := make(chan []byte)
	db := newFakeDBWriter()
	w := newRetryWorker(batchCh, db, c)
	defer w.Stop()

	sendBatch(t, batchCh, "foo")

	spouttest.AssertRecv(t, db.Writes, "write", "foo")
	spouttest.AssertNoMore(t, db.Writes)
	assertNoErrors(t, w)
}

func TestRetryWorkerTriesUntilTimeout(t *testing.T) {
	// Retry every 250ms for up to 1s.
	c := &config.Config{
		WriterRetryBatches:  1,
		WriterRetryInterval: config.NewDuration(250 * time.Millisecond),
		WriterRetryTimeout:  config.NewDuration(1 * time.Second),
	}

	batchCh := make(chan []byte)
	db := newFakeDBWriter()
	db.MakeReturnErrors(99) // Writes will just keep failing
	w := newRetryWorker(batchCh, db, c)
	defer w.Stop()

	sendBatch(t, batchCh, "foo")

	// Wait for double the timeout to ensure the retry worker stops
	// sending after the batch has timed out.
	timeout := time.After(multDuration(c.WriterRetryTimeout, 2))
	writeCount := 0
	for {
		select {
		case <-db.Writes:
			writeCount++
		case <-timeout:
			// Should have retried every 250ms for 1s. 3 not 4 because
			// the worker waits the retry period before the first
			// send.
			assert.Equal(t, 3, writeCount)
			assertWriteErrorCount(t, w, 3)
			return
		}
	}
}

func TestRetryWorkerMultipleBatches(t *testing.T) {
	c := &config.Config{
		WriterRetryBatches:  3,
		WriterRetryInterval: config.NewDuration(250 * time.Millisecond),
		WriterRetryTimeout:  config.NewDuration(2 * time.Second),
	}

	batchCh := make(chan []byte, 3)
	db := newFakeDBWriter()
	w := newRetryWorker(batchCh, db, c)
	defer w.Stop()

	// Send 3 batches to retry.
	tStart := time.Now()
	tEndMin := tStart.Add(multDuration(c.WriterRetryInterval, 3))
	tEndMax := tEndMin.Add(time.Second)

	sendBatch(t, batchCh, "foo")
	sendBatch(t, batchCh, "bar")
	sendBatch(t, batchCh, "qaz")

	// The 3 batches should be output in the order they were sent.
	spouttest.AssertRecv(t, db.Writes, "foo", "foo")
	spouttest.AssertRecv(t, db.Writes, "bar", "bar")
	spouttest.AssertRecv(t, db.Writes, "qaz", "qaz")

	// Confirm batches were spaced out at retry interval.
	assertTimeBetween(t, tEndMin, tEndMax)

	// Nothing more should be sent.
	spouttest.AssertNoMore(t, db.Writes)
	assertNoErrors(t, w)
}

func TestRetryWorkerLimitsBatches(t *testing.T) {
	c := &config.Config{
		WriterRetryBatches:  2,
		WriterRetryInterval: config.NewDuration(100 * time.Millisecond),
		WriterRetryTimeout:  config.NewDuration(2 * time.Second),
	}

	// Preload 3 batches into the retry channel. Retry worker is
	// configured to only retry up to 2 concurrently so "foo" should
	// get dropped.
	batchCh := make(chan []byte, 3)
	sendBatch(t, batchCh, "foo")
	sendBatch(t, batchCh, "bar")
	sendBatch(t, batchCh, "qaz")

	db := newFakeDBWriter()
	w := newRetryWorker(batchCh, db, c)
	defer w.Stop()

	spouttest.AssertRecv(t, db.Writes, "bar", "bar")
	spouttest.AssertRecv(t, db.Writes, "qaz", "qaz")
	spouttest.AssertNoMore(t, db.Writes)
	assertNoErrors(t, w)
}

func TestRetryWorkerPermanentErrors(t *testing.T) {
	c := &config.Config{
		WriterRetryBatches:  1,
		WriterRetryInterval: config.NewDuration(100 * time.Millisecond),
		WriterRetryTimeout:  config.NewDuration(time.Second),
	}

	batchCh := make(chan []byte, 1)
	sendBatch(t, batchCh, "foo")

	// Set db writer to returns a permanent error for the first write attempt.
	db := newFakeDBWriter()
	db.AddReturnError(newClientError("oh no", true))

	w := newRetryWorker(batchCh, db, c)
	defer w.Stop()

	spouttest.AssertRecv(t, db.Writes, "(failed) write", "foo")
	spouttest.AssertNoMore(t, db.Writes)

	// Check the error report.
	assertWriteErrorCount(t, w, 1)
}

func TestRetryWorkerMixedErrors(t *testing.T) {
	c := &config.Config{
		WriterRetryBatches:  1,
		WriterRetryInterval: config.NewDuration(100 * time.Millisecond),
		WriterRetryTimeout:  config.NewDuration(time.Second),
	}

	batchCh := make(chan []byte, 1)
	sendBatch(t, batchCh, "foo")

	// Set db writer to returns a temporary error and then a permanent error.
	db := newFakeDBWriter()
	db.AddReturnError(newClientError("oh", false))
	db.AddReturnError(newClientError("no", true))

	w := newRetryWorker(batchCh, db, c)
	defer w.Stop()

	// Expect 2 write attempts.
	spouttest.AssertRecv(t, db.Writes, "write 1", "foo")
	spouttest.AssertRecv(t, db.Writes, "write 2", "foo")
	spouttest.AssertNoMore(t, db.Writes)

	// Check the write error reports.
	assertWriteErrorCount(t, w, 2)
}

func newFakeDBWriter() *fakeDBWriter {
	return &fakeDBWriter{
		Writes: make(chan string),
	}
}

type fakeDBWriter struct {
	errs   []error
	Writes chan string
}

func (w *fakeDBWriter) AddReturnError(err error) {
	w.errs = append(w.errs, err)
}

func (w *fakeDBWriter) MakeReturnErrors(n int) {
	for i := 0; i < n; i++ {
		w.AddReturnError(errors.New("boom"))
	}
}

func (w *fakeDBWriter) Write(buf []byte) (err error) {
	w.Writes <- string(buf)
	if len(w.errs) > 0 {
		err, w.errs = w.errs[0], w.errs[1:]
	}
	return
}

func sendBatch(t *testing.T, ch chan []byte, buf string) {
	select {
	case ch <- []byte(buf):
	case <-time.After(spouttest.LongWait):
		t.Fatal("timed out sending to retry worker")
	}
}

func multDuration(d config.Duration, by int) time.Duration {
	return time.Duration(int64(by) * int64(d.Duration))
}

func assertTimeBetween(t *testing.T, minTime, maxTime time.Time) {
	now := time.Now()
	assert.Conditionf(t, func() bool {
		return now.After(minTime) && now.Before(maxTime)
	}, "%s not between %s and %s", fmtT(now), fmtT(minTime), fmtT(maxTime))
}

func fmtT(t time.Time) string {
	return t.Format("15:04:05.999999999")
}

func assertNoErrors(t *testing.T, w *retryWorker) {
	select {
	case err := <-w.WriteErrors():
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(spouttest.ShortWait):
	}
}

func assertWriteErrorCount(t *testing.T, w *retryWorker, expectedCount int) {
	count := 0

loop:
	for {
		select {
		case <-w.WriteErrors():
			count++
			if count == expectedCount {
				break loop
			}
		case <-time.After(spouttest.LongWait):
			t.Fatalf("timed out waiting for errors; saw %d", count)
		}
	}

	assertNoErrors(t, w) // Ensure there aren't extra errors reported
}
