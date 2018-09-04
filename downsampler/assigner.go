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
	"bytes"
	"fmt"
	"time"

	"github.com/jumptrading/influx-spout/influx"
)

// bucket defines the interface for some type which collects lines for
// some time slot.
type bucket interface {
	AddLine([]byte) []error
	Bytes() []byte
	EndTime() time.Time
}

// bucketFactory defines the type of a function which creates new
// buckets.
type bucketFactory func(time.Time) bucket

// newAssigner creates a assigner using the sampling period
// specified. The returned assigner creates buckets using the provided
// factory function and obtains the current time using the provided
// clock.
func newAssigner(period time.Duration, newBucket bucketFactory, clock clock) *assigner {
	a := &assigner{
		period:    period,
		clock:     clock,
		newBucket: newBucket,
	}
	tc := a.nextT(clock.Now())
	tp := a.prevT(tc)
	tn := a.nextT(tc)
	a.buckets[prev] = newBucket(tp)
	a.buckets[curr] = newBucket(tc)
	a.buckets[next] = newBucket(tn)
	a.tmin = tp.Add(-a.period)
	return a
}

// These consts define the indexes for the buckets available to the
// assigner at any given point in time.
const (
	prev = 0
	curr = 1
	next = 2
)

/* assigner takes batches of measurement lines, extracts the timestamp
from each line and assigns each line to one of 3 buckets (previous,
current or next). Multiple buckets are used in order to account for
clock skew between the receiving host and the hosts that generated the
measurements.

Each bucket aggregates measurements for a specific time range and has
a width of the downsampler sampling period.

When the clock reaches the end time of the "current" bucket, the
"previous" bucket is emitted and discarded. The "current" bucket then
becomes the "previous" bucket, "next becomes "current" and a new
"next" bucket is created.

Measurements which fall outside of the range of the buckets being
tracked by the assigner are discarded (errors are reported).
*/
type assigner struct {
	period    time.Duration
	clock     clock
	newBucket bucketFactory
	buckets   [3]bucket
	tmin      time.Time
}

// Update takes a byte slice of measurement lines and assigns them to
// an internal bucket based on timestamp. Any lines which contain
// timestamps which fall outside of the range of the assigner's
// buckets will result in an error. Data errors for any lines will
// also result in errors being returned.
func (a *assigner) Update(lines []byte) (errs []error) {
	for _, line := range bytes.Split(lines, []byte{'\n'}) {
		if len(line) < 1 {
			continue
		}

		ts, line := a.extractTimestamp(line)
		b, err := a.findBucket(ts)
		if err != nil {
			errs = append(errs, err)
		} else {
			lineErrs := b.AddLine(line)
			errs = append(errs, lineErrs...)
		}
	}
	return errs
}

// Bytes returns the aggregated lines for the oldest bucket, if it is
// ready to be returned. Nil is returned otherwise.
//
// When data is returned, the oldest bucket is discarded and the other
// buckets are cycled down.
func (a *assigner) Bytes() []byte {
	if a.clock.Now().Before(a.buckets[curr].EndTime()) {
		return nil
	}

	out := a.buckets[prev].Bytes()
	a.buckets[prev] = a.buckets[curr]
	a.buckets[curr] = a.buckets[next]
	a.buckets[next] = a.newBucket(a.nextT(a.buckets[curr].EndTime()))
	a.tmin = a.buckets[prev].EndTime().Add(-a.period)
	return out
}

// UntilNext returns the time until the next bucket will be released
// for output.
func (a *assigner) UntilNext() time.Duration {
	d := a.buckets[curr].EndTime().Sub(a.clock.Now())
	if d < time.Duration(0) {
		return time.Duration(0)
	}
	return d
}

func (a *assigner) prevT(t time.Time) time.Time {
	return t.Add(-a.period).Truncate(a.period)
}

func (a *assigner) nextT(t time.Time) time.Time {
	return t.Add(a.period).Truncate(a.period)
}

func (a *assigner) findBucket(ts time.Time) (bucket, error) {
	if ts.Before(a.tmin) {
		return nil, fmt.Errorf("timestamp too old: %s", ts)
	}
	for _, b := range a.buckets {
		if ts.Before(b.EndTime()) {
			return b, nil
		}
	}
	return nil, fmt.Errorf("timestamp too new: %s", ts)
}

// extractTimestamp returns the timestamp from the end of the line and
// returns the line with the timestamp removed. The current time is
// used if no timestamp was present.
func (a *assigner) extractTimestamp(line []byte) (time.Time, []byte) {
	nanos, offset := influx.ExtractTimestamp(line)
	if offset < 0 {
		return a.clock.Now(), line
	}
	return time.Unix(0, nanos), line[:offset-1]
}

// clock define an interface for retrieving the current time.
type clock interface {
	Now() time.Time
}

// realClock conforms to the clock interface and reports the actual
// time.
type realClock struct{}

func (c *realClock) Now() time.Time {
	return time.Now()
}
