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

// +build small

package downsampler

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssignerEmpty(t *testing.T) {
	clock := newFakeClock()
	a := newAssigner(time.Minute, newFakeBucket, clock)
	require.Nil(t, a.Bytes())

	// Advance time and see that nothing is ever emitted.
	for i := 0; i < 10; i++ {
		clock.Advance(time.Minute)
		require.Nil(t, a.Bytes())
	}
}

func TestTooOld(t *testing.T) {
	clock := newFakeClock() // clock starts right on bucket boundary (Tp)
	a := newAssigner(time.Minute, newFakeBucket, clock)

	ts := clock.now.Add(-61 * time.Second)
	errs := a.Update(makeLine("x", ts))
	assertSingleError(t, errs, "timestamp too old: "+fmt.Sprint(ts))
}

func TestPrevSlot(t *testing.T) {
	clock := newFakeClock() // clock starts right on bucket boundary (Tp)
	a := newAssigner(time.Minute, newFakeBucket, clock)

	// Put some values in the "prev" slot.
	require.Nil(t, a.Update(makeLine("a", clock.now.Add(-30*time.Second))))
	require.Nil(t, a.Update(makeLine("b", clock.now.Add(-time.Minute))))
	require.Nil(t, a.Update(makeLine("c", clock.now.Add(-time.Second))))

	// Ensure nothing being emitted yet (time hasn't advanced).
	assert.Nil(t, a.Bytes())

	// Advance time to just before the bucket boundary. Nothing should
	// be emitted.
	clock.Advance(59 * time.Second)
	assert.Nil(t, a.Bytes())

	// Now advance to the next boundary and see that the correct data
	// was emitted.
	clock.Advance(time.Second)
	assert.Equal(t, "a|b|c", string(a.Bytes()))

	// Try again, and now nothing should be emitted.
	assert.Nil(t, a.Bytes())
}

func TestCurrSlot(t *testing.T) {
	clock := newFakeClock()
	a := newAssigner(time.Minute, newFakeBucket, clock)

	// Put some lines in the "curr" slot.
	require.Nil(t, a.Update(makeLine("a", clock.now)))
	require.Nil(t, a.Update(makeLine("b", clock.now.Add(30*time.Second))))
	require.Nil(t, a.Update(makeLine("c", clock.now.Add(59*time.Second))))

	// Ensure nothing being emitted yet (time hasn't advanced).
	assert.Nil(t, a.Bytes())

	// Advance time to Tc. This should cause the "prev" slot to be
	// emitted (but it should be empty).
	clock.Advance(time.Minute)
	assert.Nil(t, a.Bytes())

	// Advance to just before the next boundary.
	clock.Advance(59 * time.Second)
	assert.Nil(t, a.Bytes())

	// Now advance to the next boundary which should cause the lines
	// to be emitted.
	clock.Advance(time.Second)
	assert.Equal(t, "a|b|c", string(a.Bytes()))

	// Try again, and now nothing should be emitted.
	assert.Nil(t, a.Bytes())
}

func TestNextSlot(t *testing.T) {
	clock := newFakeClock()
	a := newAssigner(time.Minute, newFakeBucket, clock)

	// Put some lines in the "next" slot.
	tc := clock.now.Add(time.Minute)
	require.Nil(t, a.Update(makeLine("a", tc)))
	require.Nil(t, a.Update(makeLine("b", tc.Add(30*time.Second))))
	require.Nil(t, a.Update(makeLine("c", tc.Add(59*time.Second))))

	// Ensure nothing being emitted yet (time hasn't advanced).
	assert.Nil(t, a.Bytes())

	// Advance time to Tc. This should cause the "prev" slot to be
	// emitted (but it should be empty).
	clock.Advance(time.Minute)
	assert.Nil(t, a.Bytes())

	// Advance to next again. This should cause the original "curr"
	// slot to be emitted (but it should be empty).
	clock.Advance(time.Minute)
	assert.Nil(t, a.Bytes())

	// Advance to just before the next boundary.
	clock.Advance(59 * time.Second)
	assert.Nil(t, a.Bytes())

	// Now advance to the next boundary which should cause the lines
	// to be emitted.
	clock.Advance(time.Second)
	assert.Equal(t, "a|b|c", string(a.Bytes()))

	// Try again, and now nothing should be emitted.
	assert.Nil(t, a.Bytes())
}

func TestTooNew(t *testing.T) {
	clock := newFakeClock()
	a := newAssigner(time.Minute, newFakeBucket, clock)

	ts := clock.now.Add(2 * time.Minute)
	errs := a.Update(makeLine("x", ts))
	assertSingleError(t, errs, "timestamp too new: "+fmt.Sprint(ts))
}

func TestAllSlots(t *testing.T) {
	clock := newFakeClock()
	a := newAssigner(time.Minute, newFakeBucket, clock)

	// Put one line in each slot.
	require.Nil(t, a.Update(makeLine("a", clock.now.Add(-time.Second))))
	require.Nil(t, a.Update(makeLine("b", clock.now)))
	require.Nil(t, a.Update(makeLine("c", clock.now.Add(time.Minute))))

	// Ensure nothing being emitted yet (time hasn't advanced).
	assert.Nil(t, a.Bytes())

	// Advance a period. First slot should be emitted.
	clock.Advance(time.Minute)
	assert.Equal(t, "a", string(a.Bytes()))

	// Advance a period. Second slot should be emitted.
	clock.Advance(time.Minute)
	assert.Equal(t, "b", string(a.Bytes()))

	// Advance a period. Third slot should be emitted.
	clock.Advance(time.Minute)
	assert.Equal(t, "c", string(a.Bytes()))

	// Advance again. Nothing left to emit.
	clock.Advance(time.Minute)
	assert.Nil(t, a.Bytes())
}

func TestMultipleInputLines(t *testing.T) {
	clock := newFakeClock()
	a := newAssigner(time.Minute, newFakeBucket, clock)

	// Put two lines in each slot but with a single Update call.
	var lines []byte
	lines = append(lines, makeLine("a0", clock.now.Add(-time.Second))...)
	lines = append(lines, makeLine("a1", clock.now.Add(-20*time.Second))...)
	lines = append(lines, makeLine("b0", clock.now)...)
	lines = append(lines, makeLine("b1", clock.now.Add(10*time.Second))...)
	lines = append(lines, makeLine("c0", clock.now.Add(60*time.Second))...)
	lines = append(lines, makeLine("c1", clock.now.Add(90*time.Second))...)
	require.Nil(t, a.Update(lines))

	// Advance a period. First slot should be emitted.
	clock.Advance(time.Minute)
	assert.Equal(t, "a0|a1", string(a.Bytes()))

	// Advance a period. Second slot should be emitted.
	clock.Advance(time.Minute)
	assert.Equal(t, "b0|b1", string(a.Bytes()))

	// Advance a period. Third slot should be emitted.
	clock.Advance(time.Minute)
	assert.Equal(t, "c0|c1", string(a.Bytes()))

	// Advance again. Nothing left to emit.
	clock.Advance(time.Minute)
	assert.Nil(t, a.Bytes())
}

func TestNoTimestamp(t *testing.T) {
	clock := newFakeClock()
	a := newAssigner(time.Minute, newFakeBucket, clock)

	// The current time is assumed if there's no timestamp on the line.
	require.Nil(t, a.Update([]byte{'x'}))

	clock.Advance(time.Minute)
	assert.Nil(t, a.Bytes())

	clock.Advance(time.Minute)
	assert.Equal(t, "x", string(a.Bytes()))
}

func TestBucketErrors(t *testing.T) {
	clock := newFakeClock()
	a := newAssigner(time.Minute, newFailBucket, clock)

	errs := a.Update(makeLine("x", clock.now))
	assertSingleError(t, errs, "boom")

	errs = a.Update(makeLine("y", clock.now))
	assertSingleError(t, errs, "boom")
}

func TestUntilNext(t *testing.T) {
	clock := newFakeClock()
	a := newAssigner(time.Minute, newFakeBucket, clock)

	assert.Equal(t, time.Minute, a.UntilNext())

	clock.Advance(30 * time.Second)
	assert.Equal(t, 30*time.Second, a.UntilNext())

	clock.Advance(29 * time.Second)
	assert.Equal(t, time.Second, a.UntilNext())

	clock.Advance(time.Second)
	assert.Equal(t, time.Duration(0), a.UntilNext())
	a.Bytes()
	assert.Equal(t, time.Minute, a.UntilNext())

	clock.Advance(time.Second)
	assert.Equal(t, 59*time.Second, a.UntilNext())
}

func newFakeBucket(t time.Time) bucket {
	return &fakeBucket{endTime: t}
}

// fakeBucket keeps the first line of each input to Update. Its Bytes
// method returns those words joined by the pipe symbol.
type fakeBucket struct {
	endTime time.Time
	seen    [][]byte
}

func (b *fakeBucket) EndTime() time.Time {
	return b.endTime
}

func (b *fakeBucket) AddLine(line []byte) []error {
	parts := bytes.SplitN(line, []byte{'\n'}, 2)
	b.seen = append(b.seen, parts[0])
	return nil
}

func (b *fakeBucket) Bytes() []byte {
	if len(b.seen) == 0 {
		return nil
	}
	return bytes.Join(b.seen, []byte{'|'})
}

func newFailBucket(t time.Time) bucket {
	return &failBucket{endTime: t}
}

type failBucket struct {
	endTime time.Time
}

func (b *failBucket) EndTime() time.Time {
	return b.endTime
}

func (b *failBucket) AddLine(line []byte) []error {
	return []error{errors.New("boom")}
}

func (b *failBucket) Bytes() []byte {
	return nil
}

func newFakeClock() *fakeClock {
	return &fakeClock{
		now: time.Date(2018, 8, 28, 14, 11, 0, 0, time.Local),
	}
}

// fakeClock conforms to the clock interface, providing a clock that
// can be manually advanced.
type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func makeLine(text string, t time.Time) []byte {
	out := []byte(text)
	out = append(out, ' ')
	out = append(out, timestamp(t)...)
	out = append(out, '\n')
	return out
}

func timestamp(t time.Time) []byte {
	return []byte(strconv.FormatInt(t.UnixNano(), 10))
}
