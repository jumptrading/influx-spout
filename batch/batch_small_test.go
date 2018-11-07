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

package batch

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	b := New(64)

	assert.Equal(t, 0, b.Size())
	assert.Equal(t, 64, b.Remaining())
	assert.Equal(t, 0, b.Writes())
	assert.Equal(t, time.Duration(0), b.Age())
	assert.Equal(t, []byte{}, b.Bytes())
}

func TestAppend(t *testing.T) {
	b := New(10)

	b.Append([]byte("foo"))
	assert.Equal(t, 3, b.Size())
	assert.Equal(t, 7, b.Remaining())
	assert.Equal(t, 1, b.Writes())
	assert.Equal(t, []byte("foo"), b.Bytes())

	b.Append([]byte("bar"))
	assert.Equal(t, 6, b.Size())
	assert.Equal(t, 4, b.Remaining())
	assert.Equal(t, 2, b.Writes())
	assert.Equal(t, []byte("foobar"), b.Bytes())
}

func TestAppendGrow(t *testing.T) {
	b := New(2) // only 2 bytes!

	b.Append([]byte("foo")) // add 3 bytes of data to cause growth
	assert.Equal(t, 3, b.Size())
	assert.Equal(t, 1, b.Remaining())
	assert.Equal(t, 1, b.Writes())
	assert.Equal(t, []byte("foo"), b.Bytes())

	b.Append([]byte("bar")) // add another 3 bytes of data to cause growth again
	assert.Equal(t, 6, b.Size())
	assert.Equal(t, 2, b.Remaining())
	assert.Equal(t, 2, b.Writes())
	assert.Equal(t, []byte("foobar"), b.Bytes())
}

func TestReadFrom(t *testing.T) {
	b := New(10)
	r := bytes.NewReader([]byte("foo"))
	count, err := b.ReadFrom(r)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)

	assert.Equal(t, 3, b.Size())
	assert.Equal(t, 37, b.Remaining())
	assert.Equal(t, 1, b.Writes())
	assert.Equal(t, []byte("foo"), b.Bytes())
}

func TestReadOnceFrom(t *testing.T) {
	b := New(10)
	r := bytes.NewReader([]byte("foo"))
	count, err := b.ReadOnceFrom(r)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	assert.Equal(t, 3, b.Size())
	assert.Equal(t, 17, b.Remaining())
	assert.Equal(t, 1, b.Writes())
	assert.Equal(t, []byte("foo"), b.Bytes())
}

func TestReset(t *testing.T) {
	b := New(10)
	b.Append([]byte("foo"))
	assert.Equal(t, []byte("foo"), b.Bytes())

	b.Reset()
	assert.Equal(t, 0, b.Size())
	assert.Equal(t, 10, b.Remaining())
	assert.Equal(t, 0, b.Writes())
	assert.Equal(t, time.Duration(0), b.Age())
	assert.Equal(t, []byte{}, b.Bytes())
}

func TestCopyBytes(t *testing.T) {
	b := New(10)
	b.Append([]byte("foo"))

	b0 := b.Bytes()
	assert.Equal(t, []byte("foo"), b0)

	b1 := b.CopyBytes()
	assert.Equal(t, []byte("foo"), b1)

	// Reset and reuse the batch.
	b.Reset()
	b.Append([]byte("bar"))

	// b0 should reflect the new data. b1 should remain unchanged.
	assert.Equal(t, []byte("bar"), b0)
	assert.Equal(t, []byte("foo"), b1)
}

func TestEnsureNewline(t *testing.T) {
	b := New(64)

	// Does nothing if batch is empty.
	b.EnsureNewline()
	assert.Equal(t, []byte{}, b.Bytes())

	// Newline added if needed.
	b.Append([]byte("foo"))
	b.EnsureNewline()
	assert.Equal(t, []byte("foo\n"), b.Bytes())

	// No newline added if not required.
	b.Append([]byte("bar\n"))
	b.EnsureNewline()
	assert.Equal(t, []byte("foo\nbar\n"), b.Bytes())

	// Addition of newlines shouldn't contribute to write count.
	assert.Equal(t, 2, b.Writes())
}

func TestAge(t *testing.T) {
	// Batch shouldn't age if there's no data in it.
	b := New(10)
	assert.Equal(t, time.Duration(0), b.Age())
	testClock.advance(time.Minute)
	assert.Equal(t, time.Duration(0), b.Age())
	testClock.advance(time.Minute)
	assert.Equal(t, time.Duration(0), b.Age())

	// First write.
	b.Append([]byte("foo"))
	assert.Equal(t, time.Duration(0), b.Age())

	testClock.advance(time.Minute)
	assert.Equal(t, time.Minute, b.Age())

	b.Append([]byte("bar"))
	assert.Equal(t, time.Minute, b.Age())

	testClock.advance(time.Minute)
	assert.Equal(t, 2*time.Minute, b.Age())

	// Reset should reset the age again
	testClock.advance(time.Minute)
	b.Reset()
	assert.Equal(t, time.Duration(0), b.Age())

	testClock.advance(time.Second)
	b.Append([]byte("foo"))
	assert.Equal(t, time.Duration(0), b.Age())

	testClock.advance(time.Second)
	assert.Equal(t, time.Second, b.Age())
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) advance(d time.Duration)         { c.now = c.now.Add(d) }
func (c *fakeClock) Now() time.Time                  { return c.now }
func (c *fakeClock) Since(t time.Time) time.Duration { return c.now.Sub(t) }

var testClock *fakeClock

func init() {
	testClock = &fakeClock{
		now: time.Now(),
	}
	clock = testClock
}
