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

package stats_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jumptrading/influx-spout/stats"
)

func TestAnonBasics(t *testing.T) {
	s := stats.NewAnon(2)

	assert.Equal(t, uint64(0), s.Get(0))
	assert.Equal(t, uint64(0), s.Get(1))

	assert.Equal(t, uint64(1), s.Inc(0))
	assert.Equal(t, uint64(1), s.Get(0))
	assert.Equal(t, uint64(2), s.Inc(0))
	assert.Equal(t, uint64(2), s.Get(0))
	assert.Equal(t, uint64(3), s.Inc(0))
	assert.Equal(t, uint64(4), s.Inc(0))
	assert.Equal(t, uint64(4), s.Get(0))

	assert.Equal(t, uint64(0), s.Get(1))
}

func TestAnonConcurrentAdd(t *testing.T) {
	s := stats.NewAnon(1)

	const goroutines = 100
	const incs = 100
	wg := new(sync.WaitGroup)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			var last uint64
			defer wg.Done()
			for i := 0; i < incs; i++ {
				current := s.Inc(0)
				if current <= last {
					panic("did not increment")
				}
				last = current
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, uint64(goroutines*incs), s.Get(0))
}

func TestAnonMax(t *testing.T) {
	s := stats.NewAnon(1)

	assert.Equal(t, uint64(0), s.Get(0))
	assert.Equal(t, uint64(0), s.Max(0, 0))
	assert.Equal(t, uint64(0), s.Get(0))

	assert.Equal(t, uint64(4), s.Max(0, 4))
	assert.Equal(t, uint64(4), s.Get(0))

	assert.Equal(t, uint64(4), s.Max(0, 3))
	assert.Equal(t, uint64(4), s.Get(0))

	assert.Equal(t, uint64(5), s.Max(0, 5))
	assert.Equal(t, uint64(5), s.Get(0))
}

func TestAnonConcurrentMax(t *testing.T) {
	s := stats.NewAnon(1)

	const goroutines = 100
	const top = 100
	wg := new(sync.WaitGroup)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for i := uint64(1); i <= top; i++ {
				if s.Max(0, uint64(i)) < i {
					panic("max failed")
				}
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, uint64(top), s.Get(0))
}

func TestAnonInvalid(t *testing.T) {
	s := stats.NewAnon(1)

	assert.Equal(t, uint64(0), s.Get(0))
	assert.Panics(t, func() { s.Get(1) })
	assert.Panics(t, func() { s.Inc(1) })
	assert.Panics(t, func() { s.Max(1, 1) })
	assert.Equal(t, uint64(0), s.Get(0))
}

func TestAnonSnapshot(t *testing.T) {
	s := stats.NewAnon(3)
	s.Inc(0)
	s.Inc(1)
	s.Inc(1)

	assert.Equal(t, []uint64{1, 2, 0}, s.Snapshot())
}

func BenchmarkAnonStats(b *testing.B) {
	s := stats.NewAnon(2)

	for i := 0; i < b.N; i++ {
		s.Inc(0)
		_ = s.Get(0)

		_ = s.Get(1)
		s.Inc(1)
	}
}
