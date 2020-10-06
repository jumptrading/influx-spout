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

package stats

import (
	"sync/atomic"
)

// NewAnon creates a new AnonStats instance of the given size.
func NewAnon(size int) *AnonStats {
	return &AnonStats{
		counts: make([]uint64, size),
	}
}

// AnonStats tracks a number of counters in a goroutine safe
// way. Counters are addressed by integer index.
type AnonStats struct {
	counts []uint64
}

// Get retrieves the current value of a counter. It panics if the
// counter is not valid.
func (s *AnonStats) Get(i int) uint64 {
	return atomic.LoadUint64(&s.counts[i])
}

// Inc increments a stats counter, returning the new value. It panics
// if the counter is not valid.
func (s *AnonStats) Inc(i int) uint64 {
	return atomic.AddUint64(&s.counts[i], 1)
}

// Max updates a counter if the value supplied is greater than the
// previous one. It panics if the counter is not valid.
func (s *AnonStats) Max(i int, newVal uint64) uint64 {
	pCount := &s.counts[i]
	for {
		curVal := atomic.LoadUint64(pCount)
		if newVal <= curVal {
			return curVal
		}
		if atomic.CompareAndSwapUint64(pCount, curVal, newVal) {
			return newVal
		}
	}
}

// Snapshot returns the current values of all the counters.
func (s *AnonStats) Snapshot() []uint64 {
	numCounts := len(s.counts)
	out := make([]uint64, numCounts)
	for i := 0; i < numCounts; i++ {
		out[i] = s.Get(i)
	}
	return out
}
