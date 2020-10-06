// Copyright 2017 Jump Trading
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
	"fmt"
)

// New creates a new Stats instance using the provided stats names.
func New(names ...string) *Stats {
	nameIndex := make(map[string]int)
	for i, name := range names {
		nameIndex[name] = i
	}
	return &Stats{
		counts:    NewAnon(len(names)),
		nameIndex: nameIndex,
	}
}

// Stats tracks a number of named counters in a goroutine safe way. It
// wraps an AnonStats.
type Stats struct {
	counts    *AnonStats
	nameIndex map[string]int
}

// Get retrieves the current value of a counter. It panics if the
// counter is not valid.
func (s *Stats) Get(name string) uint64 {
	return s.counts.Get(s.lookup(name))
}

// Inc increments a stats counter, returning the new value. It panics
// if the counter is not valid.
func (s *Stats) Inc(name string) uint64 {
	return s.counts.Inc(s.lookup(name))
}

// Max updates a counter if the value supplied is greater than the
// previous one. It panics if the name is not valid.
func (s *Stats) Max(name string, newVal uint64) uint64 {
	return s.counts.Max(s.lookup(name), newVal)
}

func (s *Stats) lookup(name string) int {
	i, ok := s.nameIndex[name]
	if !ok {
		panic(fmt.Sprintf("unknown stat: %q", name))
	}
	return i
}

// CounterPair holds the and value for one Stats counter at a given
// point in time.
type CounterPair struct {
	Name  string
	Value uint64
}

// Snapshot holds the names and values of some counters.
type Snapshot []CounterPair

// Snapshot returns the current values of all the counters.
func (s *Stats) Snapshot() Snapshot {
	out := make([]CounterPair, 0, len(s.nameIndex))
	for name, i := range s.nameIndex {
		out = append(out, CounterPair{name, s.counts.Get(i)})
	}
	return out
}
