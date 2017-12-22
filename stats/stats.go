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
	"sync"
)

// New creates a new Stats instance using the provided stats names.
func New(names ...string) *Stats {
	s := &Stats{
		counts: make(map[string]int),
	}
	for _, name := range names {
		s.counts[name] = 0
	}
	return s
}

// Stats tracks a number of counters in a goroutine safe way.
type Stats struct {
	mu     sync.Mutex
	counts map[string]int
}

// Get retrieves the current value of a counter. It panics if the
// counter is not valid.
func (s *Stats) Get(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count, ok := s.counts[name]
	if !ok {
		panic(fmt.Sprintf("unknown stat: %q", name))
	}
	return count
}

// Inc increments a stats counter, returning the new value. It panics
// if the counter is not valid.
func (s *Stats) Inc(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.counts[name]; !ok {
		panic(fmt.Sprintf("unknown stat: %q", name))
	}
	s.counts[name]++
	return s.counts[name]
}

// Clone returns a new Stats instance, copying the source Stats
// counts.
func (s *Stats) Clone() *Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := &Stats{
		counts: make(map[string]int),
	}
	for name, count := range s.counts {
		out.counts[name] = count
	}
	return out
}
