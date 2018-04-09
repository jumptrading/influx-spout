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

// +build small

package stats_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jumptrading/influx-spout/stats"
)

func TestBasics(t *testing.T) {
	s := stats.New("foo", "bar")

	assert.Equal(t, 0, s.Get("foo"))
	assert.Equal(t, 0, s.Get("bar"))

	assert.Equal(t, 1, s.Inc("foo"))
	assert.Equal(t, 1, s.Get("foo"))
	assert.Equal(t, 2, s.Inc("foo"))
	assert.Equal(t, 2, s.Get("foo"))
	assert.Equal(t, 3, s.Inc("foo"))
	assert.Equal(t, 4, s.Inc("foo"))
	assert.Equal(t, 4, s.Get("foo"))

	assert.Equal(t, 0, s.Get("bar"))
}

func TestInvalid(t *testing.T) {
	s := stats.New("foo")

	assert.Panics(t, func() { s.Get("bar") })
	assert.Panics(t, func() { s.Inc("bar") })
	assert.Equal(t, 0, s.Get("foo"))
}

func TestSnapshot(t *testing.T) {
	s := stats.New("foo", "bar", "qaz")
	s.Inc("foo")
	s.Inc("bar")
	s.Inc("bar")

	assert.ElementsMatch(t, []stats.CounterPair{
		{"foo", 1},
		{"bar", 2},
		{"qaz", 0},
	}, s.Snapshot())
}

func TestClone(t *testing.T) {
	s := stats.New("foo", "bar", "qaz")
	s.Inc("foo")
	s.Inc("bar")
	s.Inc("bar")

	s2 := s.Clone()
	assert.Equal(t, 1, s2.Get("foo"))
	assert.Equal(t, 2, s2.Get("bar"))
	assert.Equal(t, 0, s2.Get("qaz"))

	// Make sure they're independent
	s2.Inc("foo")
	assert.Equal(t, 2, s2.Get("foo"))
	assert.Equal(t, 1, s.Get("foo"))
}

func BenchmarkStats(b *testing.B) {
	s := stats.New("foo", "bar")

	for i := 0; i < b.N; i++ {
		s.Inc("foo")
		_ = s.Get("foo")

		_ = s.Get("bar")
		s.Inc("bar")
	}
}
