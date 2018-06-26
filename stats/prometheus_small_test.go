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
	"testing"
	"time"

	"github.com/jumptrading/influx-spout/prometheus"
	"github.com/jumptrading/influx-spout/stats"
	"github.com/stretchr/testify/assert"
)

func TestToPrometheus(t *testing.T) {
	snap := stats.Snapshot{
		{"foo", 42},
		{"bar", 99},
		{"qaz", 0},
	}
	ts := time.Date(2009, 2, 13, 23, 31, 30, 0, time.UTC)

	actual := stats.SnapshotToPrometheus(snap, ts, nil)
	expected := []byte(`
bar 99 1234567890000
foo 42 1234567890000
qaz 0 1234567890000
`[1:])
	assert.Equal(t, expected, actual)
}

func TestToPrometheusWithLabels(t *testing.T) {
	snap := stats.Snapshot{
		{Name: "foo", Value: 42},
		{Name: "bar", Value: 99},
		{Name: "qaz", Value: 0},
	}
	ts := time.Date(2009, 2, 13, 23, 31, 30, 0, time.UTC)
	labels := prometheus.Labels{}.With("host", "nyc01").With("level", "high")
	actual := stats.SnapshotToPrometheus(snap, ts, labels)
	expected := []byte(`
bar{host="nyc01",level="high"} 99 1234567890000
foo{host="nyc01",level="high"} 42 1234567890000
qaz{host="nyc01",level="high"} 0 1234567890000
`[1:])
	assert.Equal(t, expected, actual)
}

func TestCounterToPrometheus(t *testing.T) {
	ts := time.Date(2009, 2, 13, 23, 31, 30, 0, time.UTC)
	labels := prometheus.Labels{}.With("host", "nyc01")

	actual := stats.CounterToPrometheus("foo", 99, ts, labels)
	expected := []byte(`foo{host="nyc01"} 99 1234567890000`)
	assert.Equal(t, expected, actual)
}
