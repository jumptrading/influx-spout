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
	"time"

	"github.com/jumptrading/influx-spout/prometheus"
)

// NewLabels creates a new prometheus.Labels with the component and
// name set as given.
func NewLabels(component, name string) prometheus.Labels {
	return prometheus.Labels{
		{Name: []byte("component"), Value: []byte(component)},
		{Name: []byte("name"), Value: []byte(name)},
	}
}

// SnapshotToPrometheus takes Snapshot produced by a Stats instance
// and formats it into Prometheus metrics lines using the timestamp
// and labels provided.
func SnapshotToPrometheus(snap Snapshot, now time.Time, labels prometheus.Labels) []byte {
	millis := timeToMillis(now)
	set := prometheus.NewMetricSet()
	for _, counter := range snap {
		set.Update(&prometheus.Metric{
			Name:         []byte(counter.Name),
			Labels:       labels,
			Value:        int64(counter.Value),
			Milliseconds: millis,
		})
	}
	return set.ToBytes()
}

// CounterToPrometheus generates a single Prometheus line for a counter.
func CounterToPrometheus(name string, value int, now time.Time, labels prometheus.Labels) []byte {
	metric := &prometheus.Metric{
		Name:         []byte(name),
		Labels:       labels,
		Value:        int64(value),
		Milliseconds: timeToMillis(now),
	}
	return metric.ToBytes()
}

func timeToMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}
