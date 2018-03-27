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

package prometheus

import (
	"bytes"
	"fmt"
	"sort"
)

// NewMetricSet returns an empty MetricSet.
func NewMetricSet() *MetricSet {
	return &MetricSet{
		metrics: make(map[string]*Metric),
	}
}

// MetricSet is a collection of metrics. Metrics are indexed by name
// and labels combined. Existing metrics will be updated if an update
// for the same name and labels arrives.
type MetricSet struct {
	metrics map[string]*Metric
}

// All returns all metrics in the set as a slice.
func (set *MetricSet) All() []*Metric {
	out := make([]*Metric, 0, len(set.metrics))
	for _, m := range set.metrics {
		out = append(out, m)
	}
	return out
}

// Update adds a new metric or updates an existing one in the set,
// overwriting previous values.
func (set *MetricSet) Update(m *Metric) {
	set.metrics[metricKey(m)] = m
}

// UpdateFromSet updates the values in the set from another MetricSet.
func (set *MetricSet) UpdateFromSet(other *MetricSet) {
	for _, m := range other.All() {
		set.Update(m)
	}
}

// ToBytes serialise the metrics contained in the MetricSet to the
// Prometheus exposition format.
func (set *MetricSet) ToBytes() []byte {
	keys := make([]string, 0, len(set.metrics))
	for key := range set.metrics {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := new(bytes.Buffer)
	for _, key := range keys {
		out.Write(set.metrics[key].ToBytes())
		out.WriteByte('\n')
	}
	return out.Bytes()
}

func metricKey(m *Metric) string {
	return fmt.Sprintf("%s%s", m.Name, m.Labels.ToBytes())
}
