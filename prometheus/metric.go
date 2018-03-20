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

// Metric represents a single Prometheus metric line, including its
// labels and timestamp.
type Metric struct {
	Name         []byte
	Labels       LabelPairs
	Value        int64
	Milliseconds int64
}

// ToBytes renders the metric to wire format.
func (m *Metric) ToBytes() []byte {
	out := bytes.NewBuffer(m.Name)
	if len(m.Labels) > 0 {
		out.Write(m.Labels.ToBytes())
	}
	fmt.Fprintf(out, " %d", m.Value)
	if m.Milliseconds > 0 {
		fmt.Fprintf(out, " %d", m.Milliseconds)
	}
	return out.Bytes()
}

// LabelPairs contains the set of labels for a metric.
type LabelPairs []LabelPair

// ToBytes renders the label name and value to wire format.
func (p LabelPairs) ToBytes() []byte {
	p.sort() // ensure consistent output order

	out := new(bytes.Buffer)
	out.WriteByte('{')
	for i, label := range p {
		fmt.Fprintf(out, `%s="%s"`, label.Name, label.Value)
		if i < len(p)-1 {
			out.WriteByte(',')
		}
	}
	out.WriteByte('}')
	return out.Bytes()
}

func (p LabelPairs) sort() {
	sort.Slice(p, func(i, j int) bool {
		return bytes.Compare(p[i].Name, p[j].Name) < 0
	})
}

// LabelPair contains a label name and value.
type LabelPair struct {
	Name  []byte
	Value []byte
}
