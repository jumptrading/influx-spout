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

// Label contains a label name and value.
type Label struct {
	Name  []byte
	Value []byte
}

// Labels contains the set of Label instances for a metric.
type Labels []Label

// ToBytes renders the label name and value to wire format.
func (labels Labels) ToBytes() []byte {
	labels.sort() // ensure consistent output order

	out := new(bytes.Buffer)
	out.WriteByte('{')
	for i, label := range labels {
		fmt.Fprintf(out, `%s="%s"`, label.Name, label.Value)
		if i < len(labels)-1 {
			out.WriteByte(',')
		}
	}
	out.WriteByte('}')
	return out.Bytes()
}

func (labels Labels) sort() {
	sort.Slice(labels, func(i, j int) bool {
		return bytes.Compare(labels[i].Name, labels[j].Name) < 0
	})
}

// With returns a new Labels with the additional labels added.
func (labels Labels) With(name, value string) Labels {
	return append(labels, Label{
		Name:  []byte(name),
		Value: []byte(value),
	})
}
