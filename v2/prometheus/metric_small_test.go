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

package prometheus_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jumptrading/influx-spout/v2/prometheus"
)

func TestToBytesMinimal(t *testing.T) {
	m := &prometheus.Metric{
		Name:  []byte("foo"),
		Value: 42,
	}
	assert.Equal(t, []byte("foo 42"), m.ToBytes())
}

func TestToBytesTimestamp(t *testing.T) {
	m := &prometheus.Metric{
		Name:         []byte("foo"),
		Value:        42,
		Milliseconds: 123456,
	}
	assert.Equal(t, []byte("foo 42 123456"), m.ToBytes())
}

func TestToBytesLabels(t *testing.T) {
	m := &prometheus.Metric{
		Name: []byte("foo"),
		Labels: prometheus.Labels{
			{
				Name:  []byte("host"),
				Value: []byte("nyc01"),
			},
			{
				Name:  []byte("thing"),
				Value: []byte("forgot"),
			},
		},
		Value:        42,
		Milliseconds: 123456,
	}
	assert.Equal(t, []byte(`foo{host="nyc01",thing="forgot"} 42 123456`), m.ToBytes())
}
