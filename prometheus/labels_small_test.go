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

func TestToBytesEmpty(t *testing.T) {
	var labels prometheus.Labels
	assert.Equal(t, labels.ToBytes(), []byte("{}"))
}

func TestToBytesSingle(t *testing.T) {
	labels := prometheus.Labels{
		{Name: []byte("foo"), Value: []byte("aaa")},
	}
	assert.Equal(t, labels.ToBytes(), []byte(`{foo="aaa"}`))
}

func TestToBytesMultiple(t *testing.T) {
	labels := prometheus.Labels{
		{Name: []byte("foo"), Value: []byte("aaa")},
		{Name: []byte("bar"), Value: []byte("bbbb")},
	}
	// Note reordering.
	assert.Equal(t, labels.ToBytes(), []byte(`{bar="bbbb",foo="aaa"}`))
}

func TestWith(t *testing.T) {
	labels0 := prometheus.Labels{
		{Name: []byte("foo"), Value: []byte("aaa")},
	}
	labels1 := labels0.With("bar", "bbb")
	labels2 := labels0.With("bar", "none")
	labels3 := labels2.With("che", "sne")

	assert.Equal(t, labels1, prometheus.Labels{
		{Name: []byte("foo"), Value: []byte("aaa")},
		{Name: []byte("bar"), Value: []byte("bbb")},
	})

	assert.Equal(t, labels2, prometheus.Labels{
		{Name: []byte("foo"), Value: []byte("aaa")},
		{Name: []byte("bar"), Value: []byte("none")},
	})

	assert.Equal(t, labels3, prometheus.Labels{
		{Name: []byte("foo"), Value: []byte("aaa")},
		{Name: []byte("bar"), Value: []byte("none")},
		{Name: []byte("che"), Value: []byte("sne")},
	})

	// Original should be unchanged.
	assert.Equal(t, labels0, prometheus.Labels{
		{Name: []byte("foo"), Value: []byte("aaa")},
	})
}
