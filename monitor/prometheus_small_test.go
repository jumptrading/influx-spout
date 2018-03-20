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

package monitor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/monitor"
)

func TestBasic(t *testing.T) {
	m, err := monitor.ParseMetric([]byte("foo 42"))
	require.NoError(t, err)
	assert.Equal(t, &monitor.Metric{
		Name:  []byte("foo"),
		Value: 42,
	}, m)
}

func TestEmpty(t *testing.T) {
	m, err := monitor.ParseMetric([]byte(""))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid metric")
}

func TestNoValue(t *testing.T) {
	m, err := monitor.ParseMetric([]byte("what"))
	assert.Nil(t, m)
	assert.EqualError(t, err, "no value")
}

func TestEmptyValue(t *testing.T) {
	m, err := monitor.ParseMetric([]byte("what "))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid value")
}

func TestFloatValue(t *testing.T) {
	m, err := monitor.ParseMetric([]byte("foo 12.32"))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid value")
}

func TestStringValue(t *testing.T) {
	m, err := monitor.ParseMetric([]byte("foo bar"))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid value")
}

func TestLabel(t *testing.T) {
	m, err := monitor.ParseMetric([]byte(`foo{method="post"} 2`))
	require.NoError(t, err)
	assert.Equal(t, &monitor.Metric{
		Name: []byte("foo"),
		Labels: monitor.LabelPairs{{
			Name:  []byte("method"),
			Value: []byte("post"),
		}},
		Value: 2,
	}, m)
}

func TestEmptyLabels(t *testing.T) {
	m, err := monitor.ParseMetric([]byte(`foo{} 2`))
	require.NoError(t, err)
	assert.Equal(t, &monitor.Metric{
		Name:  []byte("foo"),
		Value: 2,
	}, m)
}

func TestMultipleLabels(t *testing.T) {
	m, err := monitor.ParseMetric([]byte(`foo{method="post",code="200"} 2`))
	require.NoError(t, err)
	assert.Equal(t, &monitor.Metric{
		Name: []byte("foo"),
		Labels: monitor.LabelPairs{
			{
				Name:  []byte("method"),
				Value: []byte("post"),
			},
			{
				Name:  []byte("code"),
				Value: []byte("200"),
			},
		},
		Value: 2,
	}, m)
}

func TestBadLabelSep(t *testing.T) {
	m, err := monitor.ParseMetric([]byte(`foo{method="post"/code="200"} 2`))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid label separator")
}

func TestNoLabelValue(t *testing.T) {
	m, err := monitor.ParseMetric([]byte(`foo{method} 2`))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid label")
}

func TestMissingClosingBrace(t *testing.T) {
	m, err := monitor.ParseMetric([]byte(`foo{method="post" 2`))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid label separator")
}

func TestMissingLabelOpeningQuote(t *testing.T) {
	m, err := monitor.ParseMetric([]byte(`foo{method=post} 2`))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid label")
}

func TestMissingLabelClosingQuotes(t *testing.T) {
	m, err := monitor.ParseMetric([]byte(`foo{method="post} 2`))
	assert.Nil(t, m)
	assert.EqualError(t, err, "missing label closing quote")
}

func TestTimestamp(t *testing.T) {
	m, err := monitor.ParseMetric([]byte("foo 42 1234567"))
	require.NoError(t, err)
	assert.Equal(t, &monitor.Metric{
		Name:         []byte("foo"),
		Value:        42,
		Milliseconds: 1234567,
	}, m)
}

func TestInvalidTimestamp(t *testing.T) {
	m, err := monitor.ParseMetric([]byte("foo 42 abc"))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid timestamp")
}

func TestTrailingSpace(t *testing.T) {
	m, err := monitor.ParseMetric([]byte("foo 42 "))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid timestamp")
}

func TestLabelsAndTimestamp(t *testing.T) {
	m, err := monitor.ParseMetric([]byte(`foo{host="nyc01",bar="definitely",thing="forgot"} 42 123456789`))
	require.NoError(t, err)
	assert.Equal(t, &monitor.Metric{
		Name: []byte("foo"),
		Labels: monitor.LabelPairs{
			{
				Name:  []byte("host"),
				Value: []byte("nyc01"),
			},
			{
				Name:  []byte("bar"),
				Value: []byte("definitely"),
			},
			{
				Name:  []byte("thing"),
				Value: []byte("forgot"),
			},
		},
		Value:        42,
		Milliseconds: 123456789,
	}, m)
}
