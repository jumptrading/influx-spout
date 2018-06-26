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
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/prometheus"
)

func TestParseBasic(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte("foo 42"))
	require.NoError(t, err)
	assert.Equal(t, &prometheus.Metric{
		Name:  []byte("foo"),
		Value: 42,
	}, m)
}

func TestParseEmpty(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte(""))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid metric")
}

func TestParseNoValue(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte("what"))
	assert.Nil(t, m)
	assert.EqualError(t, err, "no value")
}

func TestParseEmptyValue(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte("what "))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid value")
}

func TestParseFloatValue(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte("foo 12.32"))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid value")
}

func TestParseStringValue(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte("foo bar"))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid value")
}

func TestParseLabel(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte(`foo{method="post"} 2`))
	require.NoError(t, err)
	assert.Equal(t, &prometheus.Metric{
		Name: []byte("foo"),
		Labels: prometheus.Labels{{
			Name:  []byte("method"),
			Value: []byte("post"),
		}},
		Value: 2,
	}, m)
}

func TestParseEmptyLabels(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte(`foo{} 2`))
	require.NoError(t, err)
	assert.Equal(t, &prometheus.Metric{
		Name:  []byte("foo"),
		Value: 2,
	}, m)
}

func TestParseMultipleLabels(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte(`foo{method="post",code="200"} 2`))
	require.NoError(t, err)
	assert.Equal(t, &prometheus.Metric{
		Name: []byte("foo"),
		Labels: prometheus.Labels{
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

func TestParseBadLabelSep(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte(`foo{method="post"/code="200"} 2`))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid label separator")
}

func TestParseNoLabelValue(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte(`foo{method} 2`))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid label")
}

func TestParseMissingClosingBrace(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte(`foo{method="post" 2`))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid label separator")
}

func TestParseMissingLabelOpeningQuote(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte(`foo{method=post} 2`))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid label")
}

func TestParseMissingLabelClosingQuotes(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte(`foo{method="post} 2`))
	assert.Nil(t, m)
	assert.EqualError(t, err, "missing label closing quote")
}

func TestParseTimestamp(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte("foo 42 1234567"))
	require.NoError(t, err)
	assert.Equal(t, &prometheus.Metric{
		Name:         []byte("foo"),
		Value:        42,
		Milliseconds: 1234567,
	}, m)
}

func TestParseInvalidTimestamp(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte("foo 42 abc"))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid timestamp")
}

func TestParseTrailingSpace(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte("foo 42 "))
	assert.Nil(t, m)
	assert.EqualError(t, err, "invalid timestamp")
}

func TestParseLabelsAndTimestamp(t *testing.T) {
	m, err := prometheus.ParseMetric([]byte(`foo{host="nyc01",bar="definitely",thing="forgot"} 42 123456789`))
	require.NoError(t, err)
	assert.Equal(t, &prometheus.Metric{
		Name: []byte("foo"),
		Labels: prometheus.Labels{
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

func TestParseMetrics(t *testing.T) {
	input := []byte(`
foo{host="nyc01",bar="definitely",thing="forgot"} 42 11111111
foo{host="nyc02",bar="maybe"} 999 22222222
bar 1234`[1:])

	expected := prometheus.NewMetricSet()
	expected.Update(&prometheus.Metric{
		Name: []byte("foo"),
		Labels: prometheus.Labels{
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
		Milliseconds: 11111111,
	})
	expected.Update(&prometheus.Metric{
		Name: []byte("foo"),
		Labels: prometheus.Labels{
			{
				Name:  []byte("host"),
				Value: []byte("nyc02"),
			},
			{
				Name:  []byte("bar"),
				Value: []byte("maybe"),
			},
		},
		Value:        999,
		Milliseconds: 22222222,
	})
	expected.Update(&prometheus.Metric{
		Name:  []byte("bar"),
		Value: 1234,
	})

	actual, err := prometheus.ParseMetrics(input)
	require.NoError(t, err)
	assert.ElementsMatch(t, expected.All(), actual.All())
}

func TestParseMetricsBlankLines(t *testing.T) {
	input := []byte(`
foo 111

bar 222

`)

	expected := prometheus.NewMetricSet()
	expected.Update(&prometheus.Metric{
		Name:  []byte("foo"),
		Value: 111,
	})
	expected.Update(&prometheus.Metric{
		Name:  []byte("bar"),
		Value: 222,
	})

	actual, err := prometheus.ParseMetrics(input)
	require.NoError(t, err)
	assert.ElementsMatch(t, expected.All(), actual.All())
}

func TestParseMetricsLastWins(t *testing.T) {
	input := []byte(`
foo 1
foo 2
foo 3
foo 4
foo 5
foo 6
`[1:])

	expected := prometheus.NewMetricSet()
	expected.Update(&prometheus.Metric{
		Name:  []byte("foo"),
		Value: 6,
	})

	actual, err := prometheus.ParseMetrics(input)
	require.NoError(t, err)
	assert.ElementsMatch(t, expected.All(), actual.All())
}

func TestParseMetricsEmpty(t *testing.T) {
	expected := prometheus.NewMetricSet()

	actual, err := prometheus.ParseMetrics([]byte{})
	require.NoError(t, err)
	assert.ElementsMatch(t, expected.All(), actual.All())
}
