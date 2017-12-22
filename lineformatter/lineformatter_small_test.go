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

package lineformatter_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/jump-opensource/influxdb-relay-nova/lineformatter"
)

func TestIntField(t *testing.T) {
	f := lineformatter.New("measurement", nil, "f")

	assertFormat(t, f.Format(nil, 123), "measurement f=123")
	assertFormat(t, f.Format(nil, 66), "measurement f=66")
}

func TestInt64Field(t *testing.T) {
	f := lineformatter.New("measurement", nil, "f")

	assertFormat(t, f.Format(nil, int64(123)), "measurement f=123")
	assertFormat(t, f.Format(nil, int64(66)), "measurement f=66")
}

func TestFloat32Field(t *testing.T) {
	f := lineformatter.New("measurement", nil, "f")

	assertFormat(t, f.Format(nil, float32(1.23)), "measurement f=1.23")
	assertFormat(t, f.Format(nil, float32(-654.321)), "measurement f=-654.321")
}

func TestFloat64Field(t *testing.T) {
	f := lineformatter.New("measurement", nil, "f")

	assertFormat(t, f.Format(nil, float64(1.23)), "measurement f=1.23")
	assertFormat(t, f.Format(nil, float64(-99999999999.321)), "measurement f=-99999999999.321")
}

func TestByteSlice(t *testing.T) {
	f := lineformatter.New("measurement", nil, "f")

	// byte slices are just added raw without quoting
	assertFormat(t, f.Format(nil, []byte("abc")), "measurement f=abc")
}

func TestStringField(t *testing.T) {
	f := lineformatter.New("measurement", nil, "f")

	assertFormat(t, f.Format(nil, "foo"), `measurement f="foo"`)
	assertFormat(t, f.Format(nil, ""), `measurement f=""`)
	assertFormat(t, f.Format(nil, `foo bar`), `measurement f="foo bar"`)
	assertFormat(t, f.Format(nil, `st"uff`), `measurement f="st\"uff"`)
	assertFormat(t, f.Format(nil, `"stuff"`), `measurement f="\"stuff\""`)
}

func TestBoolField(t *testing.T) {
	f := lineformatter.New("measurement", nil, "f")

	assertFormat(t, f.Format(nil, true), "measurement f=t")
	assertFormat(t, f.Format(nil, false), "measurement f=f")
}

func TestUnknownType(t *testing.T) {
	f := lineformatter.New("measurement", nil, "f")

	assertFormat(t, f.Format(nil, uint16(42)), `measurement f=42`)
	assertFormat(t, f.Format(nil, int32(-999)), `measurement f=-999`)
	assertFormat(t, f.Format(nil, 'x'), `measurement f=120`)
}

func TestMultipleFields(t *testing.T) {
	f := lineformatter.New("status", nil, "foo", "bar", "thing")

	assertFormat(t, f.Format(nil, true, "medium to high", 99999),
		`status foo=t,bar="medium to high",thing=99999`)
	assertFormat(t, f.Format(nil, false, "low", -5),
		`status foo=f,bar="low",thing=-5`)
}

func TestTags(t *testing.T) {
	f := lineformatter.New("measurement", []string{"abc", "x"}, "f")

	assertFormat(t, f.Format([]string{"foo", "bar"}, true), "measurement,abc=foo,x=bar f=t")
}

func TestTagsAndMultipleFields(t *testing.T) {
	f := lineformatter.New("m", []string{"x", "y"}, "a", "b")

	assertFormat(t, f.Format([]string{"o", "p"}, 1, 2), "m,x=o,y=p a=1,b=2")
}

func TestFormatT(t *testing.T) {
	ts := time.Date(2017, 6, 5, 4, 3, 2, 1, time.UTC)
	f := lineformatter.New("status", nil, "foo", "bar", "thing")

	assertFormat(t, f.FormatT(ts, nil, true, "average", 100),
		`status foo=t,bar="average",thing=100 1496635382000000001`)
	assertFormat(t, f.FormatT(ts, nil, false, "subpar", 4),
		`status foo=f,bar="subpar",thing=4 1496635382000000001`)
}

func BenchmarkFormat(b *testing.B) {
	f := lineformatter.New("foo", nil, "a", "b", "c")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = f.Format(nil, 123, "foo", true)
	}
}

func BenchmarkFormatT(b *testing.B) {
	f := lineformatter.New("foo", nil, "a", "b", "c")
	ts := time.Date(2017, 6, 5, 4, 3, 2, 1, time.UTC)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = f.FormatT(ts, nil, 123, "foo", true)
	}
}

func BenchmarkFormatWithTags(b *testing.B) {
	f := lineformatter.New("foo", []string{"t1", "t2"}, "a", "b", "c")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = f.Format([]string{"x", "y"}, 123, "foo", true)
	}
}

func assertFormat(t *testing.T, actual []byte, expected string) {
	assert.Equal(t, expected+"\n", string(actual))
}
