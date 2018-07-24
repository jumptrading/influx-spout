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

package downsampler

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var now = time.Now()
var nowNano = strconv.FormatInt(now.UnixNano(), 10)

func TestFloatField(t *testing.T) {
	b := newSamplingBatch(now)

	b.Append([]byte(`foo,host=nyc01 bar=1.0`))
	assertBytes(t, "foo,host=nyc01 bar=1", b.Bytes())

	b.Append([]byte(`foo,host=nyc01 bar=2.1`))
	assertBytes(t, "foo,host=nyc01 bar=1.55", b.Bytes())

	b.Append([]byte(`foo,host=nyc01 bar=3.2`))
	assertBytes(t, "foo,host=nyc01 bar=2.1", b.Bytes())
}

func TestScientificFloatField(t *testing.T) {
	b := newSamplingBatch(now)

	b.Append([]byte(`foo,host=nyc01 sci=-1.24e+78`))
	assertBytes(t, "foo,host=nyc01 sci=-1.24e+78", b.Bytes())

	b.Append([]byte(`foo,host=nyc01 sci=-1.30e+78`))
	assertBytes(t, "foo,host=nyc01 sci=-1.27e+78", b.Bytes())
}

func TestIntField(t *testing.T) {
	b := newSamplingBatch(now)

	b.Append([]byte(`foo,host=nyc01 bar=1i`))
	assertBytes(t, "foo,host=nyc01 bar=1i", b.Bytes())

	b.Append([]byte(`foo,host=nyc01 bar=3i`))
	assertBytes(t, "foo,host=nyc01 bar=2i", b.Bytes())

	b.Append([]byte(`foo,host=nyc01 bar=3i`))
	assertBytes(t, "foo,host=nyc01 bar=2i", b.Bytes())
}

func TestStringField(t *testing.T) {
	b := newSamplingBatch(now)

	b.Append([]byte(`foo,host=nyc01 bar="alpha"`))
	assertBytes(t, `foo,host=nyc01 bar="alpha"`, b.Bytes())

	b.Append([]byte(`foo,host=nyc01 bar="beta"`))
	assertBytes(t, `foo,host=nyc01 bar="beta"`, b.Bytes())

	b.Append([]byte(`foo,host=nyc01 bar="delta omega"`))
	assertBytes(t, `foo,host=nyc01 bar="delta omega"`, b.Bytes())

	b.Append([]byte(`foo,host=nyc01 bar="delta \"omega\""`))
	assertBytes(t, `foo,host=nyc01 bar="delta \"omega\""`, b.Bytes())
}

func TestUnterminatedStringField(t *testing.T) {
	b := newSamplingBatch(now)
	b.Append([]byte(`foo,host=nyc01 bar="delta omega`))
	assert.Len(t, b.Bytes(), 0) // line should be rejected
	// XXX test for error counter
}

func TestOtherFieldValues(t *testing.T) {
	// Field values which aren't floats, ints or strings are just
	// passed through. This covers bools but also incorrectly
	// represented values (best efforts).
	b := newSamplingBatch(now)

	b.Append([]byte(`foo,host=nyc01 bar=false`))
	assertBytes(t, `foo,host=nyc01 bar=false`, b.Bytes())

	b.Append([]byte(`foo,host=nyc01 bar=T`))
	assertBytes(t, `foo,host=nyc01 bar=T`, b.Bytes())

	b.Append([]byte(`foo,host=nyc01 bar=abcdef`))
	assertBytes(t, `foo,host=nyc01 bar=abcdef`, b.Bytes())
}

func TestWithTimestamps(t *testing.T) {
	b := newSamplingBatch(now)

	b.Append([]byte(`foo,host=nyc01 v=t ` + nowNano))
	assertBytes(t, `foo,host=nyc01 v=t`, b.Bytes())

	b.Append([]byte(`foo,host=nyc01 v=f ` + nowNano))
	assertBytes(t, `foo,host=nyc01 v=f`, b.Bytes())
}

func TestMultipleFields(t *testing.T) {
	b := newSamplingBatch(now)

	b.Append([]byte(`foo,host=nyc01 b=t,c=123i ` + nowNano))
	assertBytes(t, `foo,host=nyc01 b=t,c=123i`, b.Bytes())

	b.Append([]byte(`foo,host=nyc01 c=321i,b=f ` + nowNano))
	assertBytes(t, `foo,host=nyc01 b=f,c=222i`, b.Bytes())

	b.Append([]byte(`foo,host=nyc01 c=444i,b=f,a=0.4 ` + nowNano))
	assertBytes(t, `foo,host=nyc01 a=0.4,b=f,c=296i`, b.Bytes())
}

func TestMultipleTags(t *testing.T) {
	b := newSamplingBatch(now)

	b.Append([]byte(`foo,dc=nyc,host=nyc01 b=t`))
	assertBytes(t, `foo,dc=nyc,host=nyc01 b=t`, b.Bytes())

	b.Append([]byte(`foo,dc=nyc,host=nyc01 b=f`))
	assertBytes(t, `foo,dc=nyc,host=nyc01 b=f`, b.Bytes())
}

func TestMultipleLines(t *testing.T) {
	b := newSamplingBatch(now)

	b.Append(addTs(
		`foo,host=nyc01 count=99i`,
		`foo,host=nyc02 count=123i`,
		`bar,host=nyc02 b=f,mode="fast"`,
	))
	assertBytes(t, joinLines(
		`foo,host=nyc01 count=99i`,
		`bar,host=nyc02 b=f,mode="fast"`,
		`foo,host=nyc02 count=123i`,
	), b.Bytes())

	// Now update all the measurements at once (mix up the line
	// ordering for fun).
	b.Append(addTs(
		`bar,host=nyc02 b=T,mode="crawl"`,
		`foo,host=nyc02 count=321i`,
		`foo,host=nyc01 count=99i`,
	))
	assertBytes(t, joinLines(
		`foo,host=nyc01 count=99i`,
		`bar,host=nyc02 b=T,mode="crawl"`,
		`foo,host=nyc02 count=222i`,
	), b.Bytes())

	// Now up just one line, adding a field.
	b.Append(addTs(`foo,host=nyc01 count=99i,up=true`))
	assertBytes(t, joinLines(
		`foo,host=nyc01 count=99i,up=true`,
		`bar,host=nyc02 b=T,mode="crawl"`,
		`foo,host=nyc02 count=222i`,
	), b.Bytes())
}

func TestNoTags(t *testing.T) {
	b := newSamplingBatch(now)

	b.Append([]byte(`foo b=t ` + nowNano))
	assertBytes(t, `foo b=t`, b.Bytes())

	b.Append([]byte(`foo b=false ` + nowNano))
	assertBytes(t, `foo b=false`, b.Bytes())
}

func TestMeasurementOnly(t *testing.T) {
	b := newSamplingBatch(now)

	// Degenerate case, but still needs testing.
	b.Append([]byte(`foo`))
	assertBytes(t, `foo`, b.Bytes())

	b.Append([]byte(`bar ` + nowNano))
	assertBytes(t, joinLines("foo", "bar"), b.Bytes())
}

func TestMeasurementAndTagsOnly(t *testing.T) {
	b := newSamplingBatch(now)

	// Degenerate case, but still needs testing.
	b.Append([]byte(`foo,dc=nyc`))
	assertBytes(t, `foo,dc=nyc`, b.Bytes())

	b.Append([]byte(`bar,dc=sfo ` + nowNano))
	assertBytes(t, joinLines(
		"bar,dc=sfo",
		"foo,dc=nyc",
	), b.Bytes())
}

func TestFieldTypeChange(t *testing.T) {
	b := newSamplingBatch(now)

	// Send an integer field.
	b.Append([]byte(`foo,dc=nyc x=42i`))
	assertBytes(t, `foo,dc=nyc x=42i`, b.Bytes())

	// Now send it as a string, change should be ignored.
	b.Append([]byte(`foo,dc=nyc x="foo"`))
	assertBytes(t, `foo,dc=nyc x=42i`, b.Bytes())

	// XXX test for error counter
}

func assertBytes(t *testing.T, expected string, actual []byte) {
	var expectedWithTs []string
	for _, line := range strings.Split(expected, "\n") {
		if len(line) > 0 {
			expectedWithTs = append(expectedWithTs, appendNow(line))
		}
	}

	var actualStr []string
	for _, line := range bytes.Split(actual, []byte{'\n'}) {
		if len(line) > 0 {
			actualStr = append(actualStr, string(line))
		}
	}

	assert.ElementsMatch(t, expectedWithTs, actualStr)
}

func addTs(lines ...string) []byte {
	var out [][]byte
	for _, line := range lines {
		if len(line) > 0 {
			out = append(out, []byte(appendNow(line)))
		}
	}
	return bytes.Join(out, []byte{'\n'})
}

func appendNow(in string) string {
	return in + " " + nowNano
}

func joinLines(in ...string) string {
	return strings.Join(in, "\n")
}
