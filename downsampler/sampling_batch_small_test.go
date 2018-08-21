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
	"github.com/stretchr/testify/require"
)

var now = time.Now()
var nowNano = strconv.FormatInt(now.UnixNano(), 10)

func TestFloatField(t *testing.T) {
	b := newSamplingBatch(now)

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar=1.0`)))
	assertBytes(t, "foo,host=nyc01 bar=1", b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar=2.1`)))
	assertBytes(t, "foo,host=nyc01 bar=1.55", b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar=3.2`)))
	assertBytes(t, "foo,host=nyc01 bar=2.1", b.Bytes())
}

func TestScientificFloatField(t *testing.T) {
	b := newSamplingBatch(now)

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 sci=-1.24e+78`)))
	assertBytes(t, "foo,host=nyc01 sci=-1.24e+78", b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 sci=-1.30e+78`)))
	assertBytes(t, "foo,host=nyc01 sci=-1.27e+78", b.Bytes())
}

func TestFloatTypeChange(t *testing.T) {
	b := newSamplingBatch(now)

	// Send a float field.
	require.Nil(t, b.Update([]byte(`foo,dc=nyc x=1.2`)))
	assertBytes(t, `foo,dc=nyc x=1.2`, b.Bytes())

	// Now send it as a string, change should be ignored.
	errs := b.Update([]byte(`foo,dc=nyc x="foo"`))
	assertSingleError(t, errs, `error parsing [ x="foo"]: wrong type for float: foo`)
	assertBytes(t, `foo,dc=nyc x=1.2`, b.Bytes())
}

func TestIntField(t *testing.T) {
	b := newSamplingBatch(now)

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar=1i`)))
	assertBytes(t, "foo,host=nyc01 bar=1i", b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar=3i`)))
	assertBytes(t, "foo,host=nyc01 bar=2i", b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar=3i`)))
	assertBytes(t, "foo,host=nyc01 bar=2i", b.Bytes())
}

func TestIntTypeChange(t *testing.T) {
	b := newSamplingBatch(now)

	// Send an integer field.
	require.Nil(t, b.Update([]byte(`foo,dc=nyc x=42i`)))
	assertBytes(t, `foo,dc=nyc x=42i`, b.Bytes())

	// Now send it as a string, change should be ignored.
	errs := b.Update([]byte(`foo,dc=nyc x="foo"`))
	assertSingleError(t, errs, `error parsing [ x="foo"]: wrong type for int: foo`)
	assertBytes(t, `foo,dc=nyc x=42i`, b.Bytes())
}

func TestStringField(t *testing.T) {
	b := newSamplingBatch(now)

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar="alpha"`)))
	assertBytes(t, `foo,host=nyc01 bar="alpha"`, b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar="beta"`)))
	assertBytes(t, `foo,host=nyc01 bar="beta"`, b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar="delta omega"`)))
	assertBytes(t, `foo,host=nyc01 bar="delta omega"`, b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar="delta \"omega\""`)))
	assertBytes(t, `foo,host=nyc01 bar="delta \"omega\""`, b.Bytes())
}

func TestUnterminatedStringField(t *testing.T) {
	b := newSamplingBatch(now)
	errs := b.Update([]byte(`foo,host=nyc01 bar="delta omega`))

	assertSingleError(t, errs, `error parsing [ bar="delta omega]: missing trailing double quote`)
	assert.Len(t, b.Bytes(), 0) // line should be rejected
}

func TestOtherFieldValues(t *testing.T) {
	// Field values which aren't floats, ints or strings are just
	// passed through. This covers bools but also incorrectly
	// represented values (best efforts).
	b := newSamplingBatch(now)

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar=false`)))
	assertBytes(t, `foo,host=nyc01 bar=false`, b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar=T`)))
	assertBytes(t, `foo,host=nyc01 bar=T`, b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 bar=abcdef`)))
	assertBytes(t, `foo,host=nyc01 bar=abcdef`, b.Bytes())
}

func TestWithTimestamps(t *testing.T) {
	b := newSamplingBatch(now)

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 v=t `+nowNano)))
	assertBytes(t, `foo,host=nyc01 v=t`, b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 v=f `+nowNano)))
	assertBytes(t, `foo,host=nyc01 v=f`, b.Bytes())
}

func TestMultipleFields(t *testing.T) {
	b := newSamplingBatch(now)

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 b=t,c=123i `+nowNano)))
	assertBytes(t, `foo,host=nyc01 b=t,c=123i`, b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 c=321i,b=f `+nowNano)))
	assertBytes(t, `foo,host=nyc01 b=f,c=222i`, b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,host=nyc01 c=444i,b=f,a=0.4 `+nowNano)))
	assertBytes(t, `foo,host=nyc01 a=0.4,b=f,c=296i`, b.Bytes())
}

func TestMultipleTags(t *testing.T) {
	b := newSamplingBatch(now)

	require.Nil(t, b.Update([]byte(`foo,dc=nyc,host=nyc01 b=t`)))
	assertBytes(t, `foo,dc=nyc,host=nyc01 b=t`, b.Bytes())

	require.Nil(t, b.Update([]byte(`foo,dc=nyc,host=nyc01 b=f`)))
	assertBytes(t, `foo,dc=nyc,host=nyc01 b=f`, b.Bytes())
}

func TestMultipleLines(t *testing.T) {
	b := newSamplingBatch(now)

	require.Nil(t, b.Update(addTs(
		`foo,host=nyc01 count=99i`,
		`foo,host=nyc02 count=123i`,
		`bar,host=nyc02 b=f,mode="fast"`,
	)))
	assertBytes(t, joinLines(
		`foo,host=nyc01 count=99i`,
		`bar,host=nyc02 b=f,mode="fast"`,
		`foo,host=nyc02 count=123i`,
	), b.Bytes())

	// Now update all the measurements at once (mix up the line
	// ordering for fun).
	require.Nil(t, b.Update(addTs(
		`bar,host=nyc02 b=T,mode="crawl"`,
		`foo,host=nyc02 count=321i`,
		`foo,host=nyc01 count=99i`,
	)))
	assertBytes(t, joinLines(
		`foo,host=nyc01 count=99i`,
		`bar,host=nyc02 b=T,mode="crawl"`,
		`foo,host=nyc02 count=222i`,
	), b.Bytes())

	// Now up just one line, adding a field.
	require.Nil(t, b.Update(addTs(`foo,host=nyc01 count=99i,up=true`)))
	assertBytes(t, joinLines(
		`foo,host=nyc01 count=99i,up=true`,
		`bar,host=nyc02 b=T,mode="crawl"`,
		`foo,host=nyc02 count=222i`,
	), b.Bytes())
}

func TestNoTags(t *testing.T) {
	b := newSamplingBatch(now)

	require.Nil(t, b.Update([]byte(`foo b=t `+nowNano)))
	assertBytes(t, `foo b=t`, b.Bytes())

	require.Nil(t, b.Update([]byte(`foo b=false `+nowNano)))
	assertBytes(t, `foo b=false`, b.Bytes())
}

func TestMeasurementOnly(t *testing.T) {
	b := newSamplingBatch(now)

	// Degenerate case, but still needs testing.
	require.Nil(t, b.Update([]byte(`foo`)))
	assertBytes(t, `foo`, b.Bytes())

	require.Nil(t, b.Update([]byte(`bar `+nowNano)))
	assertBytes(t, joinLines("foo", "bar"), b.Bytes())
}

func TestMeasurementAndTagsOnly(t *testing.T) {
	b := newSamplingBatch(now)

	// Degenerate case, but still needs testing.
	require.Nil(t, b.Update([]byte(`foo,dc=nyc`)))
	assertBytes(t, `foo,dc=nyc`, b.Bytes())

	require.Nil(t, b.Update([]byte(`bar,dc=sfo `+nowNano)))
	assertBytes(t, joinLines(
		"bar,dc=sfo",
		"foo,dc=nyc",
	), b.Bytes())
}

func TestInvalidField(t *testing.T) {
	b := newSamplingBatch(now)
	errs := b.Update([]byte(`foo x`))

	assertSingleError(t, errs, `error parsing [ x]: invalid field`)
	assertBytes(t, "", b.Bytes())
}

func TestMissingFieldValue(t *testing.T) {
	b := newSamplingBatch(now)
	errs := b.Update([]byte(`foo x=`))

	assertSingleError(t, errs, `error parsing [ x=]: missing field value`)
	assertBytes(t, "", b.Bytes())
}

func TestMultipleErrors(t *testing.T) {
	b := newSamplingBatch(now)

	errs := b.Update(addTs(
		`foo x=999i,y=`, // invalid so whole line ignored
		`foo x=111i`,    // valid update for x
		`foo x=1.2`,     // type change should be ignored
		`foo x=333i`,    // another valid update for x
		`foo x`,         // invalid, should be ignored
	))

	require.Len(t, errs, 3)
	assert.EqualError(t, errs[0], `error parsing [ x=999i,y=]: missing field value`)
	assert.EqualError(t, errs[1], `error parsing [ x=1.2]: wrong type for int: 1.2`)
	assert.EqualError(t, errs[2], `error parsing [ x]: invalid field`)

	// Averaged value for x should be reflected
	assertBytes(t, joinLines(
		`foo x=222i`,
	), b.Bytes())
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

func assertSingleError(t *testing.T, errs []error, expected string) {
	assert.Len(t, errs, 1)
	if len(errs) > 0 {
		assert.EqualError(t, errs[0], expected)
	}
}
