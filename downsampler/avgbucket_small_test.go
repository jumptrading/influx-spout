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
	b := newAvgBucket(now)

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar=1.0`)))
	assertBytes(t, "foo,host=nyc01 bar=1", b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar=2.1`)))
	assertBytes(t, "foo,host=nyc01 bar=1.55", b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar=3.2`)))
	assertBytes(t, "foo,host=nyc01 bar=2.1", b.Bytes())
}

func TestScientificFloatField(t *testing.T) {
	b := newAvgBucket(now)

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 sci=-1.24e+78`)))
	assertBytes(t, "foo,host=nyc01 sci=-1.24e+78", b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 sci=-1.30e+78`)))
	assertBytes(t, "foo,host=nyc01 sci=-1.27e+78", b.Bytes())
}

func TestFloatTypeChange(t *testing.T) {
	b := newAvgBucket(now)

	// Send a float field.
	require.Nil(t, b.AddLine([]byte(`foo,dc=nyc x=1.2`)))
	assertBytes(t, `foo,dc=nyc x=1.2`, b.Bytes())

	// Now send it as a string, change should be ignored.
	errs := b.AddLine([]byte(`foo,dc=nyc x="foo"`))
	assertSingleError(t, errs, `wrong type for float: foo`)
	assertBytes(t, `foo,dc=nyc x=1.2`, b.Bytes())
}

func TestIntField(t *testing.T) {
	b := newAvgBucket(now)

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar=1i`)))
	assertBytes(t, "foo,host=nyc01 bar=1i", b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar=3i`)))
	assertBytes(t, "foo,host=nyc01 bar=2i", b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar=3i`)))
	assertBytes(t, "foo,host=nyc01 bar=2i", b.Bytes())
}

func TestIntTypeChange(t *testing.T) {
	b := newAvgBucket(now)

	// Send an integer field.
	require.Nil(t, b.AddLine([]byte(`foo,dc=nyc x=42i`)))
	assertBytes(t, `foo,dc=nyc x=42i`, b.Bytes())

	// Now send it as a string, change should be ignored.
	errs := b.AddLine([]byte(`foo,dc=nyc x="foo"`))
	assertSingleError(t, errs, `wrong type for int: foo`)
	assertBytes(t, `foo,dc=nyc x=42i`, b.Bytes())
}

func TestStringField(t *testing.T) {
	b := newAvgBucket(now)

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar="alpha"`)))
	assertBytes(t, `foo,host=nyc01 bar="alpha"`, b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar="beta"`)))
	assertBytes(t, `foo,host=nyc01 bar="beta"`, b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar="delta omega"`)))
	assertBytes(t, `foo,host=nyc01 bar="delta omega"`, b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar="delta \"omega\""`)))
	assertBytes(t, `foo,host=nyc01 bar="delta \"omega\""`, b.Bytes())
}

func TestUnterminatedStringField(t *testing.T) {
	b := newAvgBucket(now)
	errs := b.AddLine([]byte(`foo,host=nyc01 bar="delta omega`))

	assertSingleError(t, errs, `missing trailing double quote`)
	assert.Len(t, b.Bytes(), 0) // line should be rejected
}

func TestOtherFieldValues(t *testing.T) {
	// Field values which aren't floats, ints or strings are just
	// passed through. This covers bools but also incorrectly
	// represented values (best efforts).
	b := newAvgBucket(now)

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar=false`)))
	assertBytes(t, `foo,host=nyc01 bar=false`, b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar=T`)))
	assertBytes(t, `foo,host=nyc01 bar=T`, b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 bar=abcdef`)))
	assertBytes(t, `foo,host=nyc01 bar=abcdef`, b.Bytes())
}

func TestMultipleFields(t *testing.T) {
	b := newAvgBucket(now)

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 b=t,c=123i`)))
	assertBytes(t, `foo,host=nyc01 b=t,c=123i`, b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 c=321i,b=f`)))
	assertBytes(t, `foo,host=nyc01 b=f,c=222i`, b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,host=nyc01 c=444i,b=f,a=0.4`)))
	assertBytes(t, `foo,host=nyc01 a=0.4,b=f,c=296i`, b.Bytes())
}

func TestMultipleTags(t *testing.T) {
	b := newAvgBucket(now)

	require.Nil(t, b.AddLine([]byte(`foo,dc=nyc,host=nyc01 b=t`)))
	assertBytes(t, `foo,dc=nyc,host=nyc01 b=t`, b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo,dc=nyc,host=nyc01 b=f`)))
	assertBytes(t, `foo,dc=nyc,host=nyc01 b=f`, b.Bytes())
}

func TestNoTags(t *testing.T) {
	b := newAvgBucket(now)

	require.Nil(t, b.AddLine([]byte(`foo b=t`)))
	assertBytes(t, `foo b=t`, b.Bytes())

	require.Nil(t, b.AddLine([]byte(`foo b=false`)))
	assertBytes(t, `foo b=false`, b.Bytes())
}

func TestMeasurementOnly(t *testing.T) {
	b := newAvgBucket(now)

	// Degenerate case, but still needs testing.
	require.Nil(t, b.AddLine([]byte(`foo`)))
	assertBytes(t, `foo`, b.Bytes())

	require.Nil(t, b.AddLine([]byte(`bar`)))
	assertBytes(t, joinLines("foo", "bar"), b.Bytes())
}

func TestMeasurementAndTagsOnly(t *testing.T) {
	b := newAvgBucket(now)

	// Degenerate case, but still needs testing.
	require.Nil(t, b.AddLine([]byte(`foo,dc=nyc`)))
	assertBytes(t, `foo,dc=nyc`, b.Bytes())

	require.Nil(t, b.AddLine([]byte(`bar,dc=sfo`)))
	assertBytes(t, joinLines(
		"bar,dc=sfo",
		"foo,dc=nyc",
	), b.Bytes())
}

func TestInvalidField(t *testing.T) {
	b := newAvgBucket(now)
	errs := b.AddLine([]byte(`foo x`))

	assertSingleError(t, errs, `invalid field`)
	assertBytes(t, "", b.Bytes())
}

func TestMissingFieldValue(t *testing.T) {
	b := newAvgBucket(now)
	errs := b.AddLine([]byte(`foo x=`))

	assertSingleError(t, errs, `missing field value`)
	assertBytes(t, "", b.Bytes())
}

func TestMultipleErrors(t *testing.T) {
	b := newAvgBucket(now)

	errs := b.AddLine([]byte(`foo x=1i,y=2`))
	require.Nil(t, errs)

	errs = b.AddLine([]byte(`foo x=1,y=2i`))

	require.Len(t, errs, 2)
	assert.EqualError(t, errs[0], `wrong type for int: 1`)
	assert.EqualError(t, errs[1], `wrong type for float: 2i`)

	assertBytes(t, `foo x=1i,y=2`, b.Bytes())
}

func BenchmarkAvgBucket(b *testing.B) {
	bucket := newAvgBucket(now)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket.AddLine([]byte(`X,host=nyc01 foo=11i bar=1.1 qaz="short"`))
		bucket.AddLine([]byte(`X,host=nyc01 foo=22i bar=2.2 qaz="long"`))
		_ = bucket.Bytes()
	}
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
