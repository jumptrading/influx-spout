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

package influx_test

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/jumptrading/influx-spout/influx"
	"github.com/stretchr/testify/assert"
)

func TestExtractTimestamp(t *testing.T) {
	ts := int64(12345)
	tsStr := strconv.FormatInt(ts, 10)

	check := func(input string, expectedTs int64, expectedOffset int) {
		ts, offset := influx.ExtractTimestamp([]byte(input))
		assert.Equal(t, expectedTs, ts, "ExtractTimestamp(%q)", input)
		assert.Equal(t, expectedOffset, offset, "ExtractTimestamp(%q)", input)
	}

	noTimestamp := func(input string) {
		ts, offset := influx.ExtractTimestamp([]byte(input))
		assert.Equal(t, -1, offset, "ExtractTimestamp(%q)", input)
		assert.Equal(t, int64(-1), ts, "ExtractTimestamp(%q)", input)
	}

	noTimestamp("")
	noTimestamp(" ")
	noTimestamp("weather temp=99")
	noTimestamp("weather,city=paris temp=60")
	noTimestamp("weather,city=paris temp=99,humidity=100")
	check("weather temp=99 "+tsStr, ts, 16)
	check("weather temp=99 "+tsStr+"\n", ts, 16)
	check("weather,city=paris temp=60 "+tsStr, ts, 27)
	check("weather,city=paris temp=60,humidity=100 "+tsStr, ts, 40)
	check("weather,city=paris temp=60,humidity=100 "+tsStr+"\n", ts, 40)

	// Various invalid timestamps
	noTimestamp("weather temp=99 " + tsStr + " ")       // trailing whitespace
	noTimestamp("weather temp=99 xxxxx")                // not digits
	noTimestamp("weather temp=99 15x07")                // embedded non-digit
	noTimestamp("weather temp=99 00000000000000000001") // too long
	noTimestamp("weather temp=99 -" + tsStr)            // negative
	noTimestamp(tsStr)                                  // timestamp only
}

func TestExtractNanos(t *testing.T) {
	ts := time.Date(1997, 6, 5, 4, 3, 2, 1, time.UTC).UnixNano()
	tsStr := strconv.FormatInt(ts, 10)

	check := func(input string, expectedTs int64, expectedOffset int) {
		ts, offset := influx.ExtractNanos([]byte(input))
		assert.Equal(t, expectedTs, ts, "ExtractNanos(%q)", input)
		assert.Equal(t, expectedOffset, offset, "ExtractNanos(%q)", input)
	}

	noTimestamp := func(input string) {
		ts, offset := influx.ExtractNanos([]byte(input))
		assert.Equal(t, -1, offset, "ExtractNanos(%q)", input)
		assert.Equal(t, int64(-1), ts, "ExtractNanos(%q)", input)
	}

	noTimestamp("")
	noTimestamp(" ")
	noTimestamp("weather temp=99")
	noTimestamp("weather,city=paris temp=60")
	noTimestamp("weather,city=paris temp=99,humidity=100")
	check("weather temp=99 "+tsStr, ts, 16)
	check("weather temp=99 "+tsStr+"\n", ts, 16)
	check("weather,city=paris temp=60 "+tsStr, ts, 27)
	check("weather,city=paris temp=60,humidity=100 "+tsStr, ts, 40)
	check("weather,city=paris temp=60,humidity=100 "+tsStr+"\n", ts, 40)

	// Various invalid timestamps
	noTimestamp("weather temp=99 " + tsStr + " ")       // trailing whitespace
	noTimestamp("weather temp=99 xxxxxxxxxxxxxxxxxxx")  // not digits
	noTimestamp("weather temp=99 152076148x803180202")  // embedded non-digit
	noTimestamp("weather temp=99 11520761485803180202") // too long
	noTimestamp("weather temp=99 -" + tsStr)            // negative
	noTimestamp(tsStr)                                  // timestamp only
}

func TestSafeCalcTime(t *testing.T) {
	tests := []struct {
		name      string
		ts        int64
		precision string
		exp       int64
	}{
		{
			name:      "nanosecond by default",
			ts:        946730096789012345,
			precision: "",
			exp:       946730096789012345,
		},
		{
			name:      "nanosecond",
			ts:        946730096789012345,
			precision: "n",
			exp:       946730096789012345,
		},
		{
			name:      "microsecond",
			ts:        946730096789012,
			precision: "u",
			exp:       946730096789012000,
		},
		{
			name:      "millisecond",
			ts:        946730096789,
			precision: "ms",
			exp:       946730096789000000,
		},
		{
			name:      "second",
			ts:        946730096,
			precision: "s",
			exp:       946730096000000000,
		},
		{
			name:      "minute",
			ts:        15778834,
			precision: "m",
			exp:       946730040000000000,
		},
		{
			name:      "hour",
			ts:        262980,
			precision: "h",
			exp:       946728000000000000,
		},
	}

	for _, test := range tests {
		ts, err := influx.SafeCalcTime(test.ts, test.precision)
		assert.NoError(t, err, `%s: SafeCalcTime() failed. got %s`, test.name, err)
		assert.Equal(t, test.exp, ts, "%s: expected %s, got %s", test.name, test.exp, ts)
	}
}

func TestSafeCalcTimeOutOfRange(t *testing.T) {
	invalid := []int64{
		int64(math.MinInt64),
		int64(math.MinInt64) + 1,
		int64(math.MaxInt64),
	}
	for _, invalidTs := range invalid {
		_, err := influx.SafeCalcTime(invalidTs, "")
		assert.Equal(t, err, influx.ErrTimeOutOfRange,
			`SafeCalcTime() didn't fail as expected for %s`, invalidTs)
	}

	ok := []int64{
		int64(math.MinInt64) + 2,
		int64(math.MaxInt64) - 1,
	}
	for _, validTs := range ok {
		_, err := influx.SafeCalcTime(validTs, "")
		assert.NoError(t, err)
	}
}
