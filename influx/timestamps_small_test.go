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
	"strconv"
	"testing"
	"time"

	"github.com/jumptrading/influx-spout/influx"
	"github.com/stretchr/testify/assert"
)

func TestExtractTimestamp(t *testing.T) {
	ts := time.Date(1997, 6, 5, 4, 3, 2, 1, time.UTC).UnixNano()
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
	noTimestamp("weather temp=99 xxxxxxxxxxxxxxxxxxx")  // not digits
	noTimestamp("weather temp=99 152076148x803180202")  // embedded non-digit
	noTimestamp("weather temp=99 11520761485803180202") // too long
	noTimestamp("weather temp=99 -" + tsStr)            // negative
	noTimestamp(tsStr)                                  // timestamp only
}
