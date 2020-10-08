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

package filter

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExtractTimestamp(t *testing.T) {
	ts := time.Date(1997, 6, 5, 4, 3, 2, 1, time.UTC).UnixNano()
	tsStr := strconv.FormatInt(ts, 10)
	defaultTs := time.Now().UnixNano()

	check := func(input string, expected int64) {
		assert.Equal(
			t, expected,
			extractTimestamp([]byte(input), defaultTs),
			"extractTimestamp(%q)", input)
	}

	check("", defaultTs)
	check("weather,city=paris temp=60", defaultTs)
	check("weather temp=99 "+tsStr, ts)
	check("weather temp=99 "+tsStr+"\n", ts)
	check("weather,city=paris temp=60,humidity=100 "+tsStr, ts)
	check("weather,city=paris temp=60,humidity=100 "+tsStr+"\n", ts)
}
