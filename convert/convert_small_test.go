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

package convert_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/convert"
)

func TestToInt(t *testing.T) {
	check := func(input string, expected int64) {
		actual, err := convert.ToInt([]byte(input))
		require.NoError(t, err)
		assert.Equal(t, expected, actual, "ToInt(%q)", input)
	}

	shouldFail := func(input string) {
		_, err := convert.ToInt([]byte(input))
		assert.Error(t, err)
	}

	check("0", 0)
	check("1", 1)
	check("9", 9)
	check("10", 10)
	check("99", 99)
	check("101", 101)
	check("9223372036854775807", (1<<63)-1) // max int64 value

	shouldFail("9223372036854775808") // max int64 value + 1
	shouldFail("-1")                  // negatives not supported
	shouldFail("x")
}
