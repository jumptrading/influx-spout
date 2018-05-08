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

package influx

import (
	"fmt"
	"math"

	"github.com/jumptrading/influx-spout/convert"
)

// maxTsLen is maximum number of characters a valid timestamp can be.
var maxTsLen = len(fmt.Sprint(int64(math.MaxInt64)))

// ExtractTimestamp returns the value and offset of the timestamp in a
// InfluxDB line protocol line. If no valid timestamp is present, an
// offset of -1 is returned.
func ExtractTimestamp(line []byte) (int64, int) {
	length := len(line)

	if length < 6 {
		return -1, -1
	}

	// Remove trailing newline, if present.
	if line[length-1] == '\n' {
		length--
		line = line[:length]
	}

	// Expect a space just before the timestamp.
	from := length - maxTsLen - 1
	if from < 0 {
		from = 0
	}
	for i := from; i < length; i++ {
		if line[i] == ' ' {
			out, err := convert.ToInt(line[i+1:])
			if err != nil {
				return -1, -1
			}
			return out, i + 1
		}
	}
	return -1, -1
}
