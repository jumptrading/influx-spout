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

// Parts of this package are adapted from the InfluxDB code base
// (https://github.com/influxdata/influxdb).

package influx

import (
	"fmt"
	"math"
	"time"

	"github.com/jumptrading/influx-spout/convert"
)

const (
	// minNanoTime is the minumum time that can be represented.
	//
	// 1677-09-21 00:12:43.145224194 +0000 UTC
	//
	// The two lowest minimum integers are used as sentinel values.  The
	// minimum value needs to be used as a value lower than any other value for
	// comparisons and another separate value is needed to act as a sentinel
	// default value that is unusable by the user, but usable internally.
	// Because these two values need to be used for a special purpose, we do
	// not allow users to write points at these two times.
	minNanoTime = int64(math.MinInt64) + 2

	// maxNanoTime is the maximum time that can be represented.
	//
	// 2262-04-11 23:47:16.854775806 +0000 UTC
	//
	// The highest time represented by a nanosecond needs to be used for an
	// exclusive range in the shard group, so the maximum time needs to be one
	// less than the possible maximum number of nanoseconds representable by an
	// int64 so that we don't lose a point at that one time.
	maxNanoTime = int64(math.MaxInt64) - 1
)

var (
	// MaxTsLen is maximum number of characters a valid timestamp can be.
	MaxTsLen = len(fmt.Sprint(maxNanoTime))

	// ErrTimeOutOfRange gets returned when time is out of the representable range using int64 nanoseconds since the epoch.
	ErrTimeOutOfRange = fmt.Errorf("time outside range %d - %d", minNanoTime, maxNanoTime)
)

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

	to := length - MaxTsLen - 1
	if to < 0 {
		to = 0
	}
	for i := length - 1; i >= to; i-- {
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

// ExtractNanos returns the value and offset of the timestamp in a
// InfluxDB line protocol line. It is optimised for - and only works
// for - timestamps in nanosecond precision. Use ExtractTimestamp() if
// timestamps in other precisions may be present. If no valid
// timestamp is present, an offset of -1 is returned.
func ExtractNanos(line []byte) (int64, int) {
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
	from := length - MaxTsLen - 1
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

// SafeCalcTime safely calculates the time given. Will return error if
// the time is outside the supported range.
func SafeCalcTime(timestamp int64, precision string) (time.Time, error) {
	mult := getPrecisionMultiplier(precision)
	if t, ok := safeSignedMult(timestamp, mult); ok {
		if t < minNanoTime || t > maxNanoTime {
			return time.Time{}, ErrTimeOutOfRange
		}
		return time.Unix(0, t).UTC(), nil
	}
	return time.Time{}, ErrTimeOutOfRange
}

// getPrecisionMultiplier will return a multiplier for the precision specified.
func getPrecisionMultiplier(precision string) int64 {
	d := time.Nanosecond
	switch precision {
	case "u":
		d = time.Microsecond
	case "ms":
		d = time.Millisecond
	case "s":
		d = time.Second
	case "m":
		d = time.Minute
	case "h":
		d = time.Hour
	}
	return int64(d)
}

// Perform the multiplication and check to make sure it didn't overflow.
func safeSignedMult(a, b int64) (int64, bool) {
	if a == 0 || b == 0 || a == 1 || b == 1 {
		return a * b, true
	}
	if a == minNanoTime || b == maxNanoTime {
		return 0, false
	}
	c := a * b
	return c, c/b == a
}
