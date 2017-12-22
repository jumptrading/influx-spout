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

// Package lineformatter only contains the LineFormatter type and its
// tests. LineFormatter efficiently generates InfluxDB Line Protocol
// entries.
//
// The byte slices generated look something like this:
//     measurement,tag1=foo,tag2=bar field1=123,field2="string"
package lineformatter

import (
	"fmt"
	"strconv"
	"time"
)

// New creates a new LineFormatter initialised for the given
// measurement, tag names and fields names.
func New(measurement string, tags []string, fields ...string) *LineFormatter {
	f := &LineFormatter{
		prefix: []byte(measurement),
		fields: make([][]byte, len(fields)),
		tags:   make([][]byte, len(tags)),
	}

	// bufCap estimates a sensible initial capacity for the byte slice
	// returned by Format().
	f.bufCap = len(measurement)

	// Precompute tag sections
	for i, tag := range tags {
		// TODO: escaping of tag names (not critical right now).
		f.tags[i] = []byte("," + tag + "=")
		f.bufCap += len(tags[i]) + 16 // tag name + approx tag size
	}

	// Precompute label sections
	for i, field := range fields {
		// TODO: escaping of field names (not critical right now).
		if i > 0 {
			f.fields[i] = []byte("," + field + "=")
		} else {
			f.fields[i] = []byte(field + "=")
		}
		f.bufCap += len(fields[i]) + 16 // field name + approx value size
	}
	return f
}

// LineFormatter generates InfluxDB Line Protocol lines. It is ~80%
// faster than using a `[]byte(fmt.Sprintf(...))` style approach.
type LineFormatter struct {
	prefix []byte
	bufCap int
	tags   [][]byte
	fields [][]byte
}

// Format efficiently generates a new InfluxDB line as per the
// measurement, tags & fields passed to New, using the tag and field
// values provided.
//
// The number of tag values given must be equal to or less than the
// number of tags passed to New.
//
// The number of field values given must be equal to or less than the
// number of fields passed to New. The following field types are
// supported: int, int64, string, bool. A panic will occur if other
// types are passed. Strings will be correctly quoted and escaped.
//
// Format is goroutine safe.
func (f *LineFormatter) Format(tagVals []string, vals ...interface{}) []byte {
	buf := make([]byte, 0, f.bufCap)
	buf = f.format(buf, tagVals, vals...)
	return append(buf, '\n')
}

var timestampLen = len(fmt.Sprint(time.Now().UnixNano())) + 1 // 1 extra for the space prefix

// FormatT the same as Format but appends an appropriately formatted
// timestamp to the end of the line.
//
// FormatT is goroutine safe.
func (f *LineFormatter) FormatT(ts time.Time, tagVals []string, vals ...interface{}) []byte {
	buf := make([]byte, 0, f.bufCap+timestampLen)
	buf = f.format(buf, tagVals, vals...)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, ts.UnixNano(), 10)
	return append(buf, '\n')
}

func (f *LineFormatter) format(buf []byte, tagVals []string, vals ...interface{}) []byte {
	buf = append(buf, f.prefix...)

	for i, tagVal := range tagVals {
		buf = append(buf, f.tags[i]...)
		// Note: this loop is significantly faster than
		// `append(buf, []byte(tagVal))`
		for i := range tagVal {
			// TODO: escaping of tag values (not critical right now).
			buf = append(buf, tagVal[i])
		}
	}

	buf = append(buf, ' ')

	for i, val := range vals {
		buf = append(buf, f.fields[i]...)
		switch v := val.(type) {
		case int:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case string:
			buf = append(buf, '"')
			for i := range v {
				if v[i] == '"' {
					buf = append(buf, '\\')
				}
				buf = append(buf, v[i])
			}
			buf = append(buf, '"')
		case bool:
			if v {
				buf = append(buf, 't')
			} else {
				buf = append(buf, 'f')
			}
		case float32:
			buf = strconv.AppendFloat(buf, float64(v), 'f', -1, 32)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'f', -1, 64)
		case []byte:
			buf = append(buf, v...)
		default:
			// Best effort
			buf = append(buf, []byte(fmt.Sprintf("%v", val))...)
		}
	}
	return buf
}
