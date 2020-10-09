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

package downsampler

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/jumptrading/influx-spout/v2/convert"
	"github.com/jumptrading/influx-spout/v2/influx"
)

func newAvgBucket(ts time.Time) bucket {
	return &avgBucket{
		lines: make(map[string]*fieldPairs),
		ts:    ts,
	}
}

type avgBucket struct {
	lines map[string]*fieldPairs
	ts    time.Time
}

func (b *avgBucket) EndTime() time.Time {
	return b.ts
}

func (b *avgBucket) AddLine(line []byte) (errs []error) {
	var keyBytes []byte
	keyBytes, line = influx.TokenEscaped(line, []byte(" ")) // key = measurement + tags
	key := string(keyBytes)

	// Assumption: tags are already ordered (filter does this).
	fields, found := b.lines[key]
	if !found {
		fields = newFieldPairs()
	}
	if updateErrs := fields.update(line); len(updateErrs) > 0 {
		errs = append(errs, updateErrs...)
		return
	}
	b.lines[key] = fields
	return
}

func (b *avgBucket) Bytes() []byte {
	tsBytes := strconv.AppendInt(nil, b.ts.UnixNano(), 10)
	var out []byte

	for key, fields := range b.lines {
		out = append(out, key...)
		out = append(out, ' ')
		fieldBytes := fields.bytes()
		if len(fieldBytes) > 0 {
			out = append(out, fieldBytes...)
			out = append(out, ' ')
		}
		out = append(out, tsBytes...)
		out = append(out, '\n')
	}
	return out
}

func newFieldPairs() *fieldPairs {
	return &fieldPairs{
		fields: make(map[string]fieldValue),
	}
}

type fieldPairs struct {
	fields map[string]fieldValue
}

func (fp *fieldPairs) count() int {
	return len(fp.fields)
}

func (fp *fieldPairs) update(raw []byte) (errs []error) {
	for {
		if len(raw) == 0 {
			return
		}
		raw = raw[1:] // remove leading comma or space
		var nameBytes []byte
		nameBytes, raw = influx.TokenEscaped(raw, []byte("="))
		if len(raw) == 0 || raw[0] != '=' {
			errs = append(errs, errors.New("invalid field"))
			return
		}
		name := string(nameBytes)

		raw = raw[1:]
		if len(raw) == 0 {
			errs = append(errs, errors.New("missing field value"))
			return
		}

		var rawValue []byte
		var err error
		if raw[0] == '"' {
			// String field
			rawValue, raw, err = influx.QuotedString(raw)
			if err != nil {
				errs = append(errs, err)
				return
			}
			value, exists := fp.fields[name]
			if !exists {
				fp.fields[name] = newStringValue(rawValue)
			} else {
				if err := value.update(rawValue); err != nil {
					errs = append(errs, err) // Non-fatal, so keep going.
				}
			}
		} else {
			// Other field
			rawValue, raw = influx.Token(raw, []byte{','})
			value, exists := fp.fields[name]
			if !exists {
				fp.fields[name] = newFieldValue(rawValue)
			} else {
				if err := value.update(rawValue); err != nil {
					errs = append(errs, err) // Non-fatal, so keep going.
				}
			}
		}
	}
}

func (fp *fieldPairs) bytes() []byte {
	if len(fp.fields) == 0 {
		return nil
	}

	fieldNames := make([]string, 0, len(fp.fields))
	for name := range fp.fields {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)

	var out []byte
	lastI := len(fieldNames) - 1
	for i, name := range fieldNames {
		value := fp.fields[name]
		out = append(out, name...)
		out = append(out, '=')
		out = append(out, value.bytes()...)
		if i != lastI {
			out = append(out, ',')
		}
	}
	return out
}

type fieldValue interface {
	update([]byte) error
	bytes() []byte
}

func newFieldValue(v []byte) fieldValue {
	if len(v) >= 2 && v[len(v)-1] == 'i' {
		if iv, err := convert.ToInt(v[:len(v)-1]); err == nil {
			return &intFieldValue{
				average: iv,
				count:   1,
			}
		}
	}
	if fv, err := strconv.ParseFloat(string(v), 64); err == nil {
		return &floatFieldValue{
			average: fv,
			count:   1,
		}
	}
	return &rawFieldValue{v}
}

func newStringValue(v []byte) fieldValue {
	return &stringFieldValue{v}
}

type intFieldValue struct {
	average int64
	count   int64
}

func (v *intFieldValue) update(b []byte) error {
	if len(b) < 2 || b[len(b)-1] != 'i' {
		return fmt.Errorf("wrong type for int: %s", b)
	}
	bi, err := convert.ToInt(b[:len(b)-1])
	if err != nil {
		return fmt.Errorf("wrong type for int: %s", b)
	}

	// Update incremental average.
	v.count++
	v.average = v.average + ((bi - v.average) / v.count)
	return nil
}

func (v *intFieldValue) bytes() []byte {
	return append(strconv.AppendInt(nil, v.average, 10), 'i')
}

type floatFieldValue struct {
	average float64
	count   int64
}

func (v *floatFieldValue) update(b []byte) error {
	bf, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return fmt.Errorf("wrong type for float: %s", b)
	}

	// Update incremental average.
	v.count++
	v.average = v.average + ((bf - v.average) / float64(v.count))
	return nil
}

func (v *floatFieldValue) bytes() []byte {
	return []byte(strconv.AppendFloat(nil, v.average, 'g', -1, 64))
}

type stringFieldValue struct{ b []byte }

func (v *stringFieldValue) update(b []byte) error {
	v.b = b
	return nil
}
func (v *stringFieldValue) bytes() []byte {
	return influx.EscapeQuotedString(v.b)
}

type rawFieldValue struct{ b []byte }

func (v *rawFieldValue) update(b []byte) error {
	v.b = b
	return nil
}
func (v *rawFieldValue) bytes() []byte { return v.b }
