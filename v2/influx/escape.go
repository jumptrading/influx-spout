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
	"bytes"
)

// EscapeTagPart escapes the characters that need to be escaped in a
// tag key or tag value.
func EscapeTagPart(in []byte) []byte {
	return Escape(in, []byte(`,= `))
}

// EscapeMeasurement escapes the characters that need to escaped in a
// measurement name.
func EscapeMeasurement(in []byte) []byte {
	return Escape(in, []byte(`, `))
}

// EscapeQuotedString escapes the characters that need to escaped in a
// quoted string. The returned value is wrapped in double quotes.
func EscapeQuotedString(in []byte) []byte {
	toEscapeCount := countBytes(in, []byte{'"'})
	if toEscapeCount == 0 {
		// Short circuit, no escaping needed
		out := make([]byte, len(in)+2)
		out[0] = '"'
		out[len(out)-1] = '"'
		copy(out[1:], in)
		return out
	}

	// Allocate exactly the right size to avoid further allocations
	// and copies.
	out := make([]byte, 0, len(in)+toEscapeCount+2)
	out = append(out, '"')
	for _, b := range in {
		if b == '"' {
			out = append(out, '\\')
		}
		out = append(out, b)
	}
	out = append(out, '"')
	return out
}

// Escape returns IN with any bytes in CHARS backslash escaped.
func Escape(in []byte, chars []byte) []byte {
	if len(chars) == 0 {
		// Short circuit, no characters to escape.
		return in
	}
	toEscapeCount := countBytes(in, chars)
	if toEscapeCount == 0 {
		// Short circuit, no escaping needed.
		return in
	}

	// Allocate exactly the right size to avoid further allocations
	// and copies.
	out := make([]byte, 0, len(in)+toEscapeCount)

	for _, b := range in {
		if containsByte(chars, b) {
			out = append(out, '\\')
		}
		out = append(out, b)
	}
	return out
}

// countBytes returns the number of times of any of CHARS appears in
// IN.
func countBytes(in []byte, chars []byte) int {
	count := 0
	for _, c := range in {
		if containsByte(chars, c) {
			count++
		}
	}
	return count
}

func containsAny(in []byte, chars []byte) bool {
	for _, c := range chars {
		if containsByte(in, c) {
			return true
		}
	}
	return false
}

func containsByte(in []byte, b byte) bool {
	// IndexByte is fast (implemented in asm)
	return bytes.IndexByte(in, b) >= 0
}
