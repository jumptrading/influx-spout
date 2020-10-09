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

import "errors"

// Token takes an escaped line protocol line and returns the
// unescaped characters leading up to until.
func Token(s []byte, until []byte) ([]byte, []byte) {
	tok, remainder, escaped := token(s, until)
	if escaped {
		tok = Unescape(tok)
	}
	return tok, remainder
}

// TokenEscaped takes an escaped line protocol line and returns the
// escaped characters leading up to until.
func TokenEscaped(s []byte, until []byte) ([]byte, []byte) {
	tok, remainder, _ := token(s, until)
	return tok, remainder
}

func token(s []byte, until []byte) ([]byte, []byte, bool) {
	length := len(s)
	if length == 1 {
		for _, c := range until {
			if s[0] == c {
				return nil, s, false
			}
		}
		return s, nil, false
	}

	escaped := false
	i := 0
	for {
		i++
		if i >= length {
			return s, nil, escaped
		}

		if s[i-1] == '\\' {
			// Skip character (it's escaped).
			escaped = true
			continue
		}

		for _, c := range until {
			if s[i] == c {
				out := s[:i]
				return out, s[i:], escaped
			}
		}
	}
}

// QuotedString takes a byte slice which begins with a double quoted
// string and returns the unescaped contents of the string and the
// unprocessed remainder of the input. Errors are returned if the
// input isn't a valid string field value.
func QuotedString(s []byte) ([]byte, []byte, error) {
	length := len(s)
	if length < 2 {
		return nil, nil, errors.New("input too short")
	}
	if s[0] != '"' {
		return nil, nil, errors.New("first character must be double quote")
	}

	escaped := false
	i := 0
	for {
		i++
		if i >= length {
			return nil, nil, errors.New("missing trailing double quote")
		}
		if s[i] == '"' {
			if s[i-1] == '\\' {
				escaped = true
				continue
			}
			out := s[1:i]
			if escaped {
				out = Unescape(out)
			}
			return out, s[i+1:], nil
		}
	}
}
