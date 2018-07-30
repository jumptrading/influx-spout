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

// Token takes an escaped line protocol line and returns the
// unescaped characters leading up to until. It also returns the
// escaped remainder of line.
func Token(s []byte, until []byte) ([]byte, []byte) {
	if len(s) == 1 {
		for _, c := range until {
			if s[0] == c {
				return nil, s
			}
		}
		return s, nil
	}

	escaped := false
	i := 0
	for {
		i++
		if i >= len(s) {
			if escaped {
				s = Unescape(s)
			}
			return s, nil
		}

		if s[i-1] == '\\' {
			// Skip character (it's escaped).
			escaped = true
			continue
		}

		for _, c := range until {
			if s[i] == c {
				out := s[:i]
				if escaped {
					out = Unescape(out)
				}
				return out, s[i:]
			}
		}
	}
}
