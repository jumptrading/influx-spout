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

package convert

import "errors"

const int64Max = (1 << 63) - 1

// ToInt is a simpler, faster version of strconv.ParseInt().
// Differences to ParseInt:
// - input is []byte instead of a string (no type conversion required
//   by caller)
// - only supports base 10 input
// - only handles positive values
func ToInt(s []byte) (int64, error) {
	if len(s) == 0 {
		return 0, errors.New("empty")
	}

	var n uint64
	for _, c := range s {
		if '0' <= c && c <= '9' {
			c -= '0'
		} else {
			return 0, errors.New("invalid char")
		}
		n = n*10 + uint64(c)
	}

	if n > int64Max {
		return 0, errors.New("overflow")
	}
	return int64(n), nil
}
