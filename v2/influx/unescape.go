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

// Unescape returns a new slice containing the unescaped version of
// in.
//
// This has the same semantics as Unescape() from
// github.com/influxdata/influxdb/pkg/escape but is faster.
func Unescape(in []byte) []byte {
	if bytes.IndexByte(in, '\\') == -1 {
		return in
	}

	inLen := len(in)
	i := 0

	// The output will be no more than inLen. Preallocating the
	// capacity here is faster and uses less memory than letting
	// append allocate.
	out := make([]byte, inLen)
	j := 0

	for {
		if i >= inLen {
			break
		}
		ii := i + 1
		if in[i] == '\\' && ii < inLen {
			switch in[ii] {
			case ',', '"', ' ', '=':
				out[j] = in[ii]
				i, j = i+2, j+1
				continue
			}
		}
		out[j] = in[i]
		i, j = ii, j+1
	}
	return out[:j]
}
