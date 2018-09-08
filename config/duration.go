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

package config

import "time"

// Duration is used to support parsing of time durations directly into
// time.Duration instances. Use the embedded Duration field to access
// to the underlying time.Duration.
type Duration struct {
	time.Duration
}

// UnmarshalText implements the TextUnmarshaler interface.
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// IsZero returns true if the duration is 0.
func (d *Duration) IsZero() bool {
	return d.Duration == time.Duration(0)
}
