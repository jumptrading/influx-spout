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

// +build small

package influx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscape(t *testing.T) {
	check := func(input, chars, expected string) {
		assert.Equal(
			t,
			expected,
			string(Escape([]byte(input), []byte(chars))),
			"Escape(%q, %q)", input, chars,
		)
	}

	// Basic cases with no escapes
	check(``, `,`, ``)
	check("h", `,`, "h")
	check("日", `,`, "日")
	check("hello", `,`, "hello")
	check("日本語", `,`, "日本語")

	// Single characters
	check(` `, ` ,"=`, `\ `)
	check(`,`, ` ,"=`, `\,`)
	check(`"`, ` ,"=`, `\"`)
	check(`=`, ` ,"=`, `\=`)

	// Only escapes the listed characters
	check(`=`, `,`, `=`)

	// More complex
	check(`,x`, ` ,"=`, `\,x`)
	check(`x,`, ` ,"=`, `x\,`)
	check(`"x"`, ` ,"=`, `\"x\"`)
	check(` ,"=`, ` ,"=`, `\ \,\"\=`)
	check(`"stuff, =things"`, ` ,"=`, `\"stuff\,\ \=things\"`)
	check(`,日"本 語=`, ` ,"=`, `\,日\"本\ 語\=`)
}
