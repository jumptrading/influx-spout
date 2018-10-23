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

func TestUnescape(t *testing.T) {
	check := func(input, expected string) {
		assert.Equal(t, expected, string(Unescape([]byte(input))),
			"Unescape(%q)", input)
	}

	// Basic cases with no escapes
	check(``, ``)
	check(` `, ` `)
	check(`,`, `,`)
	check(`"`, `"`)
	check(`=`, `=`)
	check("h", "h")
	check("日", "日")
	check("hello", "hello")
	check("日本語", "日本語")

	// Escapes
	check(`\,`, `,`)
	check(`\"`, `"`)
	check(`\ `, ` `)
	check(`\=`, `=`)
	check(`\,x`, `,x`)
	check(`\"x`, `"x`)
	check(`\ x`, ` x`)
	check(`\=x`, `=x`)
	check(`x\,`, `x,`)
	check(`x\"`, `x"`)
	check(`x\ `, `x `)
	check(`x\=`, `x=`)
	check(`\,\"\ \=`, `," =`)
	check(`\,日\"本\ 語\=`, `,日"本 語=`)

	// Non-escapes
	check(`hell\o`, `hell\o`)
	check(`\hello`, `\hello`)
	check(`\日e\l\lo`, `\日e\l\lo`)
	check(`he\\llo`, `he\\llo`)

	// Trailing slashes
	check(`\`, `\`)
	check(`\\`, `\\`)
	check(`h\`, `h\`)
	check(`日\`, `日\`)
	check(`hello\`, `hello\`)
}

var benchOut []byte

func BenchmarkUnescape(b *testing.B) {
	var x []byte
	for i := 0; i < b.N; i++ {
		x = Unescape([]byte("hello"))
		x = Unescape([]byte(`foo\,\"\ \=bar`))
	}
	benchOut = x
}
