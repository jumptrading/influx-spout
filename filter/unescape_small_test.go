// +build small

package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnescape(t *testing.T) {
	check := func(input, expected string) {
		assert.Equal(t, expected, string(influxUnescape([]byte(input))),
			"influxUnescape(%q)", input)
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
