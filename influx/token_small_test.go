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
	"github.com/stretchr/testify/require"
)

func TestToken(t *testing.T) {
	check := func(input, until, exp, expRemainder string) {
		actual, actualRemainder := Token([]byte(input), []byte(until))
		assert.Equal(t, exp, string(actual), "Token(%q, %q)", input, until)
		assert.Equal(t, expRemainder, string(actualRemainder), "Token(%q, %q) (remainder)", input, until)
	}

	check("", " ", "", "")
	check(`a`, " ", `a`, "")
	check("日", " ", "日", "")
	check(`hello`, " ", `hello`, "")
	check("日本語", " ", "日本語", "")
	check(" ", ", ", "", " ")
	check(",", ", ", "", ",")
	check(`h world`, ", ", `h`, " world")
	check(`h,world`, ", ", `h`, ",world")
	check(`hello world`, ", ", `hello`, ` world`)
	check(`hello,world`, ", ", `hello`, `,world`)
	check(`hello\ world more`, ", ", `hello world`, ` more`)
	check(`hello\,world,more`, ", ", `hello,world`, `,more`)
	check(`hello\ 日本語 more`, ", ", `hello 日本語`, ` more`)
	check(`hello\,日本語,more`, ", ", `hello,日本語`, `,more`)
	check(`\ `, " ", " ", "")
	check(`\`, " ", `\`, "")
	check(`hello\`, " ", `hello\`, "")
}

func TestQuotedString(t *testing.T) {
	check := func(input, exp, expRemainder string) {
		actual, actualRemainder, err := QuotedString([]byte(input))
		require.NoError(t, err)
		assert.Equal(t, exp, string(actual), "QuotedString(`%s`)", input)
		assert.Equal(t, expRemainder, string(actualRemainder), "QuotedString(`%s`) (remainder)", input)
	}

	fail := func(input, expError string) {
		out, rem, err := QuotedString([]byte(input))
		assert.Nil(t, out)
		assert.Nil(t, rem)
		assert.EqualError(t, err, expError)
	}

	check(`""`, "", "")
	check(`"" foo`, "", " foo")
	check(`"a"`, "a", "")
	check(`"hello"`, "hello", "")
	check(`"hello" foo`, "hello", " foo")
	check(`"he\"llo" foo`, "he\"llo", " foo")
	check(`"\""`, `"`, "")
	check(`"he\"llo" foo`, "he\"llo", " foo")
	check(`"he\llo" foo`, `he\llo`, " foo") // non-escape

	fail("", "input too short")
	fail(`"`, "input too short")
	fail(`abc`, "first character must be double quote")
	fail(`"foo`, "missing trailing double quote")
	fail(`"foo\"`, "missing trailing double quote")
}
