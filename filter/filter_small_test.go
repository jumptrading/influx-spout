// Copyright 2017 Jump Trading
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

package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jumptrading/influx-spout/config"
)

func TestBasicRuleCreation(t *testing.T) {
	fr := CreateBasicRule("hello", "hello-chan")
	assert.Equal(t, "hello-chan", fr.natsChan, "Channel must match")
}

func TestBasicRule(t *testing.T) {
	f := &filter{c: new(config.Config)}
	f.AppendFilterRule(CreateBasicRule("hello", ""))

	assert.Equal(t, 0, LookupLine(f, []byte("hello,a=b x=y")))
	assert.Equal(t, 0, LookupLine(f, []byte("hello a=b,x=y")))

	assert.Equal(t, -1, LookupLine(f, []byte("cocacola a=b x=y")))
	assert.Equal(t, -1, LookupLine(f, []byte("pepsi,a=b x=y")))

	// Should only match the measurement name.
	assert.Equal(t, -1, LookupLine(f, []byte("pepsi,hello=b x=y")))
	assert.Equal(t, -1, LookupLine(f, []byte("pepsi,a=b hello=y")))
}

func TestBasicRuleUnescapes(t *testing.T) {
	f := &filter{c: new(config.Config)}
	f.AppendFilterRule(CreateBasicRule("hell o", ""))

	assert.Equal(t, 0, LookupLine(f, []byte(`hell\ o foo=bar`)))
	assert.Equal(t, 0, LookupLine(f, []byte(`hell\ o,foo=bar`)))
	assert.Equal(t, -1, LookupLine(f, []byte(`hell o foo=bar`)))
	assert.Equal(t, -1, LookupLine(f, []byte(`hell\,o foo=bar`)))
}

func TestRegexRule(t *testing.T) {
	f := &filter{c: new(config.Config)}
	f.AppendFilterRule(CreateRegexRule("(^hel|,etc=false)", ""))

	assert.Equal(t, 0, LookupLine(f,
		[]byte("hello,host=gopher01 somefield=11,etc=false")))
	assert.Equal(t, 0, LookupLine(f,
		[]byte("bye,host=gopher01 somefield=11,etc=false")))

	assert.Equal(t, -1, LookupLine(f,
		[]byte("cocacola,host=gopher01 somefield=11,etc=true")))
	assert.Equal(t, -1, LookupLine(f,
		[]byte("pepsi host=gopher01,somefield=11,etc=true")))
}

func TestRegexRuleUnescapes(t *testing.T) {
	f := &filter{c: new(config.Config)}
	f.AppendFilterRule(CreateRegexRule("hell +o", ""))

	assert.Equal(t, 0, LookupLine(f, []byte(`hell\ o x=y`)))
	assert.Equal(t, 0, LookupLine(f, []byte(`hell\ \ oworld x=y`)))
	assert.Equal(t, 0, LookupLine(f, []byte(`foo hell\ o=y`)))
	assert.Equal(t, -1, LookupLine(f, []byte(`hell x=1,x=y`)))
}

func TestNegativeRegexRule(t *testing.T) {
	f := &filter{c: new(config.Config)}
	f.AppendFilterRule(CreateNegativeRegexRule("hel|low", ""))

	assert.Equal(t, -1, LookupLine(f, []byte("hello,host=gopher01 x=y")))
	assert.Equal(t, -1, LookupLine(f, []byte("bye,host=gopher01 x=low")))

	assert.Equal(t, 0, LookupLine(f, []byte("HELLO,host=gopher01 x=y")))
	assert.Equal(t, 0, LookupLine(f, []byte("bye,host=gopher01 x=high")))
}

func TestNegativeRegexRuleUnescapes(t *testing.T) {
	f := &filter{c: new(config.Config)}
	f.AppendFilterRule(CreateNegativeRegexRule("hell +o", ""))

	assert.Equal(t, -1, LookupLine(f, []byte(`hell\ o,host=gopher01 x=y`)))
	assert.Equal(t, -1, LookupLine(f, []byte(`bye,host=gopher01 x=hell\ \ o`)))

	assert.Equal(t, 0, LookupLine(f, []byte("HELLO,host=gopher01 x=y")))
	assert.Equal(t, 0, LookupLine(f, []byte("bye,host=gopher01 x=hello")))
}

func TestMultipleRules(t *testing.T) {
	f := &filter{c: new(config.Config)}
	f.AppendFilterRule(CreateBasicRule("hello", ""))
	f.AppendFilterRule(CreateRegexRule(".+ing", ""))
	f.AppendFilterRule(CreateNegativeRegexRule("foo", ""))

	assert.Equal(t, 0, LookupLine(f, []byte("hello,host=gopher01")))
	assert.Equal(t, 1, LookupLine(f, []byte("singing,host=gopher01")))
	assert.Equal(t, 2, LookupLine(f, []byte("bar,host=gopher01")))
	assert.Equal(t, -1, LookupLine(f, []byte("foo,host=gopher01")))
}

func TestMeasurementName(t *testing.T) {
	check := func(input, expected string) {
		assert.Equal(t, expected, string(measurementName([]byte(input))),
			"measurementName(%q)", input)
	}

	check(``, ``)
	check(`h`, `h`)
	check("日", "日")
	check(`hello`, `hello`)
	check("日本語", "日本語")
	check(` `, ``)
	check(`,`, ``)
	check(`h world`, `h`)
	check(`h,world`, `h`)
	check(`hello world`, `hello`)
	check(`hello,world`, `hello`)
	check(`hello\ world`, `hello\ world`)
	check(`hello\,world`, `hello\,world`)
	check(`hello\ world more`, `hello\ world`)
	check(`hello\,world,more`, `hello\,world`)
	check(`hello\ 日本語 more`, `hello\ 日本語`)
	check(`hello\,日本語 more`, `hello\,日本語`)
	check(`日本語\ hello more`, `日本語\ hello`)
	check(`日本語\,hello more`, `日本語\,hello`)
	check(`\ `, `\ `)
	check(`\,`, `\,`)
	check(`\`, `\`)
	check(`h\`, `h\`)
	check(`hello\`, `hello\`)
}

func BenchmarkLineLookup(b *testing.B) {
	f := &filter{c: new(config.Config)}
	f.AppendFilterRule(CreateBasicRule("hello", ""))
	line := []byte("hello world=42")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LookupLine(f, line)
	}
}

func BenchmarkLineLookupRegex(b *testing.B) {
	f := &filter{c: new(config.Config)}
	f.AppendFilterRule(CreateRegexRule("hello|abcde", ""))
	line := []byte("hello world=42")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LookupLine(f, line)
	}
}

func BenchmarkLineLookupNegativeRegex(b *testing.B) {
	f := &filter{c: new(config.Config)}
	f.AppendFilterRule(CreateNegativeRegexRule("hello|abcde", ""))
	line := []byte("hello world=42")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LookupLine(f, line)
	}
}

func BenchmarkProcessBatch(b *testing.B) {
	// Run the filter event handler with a fake NATS connection.

	f := &filter{
		c:  new(config.Config),
		nc: new(nothingConn),
	}
	f.AppendFilterRule(CreateBasicRule("hello", "hello-out"))
	f.AppendFilterRule(CreateRegexRule("foo|bar", "foobar-out"))
	f.SetupFilter()

	batch := []byte(`
hello,host=gopher01 somefield=11,etc=false
bar,host=gopher02 somefield=14
pepsi host=gopher01,cheese=stilton
hello,host=gopher01 somefield=11,etc=false
bar,host=gopher02 somefield=14
pepsi host=gopher01,cheese=stilton
hello,host=gopher01 somefield=11,etc=false
bar,host=gopher02 somefield=14
pepsi host=gopher01,cheese=stilton
`[1:])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.ProcessBatch(batch)
	}
}

type nothingConn struct {
	natsConn
}

func (*nothingConn) Publish(string, []byte) error {
	return nil
}
