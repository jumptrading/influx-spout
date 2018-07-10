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

package filter

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/spouttest"
	"github.com/jumptrading/influx-spout/stats"
)

func TestBasicRuleCreation(t *testing.T) {
	r := CreateBasicRule("hello", "hello-subject")
	assert.Equal(t, "hello-subject", r.subject, "subject must match")
}

func TestBasicRule(t *testing.T) {
	rs := new(RuleSet)
	rs.Append(CreateBasicRule("hello", ""))

	assert.Equal(t, 0, rs.Lookup([]byte("hello,a=b x=y")))
	assert.Equal(t, 0, rs.Lookup([]byte("hello a=b,x=y")))

	assert.Equal(t, -1, rs.Lookup([]byte("cocacola a=b x=y")))
	assert.Equal(t, -1, rs.Lookup([]byte("pepsi,a=b x=y")))

	// Should only match the measurement name.
	assert.Equal(t, -1, rs.Lookup([]byte("pepsi,hello=b x=y")))
	assert.Equal(t, -1, rs.Lookup([]byte("pepsi,a=b hello=y")))
}

func TestBasicRuleUnescapes(t *testing.T) {
	rs := new(RuleSet)
	rs.Append(CreateBasicRule("hell o", ""))

	assert.Equal(t, 0, rs.Lookup([]byte(`hell\ o foo=bar`)))
	assert.Equal(t, 0, rs.Lookup([]byte(`hell\ o,foo=bar`)))
	assert.Equal(t, -1, rs.Lookup([]byte(`hell o foo=bar`)))
	assert.Equal(t, -1, rs.Lookup([]byte(`hell\,o foo=bar`)))
}

func TestRegexRule(t *testing.T) {
	rs := new(RuleSet)
	rs.Append(CreateRegexRule("(^hel|,etc=false)", ""))

	assert.Equal(t, 0, rs.Lookup(
		[]byte("hello,host=gopher01 somefield=11,etc=false")))
	assert.Equal(t, 0, rs.Lookup(
		[]byte("bye,host=gopher01 somefield=11,etc=false")))

	assert.Equal(t, -1, rs.Lookup(
		[]byte("cocacola,host=gopher01 somefield=11,etc=true")))
	assert.Equal(t, -1, rs.Lookup(
		[]byte("pepsi host=gopher01,somefield=11,etc=true")))
}

func TestRegexRuleUnescapes(t *testing.T) {
	rs := new(RuleSet)
	rs.Append(CreateRegexRule("hell +o", ""))

	assert.Equal(t, 0, rs.Lookup([]byte(`hell\ o x=y`)))
	assert.Equal(t, 0, rs.Lookup([]byte(`hell\ \ oworld x=y`)))
	assert.Equal(t, 0, rs.Lookup([]byte(`foo hell\ o=y`)))
	assert.Equal(t, -1, rs.Lookup([]byte(`hell x=1,x=y`)))
}

func TestNegativeRegexRule(t *testing.T) {
	rs := new(RuleSet)
	rs.Append(CreateNegativeRegexRule("hel|low", ""))

	assert.Equal(t, -1, rs.Lookup([]byte("hello,host=gopher01 x=y")))
	assert.Equal(t, -1, rs.Lookup([]byte("bye,host=gopher01 x=low")))

	assert.Equal(t, 0, rs.Lookup([]byte("HELLO,host=gopher01 x=y")))
	assert.Equal(t, 0, rs.Lookup([]byte("bye,host=gopher01 x=high")))
}

func TestNegativeRegexRuleUnescapes(t *testing.T) {
	rs := new(RuleSet)
	rs.Append(CreateNegativeRegexRule("hell +o", ""))

	assert.Equal(t, -1, rs.Lookup([]byte(`hell\ o,host=gopher01 x=y`)))
	assert.Equal(t, -1, rs.Lookup([]byte(`bye,host=gopher01 x=hell\ \ o`)))

	assert.Equal(t, 0, rs.Lookup([]byte("HELLO,host=gopher01 x=y")))
	assert.Equal(t, 0, rs.Lookup([]byte("bye,host=gopher01 x=hello")))
}

func TestTagRuleSingle(t *testing.T) {
	rs := new(RuleSet)
	tags := []Tag{NewTag("key", "value")}
	rs.Append(CreateTagRule(tags, ""))

	assert.Equal(t, 0, rs.Lookup([]byte(`foo,key=value x=22`)))
	assert.Equal(t, 0, rs.Lookup([]byte(`foo,key=value,host=gopher01 x=22`)))
	assert.Equal(t, 0, rs.Lookup([]byte(`foo,host=gopher01,key=value x=22`)))
	assert.Equal(t, 0, rs.Lookup([]byte(`foo,host=gopher01,key=value,dc=ac x=22`)))

	assert.Equal(t, -1, rs.Lookup([]byte("foo")))
	assert.Equal(t, -1, rs.Lookup([]byte("foo x=22")))
	assert.Equal(t, -1, rs.Lookup([]byte("foo,key=other x=22")))
	assert.Equal(t, -1, rs.Lookup([]byte("foo,host=gopher01 x=22")))
}

func TestTagRuleMulti(t *testing.T) {
	rs := new(RuleSet)
	tags := []Tag{
		NewTag("host", "db01"),
		NewTag("dc", "nyc"),
	}
	rs.Append(CreateTagRule(tags, ""))

	assert.Equal(t, 0, rs.Lookup([]byte(`foo,dc=nyc,host=db01 x=22`)))
	assert.Equal(t, 0, rs.Lookup([]byte(`foo,alt=2000,dc=nyc,host=db01,lift=123 x=22`)))

	// degenerate case (repeated keys) - but should still match
	assert.Equal(t, 0, rs.Lookup([]byte(`foo,dc=sfo,dc=nyc,host=db01,host=xxx x=22`)))

	// Each tag separately shouldn't match
	assert.Equal(t, -1, rs.Lookup([]byte(`foo,host=db01 x=22`)))
	assert.Equal(t, -1, rs.Lookup([]byte(`foo,dc=nyc x=22`)))
}

func TestTagRuleEscaping(t *testing.T) {
	rs := new(RuleSet)
	rs.Append(CreateTagRule([]Tag{NewTag("ke y", "val,ue")}, ""))
	rs.Append(CreateTagRule([]Tag{NewTag("k=k", "v=v")}, ""))

	assert.Equal(t, 0, rs.Lookup([]byte(`foo,ke\ y=val\,ue x=22`)))
	assert.Equal(t, 0, rs.Lookup([]byte(`foo,ke\ y=val\,ue,host=gopher01 x=22`)))
	assert.Equal(t, 0, rs.Lookup([]byte(`foo,host=gopher01,ke\ y=val\,ue x=22`)))
	assert.Equal(t, 0, rs.Lookup([]byte(`foo,host=gopher01,ke\ y=val\,ue,dc=ac x=22`)))
	assert.Equal(t, 1, rs.Lookup([]byte(`foo,k\=k=v\=v x=22`)))

	assert.Equal(t, -1, rs.Lookup([]byte("foo,ke y=val,ue x=22")))
	assert.Equal(t, -1, rs.Lookup([]byte("foo,f=f=v=v x=22")))
}

func TestRuleSet(t *testing.T) {
	conf := &config.Config{
		Rule: []config.Rule{
			{
				Rtype:   "basic",
				Match:   "basic-match",
				Subject: "basic-sub",
			}, {
				Rtype:   "tags",
				Tags:    [][]string{{"foo", "bar"}},
				Subject: "tags-sub",
			}, {
				Rtype:   "regex",
				Match:   "regex-match",
				Subject: "regex-sub",
			}, {
				Rtype:   "negregex",
				Match:   "negreg-match",
				Subject: "negregex-sub",
			},
		},
	}
	rs, err := RuleSetFromConfig(conf)
	require.NoError(t, err)

	assert.Equal(t, 4, rs.Count())
	assert.Equal(t, []string{"basic-sub", "tags-sub", "regex-sub", "negregex-sub"}, rs.Subjects())

	assert.Equal(t, 0, rs.Lookup([]byte("basic-match x=22")))
	assert.Equal(t, 1, rs.Lookup([]byte("blah,foo=bar x=22")))
	assert.Equal(t, 2, rs.Lookup([]byte("blah,foo=regex-match x=22")))
	assert.Equal(t, -1, rs.Lookup([]byte("blah,foo=negreg-match")))
}

func TestParseNext(t *testing.T) {
	check := func(input, until, exp, expRemainder string) {
		actual, actualRemainder := parseNext([]byte(input), []byte(until))
		assert.Equal(t, exp, string(actual), "parseNext(%q, %q)", input, until)
		assert.Equal(t, expRemainder, string(actualRemainder), "parseNext(%q, %q) (remainder)", input, until)
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

var result int

func BenchmarkLineLookup(b *testing.B) {
	spouttest.SuppressLogs()
	defer spouttest.RestoreLogs()

	rs := new(RuleSet)
	rs.Append(CreateBasicRule("hello", ""))
	line := []byte("hello world=42")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = rs.Lookup(line)
	}
}

func BenchmarkLineLookupRegex(b *testing.B) {
	spouttest.SuppressLogs()
	defer spouttest.RestoreLogs()

	rs := new(RuleSet)
	rs.Append(CreateRegexRule("hello|abcde", ""))
	line := []byte("hello world=42")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = rs.Lookup(line)
	}
}

func BenchmarkLineLookupNegativeRegex(b *testing.B) {
	spouttest.SuppressLogs()
	defer spouttest.RestoreLogs()

	rs := new(RuleSet)
	rs.Append(CreateNegativeRegexRule("hello|abcde", ""))
	line := []byte("hello world=42")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = rs.Lookup(line)
	}
}

func BenchmarkLineLookupTag(b *testing.B) {
	spouttest.SuppressLogs()
	defer spouttest.RestoreLogs()

	rs := new(RuleSet)
	rs.Append(CreateTagRule([]Tag{NewTag("foo", "bar")}, ""))
	line := []byte("hello,aaa=bbb,foo=bar,cheese=stilton world=42")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = rs.Lookup(line)
	}
}

func BenchmarkLineLookupTagMulti(b *testing.B) {
	spouttest.SuppressLogs()
	defer spouttest.RestoreLogs()

	rs := new(RuleSet)
	rs.Append(CreateTagRule([]Tag{
		NewTag("foo", "bar"),
		NewTag("cheese", "stilton"),
	}, ""))
	line := []byte("hello,aaa=bbb,foo=bar,cheese=stilton world=42")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = rs.Lookup(line)
	}
}

func BenchmarkProcessBatch(b *testing.B) {
	spouttest.SuppressLogs()
	defer spouttest.RestoreLogs()

	// Run the Filter worker with a fake NATS connection.
	rs := new(RuleSet)
	rs.Append(CreateBasicRule("hello", "hello-out"))
	rs.Append(CreateRegexRule("foo|bar", "foobar-out"))

	w, err := newWorker(
		600,
		rs,
		initStats(),
		stats.NewAnon(rs.Count()),
		false,
		nullNATSConnect,
		"junk",
	)
	require.NoError(b, err)

	lines := []string{
		"hello,host=gopher01 somefield=11,etc=false",
		"bar,host=gopher02 somefield=14",
		"pepsi host=gopher01,cheese=stilton",
		"hello,host=gopher01 somefield=11,etc=false",
		"bar,host=gopher02 somefield=14",
		"pepsi host=gopher01,cheese=stilton",
		"hello,host=gopher01 somefield=11,etc=false",
		"bar,host=gopher02 somefield=14",
		"pepsi host=gopher01,cheese=stilton",
	}

	// Add a timestamp to each line.
	ts := strconv.FormatInt(time.Now().UnixNano(), 10)
	for i, line := range lines {
		lines[i] = line + " " + ts
	}

	batch := []byte(strings.Join(lines, "\n"))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.processBatch(batch)
	}
}

func nullNATSConnect() (natsConn, error) {
	return new(nullConn), nil
}

type nullConn struct {
	natsConn
}

func (*nullConn) Publish(string, []byte) error {
	return nil
}
