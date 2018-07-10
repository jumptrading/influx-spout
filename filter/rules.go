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

package filter

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"regexp"

	"github.com/jumptrading/influx-spout/config"
)

// Rule encapsulates a matching function and the NATS topic to
// send lines to if the rule matches.
type Rule struct {
	// Function used to check if the rule matches
	match func([]byte) bool

	// needsUnescaped is true if the match function needs the
	// unescaped version of the line. The raw version of the line is
	// passed otherwise.
	needsUnescaped bool

	// if the rule matches, the measurement is sent to this NATS subject
	subject string
}

// CreateBasicRule creates a simple rule that publishes measurements
// with the name @measurement to the NATS @subject.
func CreateBasicRule(measurement string, subject string) Rule {
	hh := hashMeasurement([]byte(measurement))

	return Rule{
		match: func(line []byte) bool {
			name, _ := parseNext(line, []byte(", "))
			return hh == hashMeasurement(name)
		},
		subject: subject,
	}
}

func hashMeasurement(measurement []byte) uint32 {
	hh := fnv.New32()
	hh.Write(measurement)
	return hh.Sum32()
}

// CreateRegexRule creates a rule that publishes measurements which
// match the given @regexString to the NATS @subject.
func CreateRegexRule(regexString, subject string) Rule {
	reg := regexp.MustCompile(regexString)
	return Rule{
		match: func(line []byte) bool {
			return reg.Match(line)
		},
		needsUnescaped: true,
		subject:        subject,
	}
}

// CreateNegativeRegexRule creates a rule that publishes measurements
// which *don't* match the given @regexString to the NATS @subject.
func CreateNegativeRegexRule(regexString, subject string) Rule {
	reg := regexp.MustCompile(regexString)
	return Rule{
		match: func(line []byte) bool {
			return !reg.Match(line)
		},
		needsUnescaped: true,
		subject:        subject,
	}
}

// NewTag creates a new Tag instance from key & value strings.
func NewTag(key, value string) Tag {
	return Tag{
		Key:   []byte(key),
		Value: []byte(value),
	}
}

// Tag represents a key/value pair (both bytes).
type Tag struct {
	Key   []byte
	Value []byte
}

// CreateTagRule creates a rule that efficiently matches one or more
// measurement tags.
func CreateTagRule(tags []Tag, subject string) Rule {
	return Rule{
		match: func(line []byte) bool {
			return hasAllTags(line, tags)
		},
		subject: subject,
	}
}

// RuleSetFromConfig creates a new RuleSet instance using the rules
// from Config provided.
func RuleSetFromConfig(conf *config.Config) (*RuleSet, error) {
	rs := new(RuleSet)
	for _, r := range conf.Rule {
		switch r.Rtype {
		case "basic":
			rs.Append(CreateBasicRule(r.Match, r.Subject))
		case "tags":
			// Convert tags as [][]string from config into []Tag.
			tags := make([]Tag, 0, len(r.Tags))
			for _, raw := range r.Tags {
				// This is safe because Config is validated.
				tags = append(tags, NewTag(raw[0], raw[1]))
			}
			rs.Append(CreateTagRule(tags, r.Subject))
		case "regex":
			rs.Append(CreateRegexRule(r.Match, r.Subject))
		case "negregex":
			rs.Append(CreateNegativeRegexRule(r.Match, r.Subject))
		default:
			return nil, fmt.Errorf("Unsupported rule type: [%v]", r)
		}
	}
	return rs, nil
}

// RuleSet is a container for a number of Rules. Rules are kept in the
// order they were appended.
type RuleSet struct {
	rules []Rule
}

// Append adds a rule to the end of a RuleSet.
func (rs *RuleSet) Append(rule Rule) {
	rs.rules = append(rs.rules, rule)
}

// Count returns the number of rules in the RuleSet.
func (rs *RuleSet) Count() int {
	return len(rs.rules)
}

// Subjects returns NATS subjects of the rules in the RuleSet.
func (rs *RuleSet) Subjects() []string {
	out := make([]string, len(rs.rules))
	for i, rule := range rs.rules {
		out[i] = rule.subject
	}
	return out
}

// Lookup takes a raw line and returns the index of the rule in the
// RuleSet that matches it. Returns -1 if there was no match.
func (rs *RuleSet) Lookup(escaped []byte) int {
	var unescaped []byte
	var line []byte
	for i, rule := range rs.rules {
		if rule.needsUnescaped {
			if unescaped == nil {
				unescaped = influxUnescape(escaped)
			}
			line = unescaped
		} else {
			line = escaped
		}
		if rule.match(line) {
			return i
		}
	}
	return -1
}

func hasAllTags(line []byte, tags []Tag) bool {
	_, line = parseNext(line, []byte(", "))
	if len(line) == 0 {
		return false
	}

	numTags := len(tags)
	found := make([]bool, numTags)
	foundCount := 0
	var key, value []byte
	for {
		if len(line) == 0 || line[0] == ' ' {
			return false
		}

		key, line = parseNext(line[1:], []byte("="))
		if len(line) == 0 {
			return false
		}

		value, line = parseNext(line[1:], []byte(", "))
		if len(line) == 0 {
			return false
		}

		for t := 0; t < numTags; t++ {
			if !found[t] && bytes.Equal(key, tags[t].Key) && bytes.Equal(value, tags[t].Value) {
				found[t] = true
				foundCount++
				if foundCount == numTags {
					return true
				}
			}
		}
	}
}

// parseNext takes an escaped line protocol line and returns the
// unescaped characters leading up to until. It also returns the
// escaped remainder of line.
func parseNext(s []byte, until []byte) ([]byte, []byte) {
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
				s = influxUnescape(s)
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
					out = influxUnescape(out)
				}
				return out, s[i:]
			}
		}
	}
}
