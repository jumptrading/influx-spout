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

	// escaped is true if the match function needs the original,
	// escaped version of the line. The unescaped version of the line
	// is passed otherwise.
	escaped bool

	// if the rule matches, the measurement is sent to this NATS subject
	subject string
}

// CreateBasicRule creates a simple rule that publishes measurements
// with the name @measurement to the NATS @subject.
func CreateBasicRule(measurement string, subject string) Rule {
	hh := hashMeasurement([]byte(measurement))

	return Rule{
		match: func(line []byte) bool {
			name := influxUnescape(measurementName(line))
			return hh == hashMeasurement(name)
		},
		escaped: true,
		subject: subject,
	}
}

func hashMeasurement(measurement []byte) uint32 {
	hh := fnv.New32()
	hh.Write(measurement)
	return hh.Sum32()
}

// measurementName takes an *escaped* line protocol line and returns
// the *escaped* measurement from it.
func measurementName(s []byte) []byte {
	// Handle the unlikely case of a single character line.
	if len(s) == 1 {
		switch s[0] {
		case ' ', ',':
			return s[:0]
		default:
			return s
		}
	}

	i := 0
	for {
		i++
		if i >= len(s) {
			return s
		}

		if s[i-1] == '\\' {
			// Skip character (it's escaped).
			continue
		}

		if s[i] == ',' || s[i] == ' ' {
			return s[:i]
		}
	}
}

// CreateRegexRule creates a rule that publishes measurements which
// match the given @regexString to the NATS @subject.
func CreateRegexRule(regexString, subject string) Rule {
	reg := regexp.MustCompile(regexString)
	return Rule{
		match: func(line []byte) bool {
			return reg.Match(line)
		},
		subject: subject,
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
		subject: subject,
	}
}

// RureSetFromConfig creates a new RuleSet instance using the rules
// from Config provided.
func RuleSetFromConfig(conf *config.Config) (*RuleSet, error) {
	rs := new(RuleSet)
	for _, r := range conf.Rule {
		switch r.Rtype {
		case "basic":
			rs.Append(CreateBasicRule(r.Match, r.Subject))
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
func (rs *RuleSet) Lookup(escapedLine []byte) int {
	line := influxUnescape(escapedLine)
	for i, rule := range rs.rules {
		matchLine := line
		if rule.escaped {
			matchLine = escapedLine
		}
		if rule.match(matchLine) {
			return i
		}
	}
	return -1
}
