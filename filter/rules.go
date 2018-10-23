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
	"sort"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/influx"
)

// Rule encapsulates a matching function and the NATS subject to send
// lines to if the rule matches.
type Rule struct {
	// Function used to check if the rule matches
	match func(*parsedLine) bool

	// if the rule matches, the measurement is sent to this NATS subject
	subject string
}

// CreateBasicRule creates a simple rule that publishes measurements
// with the name @measurement to the NATS @subject.
func CreateBasicRule(measurement string, subject string) Rule {
	hh := hashMeasurement([]byte(measurement))

	return Rule{
		match: func(line *parsedLine) bool {
			return hh == line.HashedMeasurement()
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
		match: func(line *parsedLine) bool {
			return reg.Match(line.Unescaped())
		},
		subject: subject,
	}
}

// CreateNegativeRegexRule creates a rule that publishes measurements
// which *don't* match the given @regexString to the NATS @subject.
func CreateNegativeRegexRule(regexString, subject string) Rule {
	reg := regexp.MustCompile(regexString)
	return Rule{
		match: func(line *parsedLine) bool {
			return !reg.Match(line.Unescaped())
		},
		subject: subject,
	}
}

// CreateTagRule creates a rule that efficiently matches one or more
// measurement tags.
func CreateTagRule(tags influx.TagSet, subject string) Rule {
	return Rule{
		match: func(line *parsedLine) bool {
			return tags.SubsetOf(line.Tags)
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
			tags := make(influx.TagSet, 0, len(r.Tags))
			for _, raw := range r.Tags {
				// This is safe because Config is validated.
				tags = append(tags, influx.NewTag(raw[0], raw[1]))
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
	line, err := newParsedLine(escaped)
	if err != nil {
		return -1
	}
	return rs.LookupParsed(line)
}

// LookupParsed takes a parsedLine and checks for a matching rule in
// the RuleSet. The index of the matching rule is returned. Returns -1
// if there was no match.
func (rs *RuleSet) LookupParsed(line *parsedLine) int {
	for i, rule := range rs.rules {
		if rule.match(line) {
			return i
		}
	}
	return -1
}

func newParsedLine(escaped []byte) (*parsedLine, error) {
	measurement, tags, remainder, err := influx.ParseTags(escaped)
	if err != nil {
		return nil, err
	}

	return &parsedLine{
		Escaped:     escaped,
		Measurement: measurement,
		Tags:        tags,
		Remainder:   remainder,
	}, nil
}

// parsedLine caches artefacts related to a single measurement line
// being checked by the filtering. Caching avoids unnecessarily
// recalculating values which are used by multiple rules. Cached
// values are calculated lazily on first use.
//
// parseLine also handles tag ordering (see SortTags).
type parsedLine struct {
	Escaped   []byte
	unescaped []byte

	Measurement       []byte
	hashedMeasurement *uint32

	Tags      influx.TagSet
	Remainder []byte
}

func (pl *parsedLine) Unescaped() []byte {
	// Cached unescaped version if it hasn't been generated yet.
	if pl.unescaped == nil {
		pl.unescaped = influx.Unescape(pl.Escaped)
	}
	return pl.unescaped
}

func (pl *parsedLine) HashedMeasurement() uint32 {
	if pl.hashedMeasurement == nil {
		h := hashMeasurement(pl.Measurement)
		pl.hashedMeasurement = &h
	}
	return *pl.hashedMeasurement
}

func (pl *parsedLine) SortTags() {
	if sort.IsSorted(pl.Tags) {
		// Tags are already sorted so nothing to do.
		return
	}

	sort.Sort(pl.Tags)

	// Replace Escaped with sorted tags version.

	var newHdr []byte
	newHdr = append(newHdr, influx.EscapeMeasurement(pl.Measurement)...)
	newHdr = append(newHdr, ',')
	newHdr = append(newHdr, pl.Tags.Bytes()...)

	if len(pl.Remainder) == 0 {
		// Nothing left so "newHdr" is exactly what needs to be returned.
		pl.Escaped = newHdr
		return
	}

	if len(newHdr)+len(pl.Remainder)+1 == len(pl.Escaped) {
		// Length hasn't changed so reuse memory used by original,
		// overwriting the measurement name and tags part.
		copy(pl.Escaped, newHdr)
		return
	}

	// Length as changed (original escaped differently?)
	newHdr = append(newHdr, ' ')
	newHdr = append(newHdr, pl.Remainder...)
	pl.Escaped = newHdr
}
