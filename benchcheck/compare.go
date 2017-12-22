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
//
// This file contains source code adapted from software developed for
// the Go project (see https://github.com/golang/tools/).

package main

import (
	"fmt"
	"math"

	"golang.org/x/tools/benchmark/parse"
)

// BenchCmp is a pair of benchmarks.
type BenchCmp struct {
	Before *parse.Benchmark
	After  *parse.Benchmark
}

// Correlate correlates benchmarks from two BenchSets.
func Correlate(before, after parse.Set) (cmps []BenchCmp, warnings []string) {
	cmps = make([]BenchCmp, 0, len(after))
	for name, beforebb := range before {
		afterbb := after[name]
		if len(beforebb) != len(afterbb) {
			warnings = append(warnings, fmt.Sprintf("ignoring %s: before has %d instances, after has %d", name, len(beforebb), len(afterbb)))
			continue
		}
		for i, beforeb := range beforebb {
			afterb := afterbb[i]
			cmps = append(cmps, BenchCmp{beforeb, afterb})
		}
	}
	return
}

func (c BenchCmp) Name() string           { return c.Before.Name }
func (c BenchCmp) String() string         { return fmt.Sprintf("<%s, %s>", c.Before, c.After) }
func (c BenchCmp) Measured(flag int) bool { return (c.Before.Measured & c.After.Measured & flag) != 0 }
func (c BenchCmp) DeltaNsPerOp() Delta    { return Delta{c.Before.NsPerOp, c.After.NsPerOp} }
func (c BenchCmp) DeltaMBPerS() Delta     { return Delta{c.Before.MBPerS, c.After.MBPerS} }
func (c BenchCmp) DeltaAllocedBytesPerOp() Delta {
	return Delta{float64(c.Before.AllocedBytesPerOp), float64(c.After.AllocedBytesPerOp)}
}
func (c BenchCmp) DeltaAllocsPerOp() Delta {
	return Delta{float64(c.Before.AllocsPerOp), float64(c.After.AllocsPerOp)}
}

// Delta is the before and after value for a benchmark measurement.
// Both must be non-negative.
type Delta struct {
	Before float64
	After  float64
}

// mag calculates the magnitude of a change, regardless of the direction of
// the change. mag is intended for sorting and has no independent meaning.
func (d Delta) mag() float64 {
	switch {
	case d.Before != 0 && d.After != 0 && d.Before >= d.After:
		return d.After / d.Before
	case d.Before != 0 && d.After != 0 && d.Before < d.After:
		return d.Before / d.After
	case d.Before == 0 && d.After == 0:
		return 1
	default:
		// 0 -> 1 or 1 -> 0
		// These are significant changes and worth surfacing.
		return math.Inf(1)
	}
}

// Changed reports whether the benchmark quantities are different.
func (d Delta) Changed() bool { return d.Before != d.After }

// Float64 returns After / Before. If Before is 0, Float64 returns
// 1 if After is also 0, and +Inf otherwise.
func (d Delta) Float64() float64 {
	switch {
	case d.Before != 0:
		return d.After / d.Before
	case d.After == 0:
		return 1
	default:
		return math.Inf(1)
	}
}

// Percent formats a Delta as a percent change, ranging from -100% up.
func (d Delta) Percent() string {
	return fmt.Sprintf("%+.2f%%", 100*d.Float64()-100)
}

// Multiple formats a Delta as a multiplier, ranging from 0.00x up.
func (d Delta) Multiple() string {
	return fmt.Sprintf("%.2fx", d.Float64())
}

func (d Delta) String() string {
	return fmt.Sprintf("Δ(%f, %f)", d.Before, d.After)
}
