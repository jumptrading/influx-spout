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
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"text/tabwriter"

	"golang.org/x/tools/benchmark/parse"
)

var (
	theshPercent = flag.Int("threshold", 10, "maximum allowed regression (percent)")
)

const usageFooter = `
Each input file should be from:
	go test -run=NONE -bench=. > [old,new].txt

Benchmark compares old and new for each benchmark and fails if the new
benchmarks are some percentage theshold slower.
`

func threshExceeded(d Delta) bool {
	p := 100*d.Float64() - 100
	return p > float64(*theshPercent)
}

func main() {
	os.Exit(runMain())
}

func runMain() int {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s old.txt new.txt\n\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprint(os.Stderr, usageFooter)
		os.Exit(2)
	}
	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
	}

	before := parseFile(flag.Arg(0))
	after := parseFile(flag.Arg(1))

	cmps, warnings := Correlate(before, after)
	for _, warn := range warnings {
		fmt.Fprintln(os.Stderr, warn)
	}
	if len(cmps) == 0 {
		fatal("benchcmp: no repeated benchmarks")
	}
	sort.Slice(cmps, func(i, j int) bool {
		return cmps[i].Before.Ord < cmps[j].Before.Ord
	})

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 0, 5, ' ', 0)
	defer w.Flush()

	var regressions int
	var header bool // Has the header has been displayed yet for a given block?

	for _, cmp := range cmps {
		if !cmp.Measured(parse.NsPerOp) {
			continue
		}
		delta := cmp.DeltaNsPerOp()
		if threshExceeded(delta) {
			regressions++
			if !header {
				fmt.Fprint(w, "benchmark\told ns/op\tnew ns/op\tdelta\n")
				header = true
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				cmp.Name(),
				formatNs(cmp.Before.NsPerOp),
				formatNs(cmp.After.NsPerOp),
				delta.Percent(),
			)
		}
	}

	for _, cmp := range cmps {
		if !cmp.Measured(parse.MBPerS) {
			continue
		}
		delta := cmp.DeltaMBPerS()
		if threshExceeded(delta) {
			regressions++
			if !header {
				fmt.Fprint(w, "\nbenchmark\told MB/s\tnew MB/s\tspeedup\n")
				header = true
			}
			fmt.Fprintf(w, "%s\t%.2f\t%.2f\t%s\n", cmp.Name(), cmp.Before.MBPerS, cmp.After.MBPerS, delta.Multiple())
		}
	}

	header = false
	for _, cmp := range cmps {
		if !cmp.Measured(parse.AllocsPerOp) {
			continue
		}
		delta := cmp.DeltaAllocsPerOp()
		if threshExceeded(delta) {
			regressions++
			if !header {
				fmt.Fprint(w, "\nbenchmark\told allocs\tnew allocs\tdelta\n")
				header = true
			}
			fmt.Fprintf(w, "%s\t%d\t%d\t%s\n",
				cmp.Name(),
				cmp.Before.AllocsPerOp,
				cmp.After.AllocsPerOp,
				delta.Percent(),
			)
		}
	}

	header = false
	for _, cmp := range cmps {
		if !cmp.Measured(parse.AllocedBytesPerOp) {
			continue
		}
		delta := cmp.DeltaAllocedBytesPerOp()
		if threshExceeded(delta) {
			regressions++
			if !header {
				fmt.Fprint(w, "\nbenchmark\told bytes\tnew bytes\tdelta\n")
				header = true
			}
			fmt.Fprintf(w, "%s\t%d\t%d\t%s\n",
				cmp.Name(),
				cmp.Before.AllocedBytesPerOp,
				cmp.After.AllocedBytesPerOp,
				delta.Percent(),
			)
		}
	}

	w.Flush()

	if regressions > 0 {
		fmt.Println()
	}
	fmt.Printf("%d benchmarks compared.\n", len(cmps))
	if regressions > 0 {
		fmt.Fprintf(os.Stderr, "%d performance regressions detected.\n", regressions)
		return 1
	}
	return 0
}

func fatal(msg interface{}) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

func parseFile(path string) parse.Set {
	f, err := os.Open(path)
	if err != nil {
		fatal(err)
	}
	defer f.Close()
	bb, err := parse.ParseSet(f)
	if err != nil {
		fatal(err)
	}
	return bb
}

// formatNs formats ns measurements to expose a useful amount of
// precision. It mirrors the ns precision logic of testing.B.
func formatNs(ns float64) string {
	prec := 0
	switch {
	case ns < 10:
		prec = 2
	case ns < 100:
		prec = 1
	}
	return strconv.FormatFloat(ns, 'f', prec, 64)
}
