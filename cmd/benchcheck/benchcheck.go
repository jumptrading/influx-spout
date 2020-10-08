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

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	"golang.org/x/perf/benchstat"
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: benchcheck [options] old.txt new.txt\n")
	fmt.Fprintf(os.Stderr, "options:\n")
	flag.PrintDefaults()
	os.Exit(2)
}

var flagAlpha = flag.Float64("alpha", 0.05, "consider change significant if p < `α`")
var flagDelta = flag.Float64("max-delta", 10, "error if delta % larger than this")
var flagPrint = flag.Bool("print", false, "print output even when no regression found")

func main() {
	log.SetPrefix("benchstat: ")
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
	}

	c := &benchstat.Collection{
		Alpha:     *flagAlpha,
		DeltaTest: benchstat.UTest,
	}
	for _, file := range flag.Args() {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			log.Fatal(err)
		}
		c.AddConfig(file, data)
	}

	tables := c.Tables()

	// Look for problematic benchmarks
	hasProblem, err := findProblems(tables)
	if err != nil {
		log.Fatal(err)
	}

	if hasProblem || *flagPrint {
		printTables(tables)
	}
	fmt.Println()

	if hasProblem {
		fmt.Println("Performance regression found!")
		os.Exit(1)
	} else {
		fmt.Println("No performance regressions found.")
	}
}

func findProblems(tables []*benchstat.Table) (bool, error) {
	for _, table := range tables {
		for _, row := range table.Rows {
			if row.Delta == "~" {
				continue
			}
			delta, err := strconv.ParseFloat(strings.TrimRight(row.Delta, "%"), 64)
			if err != nil {
				return false, err
			}
			if delta > *flagDelta {
				return true, nil
			}
		}
	}
	return false, nil
}

func printTables(tables []*benchstat.Table) {
	var buf bytes.Buffer
	benchstat.FormatText(&buf, tables)
	os.Stdout.Write(buf.Bytes())
}
