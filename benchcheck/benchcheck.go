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

	if hasProblem {
		fmt.Println("Performance regression found!")
		fmt.Println()
		printTables(tables)
	} else if *flagPrint {
		printTables(tables)
	} else {
		fmt.Println("No performance regressions found")
	}

	if hasProblem {
		os.Exit(1)
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
