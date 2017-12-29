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

package main

import (
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/filter"
	"github.com/jumptrading/influx-spout/listener"
	"github.com/jumptrading/influx-spout/writer"
)

// These are set at build time.
var version string
var builtOn string

// getConfigFileName returns the configuration file provided on the
// command line. It will exit the program if the wrong number of
// command line arguments have been given.
func getConfigFileName() string {
	if len(os.Args) != 2 {
		usageExit()
	}
	return os.Args[1]
}

// usageExit will print a formatted output of the usage, then exit.
func usageExit() {
	fmt.Print(`
influx-spout receives incoming metrics (typically from telegraf),
filters them and selectively publishes them to one or more InfluxDB
endpoints. 

It is comprised of a number of components which communicate via a NATS
bus. This binary can run as any of the components according to the
supplied configuration.

Usage:  influx-spout <configuration-file>
`[1:])
	os.Exit(1)
}

func main() {
	configFile := getConfigFileName()

	log.Printf("Running %v version %s, built on %s, %s\n", os.Args[0], version, builtOn, runtime.Version())

	c, err := config.NewConfigFromFile(configFile)
	if err != nil {
		fmt.Printf("FATAL: Error while loading config file: %v\n", err)
		os.Exit(1)
	}

	switch c.Mode {
	case "filter":
		filter.StartFilter(c)
	case "listener":
		listener.StartListener(c)
	case "listener_http":
		listener.StartHTTPListener(c)
	case "writer":
		if c.WriterWorkers == 0 {
			// this seems to be an okay default from our testing experience:
			// aim to have on average two workers per OS-thread running.
			c.WriterWorkers = runtime.GOMAXPROCS(-1) * 2
		}
		if _, err := writer.StartWriter(c); err != nil {
			log.Fatalf("failed to start writer: %v", err)
		}
	default:
		log.Fatalf("unknown mode of operation: [%s]", c.Mode)
	}

	runtime.Goexit()
}
