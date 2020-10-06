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
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"

	// Profiling support
	_ "net/http/pprof"

	"github.com/jumptrading/influx-spout/v2/config"
	"github.com/jumptrading/influx-spout/v2/downsampler"
	"github.com/jumptrading/influx-spout/v2/filter"
	"github.com/jumptrading/influx-spout/v2/listener"
	"github.com/jumptrading/influx-spout/v2/monitor"
	"github.com/jumptrading/influx-spout/v2/writer"
)

// These are set at build time.
var version string
var builtOn string

func main() {
	configFile := getConfigFileName()

	log.Printf("Running %v version %s, built on %s, %s\n", os.Args[0], version, builtOn, runtime.Version())

	if _, err := runComponent(configFile); err != nil {
		log.Fatalf("%s", err)
	}
	runtime.Goexit()
}

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

type stoppable interface {
	Stop()
}

// runComponent parses the configuration file provided and starts
// influx-spout in the appropriate mode.
func runComponent(configFile string) (out stoppable, err error) {
	c, err := config.NewConfigFromFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("Error while loading config file: %v", err)
	}

	if c.PprofPort > 0 {
		go func() {
			log.Printf("starting pprof listener on port %d", c.PprofPort)
			err := http.ListenAndServe(fmt.Sprintf(":%d", c.PprofPort), nil)
			if err != nil {
				log.Printf("pprof listener exited: %v", err)
			}
		}()
	}

	switch c.Mode {
	case "listener":
		out, err = listener.StartListener(c)
	case "listener_http":
		out, err = listener.StartHTTPListener(c)
	case "filter":
		out, err = filter.StartFilter(c)
	case "downsampler":
		out, err = downsampler.StartDownsampler(c)
	case "writer":
		if c.Workers == 0 {
			// this seems to be an okay default from our testing experience:
			// aim to have on average two workers per OS-thread running.
			c.Workers = runtime.GOMAXPROCS(-1) * 2
		}
		out, err = writer.StartWriter(c)
	case "monitor":
		out, err = monitor.Start(c)
	default:
		return nil, fmt.Errorf("unknown mode of operation: [%s]", c.Mode)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to start %s: %v", c.Mode, err)
	}
	return out, nil
}
