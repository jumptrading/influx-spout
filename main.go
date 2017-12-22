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

// Package main kicks off the NATS-based InfluxDB relay
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/jump-opensource/influxdb-relay-nova/config"
	"github.com/jump-opensource/influxdb-relay-nova/filter"
	"github.com/jump-opensource/influxdb-relay-nova/listener"
	"github.com/jump-opensource/influxdb-relay-nova/writer"
)

const (
	// DefaultNATSAddress is the default address of the NATS instance.
	DefaultNATSAddress = "nats://localhost:4222"

	// DefaultNATSTopic is the default topic on which messages are published
	// by listeners and subscribed to by writers.
	DefaultNATSTopic = "influx-spout"

	// DefaultNATSTopicMonitor is the topic where diagnostic messages and
	// metrics are published.
	DefaultNATSTopicMonitor = "influx-spout-monitor"

	// DefaultInfluxDBAddress is the default InfluxDB location, in case it is not
	// specified on the command line or in the config file.
	DefaultInfluxDBAddress = "localhost"

	// DefaultDBName is the default name of the database that metrics are
	// written to.
	DefaultDBName = "junk_nats"

	// DefaultInfluxDBPort is the default InfluxDB port used by the writer.
	DefaultInfluxDBPort = 8086

	// DefaultListenerPort is the default port the listener
	// (publisher) will listen on.
	DefaultListenerPort = 10001

	// DefaultHTTPListenerPort is the default port a HTTP listener
	// (publisher) will listen on.
	DefaultHTTPListenerPort = 13337

	// DefaultBatchMessages is the default number of messages a writer
	// will collect before posting them to InfluxDB.
	DefaultBatchMessages = 10

	// DefaultWriteTimeoutSecs is the default number of seconds a
	// writer POST will wait before giving up.
	DefaultWriteTimeoutSecs = 30

	// DefaultNATSPendingMaxMB is the maximum size that a NATS buffer
	// for a given topic is allowed to be before messages are dropped.
	DefaultNATSPendingMaxMB = 200
)

// Flag usage strings are empty because they're not used. Custom usage output is provided.
var isTestMode = flag.Bool("t", false, "Enable test mode (see README.md)")
var port = flag.Int("p", 0,
	"Port to use in either listener/monitor mode (default 10001 (listener), 13337 (listener_http))")
var backend = flag.String("e", DefaultInfluxDBAddress, "InfluxDB endpoint location")
var dbPort = flag.Int("s", DefaultInfluxDBPort, "InfluxDB port num to write to")
var dbName = flag.String("d", DefaultDBName, "InfluxDB database name")
var batchNumber = flag.Int("b", DefaultBatchMessages, "Batch request together before flushing to InfluxDB. "+
	"Set to 1 to disable batching.")
var configFile = flag.String("c", "", "TOML configuration file (other flags are ignored if this is used)")
var version string
var builtOn string

// usageExit will print a formatted output of the usage, then exit
func usageExit(n int) {
	fmt.Printf(`
InfluxDB relay that leverages NATS message bus to receive incoming telegraf metrics and publish to InfluxDB endpoints.

Usage:  %s [flags] <mode>

  <mode> must be one of "writer", "listener", "listener_http" or "filter".

Flags:

`[1:], os.Args[0])
	flag.PrintDefaults()
	fmt.Println(`
Examples:

  # Run a writer with the given InfluxDB settings
  relay -e my.influxdb -d mydb writer
  
  # Run a listener in test mode on port 10006
  relay -t -p 10006 listener`)
	os.Exit(n)
}

func main() {
	flag.Usage = func() { usageExit(0) }
	flag.Parse()
	var c *config.Config
	var mode string
	var err error

	log.Printf("Running %v version %s, built on %s, %s\n", os.Args[0], version, builtOn, runtime.Version())

	if *configFile != "" {
		// Using configuration file.
		c, err = config.NewConfigFromFile(*configFile)
		if err != nil {
			fmt.Printf("FATAL: Error while loading config file: %v\n", err)
			os.Exit(1)
		}
		mode = c.Mode
	} else {
		// Using command line options.
		if len(flag.Args()) != 1 {
			usageExit(1)
		}
		mode = flag.Args()[0]

		c = config.NewConfig()
		c.NATSAddress = DefaultNATSAddress
		c.NATSTopic = []string{DefaultNATSTopic}
		c.NATSTopicMonitor = DefaultNATSTopicMonitor
		c.InfluxDBAddress = *backend
		c.InfluxDBPort = *dbPort
		c.BatchMessages = *batchNumber
		c.IsTesting = *isTestMode
		c.Port = *port
		c.DBName = *dbName
	}

	if c.Port == 0 {
		switch mode {
		case "listener":
			c.Port = DefaultListenerPort
		case "listener_http":
			c.Port = DefaultHTTPListenerPort
		}
	}
	if c.WriteTimeoutSecs == 0 {
		c.WriteTimeoutSecs = DefaultWriteTimeoutSecs
	}
	if c.NATSPendingMaxMB == 0 {
		c.NATSPendingMaxMB = DefaultNATSPendingMaxMB
	}

	switch mode {
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
		log.Fatalf("unknown mode of operation: [%s]", mode)
	}

	runtime.Goexit()
}
