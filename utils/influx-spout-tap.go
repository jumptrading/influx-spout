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

// Dumps influx-spout data streams with basic filtering

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"regexp"
	"runtime"

	"github.com/nats-io/go-nats"
)

func main() {
	var url = flag.String("h", nats.DefaultURL, "The NATS server URL")
	var subject = flag.String("s", "influx-spout", "The NATS subjects")
	var regexpString = flag.String("e", "", "Regexp to match")
	var r *regexp.Regexp
	flag.Parse()
	natsConnection, _ := nats.Connect(*url)
	log.Println("Connected to " + *url)
	log.Printf("Subscribing to subject %s\n", *subject)

	if *regexpString != "" {
		r = regexp.MustCompile(*regexpString)
	}
	natsConnection.Subscribe(*subject, func(msg *nats.Msg) {
		process_data(msg, r)
	})

	// Keep the connection alive
	runtime.Goexit()
}
func process_data(msg *nats.Msg, r *regexp.Regexp) {
	for _, line := range bytes.SplitAfter(msg.Data, []byte("\n")) {
		if r != nil {
			if r.Match(line) {
				fmt.Printf(string(line))
			}
		} else {
			fmt.Printf(string(line))
		}
	}
}
