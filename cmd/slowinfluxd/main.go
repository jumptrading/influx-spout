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
	"time"
)

import "flag"

var port = flag.Int("port", 8086, "port to listen on")
var delay = flag.Duration("delay", 30*time.Second, "time to wait before responding to write requests")

func main() {
	flag.Parse()

	http.HandleFunc("/write", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(*delay)
		w.WriteHeader(http.StatusNoContent)
	})
	log.Printf("listening on port %d", *port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
