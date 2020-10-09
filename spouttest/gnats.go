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

package spouttest

import (
	"log"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
)

// RunGnatsd runs a gnatsd server on the specified port for testing
// against.
func RunGnatsd(port int) *server.Server {
	// Retry because sometimes gnatsd fails to start.
	for attempt := 0; attempt < 5; attempt++ {
		srv := runServer(port)
		if srv != nil {
			return srv
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic("NATS server failed to start after multiple attempts")
}

func runServer(port int) *server.Server {
	opts := test.DefaultTestOptions
	opts.Port = port
	s := server.New(&opts)
	if s == nil {
		log.Println("No NATS Server object returned.")
		return nil
	}

	// Run server in Go routine.
	go s.Start()

	// Wait for accept loop(s) to be started
	if !s.ReadyForConnections(20 * time.Second) {
		log.Println("NATS Server failed to start.")
		s.Shutdown()
		return nil
	}
	return s
}
