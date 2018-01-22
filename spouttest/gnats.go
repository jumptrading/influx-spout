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
	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/gnatsd/test"
)

// RunGnatsd runs a gnatsd server on the specified port for testing
// against.
//
// This is essentially a copy of the unexported RunServerOnPort()
// in github.com/nats-io/go-nats/test
func RunGnatsd(port int) *server.Server {
	opts := test.DefaultTestOptions
	opts.Port = port
	return test.RunServer(&opts)
}
