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

package cmd

import (
	"fmt"
	"log"
	"net/http"
	"runtime"

	// Profiling support
	_ "net/http/pprof"

	"github.com/jumptrading/influx-spout/config"
	"github.com/jumptrading/influx-spout/filter"
	"github.com/jumptrading/influx-spout/listener"
	"github.com/jumptrading/influx-spout/monitor"
	"github.com/jumptrading/influx-spout/writer"
)

// Stoppable defines something this is a capable of being stopped.
type Stoppable interface {
	Stop()
}

// Run parses the configuration file provided and starts influx-spout
// in the appropriate mode.
func Run(configFile string) (out Stoppable, err error) {
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
