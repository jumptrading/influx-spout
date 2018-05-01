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

// Package probes defines a simpler HTTP listener for Kubernetes style
// liveness and readiness probes.
package probes

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
)

// Listen starts a simple HTTP listener for responding to Kubernetes
// liveness and readiness probes on the port specified. The returned
// Probes instance has methods for setting the liveness and readiness
// states.
//
// Liveness probes are served at /healthz.
// Readiness probes are served at /readyz.
func Listen(port int) *Probes {
	p := &Probes{
		alive: new(atomic.Value),
		ready: new(atomic.Value),
		server: &http.Server{
			Addr: fmt.Sprintf(":%d", port),
		},
	}
	p.alive.Store(true)
	p.ready.Store(false)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", newHandler(p.alive))
	mux.HandleFunc("/readyz", newHandler(p.ready))
	p.server.Handler = mux

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.server.ListenAndServe()
	}()

	return p
}

// Probes contains a simple HTTP listener for serving Kubernetes
// liveness and readiness probes.
type Probes struct {
	alive  *atomic.Value
	ready  *atomic.Value
	server *http.Server
	wg     sync.WaitGroup
}

// SetAlive set the liveness state - true means alive/healthy.
func (p *Probes) SetAlive(alive bool) {
	p.alive.Store(alive)
}

// SetReady set the readiness state - true means ready.
func (p *Probes) SetReady(ready bool) {
	p.ready.Store(ready)
}

// Close shuts down the probes listener. It blocks until the listener
// has stopped.
func (p *Probes) Close() {
	p.server.Close()
	p.wg.Wait()
}

func newHandler(value *atomic.Value) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if value.Load().(bool) {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
	}
}
