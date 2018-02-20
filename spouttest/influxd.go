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

package spouttest

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

// RunFakeInfluxd starts a fake influxd instance with the HTTP port
// given. Stop() should be called on the returned instance once it is
// no longer needed.
func RunFakeInfluxd(port int) *FakeInfluxDB {
	f := &FakeInfluxDB{
		server: &http.Server{
			Addr: fmt.Sprintf(":%d", port),
		},
		lines: make(map[string][]string),
	}

	f.wg.Add(1)
	go f.run()

	return f
}

// FakeInfluxDB implements a simple listener which mimics InfluxDB's
// HTTP write API, recording writes for later inspection.
type FakeInfluxDB struct {
	server *http.Server
	wg     sync.WaitGroup

	mu    sync.Mutex
	lines map[string][]string
}

// Stop shuts down the instance's server. It blocks until the server
// is stopped.
func (f *FakeInfluxDB) Stop() {
	f.server.Close()
	f.wg.Wait()
}

// Lines returned the lines received for each "database". The returned
// map and slices are copies. Lines() is goroutine-safe.
func (f *FakeInfluxDB) Lines() map[string][]string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make(map[string][]string)
	for db, lines := range f.lines {
		out[db] = append([]string(nil), lines...)
	}
	return out
}

func (f *FakeInfluxDB) run() {
	defer f.wg.Done()

	mux := http.NewServeMux()
	mux.HandleFunc("/write", f.handleWrite)
	f.server.Handler = mux

	log.Printf("fake influxd listening on %s", f.server.Addr)
	f.server.ListenAndServe()
}

func (f *FakeInfluxDB) handleWrite(w http.ResponseWriter, r *http.Request) {
	db := r.URL.Query().Get("db")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(fmt.Sprintf("FakeInfluxDB read failed: %v", err))
	}

	f.mu.Lock()
	lines := f.lines[db]
	for _, line := range bytes.SplitAfter(body, []byte("\n")) {
		if len(line) > 0 {
			lines = append(lines, string(line))
		}
	}
	f.lines[db] = lines
	f.mu.Unlock()

	w.WriteHeader(http.StatusNoContent)
}
