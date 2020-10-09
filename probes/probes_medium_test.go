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

// +build medium

package probes

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/jumptrading/influx-spout/v2/spouttest"
	"github.com/stretchr/testify/assert"
)

const probesPort = 44450

func TestProbes(t *testing.T) {
	p := Listen(probesPort)
	defer p.Close()

	// Starting state is alive but not ready.
	assertAlive(t)
	assertNotReady(t)

	// Toggle alive.
	p.SetAlive(false)
	assertNotAlive(t)
	p.SetAlive(true)
	assertAlive(t)

	// Toggle ready.
	p.SetReady(true)
	assertReady(t)
	p.SetReady(false)
	assertNotReady(t)
}

func TestDisabled(t *testing.T) {
	p := Listen(0)
	defer p.Close()

	assert.IsType(t, new(nullListener), p)

	// Exercise methods (won't do anything).
	p.SetAlive(false)
	p.SetReady(false)
}

func assertAlive(t *testing.T) {
	assertProbe(t, "healthz", http.StatusOK)
}

func assertNotAlive(t *testing.T) {
	assertProbe(t, "healthz", http.StatusServiceUnavailable)
}

func assertReady(t *testing.T) {
	assertProbe(t, "readyz", http.StatusOK)
}

func assertNotReady(t *testing.T) {
	assertProbe(t, "readyz", http.StatusServiceUnavailable)
}

func assertProbe(t *testing.T, path string, expectedStatus int) {
	url := fmt.Sprintf("http://localhost:%d/%s", probesPort, path)
	client := &http.Client{Timeout: 2 * time.Second}
	maxTime := time.Now().Add(spouttest.LongWait)

	var resp *http.Response
	var err error
	for {
		resp, err = client.Get(url)
		if err == nil && expectedStatus == resp.StatusCode {
			return // Success
		}
		if time.Now().Before(maxTime) {
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}

	msg := fmt.Sprintf("failed to see expected probe status. Last err=%v", err)
	if resp != nil {
		msg = fmt.Sprintf("%s. Last status: %d.", msg, resp.StatusCode)
	}
	t.Fatal(msg)
}
