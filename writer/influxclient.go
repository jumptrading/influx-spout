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

package writer

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/jumptrading/influx-spout/config"
)

func newInfluxClient(c *config.Config) *influxClient {
	return &influxClient{
		url:      fmt.Sprintf("%s://%s:%d/write?db=%s", c.InfluxDBProtocol, c.InfluxDBAddress, c.InfluxDBPort, c.DBName),
		username: c.InfluxDBUser,
		password: c.InfluxDBPass,
		debug:    c.Debug,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        2,
				MaxIdleConnsPerHost: 2,
				IdleConnTimeout:     30 * time.Second,
				DisableCompression:  true,
			},
			Timeout: c.WriteTimeout.Duration,
		},
	}
}

// influxClient supports HTTP writes to an InfluxDB instance.
type influxClient struct {
	url      string
	username string
	password string
	debug    bool
	client   *http.Client
}

// Write submits a byte slice to an InfluxDB instance. It is goroutine
// safe (because the underlying http.Client is goroutine safe).
func (ic *influxClient) Write(buf []byte) error {
	req, err := http.NewRequest("POST", ic.url, bytes.NewReader(buf))
	if err != nil {
		return newClientError(err, true)
	}
	req.Header.Add("Content-Type", "application/json; charset=UTF-8")
	if ic.username != "" {
		req.SetBasicAuth(ic.username, ic.password)
	}
	resp, err := ic.client.Do(req)
	if err != nil {
		return newClientError(fmt.Sprintf("failed to send HTTP request: %v", err), false)
	}
	defer resp.Body.Close()

	if !isHTTPPOSTSuccess(resp.StatusCode) {
		errText := fmt.Sprintf("received HTTP %v from %v", resp.Status, ic.url)
		if ic.debug {
			body, err := ioutil.ReadAll(resp.Body)
			if err == nil {
				errText += fmt.Sprintf("\nresponse body: %s\n", body)
			}
		}
		return newClientError(errText, isHTTPClientError(resp.StatusCode))
	}

	return nil
}

func isHTTPPOSTSuccess(code int) bool {
	return code >= 200 && code < 300
}

func isHTTPClientError(code int) bool {
	return code >= 400 && code < 500
}

func newClientError(err interface{}, permanent bool) *clientError {
	return &clientError{
		message:   fmt.Sprint(err),
		permanent: permanent,
	}
}

// clientError may be returned by Write. As well as an error message, it
// includes whether the error is permanent or not.
type clientError struct {
	message   string
	permanent bool
}

// clientError implements the error interface.
func (e *clientError) Error() string {
	return e.message
}

// Permanent returns true if the error is permanent. Operations
// resulting in non-permanent/temporary errors may be retried.
func (e *clientError) Permanent() bool {
	return e.permanent
}

// isPermanentError examines the supplied error and returns true if it
// is permanent.
func isPermanentError(err error) bool {
	if err == nil {
		return false
	}
	if apiErr, ok := err.(*clientError); ok {
		return apiErr.Permanent()
	}
	return false // non-clientErrors are considered temporary
}
