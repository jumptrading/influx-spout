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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/jumptrading/influx-spout/config"
)

func newInfluxClient(c *config.Config) *influxClient {
	return &influxClient{
		url:      fmt.Sprintf("http://%s:%d/write?db=%s", c.InfluxDBAddress, c.InfluxDBPort, c.DBName),
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
		return err
	}
	req.Header.Add("Content-Type", "application/json; charset=UTF-8")
	if ic.username != "" {
		req.SetBasicAuth(ic.username, ic.password)
	}
	resp, err := ic.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %v\n", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode > 300 {
		errText := fmt.Sprintf("received HTTP %v from %v", resp.Status, ic.url)
		if ic.debug {
			body, err := ioutil.ReadAll(resp.Body)
			if err == nil {
				errText += fmt.Sprintf("\nresponse body: %s\n", body)
			}
		}
		return errors.New(errText)
	}

	return nil
}
