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

package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/c2h5oh/datasize"
	"github.com/spf13/afero"
)

// The file here is parsed first. The config file given on the command
// line is then overlaid on top of it.
const commonFileName = "/etc/influx-spout.toml"

// Config represents the configuration for a single influx-spout
// component.
type Config struct {
	Name                string            `toml:"name"`
	Mode                string            `toml:"mode"`
	NATSAddress         string            `toml:"nats_address"`
	NATSSubject         []string          `toml:"nats_subject"`
	NATSSubjectMonitor  string            `toml:"nats_subject_monitor"`
	NATSSubjectJunkyard string            `toml:"nats_subject_junkyard"`
	InfluxDBAddress     string            `toml:"influxdb_address"`
	InfluxDBPort        int               `toml:"influxdb_port"`
	DBName              string            `toml:"influxdb_dbname"`
	BatchMessages       int               `toml:"batch"`
	BatchMaxSize        datasize.ByteSize `toml:"batch_max_size"`
	BatchMaxSecs        int               `toml:"batch_max_secs"`
	Port                int               `toml:"port"`
	Workers             int               `toml:"workers"`
	WriteTimeoutSecs    int               `toml:"write_timeout_secs"`
	ReadBufferSize      datasize.ByteSize `toml:"read_buffer_size"`
	NATSMaxPendingSize  datasize.ByteSize `toml:"nats_max_pending_size"`
	Rule                []Rule            `toml:"rule"`
	MaxTimeDeltaSecs    int               `toml:"max_time_delta_secs"`
	ProbePort           int               `toml:"probe_port"`
	PprofPort           int               `toml:"pprof_port"`
	Debug               bool              `toml:"debug"`
}

// Rule contains the configuration for a single filter rule.
type Rule struct {
	Rtype   string `toml:"type"`
	Match   string `toml:"match"`
	Subject string `toml:"subject"`
}

func newDefaultConfig() *Config {
	return &Config{
		NATSAddress:         "nats://localhost:4222",
		NATSSubject:         []string{"influx-spout"},
		NATSSubjectMonitor:  "influx-spout-monitor",
		NATSSubjectJunkyard: "influx-spout-junk",
		InfluxDBAddress:     "localhost",
		InfluxDBPort:        8086,
		DBName:              "influx-spout-junk",
		BatchMessages:       10,
		BatchMaxSecs:        300,
		Workers:             8,
		WriteTimeoutSecs:    30,
		ReadBufferSize:      4 * datasize.MB,
		NATSMaxPendingSize:  200 * datasize.MB,
		MaxTimeDeltaSecs:    600,
		ProbePort:           0,
		PprofPort:           0,
	}
}

// NewConfigFromFile parses the specified configuration file and
// returns a Config.
func NewConfigFromFile(fileName string) (*Config, error) {
	conf := newDefaultConfig()
	if err := readConfig(commonFileName, conf); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err := readConfig(fileName, conf); err != nil {
		return nil, err
	}

	if conf.Mode == "" {
		return nil, errors.New("mode not specified in config")
	}

	// Set dynamic defaults.
	if conf.Name == "" {
		conf.Name = pathToConfigName(fileName)
	}

	if conf.BatchMaxSize == 0 {
		switch conf.Mode {
		case "listener", "listener_http":
			conf.BatchMaxSize = datasize.MB
		default:
			conf.BatchMaxSize = 10 * datasize.MB
		}
	}

	if conf.Port == 0 {
		switch conf.Mode {
		case "listener":
			conf.Port = 10001
		case "listener_http":
			conf.Port = 13337
		case "monitor":
			conf.Port = 9331
		}
	}

	return conf, nil
}

func readConfig(fileName string, conf *Config) error {
	f, err := Fs.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = toml.DecodeReader(f, conf)
	if err != nil {
		return fmt.Errorf("%s: %v", fileName, err)
	}
	return nil
}

func pathToConfigName(path string) string {
	// Remove directory (if any)
	path = filepath.Base(path)

	// Remove the file extension (if any)
	ext := filepath.Ext(path)
	if ext == "" {
		return path
	}
	return path[:len(path)-len(ext)]
}

// Fs abstracts away filesystem access for the config package. It
// should only be modified by tests.
var Fs = afero.NewOsFs()
