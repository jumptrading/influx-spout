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
	"time"

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
	BatchMaxCount       int               `toml:"batch_max_count"`
	BatchMaxSize        datasize.ByteSize `toml:"batch_max_size"`
	BatchMaxAge         Duration          `toml:"batch_max_age"`
	Port                int               `toml:"port"`
	Workers             int               `toml:"workers"`
	WriteTimeout        Duration          `toml:"write_timeout"`
	ReadBufferSize      datasize.ByteSize `toml:"read_buffer_size"`
	NATSMaxPendingSize  datasize.ByteSize `toml:"nats_max_pending_size"`
	Rule                []Rule            `toml:"rule"`
	MaxTimeDelta        Duration          `toml:"max_time_delta"`
	StatsInterval       Duration          `toml:"stats_interval"`
	ProbePort           int               `toml:"probe_port"`
	PprofPort           int               `toml:"pprof_port"`
	Debug               bool              `toml:"debug"`
	DownsamplePeriod    Duration          `toml:"downsample_period"`
	DownsampleSuffix    string            `toml:"downsample_suffix"`
}

// Rule contains the configuration for a single filter rule.
type Rule struct {
	Rtype   string     `toml:"type"`
	Match   string     `toml:"match"`
	Tags    [][]string `toml:"tags"`
	Subject string     `toml:"subject"`
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
		BatchMaxCount:       10,
		BatchMaxAge:         Duration{5 * time.Minute},
		Workers:             8,
		WriteTimeout:        Duration{30 * time.Second},
		ReadBufferSize:      4 * datasize.MB,
		NATSMaxPendingSize:  200 * datasize.MB,
		MaxTimeDelta:        Duration{10 * time.Minute},
		DownsampleSuffix:    "-archive",
		StatsInterval:       Duration{3 * time.Second},
		ProbePort:           0,
		PprofPort:           0,
	}
}

// Validate returns an error if a problem is detected with the
// configuration. It checks a variety of possible configuration errors
// but these aren't exhaustive.
func (c *Config) Validate() error {
	if c.ProbePort < 0 || c.ProbePort > 65535 {
		return errors.New("probe_port out of range")
	}
	if c.PprofPort < 0 || c.PprofPort > 65535 {
		return errors.New("pprof_port out of range")
	}

	if c.Mode != "downsampler" && c.DownsamplePeriod.Duration != time.Duration(0) {
		return errors.New("downsample_period is only for downsampler mode")
	}

	switch c.Mode {
	case "listener", "listener_http":
		return c.validateListener()
	case "filter":
		return c.validateFilter()
	case "downsampler":
		return c.validateDownsampler()
	case "writer":
		return c.validateWriter()
	case "monitor":
	default:
		return fmt.Errorf("invalid mode: %s", c.Mode)
	}
	return nil
}

func (c *Config) validateListener() error {
	if c.Port < 1 || c.Port > 65535 {
		return errors.New("listener port out of range")
	}
	if len(c.NATSSubject) != 1 {
		return errors.New("listener should only use one NATS subject")
	}
	return nil
}

func (c *Config) validateFilter() error {
	if len(c.NATSSubject) != 1 {
		return errors.New("filter should only use one NATS subject")
	}
	for _, rule := range c.Rule {
		if rule.Rtype == "" {
			return errors.New(`rule missing "type"`)
		}
		if rule.Subject == "" {
			return errors.New(`rule missing "subject"`)
		}
		if rule.Rtype != "tags" && rule.Match == "" {
			return errors.New(`rule missing "match"`)
		}
		if rule.Rtype != "tags" && len(rule.Tags) > 0 {
			return fmt.Errorf(`%s rule cannot use "tags"`, rule.Rtype)
		}
		if rule.Rtype == "tags" {
			if rule.Match != "" {
				return errors.New(`tags rule cannot use "match"`)
			}
			if len(rule.Tags) == 0 {
				return errors.New(`tags rule missing "tags"`)
			}
			for _, tag := range rule.Tags {
				if len(tag) != 2 {
					return errors.New("each tags item must contain 2 values")
				}
			}
		}
	}
	return nil
}

func (c *Config) validateDownsampler() error {
	if c.DownsamplePeriod.Duration == time.Duration(0) {
		return errors.New("downsample_period must be set")
	}
	if c.DownsampleSuffix == "" {
		return errors.New("downsample_suffix must be set")
	}
	return nil
}

func (c *Config) validateWriter() error {
	if len(c.NATSSubject) < 1 {
		return errors.New("writer needs at least one NATS subject")
	}
	if c.InfluxDBPort < 1 || c.InfluxDBPort > 65535 {
		return errors.New("influxdb_port out of range")
	}

	for _, rule := range c.Rule {
		if rule.Rtype == "" {
			return errors.New(`rule missing "type"`)
		}
		if rule.Match == "" {
			return errors.New(`rule missing "match"`)
		}
		if rule.Subject != "" {
			return fmt.Errorf("writer rules shouldn't have subject (%q)", rule.Subject)
		}
	}
	return nil
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

	if conf.Mode == "downsampler" && conf.DownsamplePeriod.IsZero() {
		conf.DownsamplePeriod.Duration = time.Minute
	}

	if err := conf.Validate(); err != nil {
		return nil, err
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
