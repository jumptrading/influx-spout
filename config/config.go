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
	"github.com/BurntSushi/toml"
	"github.com/spf13/afero"
)

type RawRule struct {
	Rtype   string `toml:"type"`
	Match   string `toml:"match"`
	Channel string `toml:"channel"`
}

// Config replaces our configuration values
type Config struct {
	NATSAddress       string    `toml:"nats_address"`
	NATSTopic         []string  `toml:"nats_topic"`
	NATSTopicMonitor  string    `toml:"nats_topic_monitor"`
	NATSTopicJunkyard string    `toml:"nats_topic_junkyard"`
	InfluxDBAddress   string    `toml:"influxdb_address"`
	InfluxDBPort      int       `toml:"influxdb_port"`
	DBName            string    `toml:"influxdb_dbname"`
	BatchMessages     int       `toml:"batch"`
	IsTesting         bool      `toml:"testing_mode"`
	Port              int       `toml:"port"`
	Mode              string    `toml:"mode"`
	WriterWorkers     int       `toml:"workers"`
	WriteTimeoutSecs  int       `toml:"write_timeout_secs"`
	NATSPendingMaxMB  int       `toml:"nats_pending_max_mb"`
	Rule              []RawRule `toml:"rule"`
	Debug             bool      `toml:"debug"`
}

// NewConfig returns an instance of our config struct
func NewConfig() *Config {
	return &Config{}
}

// NewConfig parses the specified configuration file and returns a
// Config.
func NewConfigFromFile(fileName string) (*Config, error) {
	f, err := fs.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	conf := new(Config)
	_, err = toml.DecodeReader(f, conf)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

var fs = afero.NewOsFs()
