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

// +build small

package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testConfigBaseName = "something"

var testConfigFileName = filepath.Join("some", "dir", testConfigBaseName+".toml")

func init() {
	// Clear these to ensure consistent test environment.
	os.Setenv("INFLUXDB_USER", "")
	os.Setenv("INFLUXDB_PASS", "")
}

func TestCorrectConfigFile(t *testing.T) {
	const validConfigSample = `
name = "thor"

mode = "writer"
port = 10001

nats_address = "nats://localhost:4222"
nats_subject = ["spout"]
nats_subject_monitor = "spout-monitor"

influxdb_address = "localhost"
influxdb_port = 8086
influxdb_dbname = "junk_nats"

batch_max_count = 10
batch_max_size = "5m"
batch_max_age = "1m"
workers = 96

write_timeout = "32s"
read_buffer_size = 43210
nats_max_pending_size = "100MB"
max_time_delta = "789s"

stats_interval = "5s"

probe_port = 6789
pprof_port = 5432
`
	conf, err := parseConfig(validConfigSample)
	require.NoError(t, err, "Couldn't parse a valid config: %v\n", err)

	assert.Equal(t, "thor", conf.Name, "Name must match")
	assert.Equal(t, "writer", conf.Mode, "Mode must match")
	assert.Equal(t, 10001, conf.Port, "Port must match")
	assert.Equal(t, 10, conf.BatchMaxCount, "Batching must match")
	assert.Equal(t, 5*datasize.MB, conf.BatchMaxSize)
	assert.Equal(t, time.Minute, conf.BatchMaxAge.Duration)
	assert.Equal(t, 96, conf.Workers)
	assert.Equal(t, 32*time.Second, conf.WriteTimeout.Duration)
	assert.Equal(t, 43210*datasize.B, conf.ReadBufferSize)
	assert.Equal(t, 100*datasize.MB, conf.NATSMaxPendingSize)
	assert.Equal(t, 789*time.Second, conf.MaxTimeDelta.Duration)

	assert.Equal(t, 8086, conf.InfluxDBPort, "InfluxDB Port must match")
	assert.Equal(t, "junk_nats", conf.DBName, "InfluxDB DBname must match")
	assert.Equal(t, "localhost", conf.InfluxDBAddress, "InfluxDB address must match")

	assert.Equal(t, "spout", conf.NATSSubject[0], "Subject must match")
	assert.Equal(t, "spout-monitor", conf.NATSSubjectMonitor, "Monitor subject must match")
	assert.Equal(t, "nats://localhost:4222", conf.NATSAddress, "Address must match")

	assert.Equal(t, 5*time.Second, conf.StatsInterval.Duration)
	assert.Equal(t, 6789, conf.ProbePort)
	assert.Equal(t, 5432, conf.PprofPort)
}

func TestAllDefaults(t *testing.T) {
	conf, err := parseConfig(`mode = "writer"`)
	require.NoError(t, err)

	assert.Equal(t, testConfigBaseName, conf.Name)
	assert.Equal(t, "nats://localhost:4222", conf.NATSAddress)
	assert.Equal(t, []string{"influx-spout"}, conf.NATSSubject)
	assert.Equal(t, "influx-spout-monitor", conf.NATSSubjectMonitor)
	assert.Equal(t, "influx-spout-junk", conf.NATSSubjectJunkyard)
	assert.Equal(t, "localhost", conf.InfluxDBAddress)
	assert.Equal(t, 8086, conf.InfluxDBPort)
	assert.Equal(t, "", conf.InfluxDBUser)
	assert.Equal(t, "", conf.InfluxDBPass)
	assert.Equal(t, "influx-spout-junk", conf.DBName)
	assert.Equal(t, 10, conf.BatchMaxCount)
	assert.Equal(t, 10*datasize.MB, conf.BatchMaxSize)
	assert.Equal(t, 5*time.Minute, conf.BatchMaxAge.Duration)
	assert.Equal(t, 0, conf.Port)
	assert.Equal(t, "writer", conf.Mode)
	assert.Equal(t, 8, conf.Workers)
	assert.Equal(t, 30*time.Second, conf.WriteTimeout.Duration)
	assert.Equal(t, time.Duration(0), conf.DownsamplePeriod.Duration)
	assert.Equal(t, "-archive", conf.DownsampleSuffix)
	assert.Equal(t, 4*datasize.MB, conf.ReadBufferSize)
	assert.Equal(t, 200*datasize.MB, conf.NATSMaxPendingSize)
	assert.Equal(t, 10*time.Minute, conf.MaxTimeDelta.Duration)
	assert.Equal(t, 3*time.Second, conf.StatsInterval.Duration)
	assert.Equal(t, 0, conf.ProbePort)
	assert.Equal(t, 0, conf.PprofPort)
	assert.Equal(t, false, conf.Debug)
	assert.Len(t, conf.Rule, 0)
}

func TestDefaultPortListener(t *testing.T) {
	conf, err := parseConfig(`mode = "listener"`)
	require.NoError(t, err)
	assert.Equal(t, 10001, conf.Port)
}

func TestDefaultPortHTTPListener(t *testing.T) {
	conf, err := parseConfig(`mode = "listener_http"`)
	require.NoError(t, err)
	assert.Equal(t, 13337, conf.Port)
}

func TestDefaultPortMonitor(t *testing.T) {
	conf, err := parseConfig(`mode = "monitor"`)
	require.NoError(t, err)
	assert.Equal(t, 9331, conf.Port)
}

func TestDefaultListenerBatchSize(t *testing.T) {
	conf, err := parseConfig(`mode = "listener"`)
	require.NoError(t, err)
	assert.Equal(t, 1*datasize.MB, conf.BatchMaxSize)
}

func TestDefaultHTTPListenerBatchSize(t *testing.T) {
	conf, err := parseConfig(`mode = "listener_http"`)
	require.NoError(t, err)
	assert.Equal(t, 1*datasize.MB, conf.BatchMaxSize)
}

func TestDefaultDownsamplePeriod(t *testing.T) {
	conf, err := parseConfig(`mode = "downsampler"`)
	require.NoError(t, err)
	assert.Equal(t, time.Minute, conf.DownsamplePeriod.Duration)
}

func TestInfluxAuth(t *testing.T) {
	os.Setenv("INFLUXDB_USER", "user")
	defer os.Setenv("INFLUXDB_USER", "")
	os.Setenv("INFLUXDB_PASS", "secret")
	defer os.Setenv("INFLUXDB_PASS", "")

	conf, err := parseConfig(`mode = "writer"`)
	require.NoError(t, err)
	assert.Equal(t, "user", conf.InfluxDBUser)
	assert.Equal(t, "secret", conf.InfluxDBPass)
}

func TestNoMode(t *testing.T) {
	_, err := parseConfig("")
	assert.EqualError(t, err, "mode not specified in config")
}

func TestInvalidTOML(t *testing.T) {
	_, err := parseConfig("mode=\"writer\"\nbatch = abc")
	require.Error(t, err)
	assert.Regexp(t, ".+expected value but found.+", err.Error())
}

func TestRulesConfig(t *testing.T) {
	const rulesConfig = `
mode = "listener"
port = 10001

nats_address = "nats://localhost:4222"
nats_subject = ["spout"]
nats_subject_monitor = "spout-monitor"

influxdb_address = "localhost"
influxdb_port = 8086
influxdb_dbname = "junk_nats"

batch = 10
workers = 96

[[rule]]
type = "basic"
match = "hello"
subject = "hello-subject"

[[rule]]
type = "regex"
match = "world"
subject = "world-subject"

[[rule]]
type = "tags"
tags = [
   ["foo", "bar"],
   ["aaa", "zzz"],
]
subject = "foo-subject"
`
	conf, err := parseConfig(rulesConfig)
	require.NoError(t, err, "config should be parsed")

	assert.Len(t, conf.Rule, 3)
	assert.Equal(t, conf.Rule[0], Rule{
		Rtype:   "basic",
		Match:   "hello",
		Subject: "hello-subject",
	})
	assert.Equal(t, conf.Rule[1], Rule{
		Rtype:   "regex",
		Match:   "world",
		Subject: "world-subject",
	})
	assert.Equal(t, conf.Rule[2], Rule{
		Rtype: "tags",
		Tags: [][]string{
			{"foo", "bar"},
			{"aaa", "zzz"},
		},
		Subject: "foo-subject",
	})
}

func TestCommonOverlay(t *testing.T) {
	const commonConfig = `
batch_max_count = 50
influxdb_dbname = "massive"
`
	const specificConfig = `
mode = "listener"
batch_max_count = 100
debug = true
`
	Fs = afero.NewMemMapFs()
	afero.WriteFile(Fs, commonFileName, []byte(commonConfig), 0600)
	afero.WriteFile(Fs, testConfigFileName, []byte(specificConfig), 0600)

	conf, err := NewConfigFromFile(testConfigFileName)
	require.NoError(t, err)

	assert.Equal(t, "listener", conf.Mode)   // only set in specific config
	assert.Equal(t, 100, conf.BatchMaxCount) // overridden in specific config
	assert.Equal(t, "massive", conf.DBName)  // only set in common config
}

func TestInvalidTOMLInCommonConfig(t *testing.T) {
	const commonConfig = `
wat
`
	const specificConfig = `
mode = "listener"
batch_max_count = 100
debug = true
`

	Fs = afero.NewMemMapFs()
	afero.WriteFile(Fs, commonFileName, []byte(commonConfig), 0600)
	afero.WriteFile(Fs, testConfigFileName, []byte(specificConfig), 0600)

	_, err := NewConfigFromFile(testConfigFileName)
	require.Error(t, err)
	assert.Regexp(t, "/etc/influx-spout.toml: .+", err)
}

type failingOpenFs struct{ afero.Fs }

func (*failingOpenFs) Open(string) (afero.File, error) {
	return nil, errors.New("boom")
}

func TestErrorOpeningCommonFile(t *testing.T) {
	Fs = new(failingOpenFs)

	_, err := NewConfigFromFile(testConfigFileName)
	assert.EqualError(t, err, "boom")
}

func TestOpenError(t *testing.T) {
	Fs = afero.NewMemMapFs()

	conf, err := NewConfigFromFile("/does/not/exist")
	assert.Nil(t, conf)
	assert.Error(t, err)
}

func parseConfig(content string) (*Config, error) {
	Fs = afero.NewMemMapFs()
	afero.WriteFile(Fs, testConfigFileName, []byte(content), 0600)

	return NewConfigFromFile(testConfigFileName)
}

func TestValidateDefaults(t *testing.T) {
	conf := newDefaultConfig()
	conf.Mode = "writer"
	assert.NoError(t, conf.Validate())
}

func TestValidateProblems(t *testing.T) {
	Fs = afero.NewMemMapFs()

	tests := []struct {
		err    string
		config string
	}{{
		"invalid mode: frog",
		`
mode = "frog"
`,
	}, {
		"listener port out of range",
		`
mode = "listener"
port = -1
`,
	}, {
		"listener port out of range",
		`
mode = "listener"
port = 65536
`,
	}, {
		"listener port out of range",
		`
mode = "listener_http"
port = -1
`,
	}, {
		"listener port out of range",
		`
mode = "listener_http"
port = 65536
`,
	}, {
		"probe_port out of range",
		`
mode = "filter"
probe_port = 65536
`,
	}, {
		"probe_port out of range",
		`
mode = "writer"
probe_port = -1
`,
	}, {
		"pprof_port out of range",
		`
mode = "filter"
pprof_port = 65536
`,
	}, {
		"pprof_port out of range",
		`
mode = "writer"
pprof_port = -1
`,
	}, {
		"listener should only use one NATS subject",
		`
mode = "listener"
nats_subject = ["one", "two"]
`,
	}, {
		"listener should only use one NATS subject",
		`
mode = "listener_http"
nats_subject = ["one", "two"]
`,
	}, {
		"listener batch must be 1MB or smaller",
		`
mode = "listener"
batch_max_size = 1048577
`,
	}, {
		"listener batch must be 1MB or smaller",
		`
mode = "listener_http"
batch_max_size = 1048577
`,
	}, {
		"filter should only use one NATS subject",
		`
mode = "filter"
nats_subject = ["one", "two"]
`,
	}, {
		`rule missing "type"`,
		`
mode = "filter"

[[rule]]
match = "needle"
subject = "foo"
`,
	}, {
		`rule missing "match"`,
		`
mode = "filter"

[[rule]]
type = "basic"
subject = "foo"
`,
	}, {
		`rule missing "subject"`,
		`
mode = "filter"

[[rule]]
type = "basic"
match = "needle"
`,
	}, {
		`basic rule cannot use "tags"`,
		`
mode = "filter"

[[rule]]
type = "basic"
match = "something"
subject = "out"
tags = [["abc", "def"]]
`,
	}, {
		`tags rule cannot use "match"`,
		`
mode = "filter"

[[rule]]
type = "tags"
subject = "out"
match = "something"
`,
	}, {
		`tags rule missing "tags"`,
		`
mode = "filter"

[[rule]]
type = "tags"
subject = "out"
`,
	}, {
		`each tags item must contain 2 values`,
		`
mode = "filter"

[[rule]]
type = "tags"
tags = [["abc", "def", "zzz"]]
subject = "out"
`,
	}, {
		"writer needs at least one NATS subject",
		`
mode = "writer"
nats_subject = []
`,
	}, {
		"influxdb_port out of range",
		`
mode = "writer"
influxdb_port = 65536
`,
	}, {
		"influxdb_port out of range",
		`
mode = "writer"
influxdb_port = 0
`,
	}, {
		`rule missing "type"`,
		`
mode = "writer"

[[rule]]
match = "needle"
`,
	}, {
		`rule missing "match"`,
		`
mode = "writer"

[[rule]]
type = "basic"
`,
	}, {
		`writer rules shouldn't have subject ("matches")`,
		`
mode = "writer"

[[rule]]
type = "basic"
match = "needle"
subject = "matches"
`,
	}, {
		`downsample_period is only for downsampler mode`,
		`
mode = "filter"
downsample_period = "30s"
`,
	}, {
		`downsample_suffix must be set`,
		`
mode = "downsampler"
downsample_period = "30s"
downsample_suffix = ""
`,
	}, {
		`downsample_suffix must be set`,
		`
mode = "downsampler"

downsample_period = "30s"
downsample_suffix = ""
`,
	},
	}

	for i, test := range tests {
		t.Logf("Validate test %d (%s)", i, test.err)
		afero.WriteFile(Fs, testConfigFileName, []byte(test.config), 0600)
		conf, err := NewConfigFromFile(testConfigFileName)
		assert.Nil(t, conf)
		assert.EqualError(t, err, test.err)
	}
}

func TestTextValidateAuth(t *testing.T) {
	config0 := newDefaultConfig()
	config0.Mode = "writer"
	config0.InfluxDBUser = "hello"
	assert.EqualError(t, config0.Validate(), "$INFLUXDB_USER without $INFLUXDB_PASS")

	config1 := newDefaultConfig()
	config1.Mode = "writer"
	config1.InfluxDBPass = "hello"
	assert.EqualError(t, config1.Validate(), "$INFLUXDB_PASS without $INFLUXDB_USER")
}
