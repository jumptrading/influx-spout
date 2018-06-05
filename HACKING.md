# Hacking on influx-spout

## Running influx-spout on a developer machine

The `tools` directory contains a set of configuration files for
running a full influx-spout system on a single machine. These should
work together and can be used as-is or tweaked as required.

Before running influx-spout, InfluxDB needs to be installed and set up:

* Install InfluxDB as per the [official documentation](https://docs.influxdata.com/influxdb/latest/introduction/installation).
* Start influxdb. How to do this is dependent on your Linux
  distribution. Starting InfluxDB may involve `sudo systemctl start
  influxdb` or `sudo influxd`.
* Create a `spout` database: `influx -execute "create database spout"`

A [NATS server](https://nats.io/) is also required:

* Install NATS as per the [official documentation](https://nats.io/documentation/tutorials/gnatsd-install/).
* Start a NATS server with the default configuration by running: `gnatsd`

Now build and install influx-spout: `go install
github.com/jumptrading/influx-spout/...`. Then start the various
influx-spout components:

```
influx-spout tools/monitor.toml
influx-spout tools/listener.toml
influx-spout tools/listener_http.toml
influx-spout tools/filter.toml
influx-spout tools/writer.toml
```

You'll want to run each component in its own termnial session.

At this point influx-spout should be ready to accept measurements via
UDP or HTTP.

## Monitoring memory usage

The `tools/watchmem` script can be used to monitor the memory usage of
a program. This is useful for checking influx-spout for memory
leaks. For example, to monitor the memory used by a influx-spout writer over time, run:

```
tools/watchmem writer-mem.dat influx-spout tools/writer.toml
```

The elapsed time and current resident set size (RSS) of the writer
processes will be emitted every 2s as well as being written out to
`writer-mem.dat`. This file can be directly plotted by tools like
[gnuplot](http://gnuplot.info/) or imported into a spreadsheet for
analysis.
