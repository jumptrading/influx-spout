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
leaks. For example, to monitor the memory used by a influx-spout
writer over time, run:

```
tools/watchmem writer-mem.dat influx-spout tools/writer.toml
```

The elapsed time and current resident set size (RSS) of the writer
processes will be emitted every 2s as well as being written out to
`writer-mem.dat`. This file can be directly plotted by tools like
[gnuplot](http://gnuplot.info/) or imported into a spreadsheet for
analysis.

## Slow consumer testing

It can be useful to check the effect of a slow InfluxDB instance on
influx-spout. Slow consumers have historically caused problems for
influx-spout.

To run a slow consumer test, start influx-spout as described above but
replace InfluxDB with the `slowinfluxd` utility provided with
influx-spout.

* Build `slowinfluxd`: `go install github.com/jumptrading/influx-spout/cmd/slowinfluxd`
* Ensure `influxd` isn't running (`sudo systemctl stop influxdb`)
* Run `slowinfluxd`
* Ensure `gnatsd` is running (as above).
* Run the various influx-spout components as described above. Run the
  writers using the `watchmem` tool above.

Any measurements fed into influx-spout will now end up being written
to the `slowinfluxd` server which holds up the writer(s) by delaying
responses to write requests. Even with a high rate of measurements
into the listener, memory usage for all the influx-spout components
should increase to some threshold and then plateau. If memory
consumption increases without bound, there could be a configuration
problem or a bug in influx-spout.

## Releases

Public releases of influx-spout are managed using
[GoReleaser](https://goreleaser.com/). It must be installed on any
host that is going to create and publish a release.

To publish a new release:

* Create an annotated tag for the release. For example: `git tag v3.2.1 -m "3.2.1 release"`
* Push the tag to Github. For example: `git push origin 3.2.1`
* Run GoReleaser: `goreleaser --rm-dist`

This will build the release artifacts, generate a changelog and
publish these to [Github](https://github.com/jumptrading/influx-spout/releases).
