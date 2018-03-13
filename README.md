# influx-spout [![Build Status](https://travis-ci.com/jumptrading/influx-spout.svg?token=tnGgsxkjnTL4Es3TAsMc&branch=master)](https://travis-ci.com/jumptrading/influx-spout)

## Overview

influx-spout is an open source messaging system that routes and processes 
[InfluxDB line protocol] metrics from agents (for example [Telegraf]) to 
processing and storage backends ([InfluxDB], [Kapacitor] etc.). 

Key features:
- Proven ability to handle high volumes of data (>500k points per second) 
  in a production environment
- Horizontal scalability 
  - Multithreaded data processing within components
  - Components can be distributed on multiple servers or a fleet of containers
- Ability to add and remove endpoints without disrupting existing data flows
- Fine-grained, regexp-based control over routing of metrics data to specific 
  backends
- Sanity checking of the data stream that prevents corrupt metrics data from 
  reaching backends
- Batching of outgoing data to larger chunks, making it easier for backends 
  to handle high-volume dataflows
- Leverages the high-performance [NATS] messaging system

[InfluxDB]: https://www.influxdata.com/time-series-platform/influxdb/
[InfluxDB line protocol]: https://docs.influxdata.com/influxdb/v1.4/write_protocols/line_protocol_tutorial/
[Telegraf]: https://www.influxdata.com/time-series-platform/telegraf/
[NATS]: https://nats.io/
[Kapacitor]: https://www.influxdata.com/time-series-platform/kapacitor/

The following diagram shows a typical influx-spout deployment, and the
flow of data between the various components:

```
                           +-----+              +------+
                           | UDP |              | HTTP |
                           +-----+              +------+
                              |                     |
                              v                     v
                     +-----------------+   +-----------------+
                     |                 |   |                 |
                     |     Listener    |   |  HTTP Listener  |
                     |                 |   |                 |
                     +-----------------+   +-----------------+
                              |                     |
                              v                     v
 +----------+   +-------------------------------------------------+
 |          |<--+                                                 |
 |  Filter  |   |                     NATS                        |
 |          +-->|                                                 |
 +----------+   +--------+----------------+----------------+------+
                         |                |                |
                         v                v                v
                   +----------+     +----------+     +----------+
                   |          |     |          |     |          |
                   |  Writer  |     |  Writer  |     |  Writer  |
                   |          |     |          |     |          |
                   +-----+----+     +-----+----+     +-----+----+
                         |                |                |
                         v                v                v
                   +----------+     +----------+     +----------+
                   | InfluxDB |     | InfluxDB |     | InfluxDB |
                   +----------+     +----------+     +----------+
```

All the influx-spout components may be run on a single host or may be
spread across multiple hosts depending on scaling and operational
requirements.

## Building

Ensure the `GOPATH` environment is properly exported and simply run
`make` to build influx-spout. We recommend building with Go 1.9+.


## Configuration

influx-spout is a single binary which will run as any of the component
modes, depending on the configuration provided. The binary takes a
single mandatory positional argument which is the path to the [TOML]
configuration file to use:

```
influx-spout [/path/to/config.toml]
```

During startup, `/etc/influx-spout.toml` is parsed first, if it exists. The
configuration specified in the file with provided on the command line overrides
any configuration in `/etc/influx-spout.toml`. This allows configuration common
to different influx-spout components to be shared.

[TOML]: https://github.com/toml-lang/toml

## Modes

This section documents the various influx-spout component modes and
how to configure them.

### Listener

The listener is responsible for receiving InfluxDB line protocol measurements
on a UDP port, batching them and sending them on to a single NATS subject. In
typical deployments, a single listener will exist but it is possible to run
multiple listeners.

The supported configuration options for the listener mode follow. Defaults are
shown.


```toml
mode = "listener"  # Required

# UDP port to listen on.
port = 10001

# Address of NATS server.
nats_address = "nats://localhost:4222"

# Subject to publish received measurements on. This must be a list with one item.
nats_subject = ["influx-spout"]

# How many messages to collect before forwarding to the NATS server.
# Increasing this number reduces NATS communication overhead but increases
# latency.
batch = 10

# Maximum UDP socket receive buffer size in bytes. A higher value this increases
# the peak inbound traffic the listener can handle at the cost of higher memory
# consumption.
#
# The default value is good for low to medium loads but should be increased to
# support higher receive rates.
read_buffer_bytes = 4194304

# Out-of-bound metrics and diagnostic messages are published to this NATS subject
# (in InfluxDB line protocol format).
nats_subject_monitor = "influx-spout-monitor"
```

### HTTP Listener

The HTTP listener is like the UDP listener (above) except that it receives
measurements sent using HTTP request bodies to a `/write` endpoint on the
configured port.

The supported configuration options for the HTTP listener mode follow. Defaults
are shown.

```toml
mode = "listener_http"  # Required

# TCP port to server a HTTP server on. A single "/write" endpoint is available here.
port = 13337

# Address of NATS server.
nats_address = "nats://localhost:4222"

# Subject to publish received measurements on. This must be a list with one item.
nats_subject = ["influx-spout"]

# How many messages to collect before forwarding to the NATS server.
# Increasing this number reduces NATS communication overhead but increases
# latency.
batch = 10

# Maximum UDP socket receive buffer size in bytes. A higher value this increases
# the peak inbound traffic the listener can handle at the cost of higher memory
# consumption.
#
# The default value is good for low to medium loads but should be increased to
# support higher receive rates.
read_buffer_bytes = 4194304

# The maximum number of bytes that the listener should send at once to NATS.
# This should be no bigger than the NATS server's max_payload setting (which
# defaults to 1 MB).
listener_batch_bytes = 1048576

# Out-of-bound metrics and diagnostic messages are published to this NATS subject
# (in InfluxDB line protocol format).
nats_subject_monitor = "influx-spout-monitor"
```

### Filter

The filter is responsible for filtering measurements published to NATS by the
listener, and forwarding them on to other NATS subjects.

The supported configuration options for the filter mode follow. Defaults are
shown.

```toml
mode = "filter"  # Required

# Address of NATS server.
nats_address = "nats://localhost:4222"

# Subject to receive measurements from (presumably from the listener).
# This must be a list with one item.
nats_subject = ["influx-spout"]

# Measurements which do not match any rule (below) are sent to this NATS subject.
nats_subject_junkyard = "influx-spout-junk"

# Out-of-bound metrics and diagnostic messages are published to this NATS subject
# (in InfluxDB line protocol format).
nats_subject_monitor = "influx-spout-monitor"

# The number of filter workers to spawn.
workers = 8

# Incoming metrics with timestamps ± this value from the current time will be
# rejected. Metrics with timestamps that are significantly different from previously
# written timestamps negatively impact InfluxDB performance.
max_time_delta_secs = 600

# At least one rule should be defined. Rules are defined using TOML's table
# syntax. The following examples show each rule type.

[[rule]]
# "basic" rules match a measurement name exactly.
type = "basic"

# For basic rules, "match" specifies an exact measurement name.
match = "cgroup"

# Measurements matching the rule are forwarded to this subject.
subject = "measurement.cgroup"


[[rule]]
# "regex" rules apply a regular expression to full measurement lines.
# Note: regex rules are significantly slower than basic rules. Use with care.
type = "regex"

# For regex rules, "match" specifies regular expression pattern to apply.
match = "host=web.+,"

# As above.
subject = "hosts.web"


[[rule]]
# "negregex" rules apply a regular expression to full measurement lines and
# match measurements which *don't* match the regular expression.
# Note: negregex rules are significantly slower than basic rules. Use with care.
type = "negregex"

# For negregex rules, "match" specifies regular expression pattern to apply.
match = "host=web.+,"

# As above.
subject = "not-web"
```

Ordering of rules in the configuration is important. Only the first rule that
matches a given measurement is applied.

### Writer

A writer is responsible for reading measurements from one or more NATS subjects,
optionally filtering them and then writing matching them to an InfluxDB
instances. A writer should be run for each InfluxDB backend that influx-spout
should send measurements to.

The supported configuration options for the writer mode follow. Defaults are
shown.

```toml
mode = "writer"  # Required

# Name to use for identifying a writer's internal metrics.
# (to separate metrics from different writers from each other)
name = "[default is configuration file path with directory & extension stripped]"

# Address of NATS server.
nats_address = "nats://localhost:4222"

# The NATS subjects to receive measurements from.
nats_subject = ["influx-spout"]

# Address of the InfluxDB instance to write to.
influxdb_address = "localhost"

# TCP port where the InfluxDB backend can be found.
influxdb_port = 8086

# The InfluxDB database name to write to. The default value is unlikely to be
# useful. Please set to an appropriate value.
influxdb_dbname = "influx-spout-junk"

# How many messages to collect before writing to InfluxDB.
# Increasing this number reduces InfluxDB communication overhead but increases
# latency.
batch = 10

# The maximum amount of message data a writer worker is allowed to collect. If
# this limit is reached, a write to InfluxDB is performed.
batch_max_mb = 10

# The maximum amount of time a writer worker is allowed to hold on to collected
# data. If this limit is reached, a write to InfluxDB is performed.
batch_max_secs = 300

# The number of writer workers to spawn.
workers = 8

# The maximum number of seconds a writer will wait for an InfluxDB write to
# complete. Writes which time out will be dropped.
write_timeout_secs = 30

# The maximum size that the pending buffer for a NATS subject that the writer
# is reading from may become (in megabytes). Measurements will be dropped if
# this limit is reached. This helps to deal with slow InfluxDB instances.
nats_pending_max_mb = 200

# Out-of-bound metrics and diagnostic messages are published to this NATS subject
# (in InfluxDB line protocol format).
nats_subject_monitor = "influx-spout-monitor"
```

A writer will batch up messages until one of the limits defined by the
`batch`, `batch_max_mb` or `batch_max_secs` options is reached.

Writers can optionally include filter rules. When filter rules are configured
measurements which don't match a rule will be dropped by the writer instead of
being written to InfluxDB. Rule configuration is the same as for the filter
component, but the rule subject should be omitted.

## Running tests

influx-spout's tests are classified as either "small", "medium" or "large",
where "small" tests are unit tests with no external dependencies and "large"
test are full system tests. To simplify running of tests a `runtests` helper is
provided.

Here's some example usage:

```
# Just run the small tests
$ ./runtests small

# Run the small & medium tests
$ ./runtests small medium

# Run the medium tests including benchmarks
$ ./runtests -b medium

# Run the small tests & generate coverage profiles
$ ./runtests -c medium

# Run the small tests with verbose output
$ ./runtests -v medium
```

## Checking for performance regressions

Benchmarks have been implemented for key areas of influx-spout's
functionality. The `perfcheck` script will compare the performance of these
benchmarks in the working tree against a fixed reference revision. Any
benchmarks that regress by 10% or more will be reported.

As for `runtests`, `perfcheck` takes the benchmark sizes to run:

```
# Just run the small benchmarks
$ ./perfcheck small

# Run the small & medium tests
$ ./perfcheck small medium
```

Here's an example of a detected performance regression:

```
$ ./perfcheck small
Comparing working tree against 948733acdff077f0e2910a4334ac8698a480ba81
>>>> Building benchcheck tool
>>>> Running current benchmarks
>>>> Setting up reference branch
Cloning to /tmp/tmp.uTPpVD1hvD/src/github.com/jumptrading/influx-spout
>>>> Running reference benchmarks
>>>> Comparing benchmarks
benchmark                 old ns/op     new ns/op     delta
BenchmarkLineLookup-4     75.5          88.4          +17.09%

3 benchmarks compared.
1 performance regressions detected.
```


## Docker

To ease development or deployment to fancy infrastructure such as
[kubernetes](https://kubernetes.io/), a [Dockerfile](Dockerfile)
is provided.


### Building a docker image

The [Makefile](Makefile) contains the magic to build the image easily:

    $ make docker

Check the version:

    $ docker images | grep influx-spout
    influx-spout                                      0.1.e6514f8          4af1ff8077d5        30 seconds ago      11.62 MB

To build images for a custom docker registry pass the `DOCKER_NAME` variable with the full name sans the tag:

    $ make docker DOCKER_NAME=quay.io/foo/influx-spout
    ... docker build output ...

    $ docker images | grep quay\.io
    quay.io/foo/influx-spout                          0.1.9adf7e6          94078aaba5f1        24 minutes ago      11.62 MB


### Setting up a test influx-spout instance using Docker

This requires running a nats container and an influx-spout container with
a shared network namespace. First things first, setup nats and expose
the ports for either of the example [http](example-http-listener.conf)
or [udp](example-udp-listener.conf) listener example configurations:

    $ docker run -p 11001:11001 -p 10001:10001 --rm -it --name nats-main nats
    [1] 2018/01/24 18:52:33.326930 [INF] Starting nats-server version 1.0.4
    [1] 2018/01/24 18:52:33.327212 [INF] Starting http monitor on 0.0.0.0:8222
    [1] 2018/01/24 18:52:33.327325 [INF] Listening for client connections on 0.0.0.0:4222
    [1] 2018/01/24 18:52:33.327391 [INF] Server is ready
    [1] 2018/01/24 18:52:33.328545 [INF] Listening for route connections on 0.0.0.0:6222

Next, run the influx-spout container, making sure to use the right
network namespace. It defaults to the http listener if no arguments are provided.

    $ docker run --network=container:nats-main -it influx-spout:0.1.e6514f8
    2018/01/24 19:23:03 Running /bin/influx-spout version e6514f8, built on 2018-01-24, go1.9.2
    2018/01/24 19:23:03 Listener bound to HTTP socket: :11001

An example UDP listener can be setup by specifying the full config path:

    $ docker run --network=container:nats-main -it influx-spout:0.1.e6514f8 /etc/influx-spout/udp-listener.toml
    2018/01/24 19:24:02 Running /bin/influx-spout version e6514f8, built on 2018-01-24, go1.9.2
    2018/01/24 19:24:02 Listener bound to UDP socket: 0.0.0.0:10001

Since the nats-main container is exporting both ports, they're ready to
accept traffic: The HTTP listener is easiest to test:

    $ curl -i -XPOST 'http://localhost:11001/write?db=mydb' --data-binary 'cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000'
    HTTP/1.1 200 OK
    Date: Wed, 24 Jan 2018 19:24:53 GMT
    Content-Length: 0
    Content-Type: text/plain; charset=utf-8
