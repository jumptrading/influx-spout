# InfluxDB Relay

## Overview

Below is a quick overview graph:

                               +-------------------------------------------------+
    +---+     +----------+     |                                                 |
    |UDP|---->| Listener |---->|                      NATS                       |
    +---+     +----------+     |                                                 |
                               +----+------^---+--------------------+------------+
                                    |      |   |                    |
                                    v      |   |                    |
                               +--------+  |   |                    |
                               |        |  |   |                    |
                               | Filter |--+   |                    |
                               |        |      |                    |
                               +--------+      |                    |
                                               v                    v
                                      +----------------+   +----------------+
                                      |                |   |                |
                                      |     Writer     |   |     Writer     |
                                      |                |   |                |
                                      +----------------+   +----------------+
                                               |                    |
                                               v                    v
                                      +----------------+   +----------------+
                                      |                |   |                |
                                      |    InfluxDB    |   |    InfluxDB    |
                                      |                |   |                |
                                      +----------------+   +----------------+

The relay can operate in different modes, each of which are outlined below.

## Listener mode

The Listener is responsible for receiving metrics on a UDP port and preparing
to send to a NATS bus (optionally batching metrics prior to send).

### Required configuration options

    mode = "listener"
    port = 10001
    nats_address = "nats://localhost:4222"
    nats_topic = ["influxdb-relay"]
    nats_topic_monitor = "influxdb-relay-monitor"
    batch = 100

The options are:

- `mode`: Required, must be "listener" to start the relay in listener mode.
- `port`: Required, the UDP port to listen on.
- `nats_address`: Required, the NATS bus address.
- `nats_topic`: Required, MUST be a singleton list containing the topic to
  publish messages to.
- `nats_topic_monitor`: Required, an extra topic used for out-of-band,
  diagnostic messages concerning the relay.
- `batch`: Optional, defaults to 1, the number of messages to collect before
  sending to the NATS bus.

## HTTP Listener mode

The HTTP listener is the same as the listener except that it received metrics
via HTTP request bodies sent to a `/write` endpoint on the configured port.

### Required configuration options

    mode = "listener_http"
    port = 13337
    nats_address = "nats://localhost:4222"
    nats_topic = ["influxdb-relay"]
    nats_topic_monitor = "influxdb-relay-monitor"
    batch = 100

The options are:

- `mode`: Required, must be "listener_http" to start the relay in HTTP listener mode.
- `port`: Required, the TCP port to server the receiving HTTP endpoint on.

All other options are as per the UDP listener above.

## Filter mode

The filter is responsible for filtering messages and forward them to their
respective topic on the NATS bus. This is meant to be a high-level filter that
forwards incoming lines to other NATS topics based on the metric.

### Required configuration options

    mode = "filter"
    nats_address = "nats://localhost:4222"
    nats_topic = ["influxdb-relay"]
    nats_topic_monitor = "influx-spout-monitor"
    nats_topic_junkyard = "__junkyard"

    [[rule]]
    type="basic"
    match="cgroup"
    channel="measurement.cgroup"

The options are:

- `mode`: Required, must be "filter" to start the relay in filter mode.
- `nats_address`: Required, the NATS bus' address.
- `nats_topic`: Required, MUST be a singleton list containing the topic to get
  messages from.
- `nats_topic_monitor`: Required, an extra topic used for out-of-band,
  diagnostic messages concerning the relay.
- `nats_topic_junkyard`: Required, the topic to which metrics that do not match
  any rule (see below) are forwarded to.

Additionally, at least one rule is required:

- `mode`: Can be `basic` or `regex`, however using `regex` is discouraged in filter mode.
- `match`: A string describing what to match. In `basic` mode, it is the name
  of the measurement. In `regex` mode, this is a Golang regex that can match
  the entire line.
- `channel`: The NATS channel to forward metrics matching this rule to.

## Writer mode

A Writer is responsible for writing matching metrics to backends, currently
InfluxDB nodes.

### Required configuration options

    mode = "writer"
    nats_address = "nats://localhost:4222"
    nats_topic = ["measurement.*", "__junkyard"]
    nats_topic_monitor = "influxdb-relay-monitor"

    influxdb_address = "influx-node01"
    influxdb_port = 8086
    influxdb_dbname = "relay_nats"

    batch = 100
    workers = 96
    write_timeout_secs = 30
    nats_pending_max_mb = 200

The options are:

- `mode`: Required, must be "writer" to start the relay in writer mode.
- `nats_address`: Required, the NATS bus' address.
- `nats_topic`: Required, a list of NATS topics to get the metrics from.
- `nats_topic_monitor`: Required, an extra topic used for out-of-band,
  diagnostic messages concerning the relay.
- `influxdb_address`: Required, Hostname of the InfluxDB instance.
- `influxdb_port`: Required, TCP port where the InfluxDB backend can be found.
- `influxdb_dbname`: Required, the database name inside InfluxDB to write to.
- `batch`: Optional, defaults to 1, the number of matching metrics to collect
  before issuing a write to InfluxDB.
- `workers:` Optional, defaults to 1, the number of worker threads to spawn.
- `write_timeout_secs:` Optional, defaults to 30, the maximum number of seconds
  a writer will wait when writing to an InfluxDB endpoint.
- `nats_pending_max_mb:` Optional, defaults to 200, the maximum number of megabytes
  that the NATS pending buffer for a topic may become. Data will be dropped if
  this limit is reached. This helps to deal with slow consumers.

## Running tests

The relay's tests are classified as either "small", "medium" or "large", where
"small" tests are unit tests with no external dependencies and "large" test are
full system tests. To simplify running of tests a `runtests` helper is
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

Benchmarks have been implemented for key areas of the relay's
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
Cloning to /tmp/tmp.uTPpVD1hvD/src/github.com/jump-opensource/influxdb-relay-nova
>>>> Running reference benchmarks
>>>> Comparing benchmarks
benchmark                 old ns/op     new ns/op     delta
BenchmarkLineLookup-4     75.5          88.4          +17.09%

3 benchmarks compared.
1 performance regressions detected.
```
