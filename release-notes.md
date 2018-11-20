# v2.2.0

## Listener batch size handling

- Fail config validation if listener batch size is greater than
  1MB. NATS will only accept messages of up to 1MB in size.
- Ensure the listener will never send a batch to NATS that is bigger
  than the 1MB which NATS will accept. This was previously possible
  under some circumstances.

## InfluxDB HTTP basic auth support

influx-spout writers can now authenticate to InfluxDB backends if
required. The username and password are read from two environment
variables (`INFLUXDB_USER` and `INFLUXDB_PASS`).

## Write retries

influx-spout writers will now retry failed writes for a configurable
number of times. Retries are configured by the following new options:

* `writer_retry_batches`: The maximum number of batches that failed to
  write to InfluxDB to track for retry at any given time. The oldest retry batch will be
  discarded if necessary to keep the set of batches being retried within this
  limit. Default is 1. Set to 0 to disable write retries.
* `writer_retry_interval`: The amount of time between write retry
  attempts. Default is "10s".
* `writer_retry_timeout`: The maximum amount of time to keep retrying
  a given batch. Default is "1m"

## Enforce maximum writer batch size

When writing a batch to InfluxDB it is possible for the batch size to
exceed the maximum batch size because incoming lines are unlikely to
exactly fit inside the desired batch size. A BatchSplitter is now used
to ensure that the configured batch size is never exceeded.

This is important because InfluxDB has a maximum body size that it will accept.

## Other

- Fixed escape handling bugs in downsampler component.
- The Prometheus metrics published by each influx-spout component is
  now documented in README.md.
- InfluxDB Line Protocol unescaping is now about 4% faster.
- Batches recevied by the listener component will now have a newline
  added if it is missing.
- Fixed incorrect accounting of received batches for the UDP listener.
- Fix potential issues with batch memory growth. Batch buffers now
  grow by at least the maximum UDP packet size to ensure that there is
  always sufficient memory available.

# v2.1.0

## New downsampler component

The downsamper is useful for creating rolled up versions of metrics
streams for long term archive.

It reads measurements from one or more NATS subjects, averaging
numeric fields over a sampling period (as set by `downsample_period`),
emitting the averaged values every sampling period. For non-numeric
fields, the last value seen for each sampling period is
emitted. Measurements are emitted on clean time boundaries regardless
of when the downsampler was started.

Downsampled lines are emitted to a NATS subject with the same name as
the input subject but with a suffix as set by the `downsample_suffix`
configuration option (default is `-archive`)

## filter: New "tags rule type

A new rule type has been introduced which allows matching against one
or more exact tags on measurement lines. The approach taken is much
faster than using regexes to achieve the same kinds of matches
(80-1000% depending on number tags to match and line size). This
approach is also much safer as matching is independent of tag order
and matches tag keys and values precisely.

## filter: Tag sorting

The filter now orders tag keys in all measurement lines passing
through it. This ensures the best performance when lines are inserted
into InfluxDB and is also required for the downsampler component.

## filter: Avoid unnecessary escaping

Unescaping of lines is now avoided where possible avoiding unnecessary
computation and memory allocation. This improves performance
significantly in many cases.

## filter: Measurement name hash caching

The hash of measurement names in lines are now cached. This speeds up
matching when there are multiple "basic" filter rules.

## Other

- Fixed invalid TOML in README
- Fix race in `probes` package tests
- Retry gnatsd startup in tests (a common reason for CI failure)
- Automatically manage probe port allocations in end-to-end test
- Generate releases out of Travis CI

# v2.0.0

## Configuration cleanup

**Compatibility**

Some significant changes to influx-spout's configuration file have
been made to improvement usability, clarity and consistency. These
changes break compatibility with the previous configuration file
format. Some configuration options have been renamed, some take a
different value type and one has been removed.

Here's a summary of the changes:

| Old Name              | New Name                | Old Format        | New Format                             |
|-----------------------|-------------------------|-------------------|----------------------------------------|
| `batch_max_mb`        | `batch_max_size`        | integer (MB)      | string, flexible units (e.g. `"20MB"`) |
| `nats_pending_max_mb` | `nats_max_pending_size` | integer (MB)      | string, flexible units (e.g. `"1GB"`)  |
| `read_buffer_bytes`   | `read_buffer_size`      | integer (bytes)   | string, flexible units (e.g. `"512K"`) |
| `batch`               | `batch_max_count`       | integer count     | (unchanged)                            |
| `listener_batch_size` | **Removed**             | -                 | use `batch_max_size` instead           |
| `batch_max_secs`      | `batch_max_age`         | integer (seconds) | string, flexible units (e.g. `"5m"`)   |
| `max_time_delta_secs` | `max_time_delta`        | integer (seconds) | string, flexible units (e.g. `"30s"`)  |
| `write_timeout_secs`  | `write_timeout`         | integer (seconds) | string, flexible units (e.g. `"10s"`)  |

## Listener batch aging

The listener component will now send its batch if the age of the batch
exceeds a configured limit, as set by the `batch_max_age` option. This
prevents batched lines from being held up if the rate of data received
by the listener drops. The default listener batch age is 5 minutes.

## Hostname included in internal metrics

The metrics that each influx-spout component exposes (in Prometheus
format) now include a hostname label.

## Configurable stats interval

The interval at which each influx-spout component publishes its own
metrics is now configurable using the new `stats_interval`
option. This was previously fixed at 3 seconds.

## Configuration validation

Various errors in influx-spout's configuration are now checked
for. influx-spout will fail to start if a configuration error is
detected.

## Performance improvements for internal stats

- Avoid unnecessary name lookups for index based metrics.
- Use atomic counter updates instead of mutex.
- `stats` package microbenchmark is now 78% faster.

## Developer tooling for manual tests

- Added slow (fake) influxd tool (`cmd/slowinfluxd`). This is useful
  for testing how influx-spout behaves when dealing with slow
  consumers.
- Added `watchmem` tool for monitoring memory consumption.
- Documented how to run a manual slow consumer test.

## Other

- Code structure: all binaries now live as subpackages under the `cmd`
  top level package.
- The writer and listener now share the same batch type instead of
  having independent but similar implementations.
- Cleaner handling for internal Prometheus labels.
- Fixed minor linter issues

# v1.1.0

For this release and older see the notes at
https://github.com/jumptrading/influx-spout/releases.

