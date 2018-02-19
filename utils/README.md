# Influx Spout Utilities
This directory contains small helper utilities and scripts to make InfluxDB data wrangling easier

## influx-spout-tap
Dumps a stream of influx-spout data to STDOUT. It has the following features:
 - Subscribing to individual subjects (-s flag)
 - Lines can be further filtered with regular expressions (-e flag)
