# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- Add bounded completion telemetry for log and metric export batches.
- Make disabled exporters inert and reject live log transport changes that
  would invalidate their startup resources.
- Add configurable cumulative or delta aggregation temporality for metric
  sums, counters, and histograms.
- Skip non-numeric distribution measurements, bound retained failed metric
  generations, and correct Gauge timestamps for Datadog receivers.
- Precompute metric names and export lookup data during setup.

## [0.3.6] - 2025-04-08

- Fix protobuf encoding of `:logger.report()` events

## [0.3.5] - 2025-04-07

- Fix race conditions registering metrics handlers before `MetricStore` is ready
- Add retries to HTTP POST metric data
