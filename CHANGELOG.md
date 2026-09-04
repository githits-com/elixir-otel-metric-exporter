# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- Encode valid Logger report maps with nested structs or arbitrary term keys
  without detaching the log handler.
- Emit bounded telemetry before preserving a Logger callback failure so handler
  removal can be diagnosed without exposing log or transport data.
- Add bounded completion telemetry for log and metric export batches.
- Make disabled exporters inert and reject live log transport changes that
  would invalidate their startup resources.
- Add configurable cumulative or delta aggregation temporality for metric
  sums, counters, and histograms.
- Correct Gauge timestamps for Datadog receivers.
- Correct MetricStore aggregation state: delta retry retention is capped at ten
  immutable pending intervals, reaggregated to one point per series, with
  pruned points included in dropped-item accounting; cumulative aggregates
  retain lifetime totals.
- Preserve counter presence semantics, isolate invalid measurements with the
  bounded `[:otel_metric_exporter, :metric, :measurement_dropped]` event, and
  use ETS `update_counter` for integer sums, exact-object CAS for float sums,
  and exact scaled-integer state for distributions.
- Flush metrics during graceful supervised shutdown while Finch remains alive,
  with the final-drain linearization boundary documented in the README.
- Add the dependency-free production benchmark (`MIX_ENV=prod mix run
  bench/metric_store_bench.exs`) for paired write-path and collection/recovery
  verification.
- Precompute metric names and export lookup data during setup.

## [0.3.6] - 2025-04-08

- Fix protobuf encoding of `:logger.report()` events

## [0.3.5] - 2025-04-07

- Fix race conditions registering metrics handlers before `MetricStore` is ready
- Add retries to HTTP POST metric data
