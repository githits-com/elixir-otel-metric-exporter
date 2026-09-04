# OtelMetricExporter

This is an **unofficial** OTel-compatible `:telemetry` exporter that collects specified metrics
and then exports them to an OTel endpoint. It uses metric definitions
from `:telemetry_metrics` library and does not currently support `Summary` metric type.

This diverges from the official [OTel API requirements](https://opentelemetry.io/docs/specs/otel/metrics/api/) for metrics
in favour of reusing existing metric definitions from `:telemetry_metrics` library. Consequently, it
does not integrate with `:opentelemetry_api` library and the metrics won't have Exemplars with span/trace names
associated.

Of OTel metric types, this currently doesn't support `ExponentialHistogram` and `Summary` metric types.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `otel_metric_exporter` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:otel_metric_exporter, "~> 0.3.6"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/otel_metric_exporter>.

## Export completion telemetry

The exporter emits one `[:otel_metric_exporter, :export, :stop]` event for each
final log or metric batch outcome. Measurements are exactly
`duration_ms`, `batch_size`, `rejected_items`, and `dropped_items`; all are
non-negative integers. Metadata is exactly `scope` (`:logs` or `:metrics`) and
the bounded `outcome` atom. No endpoint, headers, response content, or error
terms are included.

For logs, a final failure drops the full batch. For metrics, a terminal failure
drops the full data-point batch, while a retryable failure retains it. Delta
retry state is an immutable history of at most ten pending intervals; older
intervals are pruned with their points included in `dropped_items`, and retained
intervals are reaggregated into one point per series. Partial success reports
rejected items separately and does not count them as dropped.

Invalid metric measurements are isolated per definition so sibling metrics keep
running. The exporter emits exactly
`[:otel_metric_exporter, :metric, :measurement_dropped]` with measurements
`%{count: 1}` and bounded metadata with exactly the keys
`%{metric_type: metric_type, reason: reason}`; both values are bounded
atoms. Missing values (`nil` or `:undefined`) are reported as
`reason: :missing`; non-numeric values for value-bearing metrics are reported
as `reason: :non_numeric`.
Counters retain presence semantics: every present measurement increments the
counter by one, regardless of its value or type.

If the Logger callback raises, exits, or throws while preparing or loading a log
event, the handler emits `[:otel_metric_exporter, :log_handler, :exception]`
before preserving the original failure. Its sole measurement is `count: 1`.
Metadata contains only fixed classifications: `stage` (`olp_liveness`,
`prepare`, `load`, or `handler`), `failure_source` (`trace_context`, `body`,
`attributes`, `protocol`, `olp`, `handler`, or `unknown`), `exception`,
`message_shape`, `trace_context` (`valid`, `missing`, `partial`, or `invalid`),
and the boolean `olp_alive`. The module types enumerate the exception and
message-shape values. It never includes the log event, Logger metadata,
exception reason, stacktrace, endpoint, headers, or response content.

Logger report maps may use arbitrary Erlang terms as keys and values. Nested
plain maps retain their flattened dotted-key representation, while structs and
other scalar terms are encoded through their inspected representation. These
valid report shapes do not detach the handler.

## Configuration changes

The log handler resolves its exporter, timeout, and concurrency settings when it
starts. `exporter: :none` leaves a configured Logger handler as an inert
no-op and starts no transport processes. Changing the exporter, OTLP timeout,
or concurrent-request limit requires removing and adding the handler again so
its shutdown allowance and connection pool are rebuilt together.

For metrics, `exporter: :none` keeps the top-level supervisor alive but starts
neither `MetricStore` nor telemetry handlers, ETS state, or export timers.

`filter_config/1` output is diagnostic: header values and endpoint userinfo are
redacted, so it must not be reused as a handler configuration. OTP applies OLP
options and accumulator configuration through separate calls; if the
accumulator stays busy long enough to reject a live change, remove and add the
handler again before retrying the change.

Logger `:set` operations rebuild handler configuration from application and
environment defaults; values supplied only when the handler was first added
must be supplied again or moved into those defaults.

## Metric aggregation temporality

Metric sums, counters, and histograms are encoded with cumulative OTLP
aggregation temporality by default for compatibility. Configure
`aggregation_temporality: :delta` for receivers that expect delta intervals,
such as Datadog. Gauges are point-in-time values and are unaffected by this
option. Cumulative aggregates retain their lifetime state across collections;
delta aggregates drain into intervals and retryable failures retain the bounded
pending history described above. Empty delta collections do not send a request.

Integer sums use ETS `update_counter` for their atomic fast path. Float sums
use an exact-object, unbounded compare-and-swap update; there is no arbitrary
retry cap or fallback. Distributions use exact scaled-integer atomic
accumulation; their exact sum is rounded once at export to IEEE 754 binary64
with ties-to-even rounding, while min/max are decoded to binary64 and signed
zero is canonicalized to positive zero.

On graceful supervised shutdown, telemetry handlers detach before MetricStore's
single final collection/export, and Finch remains alive for that bounded
attempt. The supervisor allowance is `2 * otlp_timeout + 1_000` milliseconds,
covering a possible in-flight regular export plus the final bounded attempt;
the final OTLP request itself does not receive two timeouts. A final attempt
that fails terminally or with a retryable outcome counts all attempted payload
points in `dropped_items`. The final drain covers observations linearized
before each series' final take/snapshot; a callback already executing at detach
may write afterward and is outside that exact accounting boundary. Forced or
otherwise abnormal termination is outside the flush guarantee.

The dependency-free production benchmark exercises paired write-path scenarios
and normal plus ten-interval retry/recovery collection. Run it with:

```text
MIX_ENV=prod mix run bench/metric_store_bench.exs
```
