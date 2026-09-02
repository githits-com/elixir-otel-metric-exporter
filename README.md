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
drops the full data-point batch, while a retryable failure retains it. Partial
success reports rejected items separately and does not count them as dropped.

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
