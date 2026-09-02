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
