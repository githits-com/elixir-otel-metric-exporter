defmodule OtelMetricExporter.ExportTelemetry do
  @moduledoc """
  Owns the bounded telemetry contract for completed OTLP log and metric batches.

  A pipeline calls `start/0` when normal export dispatch begins. Queued batches
  also call it when queueing starts so an undispatched shutdown drop has a
  lifecycle duration. The pipeline calls `stop/5` once the typed export outcome
  is final. The emitted event contains only bounded measurements and metadata
  suitable for downstream aggregation.
  """

  alias OtelMetricExporter.OtelApi

  @event [:otel_metric_exporter, :export, :stop]

  @type scope :: :logs | :metrics
  @opaque start_time :: integer()
  @type outcome ::
          :success
          | :partial_success
          | :terminal_invalid_response
          | :terminal_encoding_failed
          | :terminal_response_too_large
          | :terminal_http_status
          | :retryable_deadline_exceeded
          | :retryable_pool_timeout
          | :retryable_transport_failure
          | :retryable_request_failed
          | :retryable_export_task_failed
          | :retryable_http_status

  @type measurements :: %{
          duration_ms: non_neg_integer(),
          batch_size: non_neg_integer(),
          rejected_items: non_neg_integer(),
          dropped_items: non_neg_integer()
        }

  @type metadata :: %{scope: scope(), outcome: outcome()}

  @doc "Starts a monotonic duration measurement for one export batch."
  @spec start() :: start_time()
  def start, do: System.monotonic_time(:millisecond)

  @doc """
  Emits one `[:otel_metric_exporter, :export, :stop]` event for the final
  `result` of a batch started at `start_time`.

  The event measurements are exactly `%{duration_ms: non_neg_integer(),
  batch_size: non_neg_integer(), rejected_items: non_neg_integer(),
  dropped_items: non_neg_integer()}`. Its metadata is exactly
  `%{scope: :logs | :metrics, outcome: outcome()}`. Returns `:ok`.
  """
  @spec stop(start_time(), scope(), non_neg_integer(), OtelApi.export_result(), non_neg_integer()) ::
          :ok
  def stop(start_time, scope, batch_size, result, dropped_items) do
    {outcome, rejected_items} = outcome_and_rejected_items(result)

    measurements = %{
      duration_ms: System.monotonic_time(:millisecond) - start_time,
      batch_size: batch_size,
      rejected_items: rejected_items,
      dropped_items: dropped_items
    }

    :telemetry.execute(@event, measurements, %{scope: scope, outcome: outcome})
    :ok
  end

  @spec outcome_and_rejected_items(OtelApi.export_result()) :: {outcome(), non_neg_integer()}
  defp outcome_and_rejected_items(:ok), do: {:success, 0}

  defp outcome_and_rejected_items({:partial_success, rejected_items}),
    do: {:partial_success, rejected_items}

  defp outcome_and_rejected_items({:error, disposition, {:http_status, _status}}),
    do: {http_status_outcome(disposition), 0}

  defp outcome_and_rejected_items({:error, disposition, reason}),
    do: {failure_outcome(disposition, reason), 0}

  @spec http_status_outcome(OtelApi.failure_disposition()) :: outcome()
  defp http_status_outcome(:terminal), do: :terminal_http_status
  defp http_status_outcome(:retryable), do: :retryable_http_status

  @spec failure_outcome(OtelApi.failure_disposition(), OtelApi.failure_reason()) :: outcome()
  defp failure_outcome(:terminal, :invalid_response), do: :terminal_invalid_response
  defp failure_outcome(:terminal, :encoding_failed), do: :terminal_encoding_failed
  defp failure_outcome(:terminal, :response_too_large), do: :terminal_response_too_large
  defp failure_outcome(:retryable, :deadline_exceeded), do: :retryable_deadline_exceeded
  defp failure_outcome(:retryable, :pool_timeout), do: :retryable_pool_timeout
  defp failure_outcome(:retryable, :transport_failure), do: :retryable_transport_failure
  defp failure_outcome(:retryable, :request_failed), do: :retryable_request_failed
  defp failure_outcome(:retryable, :export_task_failed), do: :retryable_export_task_failed
end
