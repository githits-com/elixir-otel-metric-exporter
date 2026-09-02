defmodule OtelMetricExporter.ExportTelemetryTest do
  use ExUnit.Case, async: false

  alias OtelMetricExporter.ExportTelemetry

  @event [:otel_metric_exporter, :export, :stop]

  setup do
    handler_id = {__MODULE__, make_ref()}
    parent = self()

    :telemetry.attach(
      handler_id,
      @event,
      fn event, measurements, metadata, _config ->
        send(parent, {:export_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  test "emits the exact bounded event contract for full success" do
    assert :ok = ExportTelemetry.stop(ExportTelemetry.start(), :logs, 3, :ok, 0)

    assert_receive {:export_event, @event, measurements, metadata}

    assert %{duration_ms: duration, batch_size: 3, rejected_items: 0, dropped_items: 0} =
             measurements

    assert is_integer(duration) and duration >= 0
    assert metadata == %{scope: :logs, outcome: :success}

    assert Map.keys(measurements) |> Enum.sort() == [
             :batch_size,
             :dropped_items,
             :duration_ms,
             :rejected_items
           ]

    assert Map.keys(metadata) |> Enum.sort() == [:outcome, :scope]
  end

  test "maps partial success and every bounded failure to fixed outcomes" do
    cases = [
      {{:partial_success, 2}, :partial_success, 2},
      {{:error, :terminal, :invalid_response}, :terminal_invalid_response, 0},
      {{:error, :terminal, :encoding_failed}, :terminal_encoding_failed, 0},
      {{:error, :terminal, :response_too_large}, :terminal_response_too_large, 0},
      {{:error, :terminal, {:http_status, 500}}, :terminal_http_status, 0},
      {{:error, :retryable, :deadline_exceeded}, :retryable_deadline_exceeded, 0},
      {{:error, :retryable, :pool_timeout}, :retryable_pool_timeout, 0},
      {{:error, :retryable, :transport_failure}, :retryable_transport_failure, 0},
      {{:error, :retryable, :request_failed}, :retryable_request_failed, 0},
      {{:error, :retryable, :export_task_failed}, :retryable_export_task_failed, 0},
      {{:error, :retryable, {:http_status, 503}}, :retryable_http_status, 0}
    ]

    Enum.each(cases, fn {result, outcome, rejected_items} ->
      started_at = ExportTelemetry.start()

      assert :ok =
               ExportTelemetry.stop(started_at, :metrics, 4, result, 1)

      assert_receive {:export_event, @event, measurements, %{scope: :metrics, outcome: ^outcome}}
      assert measurements.batch_size == 4
      assert measurements.rejected_items == rejected_items
      assert measurements.dropped_items == 1
      assert measurements.duration_ms >= 0
    end)
  end
end
