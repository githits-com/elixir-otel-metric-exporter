defmodule OtelMetricExporter.LogHandlerFailureTelemetryTest do
  use ExUnit.Case, async: false

  alias OtelMetricExporter.LogHandler
  alias OtelMetricExporter.LogHandlerFailureTelemetry

  @event [:otel_metric_exporter, :log_handler, :exception]
  @secret "authorization=secret-placeholder"

  setup do
    handler_id = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler_id,
      @event,
      &__MODULE__.handle_exception/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  def handle_exception(event, measurements, metadata, parent) do
    send(parent, {:handler_exception, event, measurements, metadata})
  end

  test "reports invalid trace context without copying the log event" do
    event = log_event({:string, @secret}, %{otel_trace_id: "not-hex"})

    assert_raise ArgumentError, fn ->
      LogHandler.log(event, handler_config({:test_olp, self(), make_ref()}))
    end

    assert_receive {:handler_exception, @event, measurements, metadata}
    assert measurements == %{count: 1}

    assert metadata == %{
             exception: :argument_error,
             failure_source: :trace_context,
             message_shape: :string,
             olp_alive: true,
             stage: :prepare,
             trace_context: :invalid
           }

    refute inspect(metadata) =~ @secret
  end

  test "reports unsupported report bodies without copying exception details" do
    event = log_event({:report, %{%{} => @secret}})

    try do
      LogHandler.log(event, handler_config({:test_olp, self(), make_ref()}))
      flunk("expected report conversion to fail")
    catch
      :error, %Protocol.UndefinedError{} ->
        assert [{String.Chars, :impl_for!, 1, _} | _] = __STACKTRACE__
    end

    assert_receive {:handler_exception, @event, measurements, metadata}
    assert measurements == %{count: 1}

    assert metadata == %{
             exception: :protocol_undefined,
             failure_source: :body,
             message_shape: :report_map,
             olp_alive: true,
             stage: :prepare,
             trace_context: :missing
           }

    refute inspect(metadata) =~ @secret
  end

  test "reports an unavailable OLP process before preserving handler failure" do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

    assert_raise MatchError, fn ->
      LogHandler.log(log_event({:string, "safe"}), handler_config({:test_olp, pid, make_ref()}))
    end

    assert_receive {:handler_exception, @event, measurements, metadata}
    assert measurements == %{count: 1}

    assert metadata == %{
             exception: :match_error,
             failure_source: :handler,
             message_shape: :string,
             olp_alive: false,
             stage: :olp_liveness,
             trace_context: :missing
           }
  end

  test "collapses unknown failure inputs to fixed values" do
    assert :ok =
             LogHandlerFailureTelemetry.emit(
               log_event(:unexpected),
               self(),
               :error,
               RuntimeError.exception(@secret),
               []
             )

    assert_receive {:handler_exception, @event, measurements, metadata}
    assert measurements == %{count: 1}

    assert metadata == %{
             exception: :other_error,
             failure_source: :unknown,
             message_shape: :other,
             olp_alive: true,
             stage: :handler,
             trace_context: :missing
           }

    refute inspect(metadata) =~ @secret
  end

  test "preserves a known failure stage if the OLP process also stops" do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

    stacktrace = [{OtelMetricExporter.Protocol, :prepare_log_event, 2, []}]

    assert :ok =
             LogHandlerFailureTelemetry.emit(
               log_event({:string, "safe"}),
               pid,
               :error,
               RuntimeError.exception("safe"),
               stacktrace
             )

    assert_receive {:handler_exception, @event, measurements, metadata}
    assert measurements == %{count: 1}
    assert metadata.stage == :prepare
    assert metadata.failure_source == :protocol
    assert metadata.olp_alive == false
  end

  test "does not recursively emit when a telemetry consumer re-enters the handler" do
    event = log_event({:string, @secret}, %{otel_trace_id: "not-hex"})
    config = handler_config({:test_olp, self(), make_ref()})
    handler_id = {__MODULE__, :reentrant, make_ref()}

    :telemetry.attach(
      handler_id,
      @event,
      &__MODULE__.reenter_handler/4,
      {self(), event, config}
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert_raise ArgumentError, fn -> LogHandler.log(event, config) end

    assert_receive :reentered_handler
    assert_receive {:handler_exception, @event, %{count: 1}, _metadata}
    refute_receive {:handler_exception, @event, _measurements, _metadata}
  end

  test "classifies binary, charlist, and partial trace context" do
    valid_trace_id = List.duplicate(?a, 32)
    valid_span_id = List.duplicate(?b, 16)

    for {metadata, expected} <- [
          {%{otel_trace_id: valid_trace_id, otel_span_id: valid_span_id}, :valid},
          {%{otel_trace_id: String.duplicate("a", 32), otel_span_id: String.duplicate("b", 16)},
           :valid},
          {%{otel_trace_id: valid_trace_id}, :partial}
        ] do
      assert :ok =
               LogHandlerFailureTelemetry.emit(
                 log_event({:string, "safe"}, metadata),
                 self(),
                 :error,
                 RuntimeError.exception("safe"),
                 []
               )

      assert_receive {:handler_exception, @event, %{count: 1}, event_metadata}
      assert event_metadata.trace_context == expected
    end
  end

  test "classifies load failures and non-error catches" do
    stacktrace = [{:logger_olp, :load, 2, []}]

    for {kind, expected} <- [exit: :exit, throw: :throw] do
      assert :ok =
               LogHandlerFailureTelemetry.emit(
                 log_event({:string, "safe"}),
                 self(),
                 kind,
                 @secret,
                 stacktrace
               )

      assert_receive {:handler_exception, @event, %{count: 1}, metadata}
      assert metadata.stage == :load
      assert metadata.failure_source == :olp
      assert metadata.exception == expected
      refute inspect(metadata) =~ @secret
    end
  end

  test "classifies every Logger message family without retaining its value" do
    cases = [
      {{:string, @secret}, :string},
      {{:report, %URI{path: @secret}}, :report_struct},
      {{:report, [message: @secret]}, :report_list},
      {{:report, @secret}, :report_other},
      {{~c"~s", [@secret]}, :format},
      {:unexpected, :other}
    ]

    for {message, expected} <- cases do
      assert :ok =
               LogHandlerFailureTelemetry.emit(
                 log_event(message),
                 self(),
                 :error,
                 RuntimeError.exception(@secret),
                 []
               )

      assert_receive {:handler_exception, @event, %{count: 1}, metadata}
      assert metadata.message_shape == expected
      refute inspect(metadata) =~ @secret
    end
  end

  def reenter_handler(_event, _measurements, _metadata, {parent, event, config}) do
    try do
      LogHandler.log(event, config)
    catch
      _kind, _reason -> send(parent, :reentered_handler)
    end
  end

  defp handler_config(olp) do
    %{
      config: %{
        metadata: [],
        metadata_map: %{},
        olp: olp
      }
    }
  end

  defp log_event(message, metadata \\ %{}) do
    %{
      level: :info,
      msg: message,
      meta: Map.put(metadata, :time, System.system_time(:microsecond))
    }
  end
end
