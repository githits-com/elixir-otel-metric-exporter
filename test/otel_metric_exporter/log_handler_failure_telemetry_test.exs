defmodule OtelMetricExporter.LogHandlerFailureTelemetryTest do
  use ExUnit.Case, async: false

  alias OtelMetricExporter.LogHandler
  alias OtelMetricExporter.LogHandlerFailureTelemetry

  @event [:otel_metric_exporter, :log_handler, :exception]
  @secret "authorization=secret-placeholder"

  defmodule FailingReport do
    @behaviour Access
    defstruct [:kind]

    @impl true
    def fetch(report, _key), do: fail(report.kind)
    @impl true
    def get_and_update(report, _key, _function), do: fail(report.kind)
    @impl true
    def pop(report, _key), do: fail(report.kind)

    defp fail(:deep_error),
      do: deep_error(16, fn -> raise ArgumentError, "synthetic deep preparation error" end)

    defp fail(:throw), do: throw(:synthetic_prepare_throw)
    defp fail(:exit), do: exit(:synthetic_prepare_exit)

    # Public test-local entry prevents the compiler proving a private callback
    # never returns and eliminating the frames this fixture needs.
    def deep_error(0, callback), do: callback.()
    # Alternate non-tail call sites so the retained stack really hides Protocol.
    def deep_error(depth, callback), do: [deep_step(depth - 1, callback)]
    defp deep_step(depth, callback), do: [deep_error(depth, callback)]
  end

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

    assert :ok = LogHandler.log(event, handler_config({:test_olp, self(), make_ref()}))
    refute_receive {:"$gen_cast", {:"$olp_load", _}}, 0

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
    event = log_event({:report, @secret})

    assert :ok = LogHandler.log(event, handler_config({:test_olp, self(), make_ref()}))
    refute_receive {:"$gen_cast", {:"$olp_load", _}}, 0

    assert_receive {:handler_exception, @event, measurements, metadata}
    assert measurements == %{count: 1}

    assert metadata == %{
             exception: :protocol_undefined,
             failure_source: :body,
             message_shape: :report_other,
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

    refute_receive {:handler_exception, @event, _, _}, 0
  end

  test "contains a deep preparation error even without a retained Protocol frame" do
    report = %FailingReport{kind: :deep_error}
    event = log_event({:report, report})
    config = handler_config({:test_olp, self(), make_ref()})

    stacktrace =
      try do
        OtelMetricExporter.Protocol.prepare_log_event(event, config.config)
        flunk("expected the synthetic deep Access callback to fail preparation")
      rescue
        _error in ArgumentError -> __STACKTRACE__
      end

    refute Enum.any?(stacktrace, fn {module, _, _, _} ->
             module == OtelMetricExporter.Protocol
           end)

    assert :ok = LogHandler.log(event, config)
    assert_receive {:handler_exception, @event, %{count: 1}, metadata}
    assert metadata.stage == :prepare
    assert metadata.failure_source == :unknown
    assert metadata.exception == :argument_error
    assert metadata.message_shape == :report_struct
    assert metadata.trace_context == :missing
    assert metadata.olp_alive
    refute inspect(metadata) =~ @secret
    refute_receive {:handler_exception, @event, _, _}, 0
    refute_receive {:"$gen_cast", {:"$olp_load", _}}, 0
  end

  test "preserves an actual load failure with the original stack and one diagnostic" do
    mode_ref = {__MODULE__, make_ref()}
    :persistent_term.put(mode_ref, :invalid_mode)

    try do
      stacktrace =
        try do
          LogHandler.log(
            log_event({:string, "safe"}),
            handler_config({:test_olp, self(), mode_ref})
          )

          flunk("expected the synthetic invalid OLP mode to fail loading")
        rescue
          _error in CaseClauseError -> __STACKTRACE__
        end

      assert [{:logger_olp, :load, 2, _} | _] = stacktrace
      assert_receive {:handler_exception, @event, %{count: 1}, metadata}
      assert metadata.stage == :load
      assert metadata.failure_source == :olp
      assert metadata.exception == :case_clause
      assert metadata.olp_alive
      refute_receive {:handler_exception, @event, _, _}, 0
      refute_receive {:"$gen_cast", {:"$olp_load", _}}, 0
    after
      :persistent_term.erase(mode_ref)
    end
  end

  test "preparation throws and exits preserve kind, reason and stack rather than being contained" do
    for {kind, reason} <- [throw: :synthetic_prepare_throw, exit: :synthetic_prepare_exit] do
      result =
        try do
          LogHandler.log(
            log_event({:report, %FailingReport{kind: kind}}),
            handler_config({:test_olp, self(), make_ref()})
          )
        catch
          caught_kind, caught_reason -> {caught_kind, caught_reason, __STACKTRACE__}
        end

      assert {^kind, ^reason, stacktrace} = result
      assert [{FailingReport, _, _, _} | _] = stacktrace

      assert Enum.any?(stacktrace, fn {module, function, _, _} ->
               module == OtelMetricExporter.Protocol and function == :encode_body
             end)

      assert_receive {:handler_exception, @event, %{count: 1}, metadata}
      assert metadata.exception == kind
      assert metadata.message_shape == :report_struct
      refute_receive {:handler_exception, @event, _, _}, 0
      refute_receive {:"$gen_cast", {:"$olp_load", _}}, 0
    end
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

    assert :ok = LogHandler.log(event, config)

    assert_receive :reentered_handler
    assert_receive {:handler_exception, @event, %{count: 1}, _metadata}
    refute_receive {:handler_exception, @event, _measurements, _metadata}, 0

    # The process-local guard must be cleared after the first emission.
    assert :ok = LogHandler.log(event, config)
    assert_receive :reentered_handler
    assert_receive {:handler_exception, @event, %{count: 1}, _metadata}
    refute_receive {:handler_exception, @event, _measurements, _metadata}, 0
    refute_receive {:"$gen_cast", {:"$olp_load", _}}, 0
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
    :ok = LogHandler.log(event, config)
    send(parent, :reentered_handler)
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
