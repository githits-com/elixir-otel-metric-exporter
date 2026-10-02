defmodule OtelMetricExporter.LogHandlerIntegrationTest do
  use ExUnit.Case, async: false
  @moduletag :capture_log

  alias OtelMetricExporter.LogHandler
  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.{AnyValue, ArrayValue, KeyValueList}
  alias OtelMetricExporter.Opentelemetry.Proto.Logs.V1.LogRecord

  require Logger

  @default_config %{
    resource: %{instance: %{id: "integration-test"}},
    # Use small debounce/buffer to make tests predictable
    debounce_ms: 10,
    max_buffer_size: 1,
    # A single request at a time makes later marker delivery an export barrier
    # for preceding loads from the same producer, only in this fixture.
    otlp_concurrent_requests: 1,
    otlp_headers: %{},
    retry: false,
    # Map request_id from metadata
    metadata_map: %{
      request_id: "http.request.id"
    }
  }

  setup do
    # Use a unique handler ID for each test run
    handler_id = :"handler_#{System.unique_integer([:positive, :monotonic])}"
    bypass = Bypass.open()

    config =
      Map.merge(@default_config, %{
        otlp_endpoint: "http://127.0.0.1:#{bypass.port}"
      })

    # Add the handler for this test
    :ok = :logger.add_handler(handler_id, LogHandler, %{config: config})

    # Ensure the handler is removed after the test finishes
    on_exit(fn ->
      # Receiver messages precede completion of the HTTP callback. Let Bypass
      # finish those callbacks before stopping the handler's HTTP tasks.
      :ok = Bypass.down(bypass)

      case Process.whereis(:"#{LogHandler}_#{handler_id}") do
        nil ->
          _ = :logger.remove_handler(handler_id)

        supervisor ->
          ref = Process.monitor(supervisor)
          :ok = :logger.remove_handler(handler_id)
          assert_receive {:DOWN, ^ref, :process, ^supervisor, _}, 1_000
      end
    end)

    {:ok, bypass: bypass, handler_id: handler_id, config: config}
  end

  defp decode_request_body(body) do
    body
    |> :zlib.gunzip()
    |> Protobuf.decode(ExportLogsServiceRequest)
  end

  # --- Test Cases Below ---

  test "captures Logger.info message", %{bypass: bypass} do
    parent = self()

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        decode_request_body(body)

      send(parent, {:logs, logs})
      Plug.Conn.resp(conn, 200, "")
    end)

    Logger.info("hello info")

    assert_receive {:logs, logs}, 500
    assert [%LogRecord{body: %{value: {:string_value, "hello info"}}}] = logs
  end

  test "keeps the handler installed after reports with arbitrary terms", %{
    bypass: bypass,
    handler_id: handler_id
  } do
    parent = self()

    Bypass.expect(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        decode_request_body(body)

      send(parent, {:logs, logs})
      Plug.Conn.resp(conn, 200, "")
    end)

    uri = URI.parse("https://example.com/packages")
    Logger.info(%{%{} => "map key", nested: uri})

    assert_receive {:logs, [%LogRecord{body: %{value: {:kvlist_value, %{values: values}}}}]},
                   500

    values = Map.new(values, fn key_value -> {key_value.key, key_value.value} end)
    assert values["nested"] == %AnyValue{value: {:string_value, inspect(uri)}}
    assert values["%{}"] == %AnyValue{value: {:string_value, "map key"}}
    assert {:ok, _config} = :logger.get_handler_config(handler_id)

    Logger.info("after report")

    assert_receive {:logs, [%LogRecord{body: %{value: {:string_value, "after report"}}}]}, 500
  end

  test "mixed and improper report values export before later logs through the same components", %{
    bypass: bypass,
    handler_id: handler_id
  } do
    observe_batches(bypass)
    attach_failure_observer()
    components = handler_components(handler_id)
    trace = String.duplicate("a", 32)
    span = String.duplicate("b", 16)

    mixed =
      {:array_value,
       %ArrayValue{
         values: [
           %AnyValue{
             value:
               {:array_value,
                %ArrayValue{
                  values: [
                    %AnyValue{value: {:string_value, "a"}},
                    %AnyValue{value: {:int_value, 1}}
                  ]
                }}
           },
           %AnyValue{value: {:string_value, "b"}}
         ]
       }}

    improper =
      {:array_value, %ArrayValue{values: [%AnyValue{value: {:string_value, inspect([1 | 2])}}]}}

    for {label, payload, expected} <- [
          {"mixed", [{:a, 1}, :b], mixed},
          {"improper", [[1 | 2]], improper}
        ],
        {context, metadata} <- [
          {"trace-free", []},
          {"traced", [otel_trace_id: trace, otel_span_id: span]}
        ] do
      marker = "#{label}-#{context}-#{System.unique_integer([:positive])}"
      # Leave the normal Logger translator installed; an unknown report label
      # can suppress the report before it reaches this handler.
      Logger.info(%{payload: payload}, [{:event_name, marker} | metadata])
      {record, _all_received} = receive_records_until(&(&1.event_name == marker))

      assert {:kvlist_value, %KeyValueList{values: [value]}} = record.body.value
      assert value.key == "payload"
      assert value.value.value == expected

      if context == "traced" do
        assert record.trace_id == :binary.copy(<<0xAA>>, 16)
        assert record.span_id == :binary.copy(<<0xBB>>, 8)
      else
        assert record.trace_id == ""
        assert record.span_id == ""
      end

      later = "after-#{marker}"
      Logger.info(later)

      {ordinary, _all_received} =
        receive_records_until(&(&1.body.value == {:string_value, later}))

      assert ordinary.body.value == {:string_value, later}
      assert_same_components(handler_id, components)
      refute_receive {:handler_failure, _, _}, 0
    end
  end

  test "one malformed preparation event is omitted from all batches and later Logger delivery continues",
       %{
         bypass: bypass,
         handler_id: handler_id
       } do
    observe_batches(bypass)
    attach_failure_observer()
    components = handler_components(handler_id)
    id = System.unique_integer([:positive])
    bad = "malformed-#{id}"
    before_marker = "before-malformed-#{id}"
    after_marker = "after-malformed-#{id}"

    Logger.info(before_marker)

    {_record, before_records} =
      receive_records_until(&(&1.body.value == {:string_value, before_marker}), bad)

    Logger.info(bad, otel_trace_id: "not-hex")

    assert_receive {:handler_failure, %{count: 1}, metadata}, 1_000

    assert metadata == %{
             stage: :prepare,
             failure_source: :trace_context,
             exception: :argument_error,
             message_shape: :string,
             trace_context: :invalid,
             olp_alive: true
           }

    Logger.info(after_marker)

    {ordinary, received} =
      receive_records_until(&(&1.body.value == {:string_value, after_marker}), bad)

    # Check all received records, including unmatched batches and the complete
    # matched batch. A single-request fixture plus same-producer OLP ordering
    # means an erroneously loaded bad event cannot arrive behind this marker.
    assert Enum.all?(before_records ++ received, &(&1.body.value != {:string_value, bad}))
    assert ordinary.body.value == {:string_value, after_marker}
    assert_same_components(handler_id, components)
    refute_receive {:handler_failure, _, _}, 0
  end

  defp observe_batches(bypass) do
    parent = self()

    Bypass.expect(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      request = decode_request_body(body)

      records =
        for resource <- request.resource_logs,
            scope <- resource.scope_logs,
            record <- scope.log_records,
            do: record

      send(parent, {:regression_batch, records})
      Plug.Conn.resp(conn, 200, "")
    end)
  end

  defp receive_records_until(predicate, forbidden \\ nil, received \\ []) do
    assert_receive {:regression_batch, records}, 1_000

    if forbidden do
      for record <- records do
        refute record.body.value == {:string_value, forbidden}
      end
    end

    received = received ++ records

    case Enum.find(records, predicate) do
      nil -> receive_records_until(predicate, forbidden, received)
      record -> {record, received}
    end
  end

  defp handler_components(id) do
    supervisor = Process.whereis(:"#{LogHandler}_#{id}")
    olp = Process.whereis(:"#{LogHandler}_#{id}_logger_olp")
    assert is_pid(supervisor) and is_pid(olp)
    {supervisor, olp}
  end

  defp assert_same_components(id, {supervisor, olp}) do
    assert id in :logger.get_handler_ids()
    assert Process.whereis(:"#{LogHandler}_#{id}") == supervisor
    assert Process.whereis(:"#{LogHandler}_#{id}_logger_olp") == olp
    assert Process.alive?(supervisor) and Process.alive?(olp)
  end

  defp attach_failure_observer do
    id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        id,
        [:otel_metric_exporter, :log_handler, :exception],
        &__MODULE__.handle_failure/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  def handle_failure(_event, measurements, metadata, parent),
    do: send(parent, {:handler_failure, measurements, metadata})

  test "captures Logger.error message with correct severity", %{bypass: bypass} do
    parent = self()

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        decode_request_body(body)

      send(parent, {:logs, logs})
      Plug.Conn.resp(conn, 200, "")
    end)

    Logger.error("hello error")

    assert_receive {:logs, logs}, 500

    assert [
             %LogRecord{
               body: %{value: {:string_value, "hello error"}},
               severity_text: "error",
               severity_number: :SEVERITY_NUMBER_ERROR
             }
           ] = logs
  end

  test "maps metadata fields correctly", %{bypass: bypass} do
    parent = self()
    request_id = "req-12345"

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        decode_request_body(body)

      send(parent, {:logs, logs})
      Plug.Conn.resp(conn, 200, "")
    end)

    Logger.metadata(request_id: request_id)
    Logger.info("metadata log")

    assert_receive {:logs, logs}, 500

    assert [
             %LogRecord{
               body: %{value: {:string_value, "metadata log"}},
               attributes: attributes
             }
           ] = logs

    assert Enum.any?(attributes, fn attr ->
             attr.key == "http.request.id" &&
               attr.value == %AnyValue{value: {:string_value, request_id}}
           end)
  end

  test "captures logs from an EXIT", %{bypass: bypass} do
    parent = self()
    Process.flag(:trap_exit, true)

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        decode_request_body(body)

      send(parent, {:logs, logs})
      Plug.Conn.resp(conn, 200, "")
    end)

    {:ok, pid} = Task.start_link(fn -> exit(:some_exit_reason) end)
    assert_receive {:EXIT, ^pid, :some_exit_reason}, 500
    assert_receive {:logs, logs}, 500

    assert [
             %LogRecord{
               severity_text: "error",
               body: %{value: {:string_value, error_message}},
               attributes: attributes
             }
           ] = logs

    assert error_message =~ ~r/Task #PID<[\d\.]+> started from #PID<[\d\.]+> terminating/

    attributes =
      Map.new(attributes, fn %{key: key, value: %{value: {:string_value, value}}} ->
        {key, value}
      end)

    assert attributes["exception.message"] == ":some_exit_reason"
    assert attributes["exception.type"] == "EXIT: :some_exit_reason"

    assert attributes["exception.stacktrace"] =~
             ~r|test/otel_metric_exporter/log_handler_integration_test.exs:\d+|
  end

  defmodule TestGenserver do
    use GenServer

    def start_link(parent), do: GenServer.start_link(__MODULE__, parent)
    def init(parent), do: {:ok, parent}

    def call_with_timeout(pid, timeout),
      do: GenServer.call(pid, {:call_with_timeout, timeout}, timeout)

    def perform(pid, func), do: GenServer.call(pid, {:perform, func})

    def handle_call({:call_with_timeout, timeout}, _from, parent) do
      Process.sleep(timeout)
      {:reply, :ok, parent}
    end

    def handle_call({:perform, func}, _from, parent) do
      {:reply, func.(), parent}
    end
  end

  test "captures logs from an EXIT in a genserver", %{bypass: bypass} do
    parent = self()
    Process.flag(:trap_exit, true)

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        decode_request_body(body)

      send(parent, {:logs, logs})
      Plug.Conn.resp(conn, 200, "")
    end)

    {:ok, pid1} = TestGenserver.start_link(parent)
    {:ok, pid2} = TestGenserver.start_link(parent)

    try do
      TestGenserver.perform(pid1, fn -> TestGenserver.call_with_timeout(pid2, 100) end)
    catch
      :exit, _ -> :ok
    end

    assert_receive {:EXIT, ^pid1, {:timeout, _}}, 500
    assert_receive {:logs, logs}, 500

    assert [
             %LogRecord{
               severity_text: "error",
               body: %{value: {:string_value, error_message}},
               attributes: attributes
             }
           ] = logs

    assert error_message =~ ~r/GenServer #PID<[\d\.]+> terminating/

    attributes =
      Map.new(attributes, fn %{key: key, value: %{value: {:string_value, value}}} ->
        {key, value}
      end)

    assert attributes["exception.message"] =~
             ~r|{:timeout, {GenServer, :call, \[#PID<[\d\.]+>, {:call_with_timeout, 100}, 100\]}}|

    assert attributes["exception.type"] == "EXIT: time out"

    assert attributes["exception.stacktrace"] =~
             ~r|test/otel_metric_exporter/log_handler_integration_test.exs:\d+|
  end

  test "captures logs from an uncaught raise", %{bypass: bypass} do
    parent = self()
    Process.flag(:trap_exit, true)

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        decode_request_body(body)

      send(parent, {:logs, logs})
      Plug.Conn.resp(conn, 200, "")
    end)

    {:ok, pid} = Task.start_link(fn -> raise RuntimeError, "test error" end)
    assert_receive {:EXIT, ^pid, {%RuntimeError{}, _}}, 500
    assert_receive {:logs, logs}, 500

    assert [
             %LogRecord{
               severity_text: "error",
               body: %{value: {:string_value, error_message}},
               attributes: attributes
             }
           ] = logs

    assert error_message =~ ~r/Task #PID<[\d\.]+> started from #PID<[\d\.]+> terminating/

    attributes =
      Map.new(attributes, fn %{key: key, value: %{value: {:string_value, value}}} ->
        {key, value}
      end)

    assert attributes["exception.message"] == "test error"
    assert attributes["exception.type"] == "Elixir.RuntimeError"

    assert attributes["exception.stacktrace"] =~
             ~r|test/otel_metric_exporter/log_handler_integration_test.exs:\d+|
  end

  test "captures logs from an uncaught throw", %{bypass: bypass} do
    parent = self()
    Process.flag(:trap_exit, true)

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        decode_request_body(body)

      send(parent, {:logs, logs})
      Plug.Conn.resp(conn, 200, "")
    end)

    {:ok, pid} = Task.start_link(fn -> throw(:test_error) end)
    assert_receive {:EXIT, ^pid, {{:nocatch, :test_error}, _}}, 500
    assert_receive {:logs, logs}, 500

    assert [
             %LogRecord{
               severity_text: "error",
               body: %{value: {:string_value, error_message}},
               attributes: attributes
             }
           ] = logs

    assert error_message =~ ~r/Task #PID<[\d\.]+> started from #PID<[\d\.]+> terminating/

    attributes =
      Map.new(attributes, fn %{key: key, value: %{value: {:string_value, value}}} ->
        {key, value}
      end)

    assert attributes["exception.message"] == "{:nocatch, :test_error}"
    assert attributes["exception.type"] == "Uncaught throw"

    assert attributes["exception.stacktrace"] =~
             ~r|test/otel_metric_exporter/log_handler_integration_test.exs:\d+|
  end

  test "sends batch when max_buffer_size is reached", %{
    bypass: bypass,
    handler_id: handler_id,
    config: initial_config
  } do
    # Reconfigure handler for this specific test, merging with initial config
    new_specific_config = %{max_buffer_size: 3, debounce_ms: 400}
    merged_config = Map.merge(initial_config, new_specific_config)
    :ok = :logger.set_handler_config(handler_id, %{config: merged_config})

    parent = self()

    Bypass.expect(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        decode_request_body(body)

      send(parent, {:logs, logs})
      Plug.Conn.resp(conn, 200, "")
    end)

    Logger.info("batch-1")
    Logger.info("batch-2")
    # No request yet
    ref = Process.monitor(bypass.pid)
    refute Process.info(self(), :messages) |> elem(1) |> Enum.any?(&match?({:logs, _}, &1))
    Process.demonitor(ref, [:flush])

    # Third log triggers the send immediately, fourth is in a separate batch after a debouce
    Logger.info("batch-3")
    Logger.info("batch-4")

    assert_receive {:logs, logs}, 500
    # Should receive exactly 3 logs due to buffer size limit
    assert length(logs) == 3

    assert Enum.all?(
             logs,
             &match?(%LogRecord{body: %{value: {:string_value, "batch-" <> _}}}, &1)
           )

    assert_receive {:logs, [_]}, 800
  end

  test "sends batch after debounce_ms timeout", %{
    bypass: bypass,
    handler_id: handler_id,
    config: initial_config
  } do
    debounce_ms = 300
    # Reconfigure handler for this specific test, merging with initial config
    new_specific_config = %{max_buffer_size: 10, debounce_ms: debounce_ms}
    merged_config = Map.merge(initial_config, new_specific_config)
    :ok = :logger.set_handler_config(handler_id, %{config: merged_config})

    parent = self()

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        decode_request_body(body)

      send(parent, {:logs, logs})
      Plug.Conn.resp(conn, 200, "")
    end)

    Logger.info("debounce log")

    # No request immediately
    ref = Process.monitor(bypass.pid)
    refute Process.info(self(), :messages) |> elem(1) |> Enum.any?(&match?({:logs, _}, &1))
    Process.demonitor(ref, [:flush])

    # Wait for debounce period + some buffer
    Process.sleep(debounce_ms + 50)

    assert_receive {:logs, logs}, 500
    # Should receive the single log after debounce
    assert [%LogRecord{body: %{value: {:string_value, "debounce log"}}}] = logs
  end

  @tag :task_lifecycle
  test "continues exporting after a task result arrives before its DOWN", %{
    bypass: bypass,
    handler_id: handler_id,
    config: initial_config
  } do
    config =
      Map.merge(initial_config, %{
        otlp_concurrent_requests: 1,
        debounce_ms: 10,
        max_buffer_size: 1
      })

    supervisor_pid = Process.whereis(:"#{LogHandler}_#{handler_id}")
    supervisor_ref = Process.monitor(supervisor_pid)
    :ok = :logger.remove_handler(handler_id)
    assert_receive {:DOWN, ^supervisor_ref, :process, ^supervisor_pid, _reason}, 1_000
    :ok = :logger.add_handler(handler_id, LogHandler, %{config: config})
    parent = self()

    Bypass.expect(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        decode_request_body(body)

      callback_pid = self()
      send(parent, {:request, logs, callback_pid})

      first_batch? =
        Enum.any?(logs, &match?(%LogRecord{body: %{value: {:string_value, "first batch"}}}, &1))

      second_batch? =
        Enum.any?(logs, &match?(%LogRecord{body: %{value: {:string_value, "second batch"}}}, &1))

      cond do
        first_batch? ->
          receive do
            :release_first_request -> :ok
          end

        second_batch? ->
          send(parent, :second_request_completed)

        true ->
          send(parent, :unexpected_request)
      end

      Plug.Conn.resp(conn, 200, "")
    end)

    Logger.info("first batch")

    assert_receive {:request, [%LogRecord{body: %{value: {:string_value, "first batch"}}}],
                    callback_pid},
                   500

    Logger.info("second batch")
    send(callback_pid, :release_first_request)

    assert_receive {:request, [%LogRecord{body: %{value: {:string_value, "second batch"}}}], _},
                   500

    assert_receive :second_request_completed, 500
    refute_receive :unexpected_request, 100
  end

  require OpenTelemetry.Tracer, as: Tracer

  test ":opentelemetry trace/span is captured correctly", %{bypass: bypass} do
    parent = self()

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        decode_request_body(body)

      send(parent, {:logs, logs})
      Plug.Conn.resp(conn, 200, "")
    end)

    span =
      Tracer.with_span "test-span" do
        Logger.info("test-log")
        Tracer.current_span_ctx()
      end

    trace_id = :otel_span.hex_trace_id(span) |> Base.decode16!(case: :mixed)
    span_id = :otel_span.hex_span_id(span) |> Base.decode16!(case: :mixed)

    assert_receive {:logs, logs}, 500

    assert [
             %LogRecord{
               body: %{value: {:string_value, "test-log"}},
               trace_id: ^trace_id,
               span_id: ^span_id
             }
           ] = logs
  end
end
