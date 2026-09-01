defmodule OtelMetricExporter.LogHandlerSupervisorTest do
  use ExUnit.Case

  alias OtelMetricExporter.Opentelemetry.Proto.Logs.V1.LogRecord
  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias OtelMetricExporter.LogAccumulator
  alias OtelMetricExporter.LogHandler
  alias OtelMetricExporter.LogHandlerSupervisor

  setup do
    bypass = Bypass.open()

    {:ok, config} =
      LogAccumulator.check_config(
        %{
          otlp_endpoint: "http://localhost:#{bypass.port}",
          resource: %{instance: %{id: "test"}},
          debounce_ms: 100,
          max_buffer_size: 20
        },
        :test_log_handler_supervisor
      )

    {:ok, %{bypass: bypass, config: config}}
  end

  test "starts the supervisor and the accumulator with overload protection", %{config: config} do
    {:ok, supervisor_pid, olp} =
      LogHandlerSupervisor.start_link(
        name: :test_log_handler_supervisor,
        accumulator_config: config,
        olp_config: %{}
      )

    Process.unlink(supervisor_pid)

    on_exit(fn ->
      assert :ok =
               Supervisor.stop(
                 supervisor_pid,
                 :normal,
                 LogHandlerSupervisor.shutdown_timeout(config) + 1_000
               )
    end)

    assert Process.alive?(supervisor_pid)
    assert :logger_olp.get_pid(olp) |> Process.alive?()
  end

  test "shutdown with an empty queue sends no request", %{bypass: bypass, config: config} do
    parent = self()

    Bypass.stub(bypass, "POST", "/v1/logs", fn conn ->
      send(parent, :unexpected_request)
      Plug.Conn.resp(conn, 200, "")
    end)

    {:ok, supervisor_pid, _olp} =
      LogHandlerSupervisor.start_link(
        name: :test_log_handler_supervisor,
        accumulator_config: config,
        olp_config: %{}
      )

    assert :ok =
             Supervisor.stop(
               supervisor_pid,
               :normal,
               LogHandlerSupervisor.shutdown_timeout(config) + 1_000
             )

    refute_receive :unexpected_request, 100
  end

  test "direct OLP shutdown flushes queued logs while dependencies are alive", %{
    bypass: bypass,
    config: initial_config
  } do
    config = %{initial_config | debounce_ms: 30_000, max_buffer_size: 20}

    {:ok, supervisor_pid, olp} =
      LogHandlerSupervisor.start_link(
        name: :test_log_handler_supervisor,
        accumulator_config: config,
        olp_config: %{}
      )

    Process.unlink(supervisor_pid)
    supervisor_ref = Process.monitor(supervisor_pid)

    parent = self()
    finch_pid = Process.whereis(config.api.finch)
    task_supervisor_pid = Process.whereis(config.task_supervisor)

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      send(parent, {
        :shutdown_request,
        Process.alive?(finch_pid),
        Process.alive?(task_supervisor_pid)
      })

      Plug.Conn.resp(conn, 200, "")
    end)

    :logger_olp.load(olp, log_event(config, "queued shutdown log"))

    assert :ok =
             :gen_server.stop(
               :logger_olp.get_pid(olp),
               :normal,
               LogHandlerSupervisor.shutdown_timeout(config)
             )

    assert_supervisor_stopped(supervisor_ref, supervisor_pid)
    assert_receive {:shutdown_request, true, true}, 5_000
    refute_receive {:shutdown_request, _, _}, 100
  end

  test "direct OLP shutdown waits for a controlled pending request", %{
    bypass: bypass,
    config: initial_config
  } do
    config =
      initial_config
      |> put_timeout(3_000)
      |> Map.merge(%{debounce_ms: 30_000, max_buffer_size: 1})

    {:ok, supervisor_pid, olp} =
      LogHandlerSupervisor.start_link(
        name: :test_log_handler_supervisor,
        accumulator_config: config,
        olp_config: %{}
      )

    Process.unlink(supervisor_pid)
    supervisor_ref = Process.monitor(supervisor_pid)

    parent = self()

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      send(parent, {:pending_request, self()})

      receive do
        :release -> Plug.Conn.resp(conn, 200, "")
      end
    end)

    :logger_olp.load(olp, log_event(config, "pending shutdown log"))
    assert_receive {:pending_request, request_pid}, 5_000
    on_exit(fn -> send(request_pid, :release) end)

    stop_task =
      Task.async(fn ->
        :gen_server.stop(
          :logger_olp.get_pid(olp),
          :normal,
          LogHandlerSupervisor.shutdown_timeout(config)
        )
      end)

    refute Task.yield(stop_task, 50)
    send(request_pid, :release)
    assert {:ok, :ok} = Task.yield(stop_task, 1_000)
    assert_supervisor_stopped(supervisor_ref, supervisor_pid)
    Bypass.pass(bypass)
  end

  test "shutdown deadline kills pending work without dispatching queued logs", %{
    bypass: bypass,
    config: initial_config
  } do
    config =
      initial_config
      |> put_timeout(5_000)
      |> put_concurrency(1)
      |> Map.merge(%{debounce_ms: 30_000, max_buffer_size: 1})

    {:ok, supervisor_pid, olp} =
      LogHandlerSupervisor.start_link(
        name: :test_log_handler_supervisor,
        accumulator_config: config,
        olp_config: %{}
      )

    Process.unlink(supervisor_pid)
    supervisor_ref = Process.monitor(supervisor_pid)

    parent = self()

    Bypass.expect(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        body |> :zlib.gunzip() |> Protobuf.decode(ExportLogsServiceRequest)

      send(parent, {:pending_request, logs, self()})

      receive do
        :release -> Plug.Conn.resp(conn, 200, "")
      end
    end)

    :logger_olp.load(olp, log_event(config, "first shutdown log"))
    assert_receive {:pending_request, [_], request_pid}, 5_000
    on_exit(fn -> send(request_pid, :release) end)

    queued_config = put_concurrency(config, 2)

    :ok =
      :logger_olp.call(olp, {:config_changed, %{api: queued_config.api, max_buffer_size: 20}})

    :logger_olp.load(olp, log_event(config, "queued after pending log"))

    # This test-only state update bypasses public timeout immutability to accelerate the
    # shutdown deadline; the pending task keeps its original 5-second deadline.
    # The statically reachable production path does request building, encoding, and
    # compression before the first transport deadline check; that CPU work can consume the
    # shared static deadline, so the kill path remains real.
    shutdown_config =
      config
      |> put_timeout(300)
      |> put_concurrency(1)

    :ok = :logger_olp.call(olp, {:config_changed, %{api: shutdown_config.api}})

    stop_task =
      Task.async(fn ->
        :gen_server.stop(
          :logger_olp.get_pid(olp),
          :normal,
          LogHandlerSupervisor.shutdown_timeout(shutdown_config)
        )
      end)

    assert {:ok, :ok} = Task.yield(stop_task, 1_000)
    assert_supervisor_stopped(supervisor_ref, supervisor_pid)
    refute_receive {:pending_request, _, _}, 200
    send(request_pid, :release)
    Bypass.pass(bypass)
    refute_receive {:pending_request, _, _}, 200
  end

  test "supervisor shutdown flushes queued logs before Finch stops", %{
    bypass: bypass,
    config: initial_config
  } do
    config =
      initial_config
      |> put_timeout(3_000)
      |> Map.merge(%{debounce_ms: 30_000, max_buffer_size: 20})

    {:ok, supervisor_pid, olp} =
      LogHandlerSupervisor.start_link(
        name: :test_log_handler_supervisor,
        accumulator_config: config,
        olp_config: %{}
      )

    parent = self()
    finch_pid = Process.whereis(config.api.finch)
    task_supervisor_pid = Process.whereis(config.task_supervisor)

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      send(parent, {
        :supervisor_shutdown_request,
        Process.alive?(finch_pid),
        Process.alive?(task_supervisor_pid)
      })

      Plug.Conn.resp(conn, 200, "")
    end)

    :logger_olp.load(olp, log_event(config, "supervisor shutdown log"))

    assert :ok =
             Supervisor.stop(
               supervisor_pid,
               :normal,
               LogHandlerSupervisor.shutdown_timeout(config) + 1_000
             )

    assert_receive {:supervisor_shutdown_request, finch_alive, task_supervisor_alive}, 5_000
    assert finch_alive
    assert task_supervisor_alive
  end

  test "shutdown allowances derive from the configured timeout", %{config: config} do
    config = put_timeout(config, 321)
    args = [name: :test_log_handler_supervisor, accumulator_config: config, olp_config: %{}]

    child_spec = LogHandlerSupervisor.child_spec(args)

    assert LogHandlerSupervisor.shutdown_timeout(config) > config.api.config.otlp_timeout
    assert child_spec.shutdown > LogHandlerSupervisor.shutdown_timeout(config)
    assert child_spec.restart == :temporary
    assert child_spec.type == :supervisor
    assert child_spec.start == {LogHandlerSupervisor, :start_link, [args]}
  end

  test "changing_config rejects live OTLP timeout changes without mutating state", %{
    config: initial_config
  } do
    id = String.to_atom("changing_config_#{System.unique_integer([:positive])}")
    supervisor_name = String.to_atom("Elixir.OtelMetricExporter.LogHandler_#{id}")
    config = put_timeout(initial_config, 1_000)

    {:ok, supervisor_pid, olp} =
      LogHandlerSupervisor.start_link(
        name: supervisor_name,
        accumulator_config: config,
        olp_config: %{}
      )

    Process.unlink(supervisor_pid)

    old_handler_config = Map.put(config, :olp, olp)
    old_config = %{id: id, module: LogHandler, config: old_handler_config}
    old_olp_opts = :logger_olp.get_opts(olp)

    for requested_timeout <- [2_000, 500] do
      proposed_config = %{
        otlp_endpoint: config.api.config.otlp_endpoint,
        otlp_timeout: requested_timeout,
        resource: config.api.config.resource
      }

      assert {:error, {:unsupported_live_otlp_timeout_change, 1_000, ^requested_timeout}} =
               LogHandler.changing_config(:set, old_config, %{config: proposed_config})

      assert old_olp_opts == :logger_olp.get_opts(olp)

      assert %{cb_state: %{api: %{config: %{otlp_timeout: 1_000}}}} =
               :logger_olp.info(olp)
    end

    assert :ok = Supervisor.stop(supervisor_pid, :normal, 5_000)
  end

  test "removing_handler flushes queued logs under its configured allowance", %{
    bypass: bypass,
    config: initial_config
  } do
    id = String.to_atom("removing_handler_#{System.unique_integer([:positive])}")
    supervisor_name = String.to_atom("Elixir.OtelMetricExporter.LogHandler_#{id}")

    config =
      initial_config
      |> put_timeout(3_000)
      |> Map.merge(%{debounce_ms: 30_000, max_buffer_size: 20})

    {:ok, supervisor_pid, olp} =
      LogHandlerSupervisor.start_link(
        name: supervisor_name,
        accumulator_config: config,
        olp_config: %{}
      )

    Process.unlink(supervisor_pid)
    supervisor_ref = Process.monitor(supervisor_pid)
    parent = self()

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        body |> :zlib.gunzip() |> Protobuf.decode(ExportLogsServiceRequest)

      send(parent, {:removing_handler_request, logs})
      Plug.Conn.resp(conn, 200, "")
    end)

    :logger_olp.load(olp, log_event(config, "removing handler log"))

    handler_config = %{
      module: LogHandler,
      id: id,
      config: Map.put(config, :olp, olp)
    }

    assert :ok = LogHandler.removing_handler(handler_config)
    assert_supervisor_stopped(supervisor_ref, supervisor_pid)
    assert_receive {:removing_handler_request, [%LogRecord{}]}, 5_000
  end

  test "sends logs after debounce period has passed", %{bypass: bypass, config: config} do
    {:ok, _, olp} =
      LogHandlerSupervisor.start_link(
        name: :test_log_handler_supervisor,
        accumulator_config: config,
        olp_config: %{}
      )

    parent = self()

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      assert {"content-type", "application/x-protobuf"} in conn.req_headers
      assert {"accept", "application/x-protobuf"} in conn.req_headers
      assert {"content-encoding", "gzip"} in conn.req_headers
      assert body != ""

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        body |> :zlib.gunzip() |> Protobuf.decode(ExportLogsServiceRequest)

      assert length(logs) == 5

      assert Enum.all?(
               logs,
               &match?(%LogRecord{body: %{value: {:string_value, "test log" <> _}}}, &1)
             )

      send(parent, :done)

      Plug.Conn.resp(conn, 200, "")
    end)

    for i <- 1..5 do
      :logger_olp.load(
        olp,
        LogAccumulator.prepare_log_event(
          %{
            level: :info,
            msg: {:string, "test log #{i}"},
            meta: %{time: System.system_time(:millisecond)}
          },
          config
        )
      )
    end

    assert_receive :done, 5_000
    Process.sleep(100)
  end

  test "sends logs immediately once the limit is hit", %{bypass: bypass, config: config} do
    {:ok, _, olp} =
      LogHandlerSupervisor.start_link(
        name: :test_log_handler_supervisor,
        accumulator_config: %{config | max_buffer_size: 5, debounce_ms: 30_000},
        olp_config: %{}
      )

    parent = self()

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      assert {"content-type", "application/x-protobuf"} in conn.req_headers
      assert {"accept", "application/x-protobuf"} in conn.req_headers
      assert {"content-encoding", "gzip"} in conn.req_headers
      assert body != ""

      %ExportLogsServiceRequest{resource_logs: [%{scope_logs: [%{log_records: logs}]}]} =
        body |> :zlib.gunzip() |> Protobuf.decode(ExportLogsServiceRequest)

      assert length(logs) == 5

      assert Enum.all?(
               logs,
               &match?(%LogRecord{body: %{value: {:string_value, "test log" <> _}}}, &1)
             )

      send(parent, :done)

      Plug.Conn.resp(conn, 200, "")
    end)

    for i <- 1..5 do
      :logger_olp.load(
        olp,
        LogAccumulator.prepare_log_event(
          %{
            level: :info,
            msg: {:string, "test log #{i}"},
            meta: %{
              time: System.system_time(:millisecond),
              otel_trace_id: ~C"00000000000000000000000000000000"
            }
          },
          config
        )
      )
    end

    assert_receive :done, 5_000
    Process.sleep(100)
  end

  defp log_event(config, message) do
    LogAccumulator.prepare_log_event(
      %{
        level: :info,
        msg: {:string, message},
        meta: %{time: System.system_time(:millisecond)}
      },
      config
    )
  end

  defp put_timeout(config, timeout) do
    %{config | api: %{config.api | config: %{config.api.config | otlp_timeout: timeout}}}
  end

  defp put_concurrency(config, concurrency) do
    %{
      config
      | api: %{config.api | config: %{config.api.config | otlp_concurrent_requests: concurrency}}
    }
  end

  defp assert_supervisor_stopped(supervisor_ref, supervisor_pid) do
    assert_receive {:DOWN, ^supervisor_ref, :process, ^supervisor_pid, _reason}, 5_000
  end
end
