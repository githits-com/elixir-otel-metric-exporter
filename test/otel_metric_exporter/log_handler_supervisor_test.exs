defmodule OtelMetricExporter.LogHandlerSupervisorTest do
  use ExUnit.Case

  alias OtelMetricExporter.Opentelemetry.Proto.Logs.V1.LogRecord
  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias OtelMetricExporter.LogAccumulator
  alias OtelMetricExporter.LogHandler
  alias OtelMetricExporter.LogHandlerSupervisor
  alias OtelMetricExporter.OtelApi
  alias OtelMetricExporter.OtelApi.Config

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

  test "stops transport children when the OLP child cannot start", %{config: config} do
    base_name = String.to_atom("olp_start_failure_#{System.unique_integer([:positive])}")
    olp_name = String.to_atom("#{base_name}_logger_olp")

    {:ok, existing_pid, _olp} =
      :logger_olp.start_link(olp_name, LogAccumulator, config, %{})

    Process.unlink(existing_pid)
    on_exit(fn -> :gen_server.stop(existing_pid) end)

    assert {:error, _reason} =
             LogHandlerSupervisor.start_link(
               name: base_name,
               accumulator_config: config,
               olp_config: %{}
             )

    assert Process.whereis(base_name) == nil
    assert Process.whereis(String.to_atom("#{base_name}_Finch")) == nil
    assert Process.whereis(String.to_atom("#{base_name}_TaskSupervisor")) == nil
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

  test "adding_handler propagates supervisor start errors", %{bypass: bypass} do
    id = String.to_atom("duplicate_handler_#{System.unique_integer([:positive])}")

    handler_config = %{
      module: LogHandler,
      id: id,
      config: %{otlp_endpoint: "http://localhost:#{bypass.port}"}
    }

    assert {:ok, installed} = LogHandler.adding_handler(handler_config)
    on_exit(fn -> LogHandler.removing_handler(installed) end)

    assert {:error, _reason} = LogHandler.adding_handler(handler_config)
  end

  test "changing_config updates normalized state from a partial public update", %{bypass: bypass} do
    id = String.to_atom("partial_update_#{System.unique_integer([:positive])}")

    handler_config = %{
      module: LogHandler,
      id: id,
      config: %{
        otlp_endpoint: "http://localhost:#{bypass.port}",
        otlp_headers: %{"authorization" => "placeholder-secret"},
        otlp_timeout: 1_000,
        otlp_concurrent_requests: 2,
        otlp_compression: nil,
        retry: false,
        resource: %{instance: %{id: "test"}},
        debounce_ms: 100,
        max_buffer_size: 20
      }
    }

    {:ok, installed} = LogHandler.adding_handler(handler_config)
    on_exit(fn -> LogHandler.removing_handler(installed) end)

    old_config = installed.config
    old_api_config = old_config.api.config
    old_olp_opts = :logger_olp.get_opts(old_config.olp)
    requested_count = old_olp_opts.burst_limit_max_count + 1

    assert {:ok, updated} =
             LogHandler.changing_config(:update, installed, %{
               config: %{max_buffer_size: 9, burst_limit_max_count: requested_count}
             })

    assert updated.config.max_buffer_size == 9
    assert updated.config.api.config.otlp_endpoint == old_api_config.otlp_endpoint
    assert updated.config.api.config.otlp_headers == old_api_config.otlp_headers
    assert updated.config.api.config.otlp_timeout == old_api_config.otlp_timeout

    assert updated.config.api.config.otlp_concurrent_requests ==
             old_api_config.otlp_concurrent_requests

    assert updated.config.api.config.resource == old_api_config.resource
    assert updated.config.api.config.otlp_compression == nil
    refute updated.config.api.retry

    assert :logger_olp.get_opts(updated.config.olp) ==
             Map.put(old_olp_opts, :burst_limit_max_count, requested_count)

    assert %{cb_state: %{max_buffer_size: 9}} = :logger_olp.info(updated.config.olp)

    assert %{
             otlp_endpoint: "http://localhost:" <> _,
             otlp_headers: %{"authorization" => "[REDACTED]"},
             otlp_timeout: 1_000,
             otlp_concurrent_requests: 2,
             retry: false,
             resource: %{"instance.id" => "test"},
             max_buffer_size: 9,
             burst_limit_max_count: ^requested_count
           } = LogHandler.filter_config(updated).config
  end

  test "Logger level updates strip stored runtime state before validation", %{bypass: bypass} do
    id = String.to_atom("level_update_#{System.unique_integer([:positive])}")
    Application.put_env(:otel_metric_exporter, :otlp_endpoint, "http://localhost:#{bypass.port}")

    on_exit(fn -> Application.delete_env(:otel_metric_exporter, :otlp_endpoint) end)

    assert :ok =
             :logger.add_handler(id, LogHandler, %{
               config: %{otlp_endpoint: "http://localhost:#{bypass.port}", debounce_ms: 100}
             })

    on_exit(fn -> :logger.remove_handler(id) end)

    assert :ok = :logger.update_handler_config(id, :level, :warning)
    assert {:ok, %{level: :warning}} = :logger.get_handler_config(id)

    assert :ok = :logger.set_handler_config(id, %{level: :error})
    assert {:ok, %{level: :error}} = :logger.get_handler_config(id)
  end

  test "rejects live timeout and concurrency changes for set and update", %{bypass: bypass} do
    id = String.to_atom("immutable_config_#{System.unique_integer([:positive])}")

    handler_config = %{
      module: LogHandler,
      id: id,
      config: %{
        otlp_endpoint: "http://localhost:#{bypass.port}",
        otlp_timeout: 1_000,
        otlp_concurrent_requests: 2,
        resource: %{instance: %{id: "test"}},
        debounce_ms: 100,
        max_buffer_size: 20
      }
    }

    {:ok, installed} = LogHandler.adding_handler(handler_config)
    on_exit(fn -> LogHandler.removing_handler(installed) end)

    old_olp_opts = :logger_olp.get_opts(installed.config.olp)
    old_olp_info = :logger_olp.info(installed.config.olp)

    for set_or_update <- [:set, :update], requested_timeout <- [2_000, 500] do
      config =
        if set_or_update == :set do
          %{
            otlp_endpoint: "http://localhost:#{bypass.port}",
            otlp_timeout: requested_timeout,
            otlp_concurrent_requests: 2
          }
        else
          %{otlp_timeout: requested_timeout}
        end

      assert {:error, {:unsupported_live_otlp_timeout_change, 1_000, ^requested_timeout}} =
               LogHandler.changing_config(set_or_update, installed, %{config: config})

      assert :logger_olp.get_opts(installed.config.olp) == old_olp_opts
      assert :logger_olp.info(installed.config.olp) == old_olp_info
    end

    for set_or_update <- [:set, :update], requested_concurrency <- [3, 1] do
      config =
        if set_or_update == :set do
          %{
            otlp_endpoint: "http://localhost:#{bypass.port}",
            otlp_concurrent_requests: requested_concurrency,
            otlp_timeout: 1_000
          }
        else
          %{otlp_concurrent_requests: requested_concurrency}
        end

      assert {:error,
              {:unsupported_live_otlp_concurrent_requests_change, 2, ^requested_concurrency}} =
               LogHandler.changing_config(set_or_update, installed, %{config: config})

      assert :logger_olp.get_opts(installed.config.olp) == old_olp_opts
      assert :logger_olp.info(installed.config.olp) == old_olp_info
    end
  end

  test "rejects live exporter changes without mutating the enabled handler", %{bypass: bypass} do
    id = String.to_atom("immutable_exporter_#{System.unique_integer([:positive])}")

    handler_config = %{
      module: LogHandler,
      id: id,
      config: %{otlp_endpoint: "http://localhost:#{bypass.port}", debounce_ms: 100}
    }

    {:ok, installed} = LogHandler.adding_handler(handler_config)
    on_exit(fn -> LogHandler.removing_handler(installed) end)

    old_olp_opts = :logger_olp.get_opts(installed.config.olp)
    old_olp_info = :logger_olp.info(installed.config.olp)
    Application.put_env(:otel_metric_exporter, :logs, exporter: :none)

    on_exit(fn -> Application.delete_env(:otel_metric_exporter, :logs) end)

    for set_or_update <- [:set, :update] do
      config =
        if set_or_update == :set do
          %{otlp_endpoint: "http://localhost:#{bypass.port}", logs: %{exporter: :none}}
        else
          %{logs: %{exporter: :none}}
        end

      assert {:error, {:unsupported_live_exporter_change, :otlp, :none}} =
               LogHandler.changing_config(set_or_update, installed, %{config: config})

      assert :logger_olp.get_opts(installed.config.olp) == old_olp_opts
      assert :logger_olp.info(installed.config.olp) == old_olp_info
    end

    assert {:error, {:unsupported_live_exporter_change, :otlp, :none}} =
             LogHandler.changing_config(:update, installed, %{
               config: %{logs: [exporter: :none]}
             })
  end

  test "rejects unsupported nested signal options instead of discarding them", %{bypass: bypass} do
    id = String.to_atom("nested_signal_#{System.unique_integer([:positive])}")

    assert {:error, %NimbleOptions.ValidationError{}} =
             LogHandler.adding_handler(%{
               module: LogHandler,
               id: id,
               config: %{
                 otlp_endpoint: "http://localhost:#{bypass.port}",
                 logs: %{otlp_endpoint: "http://localhost:9999/custom/logs"}
               }
             })

    assert {:ok, installed} =
             LogHandler.adding_handler(%{
               module: LogHandler,
               id: id,
               config: %{otlp_endpoint: "http://localhost:#{bypass.port}"}
             })

    on_exit(fn -> LogHandler.removing_handler(installed) end)

    assert {:error, {:unsupported_nested_signal_config, [:logs]}} =
             LogHandler.changing_config(:update, installed, %{
               config: %{logs: %{otlp_endpoint: "http://localhost:9999/custom/logs"}}
             })
  end

  test "ignores caller-supplied internal endpoint provenance", %{bypass: bypass} do
    id = String.to_atom("internal_provenance_#{System.unique_integer([:positive])}")

    assert {:ok, installed} =
             LogHandler.adding_handler(%{
               module: LogHandler,
               id: id,
               config: %{otlp_endpoint: "http://localhost:#{bypass.port}"}
             })

    on_exit(fn -> LogHandler.removing_handler(installed) end)

    assert {:ok, updated} =
             LogHandler.changing_config(:update, installed, %{
               config: %{otlp_endpoint_kind: :signal, max_buffer_size: 9}
             })

    assert updated.config.api.config.otlp_endpoint_kind == :generic
  end

  test "set resets unspecified OLP options to OTP defaults", %{bypass: bypass} do
    id = String.to_atom("olp_set_defaults_#{System.unique_integer([:positive])}")

    assert {:ok, installed} =
             LogHandler.adding_handler(%{
               module: LogHandler,
               id: id,
               config: %{
                 otlp_endpoint: "http://localhost:#{bypass.port}",
                 burst_limit_max_count: 99
               }
             })

    on_exit(fn -> LogHandler.removing_handler(installed) end)
    assert :logger_olp.get_opts(installed.config.olp).burst_limit_max_count == 99

    assert {:ok, updated} =
             LogHandler.changing_config(:set, installed, %{
               config: %{otlp_endpoint: "http://localhost:#{bypass.port}"}
             })

    assert :logger_olp.get_opts(updated.config.olp).burst_limit_max_count ==
             :logger_olp.get_default_opts().burst_limit_max_count
  end

  test "disabled handler has no transport processes and remains a no-op" do
    id = String.to_atom("disabled_handler_#{System.unique_integer([:positive])}")
    Application.put_env(:otel_metric_exporter, :logs, exporter: :none)

    on_exit(fn -> Application.delete_env(:otel_metric_exporter, :logs) end)

    handler_config = %{
      module: LogHandler,
      id: id,
      config: %{metadata: [], drop_mode_qlen: 7}
    }

    assert {:ok, installed} = LogHandler.adding_handler(handler_config)

    supervisor_name = String.to_atom("Elixir.OtelMetricExporter.LogHandler_#{id}")
    assert installed.config.api.config.exporter == :none
    assert installed.config.drop_mode_qlen == 7
    assert Process.whereis(supervisor_name) == nil
    assert Process.whereis(String.to_atom("#{supervisor_name}_Finch")) == nil
    assert Process.whereis(String.to_atom("#{supervisor_name}_TaskSupervisor")) == nil
    assert Process.whereis(String.to_atom("#{supervisor_name}_logger_olp")) == nil

    event = %{level: :info, msg: {:string, "disabled"}, meta: %{time: 0}}
    assert :ok = LogHandler.log(event, installed)
    assert :ok = LogHandler.removing_handler(installed)
  end

  test "disabled handler rejects enabling through update without starting transport" do
    id = String.to_atom("disabled_update_#{System.unique_integer([:positive])}")
    Application.put_env(:otel_metric_exporter, :logs, exporter: :none)

    on_exit(fn -> Application.delete_env(:otel_metric_exporter, :logs) end)

    handler_config = %{module: LogHandler, id: id, config: %{metadata: []}}
    assert {:ok, installed} = LogHandler.adding_handler(handler_config)

    Application.put_env(:otel_metric_exporter, :logs, exporter: :otlp)

    assert {:error, {:unsupported_live_exporter_change, :none, :otlp}} =
             LogHandler.changing_config(:update, installed, %{
               config: %{
                 logs: %{exporter: :otlp},
                 otlp_endpoint: "http://localhost:4318"
               }
             })

    assert :ok = LogHandler.removing_handler(installed)
  end

  test "filter_config removes runtime state and redacts headers", %{bypass: bypass} do
    api = %OtelApi{
      finch: :placeholder_finch,
      retry: true,
      scope: :logs,
      config: %Config{
        otlp_endpoint: "http://placeholder-secret@localhost:#{bypass.port}",
        otlp_endpoint_kind: :generic,
        otlp_protocol: :http_protobuf,
        otlp_headers: %{"authorization" => "placeholder-secret"},
        otlp_timeout: 1_000,
        exporter: :otlp,
        resource: %{"service.name" => "placeholder"},
        otlp_compression: :gzip,
        otlp_concurrent_requests: 2
      }
    }

    handler = %{
      module: LogHandler,
      id: :filter_config,
      config: %{
        api: api,
        task_supervisor: :placeholder_task_supervisor,
        olp: :placeholder_olp,
        metadata: [],
        max_buffer_size: 5
      }
    }

    filtered = LogHandler.filter_config(handler)
    refute Map.has_key?(filtered.config, :api)
    refute Map.has_key?(filtered.config, :task_supervisor)
    refute Map.has_key?(filtered.config, :olp)
    assert filtered.config.otlp_headers == %{"authorization" => "[REDACTED]"}
    refute filtered.config.otlp_endpoint =~ "placeholder-secret"
    assert filtered.config.otlp_endpoint =~ "REDACTED"
    assert handler.config.api.config.otlp_headers == %{"authorization" => "placeholder-secret"}
  end

  test "preserves signal-specific endpoint provenance during updates", %{bypass: bypass} do
    id = String.to_atom("signal_endpoint_#{System.unique_integer([:positive])}")
    endpoint = "http://localhost:#{bypass.port}/custom/logs"
    Application.put_env(:otel_metric_exporter, :logs, otlp_endpoint: endpoint)

    on_exit(fn -> Application.delete_env(:otel_metric_exporter, :logs) end)

    handler_config = %{module: LogHandler, id: id, config: %{debounce_ms: 100}}
    assert {:ok, installed} = LogHandler.adding_handler(handler_config)
    on_exit(fn -> LogHandler.removing_handler(installed) end)

    filtered = LogHandler.filter_config(installed).config
    assert filtered.logs.otlp_endpoint == endpoint
    refute Map.has_key?(filtered, :otlp_endpoint)

    assert {:ok, updated} =
             LogHandler.changing_config(:update, installed, %{config: %{max_buffer_size: 9}})

    assert updated.config.api.config.otlp_endpoint == endpoint
    assert updated.config.api.config.otlp_endpoint_kind == :signal
    assert LogHandler.filter_config(updated).config.logs.otlp_endpoint == endpoint
  end

  test "partial updates retain installed signal config when application defaults drift", %{
    bypass: bypass
  } do
    id = String.to_atom("signal_default_drift_#{System.unique_integer([:positive])}")
    old_endpoint = "http://localhost:#{bypass.port}/old/logs"
    Application.put_env(:otel_metric_exporter, :logs, otlp_endpoint: old_endpoint)

    on_exit(fn -> Application.delete_env(:otel_metric_exporter, :logs) end)

    handler_config = %{module: LogHandler, id: id, config: %{max_buffer_size: 20}}
    assert {:ok, installed} = LogHandler.adding_handler(handler_config)
    on_exit(fn -> LogHandler.removing_handler(installed) end)

    Application.put_env(:otel_metric_exporter, :logs,
      otlp_endpoint: "http://localhost:#{bypass.port}/new/logs",
      exporter: :none
    )

    assert {:ok, updated} =
             LogHandler.changing_config(:update, installed, %{config: %{max_buffer_size: 9}})

    assert updated.config.max_buffer_size == 9
    assert updated.config.api.config.exporter == :otlp
    assert updated.config.api.config.otlp_endpoint == old_endpoint
    assert updated.config.api.config.otlp_endpoint_kind == :signal
    assert LogHandler.filter_config(updated).config.logs.otlp_endpoint == old_endpoint
  end

  test "disabled handler validates overload option types without starting machinery" do
    id = String.to_atom("disabled_invalid_olp_type_#{System.unique_integer([:positive])}")
    Application.put_env(:otel_metric_exporter, :logs, exporter: :none)

    on_exit(fn -> Application.delete_env(:otel_metric_exporter, :logs) end)

    handler_config = %{
      module: LogHandler,
      id: id,
      config: %{burst_limit_enable: :invalid}
    }

    assert {:error, {:invalid_olp_config, %{burst_limit_enable: :invalid}}} =
             LogHandler.adding_handler(handler_config)

    refute_disabled_handler_processes(id)
  end

  test "disabled handler validates overload levels without starting machinery" do
    id = String.to_atom("disabled_invalid_olp_levels_#{System.unique_integer([:positive])}")
    Application.put_env(:otel_metric_exporter, :logs, exporter: :none)

    on_exit(fn -> Application.delete_env(:otel_metric_exporter, :logs) end)

    handler_config = %{module: LogHandler, id: id, config: %{drop_mode_qlen: 1}}

    assert {:error,
            {:invalid_olp_levels, %{sync_mode_qlen: 1, drop_mode_qlen: 1, flush_qlen: 1_000}}} =
             LogHandler.adding_handler(handler_config)

    refute_disabled_handler_processes(id)
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

  defp refute_disabled_handler_processes(id) do
    supervisor_name = String.to_atom("Elixir.OtelMetricExporter.LogHandler_#{id}")
    assert Process.whereis(supervisor_name) == nil
    assert Process.whereis(String.to_atom("#{supervisor_name}_Finch")) == nil
    assert Process.whereis(String.to_atom("#{supervisor_name}_TaskSupervisor")) == nil
    assert Process.whereis(String.to_atom("#{supervisor_name}_logger_olp")) == nil
  end
end
