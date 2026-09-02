defmodule OtelMetricExporter.OtelApiTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Logs.V1.{
    ExportLogsPartialSuccess,
    ExportLogsServiceResponse
  }

  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.{
    ExportMetricsPartialSuccess,
    ExportMetricsServiceResponse
  }

  alias OtelMetricExporter.OtelApi
  alias OtelMetricExporter.OtelApi.Config
  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.Metric

  setup do
    on_exit(fn ->
      System.delete_env("OTEL_SERVICE_NAME")
      System.delete_env("OTEL_RESOURCE_ATTRIBUTES")
      Application.delete_env(:otel_metric_exporter, :logs)
      Application.delete_env(:otel_metric_exporter, :metrics)
    end)
  end

  describe "new/1" do
    test "creates a new OtelApi struct" do
      assert {:ok, %OtelApi{config: %Config{otlp_endpoint: "http://localhost:4317"}}, %{}} =
               OtelApi.new(%{finch: :test_finch, otlp_endpoint: "http://localhost:4317"}, :logs)
    end

    test "returns unrecognized options" do
      assert {:ok, %OtelApi{}, %{unknown_option: "value"}} =
               OtelApi.new(
                 %{
                   finch: :test_finch,
                   otlp_endpoint: "http://localhost:4317",
                   unknown_option: "value"
                 },
                 :logs
               )
    end

    test "normalizes the resource" do
      assert {:ok, %OtelApi{config: %Config{resource: %{"service.name" => "test"}}}, %{}} =
               OtelApi.new(
                 %{
                   finch: :test_finch,
                   otlp_endpoint: "http://localhost:4317",
                   resource: %{service: %{name: "test"}}
                 },
                 :logs
               )
    end

    test "puts service name from env" do
      System.put_env("OTEL_SERVICE_NAME", "test")

      assert {:ok, %OtelApi{config: %Config{resource: %{"service.name" => "test"}}}, %{}} =
               OtelApi.new(
                 %{
                   finch: :test_finch,
                   otlp_endpoint: "http://localhost:4317",
                   resource: %{}
                 },
                 :logs
               )
    end

    test "gives priority to provided config over env for service name" do
      System.put_env("OTEL_SERVICE_NAME", "test")

      assert {:ok, %OtelApi{config: %Config{resource: %{"service.name" => "test2"}}}, %{}} =
               OtelApi.new(
                 %{
                   finch: :test_finch,
                   otlp_endpoint: "http://localhost:4317",
                   resource: %{service: %{name: "test2"}}
                 },
                 :logs
               )
    end

    test "puts resource attributes from env" do
      System.put_env("OTEL_RESOURCE_ATTRIBUTES", "test=test2,test2=test3")

      assert {:ok, %OtelApi{config: %Config{resource: %{"test" => "test2", "test2" => "test3"}}},
              %{}} =
               OtelApi.new(%{finch: :test_finch, otlp_endpoint: "http://localhost:4317"}, :logs)
    end

    test "gives priority to provided config over env for resource attributes" do
      System.put_env("OTEL_RESOURCE_ATTRIBUTES", "test=test2")

      assert {:ok, %OtelApi{config: %Config{resource: %{"test" => "test3"}}}, %{}} =
               OtelApi.new(
                 %{
                   finch: :test_finch,
                   otlp_endpoint: "http://localhost:4317",
                   resource: %{test: "test3"}
                 },
                 :metrics
               )
    end

    @tag :otlp_validation_boundary
    test "rejects a Finch pid" do
      assert {:error, %{key: :finch}} =
               OtelApi.new(
                 %{finch: self(), otlp_endpoint: "http://localhost:4317"},
                 :logs
               )
    end
  end

  describe "OTLP endpoint paths" do
    test "appends the signal path to a generic endpoint" do
      bypass = Bypass.open()
      endpoint = "http://localhost:#{bypass.port}"
      {:ok, _} = start_supervised({Finch, name: EndpointFinch})

      Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
        Plug.Conn.resp(conn, 200, "")
      end)

      assert {:ok, api, %{}} =
               OtelApi.new(%{finch: EndpointFinch, otlp_endpoint: endpoint, retry: false}, :logs)

      assert :ok = OtelApi.send_log_events(api, [])
    end

    test "appends the signal path to a generic prefix without duplicating its separator" do
      bypass = Bypass.open()
      endpoint = "http://localhost:#{bypass.port}/collector/?tenant=pkgseer#fragment"
      {:ok, _} = start_supervised({Finch, name: PrefixEndpointFinch})

      Bypass.expect_once(bypass, "POST", "/collector/v1/logs", fn conn ->
        assert conn.query_string == "tenant=pkgseer"
        Plug.Conn.resp(conn, 200, "")
      end)

      assert {:ok, api, %{}} =
               OtelApi.new(
                 %{finch: PrefixEndpointFinch, otlp_endpoint: endpoint, retry: false},
                 :logs
               )

      assert :ok = OtelApi.send_log_events(api, [])
    end

    test "appends the metrics path to a generic endpoint" do
      bypass = Bypass.open()
      endpoint = "http://localhost:#{bypass.port}"
      {:ok, _} = start_supervised({Finch, name: MetricsEndpointFinch})

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 200, "")
      end)

      assert {:ok, api, %{}} =
               OtelApi.new(
                 %{finch: MetricsEndpointFinch, otlp_endpoint: endpoint, retry: false},
                 :metrics
               )

      assert :ok = OtelApi.send_metrics(api, [])
    end

    test "uses signal-specific logs and metrics endpoints unchanged" do
      bypass = Bypass.open()
      endpoint = "http://localhost:#{bypass.port}"
      {:ok, _} = start_supervised({Finch, name: SignalEndpointFinch})

      Application.put_env(:otel_metric_exporter, :logs, otlp_endpoint: endpoint <> "/custom/logs")

      Application.put_env(:otel_metric_exporter, :metrics,
        otlp_endpoint: endpoint <> "/custom/metrics"
      )

      Bypass.expect_once(bypass, "POST", "/custom/logs", fn conn ->
        Plug.Conn.resp(conn, 200, "")
      end)

      Bypass.expect_once(bypass, "POST", "/custom/metrics", fn conn ->
        Plug.Conn.resp(conn, 200, "")
      end)

      assert {:ok, logs_api, %{}} =
               OtelApi.new(%{finch: SignalEndpointFinch, retry: false}, :logs)

      assert {:ok, metrics_api, %{}} =
               OtelApi.new(%{finch: SignalEndpointFinch, retry: false}, :metrics)

      assert :ok = OtelApi.send_log_events(logs_api, [])
      assert :ok = OtelApi.send_metrics(metrics_api, [])
    end

    test "treats a direct endpoint override as generic" do
      bypass = Bypass.open()
      endpoint = "http://localhost:#{bypass.port}"
      {:ok, _} = start_supervised({Finch, name: OverrideEndpointFinch})

      Application.put_env(:otel_metric_exporter, :logs, otlp_endpoint: endpoint <> "/custom/logs")

      Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
        Plug.Conn.resp(conn, 200, "")
      end)

      assert {:ok, api, %{}} =
               OtelApi.new(
                 %{finch: OverrideEndpointFinch, otlp_endpoint: endpoint, retry: false},
                 :logs
               )

      assert :ok = OtelApi.send_log_events(api, [])
    end
  end

  @tag :otlp_attempt_timeout
  test "uses the configured timeout for a blocked receiver" do
    bypass = Bypass.open()
    endpoint = "http://localhost:#{bypass.port}"
    {:ok, _} = start_supervised({Finch, name: TestFinch, pools: %{endpoint => [size: 1]}})
    parent = self()

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      send(parent, {:request_received, self()})

      receive do
        :release -> Plug.Conn.resp(conn, 200, "")
      end
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{
                 finch: TestFinch,
                 otlp_endpoint: endpoint,
                 otlp_timeout: 3_000,
                 retry: false
               },
               :logs
             )

    task = Task.async(fn -> OtelApi.send_log_events(api, []) end)
    assert_receive {:request_received, request_pid}, 5_000
    on_exit(fn -> send(request_pid, :release) end)
    assert {:ok, {:error, :retryable, :deadline_exceeded}} = Task.yield(task, 5_000)

    Bypass.pass(bypass)
    send(request_pid, :release)
  end

  test "does not send log or metric requests for an expired deadline" do
    bypass = Bypass.open()
    endpoint = "http://localhost:#{bypass.port}"
    {:ok, _} = start_supervised({Finch, name: TestFinch, pools: %{endpoint => [size: 1]}})
    parent = self()

    Bypass.stub(bypass, "POST", "/v1/logs", fn conn ->
      send(parent, :unexpected_request)
      Plug.Conn.resp(conn, 200, "")
    end)

    Bypass.stub(bypass, "POST", "/v1/metrics", fn conn ->
      send(parent, :unexpected_request)
      Plug.Conn.resp(conn, 200, "")
    end)

    assert {:ok, logs_api, %{}} =
             OtelApi.new(
               %{finch: TestFinch, otlp_endpoint: endpoint, otlp_timeout: 1, retry: false},
               :logs
             )

    assert {:ok, metrics_api, %{}} =
             OtelApi.new(
               %{finch: TestFinch, otlp_endpoint: endpoint, retry: false},
               :metrics
             )

    deadline = OtelApi.new_deadline(logs_api)
    Process.sleep(10)

    assert {:error, :retryable, :deadline_exceeded} =
             OtelApi.send_log_events(logs_api, [], deadline)

    assert {:error, :retryable, :deadline_exceeded} =
             OtelApi.send_metrics(metrics_api, [], deadline)

    refute_receive :unexpected_request, 100
  end

  @tag :finch_worker_ownership
  test "finch request worker is owned by its export task" do
    bypass = Bypass.open()
    endpoint = "http://localhost:#{bypass.port}"
    {:ok, _} = start_supervised({Finch, name: OwnershipFinch, pools: %{endpoint => [size: 1]}})
    {:ok, _} = start_supervised({Task.Supervisor, name: OwnershipTaskSupervisor})
    parent = self()

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      send(parent, {:request_received, self()})

      receive do
        :release -> Plug.Conn.resp(conn, 200, "")
      end
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{
                 finch: OwnershipFinch,
                 otlp_endpoint: endpoint,
                 otlp_timeout: 10_000,
                 retry: false
               },
               :logs
             )

    task =
      Task.Supervisor.async_nolink(OwnershipTaskSupervisor, fn ->
        OtelApi.send_log_events(api, [])
      end)

    task_pid = task.pid
    task_ref = task.ref

    assert_receive {:request_received, request_pid}, 5_000

    {:monitors, monitors} = Process.info(task_pid, :monitors)

    worker_pid =
      Enum.find_value(monitors, fn
        {:process, pid} when is_pid(pid) -> pid
        _ -> nil
      end)

    assert is_pid(worker_pid)
    {:links, links} = Process.info(task_pid, :links)
    assert worker_pid in links

    worker_ref = Process.monitor(worker_pid)
    Process.exit(task_pid, :kill)

    assert_receive {:DOWN, ^worker_ref, :process, ^worker_pid, _reason}, 1_000
    refute Process.alive?(worker_pid)
    assert_receive {:DOWN, ^task_ref, :process, ^task_pid, :killed}, 1_000

    send(request_pid, :release)
    Bypass.pass(bypass)
  end

  test "unresolvable Finch name returns a bounded request error" do
    {:ok, _} = start_supervised({Task.Supervisor, name: WorkerFailureTaskSupervisor})

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{
                 finch: MissingFinch,
                 otlp_endpoint: "http://localhost:4317",
                 retry: false
               },
               :logs
             )

    task =
      Task.Supervisor.async_nolink(WorkerFailureTaskSupervisor, fn ->
        OtelApi.send_log_events(api, [])
      end)

    assert {:ok, {:error, :retryable, :request_failed}} = Task.yield(task, 1_000)
  end

  @tag :otlp_deadline_handoff
  test "bounds a blocked request by the remaining absolute deadline" do
    bypass = Bypass.open()
    endpoint = "http://localhost:#{bypass.port}"
    {:ok, _} = start_supervised({Finch, name: TestFinch, pools: %{endpoint => [size: 1]}})
    parent = self()

    Bypass.expect_once(bypass, "POST", "/warm", fn conn -> Plug.Conn.resp(conn, 200, "") end)

    warm_request = Finch.build(:post, endpoint <> "/warm", [], <<>>)

    assert {:ok, %{status: 200}} =
             Finch.request(
               warm_request,
               TestFinch,
               pool_timeout: 5_000,
               receive_timeout: 5_000,
               request_timeout: 5_000
             )

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      send(parent, {:request_received, self()})

      receive do
        :release -> Plug.Conn.resp(conn, 200, "")
      end
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{
                 finch: TestFinch,
                 otlp_endpoint: endpoint,
                 otlp_timeout: 2_500,
                 retry: false
               },
               :logs
             )

    deadline = OtelApi.new_deadline(api)
    Process.sleep(1_200)
    task = Task.async(fn -> OtelApi.send_log_events(api, [], deadline) end)

    assert_receive {:request_received, request_pid}, 1_000
    on_exit(fn -> send(request_pid, :release) end)
    assert {:ok, {:error, :retryable, :deadline_exceeded}} = Task.yield(task, 2_200)

    Bypass.pass(bypass)
    send(request_pid, :release)
  end

  test "starts the first attempt immediately" do
    bypass = Bypass.open()
    endpoint = "http://localhost:#{bypass.port}"
    {:ok, _} = start_supervised({Finch, name: TestFinch, pools: %{endpoint => [size: 1]}})
    parent = self()

    Bypass.expect_once(bypass, "POST", "/warm", fn conn -> Plug.Conn.resp(conn, 200, "") end)

    warm_request = Finch.build(:post, endpoint <> "/warm", [], <<>>)

    assert {:ok, %{status: 200}} =
             Finch.request(
               warm_request,
               TestFinch,
               pool_timeout: 5_000,
               receive_timeout: 5_000,
               request_timeout: 5_000
             )

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      send(parent, :request_started)
      Plug.Conn.resp(conn, 200, "")
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{
                 finch: TestFinch,
                 otlp_endpoint: endpoint,
                 otlp_timeout: 3_000
               },
               :logs
             )

    task = Task.async(fn -> OtelApi.send_log_events(api, []) end)
    assert_receive :request_started, 800
    assert {:ok, :ok} = Task.yield(task, 1_000)
  end

  test "retries a transient response and succeeds within the timeout" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})
    parent = self()
    {:ok, attempts} = Agent.start_link(fn -> 0 end)

    Bypass.expect(bypass, "POST", "/v1/logs", fn conn ->
      attempt = Agent.get_and_update(attempts, fn count -> {count + 1, count + 1} end)
      send(parent, {:request_attempt, attempt})

      case attempt do
        1 -> Plug.Conn.resp(conn, 503, "temporary")
        2 -> Plug.Conn.resp(conn, 200, "")
      end
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{
                 finch: TestFinch,
                 otlp_endpoint: "http://localhost:#{bypass.port}",
                 otlp_timeout: 3_000
               },
               :logs
             )

    task = Task.async(fn -> OtelApi.send_log_events(api, []) end)
    assert_receive {:request_attempt, 1}, 5_000
    assert_receive {:request_attempt, 2}, 2_000
    assert {:ok, :ok} = Task.yield(task, 1_000)
    assert Agent.get(attempts, & &1) == 2
  end

  for status <- [429, 502, 503, 504] do
    test "retries HTTP #{status} and succeeds within the timeout" do
      bypass = Bypass.open()
      {:ok, _} = start_supervised({Finch, name: TestFinch})
      {:ok, attempts} = Agent.start_link(fn -> 0 end)

      Bypass.expect(bypass, "POST", "/v1/logs", fn conn ->
        attempt = Agent.get_and_update(attempts, fn count -> {count + 1, count + 1} end)

        case attempt do
          1 -> Plug.Conn.resp(conn, unquote(status), "temporary")
          2 -> Plug.Conn.resp(conn, 200, "")
        end
      end)

      assert {:ok, api, %{}} =
               OtelApi.new(
                 %{
                   finch: TestFinch,
                   otlp_endpoint: "http://localhost:#{bypass.port}",
                   otlp_timeout: 3_000
                 },
                 :logs
               )

      assert :ok = OtelApi.send_log_events(api, [])
      assert Agent.get(attempts, & &1) == 2
    end
  end

  for status <- [408, 500] do
    test "does not retry HTTP #{status}" do
      bypass = Bypass.open()
      {:ok, _} = start_supervised({Finch, name: TestFinch})
      parent = self()

      Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
        send(parent, :request_finished)
        Plug.Conn.resp(conn, unquote(status), "permanent")
      end)

      assert {:ok, api, %{}} =
               OtelApi.new(
                 %{
                   finch: TestFinch,
                   otlp_endpoint: "http://localhost:#{bypass.port}",
                   otlp_timeout: 3_000
                 },
                 :logs
               )

      task = Task.async(fn -> OtelApi.send_log_events(api, []) end)
      assert_receive :request_finished, 5_000
      assert {:ok, {:error, :terminal, {:http_status, unquote(status)}}} = Task.yield(task, 1_000)
    end
  end

  test "returns log partial success without retrying" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

    response = %ExportLogsServiceResponse{
      partial_success: %ExportLogsPartialSuccess{
        rejected_log_records: 3,
        error_message: "receiver rejected records"
      }
    }

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      Plug.Conn.resp(conn, 200, IO.iodata_to_binary(Protobuf.encode_to_iodata(response)))
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{finch: TestFinch, otlp_endpoint: "http://localhost:#{bypass.port}"},
               :logs
             )

    assert {:partial_success, 3} = OtelApi.send_log_events(api, [])
  end

  test "returns metric partial success without retrying" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

    response = %ExportMetricsServiceResponse{
      partial_success: %ExportMetricsPartialSuccess{
        rejected_data_points: 2,
        error_message: "receiver rejected points"
      }
    }

    Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
      Plug.Conn.resp(conn, 200, IO.iodata_to_binary(Protobuf.encode_to_iodata(response)))
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{finch: TestFinch, otlp_endpoint: "http://localhost:#{bypass.port}"},
               :metrics
             )

    assert {:partial_success, 2} = OtelApi.send_metrics(api, [])
  end

  test "returns an explicit empty partial success without retrying" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

    response = %ExportLogsServiceResponse{
      partial_success: %ExportLogsPartialSuccess{}
    }

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      Plug.Conn.resp(conn, 200, IO.iodata_to_binary(Protobuf.encode_to_iodata(response)))
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{finch: TestFinch, otlp_endpoint: "http://localhost:#{bypass.port}"},
               :logs
             )

    assert {:partial_success, 0} = OtelApi.send_log_events(api, [])
  end

  test "rejects a negative log partial-success count without retrying" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

    response = %ExportLogsServiceResponse{
      partial_success: %ExportLogsPartialSuccess{rejected_log_records: -1}
    }

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      Plug.Conn.resp(conn, 200, IO.iodata_to_binary(Protobuf.encode_to_iodata(response)))
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{finch: TestFinch, otlp_endpoint: "http://localhost:#{bypass.port}"},
               :logs
             )

    assert {:error, :terminal, :invalid_response} = OtelApi.send_log_events(api, [])
  end

  test "rejects a negative metric partial-success count without retrying" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

    response = %ExportMetricsServiceResponse{
      partial_success: %ExportMetricsPartialSuccess{rejected_data_points: -1}
    }

    Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
      Plug.Conn.resp(conn, 200, IO.iodata_to_binary(Protobuf.encode_to_iodata(response)))
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{finch: TestFinch, otlp_endpoint: "http://localhost:#{bypass.port}"},
               :metrics
             )

    assert {:error, :terminal, :invalid_response} = OtelApi.send_metrics(api, [])
  end

  test "returns invalid response for malformed log response without retrying" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      Plug.Conn.resp(conn, 200, "malformed response")
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{finch: TestFinch, otlp_endpoint: "http://localhost:#{bypass.port}"},
               :logs
             )

    assert {:error, :terminal, :invalid_response} = OtelApi.send_log_events(api, [])
  end

  test "returns invalid response for malformed metric response without retrying" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

    Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
      Plug.Conn.resp(conn, 200, "malformed response")
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{finch: TestFinch, otlp_endpoint: "http://localhost:#{bypass.port}"},
               :metrics
             )

    assert {:error, :terminal, :invalid_response} = OtelApi.send_metrics(api, [])
  end

  test "returns invalid response for a JSON log response without retrying" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      Plug.Conn.resp(conn, 200, ~s({"partialSuccess":{}}))
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{finch: TestFinch, otlp_endpoint: "http://localhost:#{bypass.port}"},
               :logs
             )

    assert {:error, :terminal, :invalid_response} = OtelApi.send_log_events(api, [])
  end

  test "returns invalid response for a JSON metric response without retrying" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

    Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
      Plug.Conn.resp(conn, 200, ~s({"partialSuccess":{}}))
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{finch: TestFinch, otlp_endpoint: "http://localhost:#{bypass.port}"},
               :metrics
             )

    assert {:error, :terminal, :invalid_response} = OtelApi.send_metrics(api, [])
  end

  test "returns terminal encoding failure without sending the metric payload" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: EncodingFinch})
    parent = self()

    Bypass.stub(bypass, "POST", "/v1/metrics", fn conn ->
      send(parent, :unexpected_request)
      Plug.Conn.resp(conn, 200, "")
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{finch: EncodingFinch, otlp_endpoint: "http://localhost:#{bypass.port}"},
               :metrics
             )

    log =
      capture_log(fn ->
        send(
          self(),
          {:encoding_result, OtelApi.send_metrics(api, [%Metric{name: :invalid_metric_name}])}
        )
      end)

    assert_receive {:encoding_result, result}

    assert result == {:error, :terminal, :encoding_failed}
    refute inspect(result) =~ "invalid_metric_name"
    refute log =~ "invalid_metric_name"
    refute_receive :unexpected_request, 100
  end

  test "does not retry after the timeout budget is exhausted" do
    bypass = Bypass.open()
    endpoint = "http://localhost:#{bypass.port}"
    {:ok, _} = start_supervised({Finch, name: TestFinch, pools: %{endpoint => [size: 1]}})
    parent = self()
    {:ok, attempts} = Agent.start_link(fn -> 0 end)

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      attempt = Agent.get_and_update(attempts, fn count -> {count + 1, count + 1} end)
      send(parent, {:request_attempt, attempt, self()})

      receive do
        :release -> Plug.Conn.resp(conn, 503, "temporary")
      end
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{
                 finch: TestFinch,
                 otlp_endpoint: endpoint,
                 otlp_timeout: 3_000
               },
               :logs
             )

    task = Task.async(fn -> OtelApi.send_log_events(api, []) end)
    assert_receive {:request_attempt, 1, request_pid}, 5_000
    on_exit(fn -> send(request_pid, :release) end)
    assert {:ok, {:error, :retryable, :deadline_exceeded}} = Task.yield(task, 5_000)
    assert Agent.get(attempts, & &1) == 1

    Bypass.pass(bypass)
    send(request_pid, :release)
  end

  test "does not retry a permanent response" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})
    parent = self()
    {:ok, attempts} = Agent.start_link(fn -> 0 end)

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      Agent.update(attempts, &(&1 + 1))
      send(parent, :request_finished)
      Plug.Conn.resp(conn, 400, "permanent")
    end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{
                 finch: TestFinch,
                 otlp_endpoint: "http://localhost:#{bypass.port}",
                 otlp_timeout: 3_000
               },
               :logs
             )

    task = Task.async(fn -> OtelApi.send_log_events(api, []) end)
    assert_receive :request_finished, 5_000
    assert {:ok, {:error, :terminal, {:http_status, 400}}} = Task.yield(task, 1_000)
    assert Agent.get(attempts, & &1) == 1
  end

  test "returns a bounded error when Finch connection checkout times out" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch, pools: %{default: [size: 1]}})
    parent = self()

    Bypass.expect_once(bypass, "POST", "/v1/logs", fn conn ->
      send(parent, {:holder_started, self()})

      receive do
        :release_holder -> Plug.Conn.resp(conn, 200, "")
      end
    end)

    holder_request = Finch.build(:post, "http://localhost:#{bypass.port}/v1/logs", [], <<>>)
    holder = Task.async(fn -> Finch.request(holder_request, TestFinch) end)
    assert_receive {:holder_started, holder_pid}, 5_000
    on_exit(fn -> send(holder_pid, :release_holder) end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{
                 finch: TestFinch,
                 otlp_endpoint: "http://localhost:#{bypass.port}",
                 otlp_timeout: 100
               },
               :logs
             )

    task = Task.async(fn -> OtelApi.send_log_events(api, []) end)
    assert {:ok, {:error, :retryable, reason}} = Task.yield(task, 1_000)
    assert reason in [:pool_timeout, :deadline_exceeded]

    Bypass.pass(bypass)
    send(holder_pid, :release_holder)
    assert {:ok, {:ok, %{status: 200}}} = Task.yield(holder, 5_000)
  end

  test "bounds checkout and response time to one OTLP budget" do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch, pools: %{default: [size: 1]}})
    parent = self()
    {:ok, attempts} = Agent.start_link(fn -> 0 end)

    Bypass.expect(bypass, "POST", "/v1/logs", fn conn ->
      attempt = Agent.get_and_update(attempts, fn count -> {count + 1, count + 1} end)
      send(parent, {:request_started, attempt, self()})

      receive do
        :release_holder -> Plug.Conn.resp(conn, 200, "")
        :release_request -> Plug.Conn.resp(conn, 200, "")
      end
    end)

    holder_request = Finch.build(:post, "http://localhost:#{bypass.port}/v1/logs", [], <<>>)
    holder = Task.async(fn -> Finch.request(holder_request, TestFinch) end)
    assert_receive {:request_started, 1, holder_pid}, 5_000
    on_exit(fn -> send(holder_pid, :release_holder) end)

    assert {:ok, api, %{}} =
             OtelApi.new(
               %{
                 finch: TestFinch,
                 otlp_endpoint: "http://localhost:#{bypass.port}",
                 otlp_timeout: 1_500
               },
               :logs
             )

    started_at = System.monotonic_time(:millisecond)
    task = Task.async(fn -> OtelApi.send_log_events(api, []) end)
    Process.sleep(600)
    Bypass.pass(bypass)
    send(holder_pid, :release_holder)
    assert_receive {:request_started, 2, request_pid}, 1_000
    on_exit(fn -> send(request_pid, :release_request) end)

    assert {:ok, {:error, :retryable, :deadline_exceeded}} = Task.yield(task, 1_000)
    elapsed = System.monotonic_time(:millisecond) - started_at
    assert elapsed < 1_750

    send(request_pid, :release_request)
    assert {:ok, {:ok, %{status: 200}}} = Task.yield(holder, 5_000)
    assert Agent.get(attempts, & &1) == 2
  end
end
