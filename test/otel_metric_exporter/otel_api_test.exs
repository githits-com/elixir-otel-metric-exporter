defmodule OtelMetricExporter.OtelApiTest do
  use ExUnit.Case, async: false
  alias OtelMetricExporter.OtelApi
  alias OtelMetricExporter.OtelApi.Config

  setup do
    on_exit(fn ->
      System.delete_env("OTEL_SERVICE_NAME")
      System.delete_env("OTEL_RESOURCE_ATTRIBUTES")
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
    assert {:ok, {:error, %Mint.TransportError{reason: :timeout}}} = Task.yield(task, 5_000)

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

    assert {:error, %Mint.TransportError{reason: :timeout}} =
             OtelApi.send_log_events(logs_api, [], deadline)

    assert {:error, %Mint.TransportError{reason: :timeout}} =
             OtelApi.send_metrics(metrics_api, [], deadline)

    refute_receive :unexpected_request, 100
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
    assert {:ok, {:error, %Mint.TransportError{reason: :timeout}}} = Task.yield(task, 2_200)

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
    assert {:ok, {:error, %Mint.TransportError{reason: :timeout}}} = Task.yield(task, 5_000)
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
    assert {:ok, {:error, {:unexpected_status, %{status: 400}}}} = Task.yield(task, 1_000)
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
    assert {:ok, {:error, reason}} = Task.yield(task, 1_000)
    assert reason == :pool_timeout or match?(%Mint.TransportError{reason: :timeout}, reason)

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
                 otlp_timeout: 400
               },
               :logs
             )

    started_at = System.monotonic_time(:millisecond)
    task = Task.async(fn -> OtelApi.send_log_events(api, []) end)
    Process.sleep(300)
    Bypass.pass(bypass)
    send(holder_pid, :release_holder)
    assert_receive {:request_started, 2, request_pid}, 1_000
    on_exit(fn -> send(request_pid, :release_request) end)

    assert {:ok, {:error, %Mint.TransportError{reason: :timeout}}} = Task.yield(task, 1_000)
    elapsed = System.monotonic_time(:millisecond) - started_at
    assert elapsed < 550

    send(request_pid, :release_request)
    assert {:ok, {:ok, %{status: 200}}} = Task.yield(holder, 5_000)
    assert Agent.get(attempts, & &1) == 2
  end
end
