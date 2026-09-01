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
    {:ok, _} = start_supervised({Finch, name: TestFinch})
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
                 otlp_endpoint: "http://localhost:#{bypass.port}",
                 otlp_timeout: 100,
                 retry: false
               },
               :logs
             )

    task = Task.async(fn -> OtelApi.send_log_events(api, []) end)
    assert_receive {:request_received, request_pid}, 1_000
    on_exit(fn -> send(request_pid, :release) end)
    assert {:ok, {:error, %Mint.TransportError{reason: :timeout}}} = Task.yield(task, 1_000)

    Bypass.pass(bypass)
    send(request_pid, :release)
  end
end
