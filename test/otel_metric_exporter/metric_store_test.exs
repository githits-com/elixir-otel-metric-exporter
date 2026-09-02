defmodule OtelMetricExporter.MetricStoreTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest

  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.{
    ExportMetricsPartialSuccess,
    ExportMetricsServiceResponse
  }

  alias Telemetry.Metrics
  alias OtelMetricExporter.MetricStore

  @name :metric_store_test
  @default_buckets [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]
  @response_cap 4_194_304

  setup do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

    config = %{
      otlp_protocol: :http_protobuf,
      otlp_endpoint: "http://localhost:#{bypass.port}",
      otlp_headers: %{},
      otlp_compression: nil,
      resource: %{instance: %{id: "test"}},
      export_period: 1000,
      default_buckets: @default_buckets,
      metrics: [],
      finch_pool: TestFinch,
      retry: false,
      name: @name
    }

    {:ok, bypass: bypass, store_config: config}
  end

  describe "recording metrics" do
    setup %{store_config: config}, do: {:ok, store: start_supervised!({MetricStore, config})}

    test "records counter metrics" do
      metric = Metrics.counter("test.value")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 1, tags)
      MetricStore.write_metric(@name, metric, 2, tags)

      metrics = MetricStore.get_metrics(@name)

      assert %{{:counter, "test.value"} => %{^tags => 2}} = metrics
    end

    test "records sum metrics" do
      metric = Metrics.sum("test.value")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 1, tags)
      MetricStore.write_metric(@name, metric, 2, tags)

      metrics = MetricStore.get_metrics(@name)

      assert %{{:sum, "test.value"} => %{^tags => 3}} = metrics
    end

    test "records last value metrics" do
      metric = Metrics.last_value("test.value")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 1, tags)
      MetricStore.write_metric(@name, metric, 2, tags)

      metrics = MetricStore.get_metrics(@name)

      assert %{{:last_value, "test.value"} => %{^tags => 2}} = metrics
    end

    test "records distribution metrics" do
      metric = Metrics.distribution("test.value", reporter_options: [buckets: [2, 4]])
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 2, tags)
      MetricStore.write_metric(@name, metric, 3, tags)
      MetricStore.write_metric(@name, metric, 5, tags)
      MetricStore.write_metric(@name, metric, 5, tags)

      metrics = MetricStore.get_metrics(@name)

      assert %{
               {:distribution, "test.value"} => %{
                 ^tags => %{0 => {1, 2}, 1 => {1, 3}, 2 => {2, 10}, min: {2, nil}, max: {5, nil}}
               }
             } = metrics
    end

    test "handles different tag sets independently" do
      metric = Metrics.sum("test.value")
      tags1 = %{test: "value1"}
      tags2 = %{test: "value2"}

      MetricStore.write_metric(@name, metric, 1, tags1)
      MetricStore.write_metric(@name, metric, 2, tags2)
      MetricStore.write_metric(@name, metric, 2, tags1)

      metrics = MetricStore.get_metrics(@name)

      assert %{
               {:sum, "test.value"} => %{^tags1 => 3, ^tags2 => 2}
             } = metrics
    end
  end

  describe "export flow" do
    test "exports all metrics in protobuf format", %{bypass: bypass, store_config: config} do
      metrics =
        [metric1, metric2, metric_lv_int, metric_lv_bigint, metric_lv_float, metric4] =
        [
          Metrics.sum("test.sum"),
          Metrics.counter("test.counter"),
          Metrics.last_value("test.last_value.int"),
          Metrics.last_value("test.last_value.bigint"),
          Metrics.last_value("test.last_value.float"),
          Metrics.distribution("test.distribution")
        ]

      start_supervised!({MetricStore, %{config | metrics: metrics}})

      tags = %{test: "value"}

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)

        assert {"content-type", "application/x-protobuf"} in conn.req_headers
        assert {"accept", "application/x-protobuf"} in conn.req_headers

        assert body != ""

        # Decodes withouth raising
        ExportMetricsServiceRequest.decode(body)

        Plug.Conn.resp(conn, 200, "")
      end)

      MetricStore.write_metric(@name, metric1, 1, tags)
      MetricStore.write_metric(@name, metric2, 2, tags)
      MetricStore.write_metric(@name, metric_lv_int, 2 ** 63 - 1, tags)
      MetricStore.write_metric(@name, metric_lv_bigint, 2 ** 70, tags)
      MetricStore.write_metric(@name, metric_lv_float, -1.5, tags)
      MetricStore.write_metric(@name, metric4, 4, tags)
      MetricStore.write_metric(@name, metric4, 2000, tags)

      metrics = MetricStore.get_metrics(@name)
      assert map_size(metrics) > 0

      # Export metrics synchronously
      assert :ok = MetricStore.export_sync(@name)

      # Verify metrics were cleared
      assert MetricStore.get_metrics(@name, 0) == %{}
    end

    test "exports nil and :undefined last_value as a nil data point without crashing", %{
      bypass: bypass,
      store_config: config
    } do
      metric_undef = Metrics.last_value("test.last_value.undefined")
      metric_nil = Metrics.last_value("test.last_value.nil")
      tags = %{test: "value"}
      start_supervised!({MetricStore, %{config | metrics: [metric_undef, metric_nil]}})

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        decoded = ExportMetricsServiceRequest.decode(body)

        assert [%{scope_metrics: [%{metrics: exported_metrics}]}] = decoded.resource_metrics

        Enum.each(exported_metrics, fn metric ->
          assert {:gauge, %{data_points: [point]}} = metric.data
          # protobuf elides the nil inner value, so the oneof decodes as nil
          assert point.value == nil
        end)

        Plug.Conn.resp(conn, 200, "")
      end)

      # `:telemetry` emits `:undefined` for uninitialised values
      MetricStore.write_metric(@name, metric_undef, :undefined, tags)

      # A `nil` value may slip in just as well
      MetricStore.write_metric(@name, metric_nil, nil, tags)

      assert :ok = MetricStore.export_sync(@name)
    end

    test "clears metrics after a terminal server error", %{bypass: bypass, store_config: config} do
      metric = Metrics.sum("test.sum")
      tags = %{test: "value"}
      start_supervised!({MetricStore, %{config | metrics: [metric]}})

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 500, "Internal Server Error")
      end)

      MetricStore.write_metric(@name, metric, 1, tags)

      # Export metrics synchronously
      log =
        capture_log(fn ->
          assert {:error, :terminal, {:http_status, 500}} = MetricStore.export_sync(@name)
        end)

      assert log =~ "Failed to export metrics"
      assert log =~ "disposition=terminal"
      assert log =~ "reason=http_status=500"
      refute log =~ "Internal Server Error"

      # Terminal responses are not retained because the receiver may have consumed the batch.
      assert MetricStore.get_metrics(@name, 0) == %{}
    end

    test "handles connection errors gracefully", %{bypass: bypass, store_config: config} do
      metric = Metrics.sum("test.sum")
      tags = %{test: "value"}
      start_supervised!({MetricStore, %{config | metrics: [metric]}})

      Bypass.down(bypass)

      MetricStore.write_metric(@name, metric, 1, tags)

      metrics = MetricStore.get_metrics(@name)

      # Export metrics synchronously
      log =
        capture_log(fn ->
          assert {:error, :retryable, :transport_failure} = MetricStore.export_sync(@name)
        end)

      assert log =~ "Failed to export metrics"
      assert log =~ "disposition=retryable"
      assert log =~ "reason=transport_failure"

      # Verify metrics were not cleared due to error
      assert MetricStore.get_metrics(@name, 0) == metrics
    end

    @tag :capture_log
    test "normalizes an export worker exit and retains attempted metrics", %{store_config: config} do
      metric = Metrics.sum("test.sum")

      store_pid =
        start_supervised!({MetricStore, %{config | metrics: [metric], export_period: 60_000}})

      MetricStore.write_metric(@name, metric, 1, %{test: "value"})

      :sys.replace_state(@name, fn state ->
        bad_config = %{state.api.config | resource: :invalid_resource}
        %{state | api: %{state.api | config: bad_config}}
      end)

      log =
        capture_log(fn ->
          assert {:error, :retryable, :export_task_failed} = MetricStore.export_sync(@name)
        end)

      assert Process.alive?(store_pid)
      assert MetricStore.get_metrics(@name, 0) != %{}
      refute log =~ "test.sum"
    end

    @tag :capture_log
    test "kills an overdue export worker and retains attempted metrics", %{
      bypass: bypass,
      store_config: config
    } do
      metric = Metrics.sum("test.sum")
      parent = self()

      store_pid =
        start_supervised!(
          {MetricStore,
           Map.merge(config, %{metrics: [metric], export_period: 60_000, otlp_timeout: 1_000})}
        )

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        send(parent, {:request_started, self()})

        receive do
          :release -> Plug.Conn.resp(conn, 200, "")
        end
      end)

      MetricStore.write_metric(@name, metric, 1, %{test: "value"})

      task = Task.async(fn -> MetricStore.export_sync(@name) end)
      assert_receive {:request_started, request_pid}, 5_000
      on_exit(fn -> send(request_pid, :release) end)

      assert {:ok, {:error, :retryable, :deadline_exceeded}} = Task.yield(task, 2_000)
      assert Process.alive?(store_pid)
      assert MetricStore.get_metrics(@name, 0) != %{}

      Bypass.pass(bypass)
      send(request_pid, :release)
    end

    test "clears metrics and generations after partial success", %{
      bypass: bypass,
      store_config: config
    } do
      metric = Metrics.sum("test.sum")
      tags = %{test: "value"}
      start_supervised!({MetricStore, %{config | metrics: [metric], export_period: 60_000}})

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        response = %ExportMetricsServiceResponse{
          partial_success: %ExportMetricsPartialSuccess{
            rejected_data_points: 1,
            error_message: "receiver rejected points"
          }
        }

        Plug.Conn.resp(conn, 200, IO.iodata_to_binary(Protobuf.encode_to_iodata(response)))
      end)

      MetricStore.write_metric(@name, metric, 1, tags)

      assert {:partial_success, 1} = MetricStore.export_sync(@name)
      assert MetricStore.get_metrics(@name, 0) == %{}

      state = :sys.get_state(@name)
      assert :ets.info(state.generations_table, :size) == 1
    end

    test "clears metrics and preserves the store after an invalid JSON response", %{
      bypass: bypass,
      store_config: config
    } do
      metric = Metrics.sum("test.sum")
      tags = %{test: "value"}

      store_pid =
        start_supervised!({MetricStore, %{config | metrics: [metric], export_period: 60_000}})

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 200, ~s({"partialSuccess":{}}))
      end)

      MetricStore.write_metric(@name, metric, 1, tags)
      assert MetricStore.get_metrics(@name, 0) != %{}

      log =
        capture_log(fn ->
          assert {:error, :terminal, :invalid_response} = MetricStore.export_sync(@name)
        end)

      assert log =~ "Failed to export metrics"
      assert log =~ "disposition=terminal"
      assert log =~ "reason=invalid_response"
      refute log =~ "receiver rejected"
      refute log =~ "partialSuccess"
      assert Process.alive?(store_pid)
      assert MetricStore.get_metrics(@name, 0) == %{}
      assert MetricStore.get_metrics(@name, 1) == %{}
      assert :persistent_term.get({MetricStore, @name, :generation}) == 1

      state = :sys.get_state(@name)
      assert :ets.info(state.generations_table, :size) == 1
    end

    @tag :otlp_response_cap
    test "clears metrics and preserves the store after an oversized response", %{
      bypass: bypass,
      store_config: config
    } do
      metric = Metrics.sum("test.sum")
      tags = %{test: "value"}

      store_pid =
        start_supervised!({MetricStore, %{config | metrics: [metric], export_period: 60_000}})

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 200, :binary.copy("x", @response_cap + 1))
      end)

      MetricStore.write_metric(@name, metric, 1, tags)

      log =
        capture_log(fn ->
          assert {:error, :terminal, :response_too_large} = MetricStore.export_sync(@name)
        end)

      assert log =~ "Failed to export metrics"
      assert log =~ "disposition=terminal"
      assert log =~ "reason=response_too_large"
      refute log =~ String.duplicate("x", 32)
      assert Process.alive?(store_pid)
      assert MetricStore.get_metrics(@name, 0) == %{}
      assert MetricStore.get_metrics(@name, 1) == %{}
    end

    test "preserves metrics across generations on failed exports", %{
      bypass: bypass,
      store_config: config
    } do
      metric = Metrics.sum("test.sum")
      tags = %{test: "value"}
      start_supervised!({MetricStore, %{config | metrics: [metric]}})

      # First generation
      MetricStore.write_metric(@name, metric, 1, tags)

      # First export is retryable
      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 503, "Service Unavailable")
      end)

      log =
        capture_log(fn ->
          assert {:error, :retryable, {:http_status, 503}} = MetricStore.export_sync(@name)
        end)

      assert log =~ "Failed to export metrics"
      assert log =~ "disposition=retryable"
      assert log =~ "reason=http_status=503"
      refute log =~ "Service Unavailable"

      # Second generation
      MetricStore.write_metric(@name, metric, 2, tags)

      # Second export succeeds and should include both generations
      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        metrics = ExportMetricsServiceRequest.decode(body)

        # Verify that we have one metric with sum = 3 (1 from first generation + 2 from second)
        assert [%{scope_metrics: [%{metrics: [metric]}]}] = metrics.resource_metrics

        assert {:sum, %{data_points: [point1, point2]}} = metric.data
        assert {:as_int, 1} = point1.value
        assert {:as_int, 2} = point2.value

        assert point1.time_unix_nano < point2.time_unix_nano
        assert point2.start_time_unix_nano > point1.time_unix_nano

        Plug.Conn.resp(conn, 200, "")
      end)

      assert :ok = MetricStore.export_sync(@name)

      # Both generations should be cleared after successful export
      assert MetricStore.get_metrics(@name, 0) == %{}
      assert MetricStore.get_metrics(@name, 1) == %{}
    end
  end
end
