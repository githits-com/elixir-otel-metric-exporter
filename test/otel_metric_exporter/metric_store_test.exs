defmodule OtelMetricExporter.MetricStoreTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias OtelMetricExporter.MetricStore
  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest

  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.{
    ExportMetricsPartialSuccess,
    ExportMetricsServiceResponse
  }

  alias Telemetry.Metrics

  @name :metric_store_test
  @event [:otel_metric_exporter, :export, :stop]
  @default_buckets [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]
  @response_cap 4_194_304

  setup do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

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

    config = %{
      otlp_protocol: :http_protobuf,
      otlp_endpoint: "http://localhost:#{bypass.port}",
      otlp_headers: %{},
      otlp_compression: nil,
      resource: %{instance: %{id: "test"}},
      export_period: 60_000,
      default_buckets: @default_buckets,
      metrics: [],
      finch_pool: TestFinch,
      retry: false,
      name: @name
    }

    {:ok, bypass: bypass, store_config: config}
  end

  defp start_store(config, metrics, temporality \\ nil) do
    config = Map.put(config, :metrics, metrics)

    config =
      if temporality, do: Map.put(config, :aggregation_temporality, temporality), else: config

    start_supervised!({MetricStore, config})
  end

  defp request_point(body) do
    request = ExportMetricsServiceRequest.decode(body)
    [%{scope_metrics: [%{metrics: [metric]}]}] = request.resource_metrics
    metric
  end

  defp response(conn, status) do
    Plug.Conn.resp(conn, status, if(status == 200, do: "", else: "Service Unavailable"))
  end

  describe "stable per-series aggregation" do
    test "uses one row for each series and preserves numeric values", %{store_config: config} do
      counter = Metrics.counter("test.counter")
      sum = Metrics.sum("test.sum")
      gauge = Metrics.last_value("test.gauge")
      histogram = Metrics.distribution("test.histogram", reporter_options: [buckets: [2, 4]])
      start_store(config, [counter, sum, gauge, histogram])

      MetricStore.write_metric(@name, counter, :ignored, %{kind: "one"})
      MetricStore.write_metric(@name, counter, :ignored, %{kind: "one"})
      MetricStore.write_metric(@name, sum, 1.25, %{})
      MetricStore.write_metric(@name, gauge, -2.5, %{})
      MetricStore.write_metric(@name, histogram, 1.25, %{})
      MetricStore.write_metric(@name, histogram, 2.75, %{})

      assert %{{:counter, "test.counter"} => %{%{kind: "one"} => 2}} =
               MetricStore.get_metrics(@name)

      assert %{{:sum, "test.sum"} => %{%{} => 1.25}} = MetricStore.get_metrics(@name)
      assert %{{:last_value, "test.gauge"} => %{%{} => -2.5}} = MetricStore.get_metrics(@name)

      assert %{{:distribution, "test.histogram"} => %{%{} => aggregate}} =
               MetricStore.get_metrics(@name)

      assert aggregate == %{
               count: 2,
               sum: 4.0,
               min: 1.25,
               max: 2.75,
               buckets: [1, 1, 0]
             }

      state = :sys.get_state(@name)
      assert :ets.info(state.metrics_table, :size) == 4
    end

    test "keeps concurrent numeric CAS writes exact", %{store_config: config} do
      metric = Metrics.sum("test.sum")
      start_store(config, [metric])

      tasks =
        for _ <- 1..100 do
          Task.async(fn -> MetricStore.write_metric(@name, metric, 1.25, %{}) end)
        end

      Enum.each(tasks, &Task.await(&1, 5_000))
      assert %{{:sum, "test.sum"} => %{%{} => 125.0}} = MetricStore.get_metrics(@name)
    end

    test "keeps different tag sets independent", %{store_config: config} do
      metric = Metrics.sum("test.sum")
      tags_one = %{test: "one"}
      tags_two = %{test: "two"}
      start_store(config, [metric])

      MetricStore.write_metric(@name, metric, 1, tags_one)
      MetricStore.write_metric(@name, metric, 2, tags_two)
      MetricStore.write_metric(@name, metric, 2, tags_one)

      assert %{{:sum, "test.sum"} => %{^tags_one => 3, ^tags_two => 2}} =
               MetricStore.get_metrics(@name)
    end

    test "keeps integer and float tag values in separate series", %{store_config: config} do
      counter = Metrics.counter("test.counter")
      sum = Metrics.sum("test.sum")
      gauge = Metrics.last_value("test.gauge")
      histogram = Metrics.distribution("test.histogram", reporter_options: [buckets: [2]])
      integer_tags = %{status: 1}
      float_tags = %{status: 1.0}
      start_store(config, [counter, sum, gauge, histogram])

      MetricStore.write_metric(@name, counter, :ignored, integer_tags)
      MetricStore.write_metric(@name, counter, :ignored, float_tags)
      MetricStore.write_metric(@name, sum, 1.25, integer_tags)
      MetricStore.write_metric(@name, sum, 2.5, float_tags)
      MetricStore.write_metric(@name, gauge, 3, integer_tags)
      MetricStore.write_metric(@name, gauge, 4, float_tags)
      MetricStore.write_metric(@name, histogram, 1.5, integer_tags)
      MetricStore.write_metric(@name, histogram, 2.5, float_tags)

      metrics = MetricStore.get_metrics(@name)
      assert get_in(metrics, [{:counter, "test.counter"}, integer_tags]) == 1
      assert get_in(metrics, [{:counter, "test.counter"}, float_tags]) == 1
      assert get_in(metrics, [{:sum, "test.sum"}, integer_tags]) == 1.25
      assert get_in(metrics, [{:sum, "test.sum"}, float_tags]) == 2.5
      assert get_in(metrics, [{:last_value, "test.gauge"}, integer_tags]) == 3
      assert get_in(metrics, [{:last_value, "test.gauge"}, float_tags]) == 4

      assert get_in(metrics, [{:distribution, "test.histogram"}, integer_tags]) == %{
               count: 1,
               sum: 1.5,
               min: 1.5,
               max: 1.5,
               buckets: [1, 0]
             }

      assert get_in(metrics, [{:distribution, "test.histogram"}, float_tags]) == %{
               count: 1,
               sum: 2.5,
               min: 2.5,
               max: 2.5,
               buckets: [0, 1]
             }

      state = :sys.get_state(@name)
      assert :ets.info(state.metrics_table, :size) == 8
    end

    test "skips non-numeric value-bearing inputs without changing ETS", %{store_config: config} do
      sum = Metrics.sum("test.sum")
      last_value = Metrics.last_value("test.last")
      distribution = Metrics.distribution("test.distribution")
      start_store(config, [sum, last_value, distribution])

      assert :ok = MetricStore.write_metric(@name, sum, nil, %{})
      assert :ok = MetricStore.write_metric(@name, last_value, :undefined, %{})
      assert :ok = MetricStore.write_metric(@name, distribution, "not numeric", %{})
      assert MetricStore.get_metrics(@name) == %{}
    end
  end

  test "disabled exporter does not create metric state", %{store_config: config} do
    Application.put_env(:otel_metric_exporter, :metrics, exporter: :none)
    on_exit(fn -> Application.delete_env(:otel_metric_exporter, :metrics) end)

    assert :ignore = MetricStore.start_link(%{config | name: :disabled_metric_store_test})
    assert :ets.whereis(:disabled_metric_store_test) == :undefined
  end

  describe "export result telemetry and transport" do
    test "emits exact success accounting for converted data points", %{
      bypass: bypass,
      store_config: config
    } do
      sum = Metrics.sum("test.sum")
      gauge = Metrics.last_value("test.gauge")
      histogram = Metrics.distribution("test.histogram")
      start_store(config, [sum, gauge, histogram])

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 200, "")
      end)

      MetricStore.write_metric(@name, sum, 1, %{tag: "one"})
      MetricStore.write_metric(@name, sum, 2, %{tag: "two"})
      MetricStore.write_metric(@name, gauge, 3, %{tag: "one"})
      MetricStore.write_metric(@name, histogram, 4, %{tag: "one"})

      assert :ok = MetricStore.export_sync(@name)
      assert_receive {:export_event, @event, measurements, %{scope: :metrics, outcome: :success}}

      assert measurements == %{
               duration_ms: measurements.duration_ms,
               batch_size: 4,
               rejected_items: 0,
               dropped_items: 0
             }
    end

    test "retains rows after a retryable transport failure", %{
      bypass: bypass,
      store_config: config
    } do
      metric = Metrics.distribution("test.histogram")
      start_store(config, [metric])
      Bypass.down(bypass)

      MetricStore.write_metric(@name, metric, 1, %{tag: "one"})

      log =
        capture_log(fn ->
          assert {:error, :retryable, :transport_failure} = MetricStore.export_sync(@name)
        end)

      assert log =~ "Failed to export metrics"
      assert log =~ "disposition=retryable"
      assert log =~ "reason=transport_failure"
      assert MetricStore.get_metrics(@name) != %{}
      assert_receive {:export_event, @event, %{batch_size: 1, dropped_items: 0}, _}
    end

    test "keeps the store alive when an export worker exits", %{store_config: config} do
      metric = Metrics.sum("test.sum")
      store_pid = start_store(config, [metric])
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
      assert MetricStore.get_metrics(@name) != %{}
      refute log =~ "test.sum"
      assert_receive {:export_event, @event, %{batch_size: 1, dropped_items: 0}, _}
    end

    @tag :capture_log
    test "bounds an overdue export and retains rows", %{
      bypass: bypass,
      store_config: config
    } do
      metric = Metrics.sum("test.sum")
      parent = self()

      store_pid =
        start_store(Map.put(config, :otlp_timeout, 1_000), [metric])

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
      assert {:ok, {:error, :retryable, reason}} = Task.yield(task, 2_000)
      assert reason in [:deadline_exceeded, :transport_failure]
      assert Process.alive?(store_pid)
      assert MetricStore.get_metrics(@name) != %{}
      Bypass.pass(bypass)
      send(request_pid, :release)
    end

    test "clears rows after an invalid OTLP response", %{bypass: bypass, store_config: config} do
      metric = Metrics.sum("test.sum")
      store_pid = start_store(config, [metric], :delta)

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 200, ~s({"partialSuccess":{}}))
      end)

      MetricStore.write_metric(@name, metric, 1, %{})

      log =
        capture_log(fn ->
          assert {:error, :terminal, :invalid_response} = MetricStore.export_sync(@name)
        end)

      assert log =~ "disposition=terminal"
      assert log =~ "reason=invalid_response"
      refute log =~ "partialSuccess"
      assert Process.alive?(store_pid)
      assert MetricStore.get_metrics(@name) == %{}
      assert_receive {:export_event, @event, %{batch_size: 1, dropped_items: 1}, _}
    end

    @tag :otlp_response_cap
    test "clears rows after an oversized response", %{bypass: bypass, store_config: config} do
      metric = Metrics.sum("test.sum")
      store_pid = start_store(config, [metric], :delta)

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 200, :binary.copy("x", @response_cap + 1))
      end)

      MetricStore.write_metric(@name, metric, 1, %{})

      log =
        capture_log(fn ->
          assert {:error, :terminal, :response_too_large} = MetricStore.export_sync(@name)
        end)

      assert log =~ "reason=response_too_large"
      refute log =~ String.duplicate("x", 32)
      assert Process.alive?(store_pid)
      assert MetricStore.get_metrics(@name) == %{}
      assert_receive {:export_event, @event, %{batch_size: 1, dropped_items: 1}, _}
    end

    test "encodes int64, bigint, float, counter, gauge, and histogram values", %{
      bypass: bypass,
      store_config: config
    } do
      metrics =
        [sum, counter, int_gauge, bigint_gauge, float_gauge, histogram] = [
          Metrics.sum("test.sum"),
          Metrics.counter("test.counter"),
          Metrics.last_value("test.int"),
          Metrics.last_value("test.bigint"),
          Metrics.last_value("test.float"),
          Metrics.distribution("test.distribution")
        ]

      start_store(config, metrics)

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        assert {"content-type", "application/x-protobuf"} in conn.req_headers
        assert {"accept", "application/x-protobuf"} in conn.req_headers
        assert body != ""
        request = ExportMetricsServiceRequest.decode(body)
        [%{scope_metrics: [%{metrics: exported}]}] = request.resource_metrics

        assert Enum.map(exported, & &1.name) |> Enum.sort() ==
                 [
                   "test.bigint",
                   "test.counter",
                   "test.distribution",
                   "test.float",
                   "test.int",
                   "test.sum"
                 ]

        assert {:sum,
                %{
                  data_points: [%{value: {:as_int, 1}}],
                  aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
                  is_monotonic: false
                }} = Enum.find(exported, &(&1.name == "test.sum")).data

        assert {:sum,
                %{
                  data_points: [%{value: {:as_int, 1}}],
                  aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
                  is_monotonic: true
                }} = Enum.find(exported, &(&1.name == "test.counter")).data

        assert {:gauge, %{data_points: [%{value: {:as_int, 9}}]}} =
                 Enum.find(exported, &(&1.name == "test.int")).data

        assert {:gauge, %{data_points: [%{value: {:as_double, value}}]}} =
                 Enum.find(exported, &(&1.name == "test.bigint")).data

        assert value == 2.0 ** 70

        assert {:gauge, %{data_points: [%{value: {:as_double, -1.5}}]}} =
                 Enum.find(exported, &(&1.name == "test.float")).data

        assert {:histogram, %{data_points: [%{count: 2, sum: 2004.0}]}} =
                 Enum.find(exported, &(&1.name == "test.distribution")).data

        Plug.Conn.resp(conn, 200, "")
      end)

      MetricStore.write_metric(@name, sum, 1, %{})
      MetricStore.write_metric(@name, counter, :present, %{})
      MetricStore.write_metric(@name, int_gauge, 9, %{})
      MetricStore.write_metric(@name, bigint_gauge, 2 ** 70, %{})
      MetricStore.write_metric(@name, float_gauge, -1.5, %{})
      MetricStore.write_metric(@name, histogram, 4, %{})
      MetricStore.write_metric(@name, histogram, 2000, %{})
      assert :ok = MetricStore.export_sync(@name)
    end
  end

  describe "export temporality" do
    test "exports exact fractional sum and whole histogram fields", %{
      bypass: bypass,
      store_config: config
    } do
      sum = Metrics.sum("test.sum")
      histogram = Metrics.distribution("test.histogram", reporter_options: [buckets: [2, 4]])
      start_store(config, [sum, histogram])

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        request = ExportMetricsServiceRequest.decode(body)
        [%{scope_metrics: [%{metrics: metrics}]}] = request.resource_metrics

        assert Enum.any?(metrics, fn
                 %{name: "test.sum", data: {:sum, %{data_points: [point]}}} ->
                   point.value == {:as_double, 1.25}

                 _ ->
                   false
               end)

        assert Enum.any?(metrics, fn
                 %{
                   name: "test.histogram",
                   data: {:histogram, %{data_points: [point]}}
                 } ->
                   point.count == 2 and point.sum == 4.0 and point.min == 1.25 and
                     point.max == 2.75 and point.bucket_counts == [1, 1, 0]

                 _ ->
                   false
               end)

        Plug.Conn.resp(conn, 200, "")
      end)

      MetricStore.write_metric(@name, sum, 1.25, %{})
      MetricStore.write_metric(@name, histogram, 1.25, %{})
      MetricStore.write_metric(@name, histogram, 2.75, %{})
      assert :ok = MetricStore.export_sync(@name)
    end

    test "repeats cumulative lifetime rows without inflating totals", %{
      bypass: bypass,
      store_config: config
    } do
      metric = Metrics.sum("test.sum")
      parent = self()
      start_store(config, [metric])

      Bypass.expect(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        send(parent, {:point, request_point(body)})
        Plug.Conn.resp(conn, 200, "")
      end)

      MetricStore.write_metric(@name, metric, 3, %{})
      assert :ok = MetricStore.export_sync(@name)
      assert_receive {:point, %{data: {:sum, %{data_points: [%{value: {:as_int, 3}}]}}}}
      assert :ok = MetricStore.export_sync(@name)
      assert_receive {:point, %{data: {:sum, %{data_points: [%{value: {:as_int, 3}}]}}}}
      assert :sys.get_state(@name).pending_intervals == []
    end

    test "drains delta rows and suppresses an idle request", %{
      bypass: bypass,
      store_config: config
    } do
      metric = Metrics.sum("test.sum")
      parent = self()
      start_store(config, [metric], :delta)

      Bypass.expect(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        send(parent, {:point, request_point(body)})
        Plug.Conn.resp(conn, 200, "")
      end)

      MetricStore.write_metric(@name, metric, 3, %{})
      assert :ok = MetricStore.export_sync(@name)
      assert_receive {:point, %{data: {:sum, %{data_points: [%{value: {:as_int, 3}}]}}}}
      assert_receive {:export_event, @event, %{batch_size: 1}, _}
      assert :ok = MetricStore.export_sync(@name)
      refute_receive {:point, _}
      refute_receive {:export_event, @event, _, _}
      assert MetricStore.get_metrics(@name) == %{}
    end

    test "uses sparse per-series delta timestamps", %{bypass: bypass, store_config: config} do
      metric = Metrics.sum("test.sum")
      parent = self()
      start_store(config, [metric], :delta)

      Bypass.expect(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        send(parent, {:request, ExportMetricsServiceRequest.decode(body)})
        Plug.Conn.resp(conn, 503, "Service Unavailable")
      end)

      MetricStore.write_metric(@name, metric, 1, %{series: "old"})
      assert {:error, :retryable, {:http_status, 503}} = MetricStore.export_sync(@name)
      assert_receive {:request, _}

      MetricStore.write_metric(@name, metric, 2, %{series: "new"})
      assert {:error, :retryable, {:http_status, 503}} = MetricStore.export_sync(@name)
      assert_receive {:request, _}

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        request = ExportMetricsServiceRequest.decode(body)

        [%{scope_metrics: [%{metrics: [%{data: {:sum, %{data_points: points}}}]}]}] =
          request.resource_metrics

        assert length(points) == 2
        [old, new] = Enum.sort_by(points, & &1.start_time_unix_nano)
        assert old.start_time_unix_nano < new.start_time_unix_nano
        assert old.time_unix_nano < new.time_unix_nano
        assert new.start_time_unix_nano < new.time_unix_nano
        Plug.Conn.resp(conn, 200, "")
      end)

      assert :ok = MetricStore.export_sync(@name)
    end

    test "recovery merges one point per series with type-specific rules", %{
      bypass: bypass,
      store_config: config
    } do
      metrics =
        [counter, sum, gauge, histogram] = [
          Metrics.counter("test.counter"),
          Metrics.sum("test.sum"),
          Metrics.last_value("test.gauge"),
          Metrics.distribution("test.histogram", reporter_options: [buckets: [2, 4]])
        ]

      parent = self()
      start_store(config, metrics, :delta)

      Bypass.expect(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        send(parent, {:request, self(), body})

        receive do
          :success -> Plug.Conn.resp(conn, 200, "")
          :retry -> response(conn, 503)
        end
      end)

      write_batch = fn sum_value, gauge_value, hist_value ->
        MetricStore.write_metric(@name, counter, :present, %{series: "one"})
        MetricStore.write_metric(@name, sum, sum_value, %{series: "one"})
        MetricStore.write_metric(@name, gauge, gauge_value, %{series: "one"})
        MetricStore.write_metric(@name, histogram, hist_value, %{series: "one"})
      end

      write_batch.(2, 1, 1.25)
      first = Task.async(fn -> MetricStore.export_sync(@name) end)
      assert_receive {:request, first_request, _}, 5_000
      send(first_request, :retry)
      assert {:ok, {:error, :retryable, {:http_status, 503}}} = Task.yield(first, 5_000)
      assert_receive {:export_event, @event, %{batch_size: 4, dropped_items: 0}, _}

      write_batch.(3, 2, 2.75)
      second = Task.async(fn -> MetricStore.export_sync(@name) end)
      assert_receive {:request, second_request, _}, 5_000
      send(second_request, :retry)
      assert {:ok, {:error, :retryable, {:http_status, 503}}} = Task.yield(second, 5_000)
      assert_receive {:export_event, @event, %{batch_size: 4, dropped_items: 0}, _}
      assert length(:sys.get_state(@name).pending_intervals) == 2

      recovery = Task.async(fn -> MetricStore.export_sync(@name) end)
      assert_receive {:request, recovery_request, body}, 5_000
      request = ExportMetricsServiceRequest.decode(body)
      [%{scope_metrics: [%{metrics: exported}]}] = request.resource_metrics
      assert length(exported) == 4

      assert {:sum, %{data_points: [%{value: {:as_int, 2}}]}} =
               Enum.find(exported, &(&1.name == "test.counter")).data

      assert {:sum, %{data_points: [%{value: {:as_int, 5}}]}} =
               Enum.find(exported, &(&1.name == "test.sum")).data

      assert {:gauge, %{data_points: [%{value: {:as_int, 2}}]}} =
               Enum.find(exported, &(&1.name == "test.gauge")).data

      assert {:histogram,
              %{
                data_points: [
                  %{count: 2, sum: 4.0, min: 1.25, max: 2.75, bucket_counts: [1, 1, 0]}
                ]
              }} =
               Enum.find(exported, &(&1.name == "test.histogram")).data

      send(recovery_request, :success)
      assert {:ok, :ok} = Task.yield(recovery, 5_000)
      assert :sys.get_state(@name).pending_intervals == []
    end
  end

  describe "result transitions and retention" do
    test "cumulative full, partial, retryable, terminal, and idle exports retain lifetime rows",
         %{
           bypass: bypass,
           store_config: config
         } do
      metric = Metrics.sum("test.sum")
      parent = self()
      start_store(config, [metric])

      Bypass.expect(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        send(parent, {:request, self(), body})

        receive do
          :success ->
            Plug.Conn.resp(conn, 200, "")

          :partial ->
            response = %ExportMetricsServiceResponse{
              partial_success: %ExportMetricsPartialSuccess{rejected_data_points: 1}
            }

            Plug.Conn.resp(conn, 200, Protobuf.encode_to_iodata(response))

          :retry ->
            response(conn, 503)

          :terminal ->
            response(conn, 500)
        end
      end)

      export = fn response_kind ->
        task = Task.async(fn -> MetricStore.export_sync(@name) end)
        assert_receive {:request, request_pid, body}, 5_000
        point = request_point(body)
        send(request_pid, response_kind)
        {Task.yield(task, 5_000), point}
      end

      sum_point = fn metric, value ->
        assert {:sum, %{data_points: [point]}} = metric.data
        assert point.value == value
        point
      end

      MetricStore.write_metric(@name, metric, 1, %{})
      {{:ok, :ok}, first_metric} = export.(:success)
      first_point = sum_point.(first_metric, {:as_int, 1})
      assert_receive {:export_event, @event, %{batch_size: 1, dropped_items: 0}, _}
      assert get_in(MetricStore.get_metrics(@name), [{:sum, "test.sum"}, %{}]) == 1

      MetricStore.write_metric(@name, metric, 2, %{})
      {{:ok, {:partial_success, 1}}, partial_metric} = export.(:partial)
      partial_point = sum_point.(partial_metric, {:as_int, 3})

      assert_receive {:export_event, @event,
                      %{batch_size: 1, rejected_items: 1, dropped_items: 0}, _}

      assert get_in(MetricStore.get_metrics(@name), [{:sum, "test.sum"}, %{}]) == 3
      assert :sys.get_state(@name).pending_intervals == []

      MetricStore.write_metric(@name, metric, 3, %{})
      {{:ok, {:error, :retryable, {:http_status, 503}}}, retry_metric} = export.(:retry)
      retry_point = sum_point.(retry_metric, {:as_int, 6})
      assert_receive {:export_event, @event, %{batch_size: 1, dropped_items: 0}, _}
      assert :sys.get_state(@name).pending_intervals == []

      MetricStore.write_metric(@name, metric, 4, %{})

      {{:ok, {:error, :terminal, {:http_status, 500}}}, terminal_metric} = export.(:terminal)
      terminal_point = sum_point.(terminal_metric, {:as_int, 10})

      assert first_point.start_time_unix_nano == partial_point.start_time_unix_nano
      assert first_point.start_time_unix_nano == retry_point.start_time_unix_nano
      assert first_point.start_time_unix_nano == terminal_point.start_time_unix_nano
      assert first_point.time_unix_nano <= partial_point.time_unix_nano
      assert partial_point.time_unix_nano <= retry_point.time_unix_nano
      assert retry_point.time_unix_nano <= terminal_point.time_unix_nano

      assert_receive {:export_event, @event, %{batch_size: 1, dropped_items: 1}, _}
      assert get_in(MetricStore.get_metrics(@name), [{:sum, "test.sum"}, %{}]) == 10
      assert :sys.get_state(@name).pending_intervals == []

      {{:ok, :ok}, idle_metric} = export.(:success)
      idle_point = sum_point.(idle_metric, {:as_int, 10})
      assert_receive {:export_event, @event, %{batch_size: 1, dropped_items: 0}, _}
      assert get_in(MetricStore.get_metrics(@name), [{:sum, "test.sum"}, %{}]) == 10
      assert first_point.start_time_unix_nano == idle_point.start_time_unix_nano
      assert terminal_point.time_unix_nano <= idle_point.time_unix_nano
    end

    test "uses the configured cumulative and delta OTLP enums", %{
      bypass: bypass,
      store_config: config
    } do
      cumulative = Metrics.sum("test.cumulative")
      start_store(config, [cumulative])

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        metric = request_point(body)

        assert {:sum, %{aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE}} =
                 metric.data

        Plug.Conn.resp(conn, 200, "")
      end)

      MetricStore.write_metric(@name, cumulative, 1, %{})
      assert :ok = MetricStore.export_sync(@name)

      stop_supervised!(MetricStore)
      delta = Metrics.sum("test.delta")
      start_store(config, [delta], :delta)

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        metric = request_point(body)
        assert {:sum, %{aggregation_temporality: :AGGREGATION_TEMPORALITY_DELTA}} = metric.data
        Plug.Conn.resp(conn, 200, "")
      end)

      MetricStore.write_metric(@name, delta, 1, %{})
      assert :ok = MetricStore.export_sync(@name)
    end

    test "leaves gauge start time at zero", %{bypass: bypass, store_config: config} do
      metric = Metrics.last_value("test.gauge")
      start_store(config, [metric], :delta)

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        %{data: {:gauge, %{data_points: [point]}}} = request_point(body)
        assert point.start_time_unix_nano == 0
        assert point.time_unix_nano > 0
        Plug.Conn.resp(conn, 200, "")
      end)

      MetricStore.write_metric(@name, metric, 42, %{})
      assert :ok = MetricStore.export_sync(@name)
    end

    test "partial and terminal results retire attempted delta intervals", %{
      bypass: bypass,
      store_config: config
    } do
      metric = Metrics.sum("test.sum")
      start_store(config, [metric], :delta)

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        response = %ExportMetricsServiceResponse{
          partial_success: %ExportMetricsPartialSuccess{rejected_data_points: 1}
        }

        Plug.Conn.resp(conn, 200, Protobuf.encode_to_iodata(response))
      end)

      MetricStore.write_metric(@name, metric, 1, %{})
      assert {:partial_success, 1} = MetricStore.export_sync(@name)

      assert_receive {:export_event, @event,
                      %{batch_size: 1, rejected_items: 1, dropped_items: 0}, _}

      assert :sys.get_state(@name).pending_intervals == []

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn -> response(conn, 500) end)
      MetricStore.write_metric(@name, metric, 2, %{})

      capture_log(fn ->
        assert {:error, :terminal, {:http_status, 500}} = MetricStore.export_sync(@name)
      end)

      assert_receive {:export_event, @event,
                      %{batch_size: 1, rejected_items: 0, dropped_items: 1}, _}

      assert :sys.get_state(@name).pending_intervals == []
    end

    test "retains at most ten intervals and reaggregates one recovery point", %{
      bypass: bypass,
      store_config: config
    } do
      metric = Metrics.sum("test.sum")
      parent = self()
      start_store(config, [metric], :delta)

      Bypass.expect(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        send(parent, {:request, self(), body})

        receive do
          :success -> Plug.Conn.resp(conn, 200, "")
          :retry -> response(conn, 503)
        end
      end)

      for value <- 1..11 do
        MetricStore.write_metric(@name, metric, value, %{})
        task = Task.async(fn -> MetricStore.export_sync(@name) end)
        assert_receive {:request, request_pid, _body}, 5_000
        send(request_pid, :retry)
        assert {:ok, {:error, :retryable, {:http_status, 503}}} = Task.yield(task, 5_000)
        assert_receive {:export_event, @event, measurements, _}

        if value == 11 do
          assert measurements.dropped_items == 1
        else
          assert measurements.dropped_items == 0
        end
      end

      state = :sys.get_state(@name)
      assert length(state.pending_intervals) == 10

      key = {:sum, "test.sum", :erlang.term_to_binary(%{}, [:deterministic])}

      assert Enum.map(state.pending_intervals, fn %{aggregates: aggregates} ->
               assert map_size(aggregates) == 1
               Map.fetch!(aggregates, key)
             end) == Enum.to_list(2..11)

      assert :ets.info(state.metrics_table, :size) == 0

      task = Task.async(fn -> MetricStore.export_sync(@name) end)
      assert_receive {:request, request_pid, body}, 5_000
      point = request_point(body)
      assert {:sum, %{data_points: [point]}} = point.data
      assert point.value == {:as_int, 65}
      send(request_pid, :success)
      assert {:ok, :ok} = Task.yield(task, 5_000)
      assert :sys.get_state(@name).pending_intervals == []
    end
  end
end
