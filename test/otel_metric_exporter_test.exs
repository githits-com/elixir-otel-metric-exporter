defmodule OtelMetricExporterTest do
  use ExUnit.Case
  alias Telemetry.Metrics
  alias OtelMetricExporter.MetricStore

  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.{
    ExportMetricsPartialSuccess,
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse
  }

  import ExUnit.CaptureLog

  setup do
    on_exit(fn ->
      Enum.each(:telemetry.list_handlers([:test]), fn handler ->
        :telemetry.detach(handler.id)
      end)
    end)

    :ok
  end

  @name :otel_metric_exporter_test

  @base_config [
    otlp_protocol: :http_protobuf,
    otlp_endpoint: "http://localhost:4318",
    otlp_headers: %{},
    otlp_compression: nil,
    export_period: 1000,
    name: @name
  ]

  defp start_test_exporter(config) do
    bypass = Bypass.open()

    Bypass.stub(bypass, "POST", "/v1/metrics", fn conn ->
      Plug.Conn.resp(conn, 200, "")
    end)

    config =
      config
      |> Keyword.put(:otlp_endpoint, "http://localhost:#{bypass.port}")
      |> Keyword.put(:export_period, :timer.hours(1))

    # Start Bypass before the exporter so ExUnit tears the exporter down first;
    # the final drain therefore still has a live deterministic collector.
    start_supervised!({OtelMetricExporter, config})
  end

  describe "start_link/1" do
    test "starts with valid config" do
      metrics = [
        Metrics.counter("test.counter", tags: [:test]),
        Metrics.sum("test.sum", tags: [:test]),
        Metrics.last_value("test.last_value", tags: [:test]),
        Metrics.distribution("test.distribution", tags: [:test])
      ]

      assert pid =
               start_link_supervised!({OtelMetricExporter, @base_config ++ [metrics: metrics]})

      assert Process.alive?(pid)
    end

    test "fails with invalid config" do
      assert {:error, _} = OtelMetricExporter.start_link([])
      assert {:error, _} = OtelMetricExporter.start_link(otlp_protocol: :invalid)
    end

    test "validates aggregation temporality and defaults to cumulative" do
      [metric] = [Metrics.counter("test.counter")]
      metrics = [metric]

      assert is_pid(
               start_link_supervised!({OtelMetricExporter, @base_config ++ [metrics: metrics]})
             )

      assert %{
               aggregation_temporality: :cumulative,
               metric_lookup: %{{:counter, "test.counter"} => ^metric}
             } = :sys.get_state(@name)
    end

    test "rejects invalid aggregation temporality" do
      metrics = [Metrics.counter("test.counter")]

      assert {:error, error} =
               OtelMetricExporter.start_link(
                 @base_config ++ [metrics: metrics, aggregation_temporality: :invalid]
               )

      assert error.key == :aggregation_temporality
    end

    test "rejects unsupported metric types at the public configuration boundary" do
      metrics = [Metrics.summary("test.summary")]

      assert {:error, error} = OtelMetricExporter.start_link(@base_config ++ [metrics: metrics])
      assert error.key == :metrics
    end

    test "metric lookup preserves the first configured definition for duplicate keys" do
      first = Metrics.sum("test.sum", description: "first")
      second = Metrics.sum("test.sum", description: "second")

      assert is_pid(
               start_link_supervised!(
                 {OtelMetricExporter, @base_config ++ [metrics: [first, second]]}
               )
             )

      assert %{metric_lookup: %{{:sum, "test.sum"} => ^first}} = :sys.get_state(@name)
    end

    test "redacts invalid header values from start_link errors" do
      secret = "top-level-header-secret"
      metrics = [Metrics.counter("test.counter")]

      log =
        capture_log(fn ->
          assert {:error, error} =
                   OtelMetricExporter.start_link(
                     Keyword.put(
                       @base_config ++ [metrics: metrics],
                       :otlp_headers,
                       %{"authorization" => "bad\n#{secret}"}
                     )
                   )

          assert error.key == :otlp_headers
          assert error.value == :redacted
          refute inspect(error) =~ secret
          refute Exception.message(error) =~ secret
        end)

      refute log =~ secret
    end

    test "does not start metric machinery when the metrics exporter is disabled" do
      name = String.to_atom("otel_metric_exporter_disabled_#{System.unique_integer([:positive])}")
      Application.put_env(:otel_metric_exporter, :metrics, exporter: :none)

      on_exit(fn -> Application.delete_env(:otel_metric_exporter, :metrics) end)

      metrics = [Metrics.counter("disabled.counter")]

      assert pid =
               start_link_supervised!(
                 {OtelMetricExporter,
                  @base_config
                  |> Keyword.put(:name, name)
                  |> Keyword.put(:metrics, metrics)
                  |> Keyword.put(:aggregation_temporality, :delta)}
               )

      assert Process.alive?(pid)
      assert Supervisor.which_children(pid) == []
      assert Process.whereis(name) == nil
      assert Process.whereis(:"#{name}:TelemetryHandlers") == nil
      assert :ets.whereis(name) == :undefined
    end
  end

  describe "telemetry integration" do
    test "handles telemetry events" do
      metrics = [
        Telemetry.Metrics.sum("test.event.value", event_name: [:test, :event])
      ]

      start_test_exporter(@base_config ++ [metrics: metrics])

      :telemetry.execute([:test, :event], %{value: 42}, %{test: "value"})

      # Give the GenServer time to process the event
      Process.sleep(100)

      metrics = OtelMetricExporter.MetricStore.get_metrics(@name)
      assert %{{:sum, "test.event.value"} => %{%{} => 42}} = metrics
    end

    test "handles events with keep function" do
      metrics = [
        Telemetry.Metrics.counter(
          "test.filtered.value",
          event_name: [:test, :filtered],
          measurement: :value,
          tags: [:test],
          keep: &(&1.test == "keep")
        )
      ]

      start_test_exporter(@base_config ++ [metrics: metrics])

      # This one should be kept
      :telemetry.execute([:test, :filtered], %{value: 1}, %{test: "keep"})
      # This one should be filtered out
      :telemetry.execute([:test, :filtered], %{value: 2}, %{test: "drop"})

      # Give the GenServer time to process the event
      Process.sleep(100)

      metrics = OtelMetricExporter.MetricStore.get_metrics(@name)
      assert get_in(metrics, [{:counter, "test.filtered.value"}, %{test: "keep"}]) == 1
      assert get_in(metrics, [{:counter, "test.filtered.value"}, %{test: "drop"}]) == nil
    end

    test "handles measurement functions" do
      metrics = [
        Telemetry.Metrics.sum(
          "test.measured",
          measurement: fn measurements -> measurements.value * 2 end,
          tags: [:test]
        ),
        Telemetry.Metrics.sum(
          "test.measured_with_metadata",
          measurement: fn measurements, metadata -> measurements.value * metadata.multiplier end,
          tags: [:test]
        )
      ]

      start_test_exporter(@base_config ++ [metrics: metrics])

      :telemetry.execute([:test], %{value: 21}, %{test: "value", multiplier: 3})

      # Give the GenServer time to process the event
      Process.sleep(100)

      metrics = OtelMetricExporter.MetricStore.get_metrics(@name)
      assert get_in(metrics, [{:sum, "test.measured"}, %{test: "value"}]) == 42

      assert get_in(metrics, [{:sum, "test.measured_with_metadata"}, %{test: "value"}]) ==
               63
    end

    test "skips missing distribution measurements without detaching the handler" do
      event = [:test, :missing_distribution]

      metrics = [
        Metrics.distribution("test.distribution", event_name: event, measurement: :value),
        Metrics.counter("test.counter", event_name: event, measurement: :value)
      ]

      exporter = start_test_exporter(@base_config ++ [metrics: metrics])

      :telemetry.execute(event, %{}, %{})
      :telemetry.execute(event, %{value: 3}, %{})

      assert Process.alive?(exporter)
      assert :telemetry.list_handlers(event) != []

      assert get_in(OtelMetricExporter.MetricStore.get_metrics(@name), [
               {:counter, "test.counter"},
               %{}
             ]) == 1

      assert %{count: 1, sum: 3.0, min: 3.0, max: 3.0, buckets: [0, 1 | _]} =
               get_in(OtelMetricExporter.MetricStore.get_metrics(@name), [
                 {:distribution, "test.distribution"},
                 %{}
               ])
    end

    test "isolates invalid measurements and emits bounded drop events" do
      event = [:test, :invalid_measurements]
      drop_event = [:otel_metric_exporter, :metric, :measurement_dropped]
      receiver = self()
      handler_id = {:measurement_dropped_test, System.unique_integer([:positive])}

      :telemetry.attach(
        handler_id,
        drop_event,
        fn ^drop_event, measurements, metadata, _config ->
          send(receiver, {:measurement_dropped, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      metrics = [
        Metrics.counter("test.counter_missing_absent", event_name: event, measurement: :absent),
        Metrics.counter("test.counter_missing_undefined",
          event_name: event,
          measurement: :counter_undefined
        ),
        Metrics.counter("test.counter_present", event_name: event, measurement: :counter_value),
        Metrics.sum("test.sum_missing", event_name: event, measurement: :sum_missing),
        Metrics.sum("test.sum_non_numeric", event_name: event, measurement: :sum_non_numeric),
        Metrics.last_value("test.last_value_missing",
          event_name: event,
          measurement: :last_value_missing
        ),
        Metrics.last_value("test.last_value_non_numeric",
          event_name: event,
          measurement: :last_value_non_numeric
        ),
        Metrics.distribution("test.distribution_missing",
          event_name: event,
          measurement: :distribution_missing
        ),
        Metrics.distribution("test.distribution_non_numeric",
          event_name: event,
          measurement: :distribution_non_numeric
        ),
        Metrics.sum("test.sibling", event_name: event, measurement: :sibling)
      ]

      exporter = start_test_exporter(@base_config ++ [metrics: metrics])

      :telemetry.execute(
        event,
        %{
          counter_value: "present but not numeric",
          counter_undefined: :undefined,
          sum_missing: nil,
          sum_non_numeric: "not numeric",
          last_value_missing: nil,
          last_value_non_numeric: %{},
          distribution_missing: :undefined,
          distribution_non_numeric: "not numeric",
          sibling: 7
        },
        %{}
      )

      drops =
        for _ <- 1..8 do
          assert_receive {:measurement_dropped, measurements, metadata}
          assert measurements == %{count: 1}
          assert metadata == %{metric_type: metadata.metric_type, reason: metadata.reason}
          metadata
        end

      assert Enum.frequencies(drops) == %{
               %{metric_type: :counter, reason: :missing} => 2,
               %{metric_type: :sum, reason: :missing} => 1,
               %{metric_type: :sum, reason: :non_numeric} => 1,
               %{metric_type: :last_value, reason: :missing} => 1,
               %{metric_type: :last_value, reason: :non_numeric} => 1,
               %{metric_type: :distribution, reason: :missing} => 1,
               %{metric_type: :distribution, reason: :non_numeric} => 1
             }

      refute_receive {:measurement_dropped, _measurements, _metadata}
      assert Process.alive?(exporter)
      assert :telemetry.list_handlers(event) != []

      stored = OtelMetricExporter.MetricStore.get_metrics(@name)
      assert get_in(stored, [{:counter, "test.counter_present"}, %{}]) == 1
      assert get_in(stored, [{:sum, "test.sibling"}, %{}]) == 7
    end

    test "preserves callback failures without classifying them as dropped measurements" do
      drop_event = [:otel_metric_exporter, :metric, :measurement_dropped]
      receiver = self()
      handler_id = {:measurement_dropped_callback_test, System.unique_integer([:positive])}

      :telemetry.attach(
        handler_id,
        drop_event,
        fn ^drop_event, measurements, metadata, _config ->
          send(receiver, {:measurement_dropped, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)
      start_test_exporter(@base_config ++ [metrics: []])

      keep_failure =
        Metrics.sum("test.keep_failure",
          keep: fn _metadata -> raise "keep callback failed" end
        )

      assert_raise RuntimeError, "keep callback failed", fn ->
        OtelMetricExporter.handle_metric(
          [:test],
          %{value: 1},
          %{},
          %{metrics: [{keep_failure, "test.keep_failure"}], name: @name, handler_id: :unused}
        )
      end

      measurement_failure =
        Metrics.sum("test.measurement_failure",
          measurement: fn _measurements -> raise "measurement callback failed" end
        )

      assert_raise RuntimeError, "measurement callback failed", fn ->
        OtelMetricExporter.handle_metric(
          [:test],
          %{value: 1},
          %{},
          %{
            metrics: [{measurement_failure, "test.measurement_failure"}],
            name: @name,
            handler_id: :unused
          }
        )
      end

      measurement_throw =
        Metrics.sum("test.measurement_throw",
          measurement: fn _measurements -> throw(:measurement_callback_thrown) end
        )

      assert catch_throw(
               OtelMetricExporter.handle_metric(
                 [:test],
                 %{value: 1},
                 %{},
                 %{
                   metrics: [{measurement_throw, "test.measurement_throw"}],
                   name: @name,
                   handler_id: :unused
                 }
               )
             ) == :measurement_callback_thrown

      tag_failure =
        Metrics.sum("test.tag_failure",
          measurement: :value,
          tags: [:tag],
          tag_values: fn _metadata -> exit(:tag_callback_failed) end
        )

      assert catch_exit(
               OtelMetricExporter.handle_metric(
                 [:test],
                 %{value: 1},
                 %{},
                 %{metrics: [{tag_failure, "test.tag_failure"}], name: @name, handler_id: :unused}
               )
             ) == :tag_callback_failed

      invalid_measurement_tag_failure =
        Metrics.sum("test.invalid_measurement_tag_failure",
          measurement: :missing,
          tags: [:tag],
          tag_values: fn _metadata -> raise "tag callback failed" end
        )

      assert_raise RuntimeError, "tag callback failed", fn ->
        OtelMetricExporter.handle_metric(
          [:test],
          %{missing: nil},
          %{},
          %{
            metrics: [
              {invalid_measurement_tag_failure, "test.invalid_measurement_tag_failure"}
            ],
            name: @name,
            handler_id: :unused
          }
        )
      end

      refute_receive {:measurement_dropped, _measurements, _metadata}
    end

    test "handles tag functions" do
      metrics = [
        Telemetry.Metrics.counter(
          "test.tags.value",
          measurement: :value,
          tags: [:dynamic],
          tag_values: fn metadata ->
            Map.put(metadata, :dynamic, "computed_#{metadata.input}")
          end
        )
      ]

      start_test_exporter(@base_config ++ [metrics: metrics])

      :telemetry.execute([:test, :tags], %{value: 42}, %{input: "test"})

      # Give the GenServer time to process the event
      Process.sleep(100)

      metrics = OtelMetricExporter.MetricStore.get_metrics(@name)
      assert get_in(metrics, [{:counter, "test.tags.value"}, %{dynamic: "computed_test"}]) == 1
    end

    test "handles detaching of handlers on shutdown" do
      test_event = :"event_#{inspect(self())}"

      metrics = [
        Telemetry.Metrics.sum("test.event.value", event_name: [:test, test_event])
      ]

      start_supervised!({OtelMetricExporter, @base_config ++ [metrics: metrics]})

      stop_supervised!(OtelMetricExporter)

      log =
        capture_log(fn ->
          :telemetry.execute([:test, test_event], %{value: 42}, %{test: "value"})
          # Give logger a moment to flush
          Process.sleep(50)
        end)

      refute log =~ "[:test, #{inspect(test_event)}]} has failed and has been detached."
    end

    @tag :metric_store_shutdown
    test "detaches handlers before the final drain", %{test: test_name} do
      bypass = Bypass.open()
      event = [:test, test_name, :detach_before_drain]
      parent = self()
      metric = Metrics.sum("test.detach_before_drain", event_name: event, measurement: :value)

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        assert :telemetry.list_handlers(event) == []
        :telemetry.execute(event, %{value: 99}, %{})
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        send(parent, {:detach_request, body})
        Plug.Conn.resp(conn, 200, "")
      end)

      config =
        @base_config
        |> Keyword.put(:name, test_name)
        |> Keyword.put(:otlp_endpoint, "http://localhost:#{bypass.port}")
        |> Keyword.put(:otlp_timeout, 1_000)
        |> Keyword.put(:aggregation_temporality, :delta)
        |> Keyword.put(:metrics, [metric])

      {:ok, exporter} = OtelMetricExporter.start_link(config)
      Process.unlink(exporter)

      on_exit(fn ->
        if Process.alive?(exporter), do: Supervisor.stop(exporter, :normal, 5_000)
      end)

      :telemetry.execute(event, %{value: 7}, %{})

      assert :ok = Supervisor.stop(exporter, :normal, 5_000)
      assert_receive {:detach_request, body}
      request = ExportMetricsServiceRequest.decode(body)

      assert [%{scope_metrics: [%{metrics: [%{data: {:sum, %{data_points: [point]}}}]}]}] =
               request.resource_metrics

      assert point.value == {:as_int, 7}
      refute Process.alive?(exporter)
    end

    @tag :metric_store_shutdown
    test "exports one final delta interval while Finch is alive", %{test: test_name} do
      bypass = Bypass.open()
      event = [:test, test_name, :final_delta]
      parent = self()
      metric = Metrics.sum("test.final_delta", event_name: event, measurement: :value)

      handler_id = {__MODULE__, test_name, :export_stop}

      :telemetry.attach(
        handler_id,
        [:otel_metric_exporter, :export, :stop],
        fn _event, measurements, metadata, _config ->
          send(parent, {:final_export, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        send(parent, {:final_request, Process.whereis(OtelMetricExporter.Finch), body})
        Plug.Conn.resp(conn, 200, "")
      end)

      config =
        @base_config
        |> Keyword.put(:name, test_name)
        |> Keyword.put(:otlp_endpoint, "http://localhost:#{bypass.port}")
        |> Keyword.put(:otlp_timeout, 1_000)
        |> Keyword.put(:aggregation_temporality, :delta)
        |> Keyword.put(:metrics, [metric])

      {:ok, exporter} = OtelMetricExporter.start_link(config)
      Process.unlink(exporter)

      on_exit(fn ->
        if Process.alive?(exporter), do: Supervisor.stop(exporter, :normal, 5_000)
      end)

      :telemetry.execute(event, %{value: 7}, %{})

      assert :ok = Supervisor.stop(exporter, :normal, 5_000)
      assert_receive {:final_request, finch, body}
      assert Process.alive?(finch)

      request = ExportMetricsServiceRequest.decode(body)

      assert [%{scope_metrics: [%{metrics: [%{data: {:sum, %{data_points: [point]}}}]}]}] =
               request.resource_metrics

      assert point.value == {:as_int, 7}

      assert_receive {:final_export, %{batch_size: 1, rejected_items: 0, dropped_items: 0},
                      %{scope: :metrics, outcome: :success}}

      refute_receive {:final_request, _, _}, 0
    end

    @tag :metric_store_shutdown
    test "reports attempted points when the final export fails", %{test: test_name} do
      bypass = Bypass.open()
      event = [:test, test_name, :final_failure]
      parent = self()
      metric = Metrics.sum("test.final_failure", event_name: event, measurement: :value)
      handler_id = {__MODULE__, test_name, :export_stop}

      :telemetry.attach(
        handler_id,
        [:otel_metric_exporter, :export, :stop],
        fn _event, measurements, metadata, _config ->
          send(parent, {:final_export, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        send(parent, {:final_request, Process.whereis(OtelMetricExporter.Finch)})
        Plug.Conn.resp(conn, 500, "permanent")
      end)

      config =
        @base_config
        |> Keyword.put(:name, test_name)
        |> Keyword.put(:otlp_endpoint, "http://localhost:#{bypass.port}")
        |> Keyword.put(:otlp_timeout, 1_000)
        |> Keyword.put(:metrics, [metric])

      {:ok, exporter} = OtelMetricExporter.start_link(config)
      Process.unlink(exporter)

      on_exit(fn ->
        if Process.alive?(exporter), do: Supervisor.stop(exporter, :normal, 5_000)
      end)

      :telemetry.execute(event, %{value: 7}, %{})

      capture_log(fn -> assert :ok = Supervisor.stop(exporter, :normal, 5_000) end)
      assert_receive {:final_request, finch}
      assert Process.alive?(finch)

      assert_receive {:final_export, %{batch_size: 1, rejected_items: 0, dropped_items: 1},
                      %{scope: :metrics, outcome: :terminal_http_status}}

      refute Process.alive?(exporter)
    end

    @tag :metric_store_shutdown
    test "preserves partial-success accounting on the final drain", %{test: test_name} do
      bypass = Bypass.open()
      event = [:test, test_name, :final_partial_success]
      parent = self()
      metric = Metrics.sum("test.final_partial_success", event_name: event, measurement: :value)
      handler_id = {__MODULE__, test_name, :export_stop}

      :telemetry.attach(
        handler_id,
        [:otel_metric_exporter, :export, :stop],
        fn _event, measurements, metadata, _config ->
          send(parent, {:final_export, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        response = %ExportMetricsServiceResponse{
          partial_success: %ExportMetricsPartialSuccess{rejected_data_points: 1}
        }

        Plug.Conn.resp(conn, 200, Protobuf.encode_to_iodata(response))
      end)

      config =
        @base_config
        |> Keyword.put(:name, test_name)
        |> Keyword.put(:otlp_endpoint, "http://localhost:#{bypass.port}")
        |> Keyword.put(:otlp_timeout, 1_000)
        |> Keyword.put(:metrics, [metric])

      {:ok, exporter} = OtelMetricExporter.start_link(config)
      Process.unlink(exporter)

      on_exit(fn ->
        if Process.alive?(exporter), do: Supervisor.stop(exporter, :normal, 5_000)
      end)

      :telemetry.execute(event, %{value: 7}, %{})

      assert :ok = Supervisor.stop(exporter, :normal, 5_000)

      assert_receive {:final_export, %{batch_size: 1, rejected_items: 1, dropped_items: 0},
                      %{scope: :metrics, outcome: :partial_success}}
    end

    @tag :metric_store_shutdown
    test "bounds a held final request by the shared deadline", %{test: test_name} do
      bypass = Bypass.open()
      endpoint = "http://localhost:#{bypass.port}"
      event = [:test, test_name, :held_final]
      parent = self()
      metric = Metrics.sum("test.held_final", event_name: event, measurement: :value)
      timeout = 500
      handler_id = {__MODULE__, test_name, :export_stop}

      :telemetry.attach(
        handler_id,
        [:otel_metric_exporter, :export, :stop],
        fn _event, measurements, metadata, _config ->
          send(parent, {:final_export, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      Bypass.expect_once(bypass, "POST", "/warm", fn conn ->
        Plug.Conn.resp(conn, 200, "")
      end)

      warm_request = Finch.build(:post, endpoint <> "/warm", [], <<>>)

      assert {:ok, %{status: 200}} =
               Finch.request(
                 warm_request,
                 OtelMetricExporter.Finch,
                 pool_timeout: 5_000,
                 receive_timeout: 5_000,
                 request_timeout: 5_000
               )

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        send(parent, {:held_final_request, self(), Process.whereis(OtelMetricExporter.Finch)})

        receive do
          :release ->
            Plug.Conn.resp(conn, 200, "")
        end
      end)

      config =
        @base_config
        |> Keyword.put(:name, test_name)
        |> Keyword.put(:otlp_endpoint, endpoint)
        |> Keyword.put(:otlp_timeout, timeout)
        |> Keyword.put(:metrics, [metric])

      assert {:ok, prepared} = MetricStore.prepare_config(Map.new(config))
      shutdown_allowance = MetricStore.shutdown_timeout(prepared.api)

      {:ok, exporter} = OtelMetricExporter.start_link(config)
      Process.unlink(exporter)

      on_exit(fn ->
        if Process.alive?(exporter), do: Supervisor.stop(exporter, :normal, 5_000)
      end)

      :telemetry.execute(event, %{value: 7}, %{})

      started_at = System.monotonic_time(:millisecond)
      stop_ref = make_ref()

      spawn(fn ->
        Process.flag(:trap_exit, true)
        send(parent, {stop_ref, Supervisor.stop(exporter, :normal, 5_000)})
      end)

      assert_receive {:held_final_request, request_pid, finch}, 5_000

      on_exit(fn ->
        Bypass.pass(bypass)
        send(request_pid, :release)
      end)

      assert Process.alive?(finch)
      assert_receive {^stop_ref, :ok}, shutdown_allowance
      elapsed = System.monotonic_time(:millisecond) - started_at
      assert elapsed <= shutdown_allowance

      Bypass.pass(bypass)
      send(request_pid, :release)

      assert_receive {:final_export, %{batch_size: 1, dropped_items: 1},
                      %{scope: :metrics, outcome: :retryable_deadline_exceeded}}

      refute Process.alive?(exporter)
    end

    @tag :metric_store_shutdown
    test "derives child shutdown allowance from effective OTLP timeout", %{test: test_name} do
      config =
        @base_config
        |> Keyword.put(:name, test_name)
        |> Keyword.put(:otlp_timeout, 321)

      assert {:ok, prepared} = MetricStore.prepare_config(Map.new(config))
      child_spec = MetricStore.child_spec({:prepared, prepared})

      assert child_spec.shutdown == 1_642
      assert child_spec.start == {MetricStore, :start_link, [{:prepared, prepared}]}
    end

    @tag :metric_store_shutdown
    test "allows a final drain after an in-flight export", %{test: test_name} do
      bypass = Bypass.open()
      endpoint = "http://localhost:#{bypass.port}"
      event = [:test, test_name, :overlap]
      parent = self()
      metric = Metrics.sum("test.overlap", event_name: event, measurement: :value)
      timeout = 500
      handler_id = {__MODULE__, test_name, :export_stop}

      :telemetry.attach(
        handler_id,
        [:otel_metric_exporter, :export, :stop],
        fn _event, measurements, metadata, _config ->
          send(parent, {:overlap_export, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      Bypass.expect_once(bypass, "POST", "/warm", fn conn ->
        Plug.Conn.resp(conn, 200, "")
      end)

      warm_request = Finch.build(:post, endpoint <> "/warm", [], <<>>)

      assert {:ok, %{status: 200}} =
               Finch.request(
                 warm_request,
                 OtelMetricExporter.Finch,
                 pool_timeout: 5_000,
                 receive_timeout: 5_000,
                 request_timeout: 5_000
               )

      Bypass.expect(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        send(parent, {:overlap_request, self(), body})

        receive do
          {:respond, status} ->
            Plug.Conn.resp(conn, status, "")
        end
      end)

      config =
        @base_config
        |> Keyword.put(:name, test_name)
        |> Keyword.put(:otlp_endpoint, endpoint)
        |> Keyword.put(:otlp_timeout, timeout)
        |> Keyword.put(:export_period, 60_000)
        |> Keyword.put(:aggregation_temporality, :delta)
        |> Keyword.put(:metrics, [metric])

      assert {:ok, prepared} = MetricStore.prepare_config(Map.new(config))
      shutdown_allowance = MetricStore.shutdown_timeout(prepared.api)

      {:ok, exporter} = OtelMetricExporter.start_link(config)
      Process.unlink(exporter)

      on_exit(fn ->
        if Process.alive?(exporter), do: Supervisor.stop(exporter, :normal, 5_000)
      end)

      :telemetry.execute(event, %{value: 1}, %{})
      send(Process.whereis(test_name), :export)

      assert_receive {:overlap_request, first_request_pid, first_body}, 5_000

      first_request = ExportMetricsServiceRequest.decode(first_body)

      assert [%{scope_metrics: [%{metrics: [%{data: {:sum, %{data_points: [first_point]}}}]}]}] =
               first_request.resource_metrics

      assert first_point.value == {:as_int, 1}

      on_exit(fn ->
        Bypass.pass(bypass)
        send(first_request_pid, {:respond, 200})
      end)

      :telemetry.execute(event, %{value: 2}, %{})

      started_at = System.monotonic_time(:millisecond)
      stop_ref = make_ref()

      spawn(fn ->
        send(parent, {stop_ref, Supervisor.stop(exporter, :normal, 5_000)})
      end)

      assert_receive {:overlap_request, final_request_pid, final_body}, shutdown_allowance
      send(final_request_pid, {:respond, 200})

      assert_receive {^stop_ref, :ok}, shutdown_allowance
      elapsed = System.monotonic_time(:millisecond) - started_at
      assert elapsed <= shutdown_allowance

      send(first_request_pid, {:respond, 200})
      Bypass.pass(bypass)

      final_request = ExportMetricsServiceRequest.decode(final_body)

      assert [%{scope_metrics: [%{metrics: [%{data: {:sum, %{data_points: [final_point]}}}]}]}] =
               final_request.resource_metrics

      assert final_point.value == {:as_int, 3}

      assert_receive {:overlap_export, %{batch_size: 1, rejected_items: 0, dropped_items: 0},
                      %{scope: :metrics, outcome: :retryable_deadline_exceeded}}

      assert_receive {:overlap_export, %{batch_size: 1, rejected_items: 0, dropped_items: 0},
                      %{scope: :metrics, outcome: :success}}
    end

    @tag :metric_store_shutdown
    test "does not drain on abnormal termination", %{test: test_name} do
      bypass = Bypass.open()
      parent = self()
      event = [:test, test_name, :abnormal_termination]
      metric = Metrics.sum("test.abnormal_termination", event_name: event, measurement: :value)

      Bypass.stub(bypass, "POST", "/v1/metrics", fn conn ->
        send(parent, :unexpected_final_request)
        Plug.Conn.resp(conn, 200, "")
      end)

      handler_id = {__MODULE__, test_name, :export_stop}

      :telemetry.attach(
        handler_id,
        [:otel_metric_exporter, :export, :stop],
        fn _event, measurements, metadata, _config ->
          send(parent, {:abnormal_export, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      config =
        @base_config
        |> Keyword.put(:name, test_name)
        |> Keyword.put(:otlp_endpoint, "http://localhost:#{bypass.port}")
        |> Keyword.put(:metrics, [metric])
        |> Map.new()

      {:ok, store} = MetricStore.start_link(config)
      Process.unlink(store)
      MetricStore.write_metric(test_name, metric, 7, %{})

      capture_log(fn -> assert :ok = GenServer.stop(store, :abnormal, 5_000) end)
      refute_receive :unexpected_final_request, 0
      refute_receive {:abnormal_export, _, _}, 0
    end

    @tag :metric_store_shutdown
    test "does not export an empty shutdown", %{test: test_name} do
      bypass = Bypass.open()
      parent = self()
      handler_id = {__MODULE__, test_name, :export_stop}

      Bypass.stub(bypass, "POST", "/v1/metrics", fn conn ->
        send(parent, :unexpected_final_request)
        Plug.Conn.resp(conn, 200, "")
      end)

      :telemetry.attach(
        handler_id,
        [:otel_metric_exporter, :export, :stop],
        fn event, measurements, metadata, _config ->
          send(parent, {:final_export, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      config =
        @base_config
        |> Keyword.put(:name, test_name)
        |> Keyword.put(:otlp_endpoint, "http://localhost:#{bypass.port}")
        |> Keyword.put(:metrics, [])

      {:ok, exporter} = OtelMetricExporter.start_link(config)
      Process.unlink(exporter)

      on_exit(fn ->
        if Process.alive?(exporter), do: Supervisor.stop(exporter, :normal, 5_000)
      end)

      assert :ok = Supervisor.stop(exporter, :normal, 5_000)
      refute_receive :unexpected_final_request, 0
      refute_receive {:final_export, _, _, _}, 0
    end

    test "handles detaching of handlers if ETS table missing" do
      test_event = :"event_#{inspect(self())}"

      metrics = [
        Telemetry.Metrics.sum("test.event.value", event_name: [:test, test_event])
      ]

      start_test_exporter(@base_config ++ [metrics: metrics])

      :ets.delete(@name)

      log =
        capture_log(fn ->
          :telemetry.execute([:test, test_event], %{value: 42}, %{test: "value"})
          # Give logger a moment to flush
          Process.sleep(50)
        end)

      assert log =~ "OtelMetricExporter failed to process event due to ETS table missing"
      refute log =~ "[:test, #{inspect(test_event)}]} has failed and has been detached."
    end
  end
end
