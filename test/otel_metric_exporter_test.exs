defmodule OtelMetricExporterTest do
  use ExUnit.Case
  alias Telemetry.Metrics
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

      start_supervised!({OtelMetricExporter, @base_config ++ [metrics: metrics]})

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

      start_supervised!({OtelMetricExporter, @base_config ++ [metrics: metrics]})

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

      start_supervised!({OtelMetricExporter, @base_config ++ [metrics: metrics]})

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

      exporter = start_supervised!({OtelMetricExporter, @base_config ++ [metrics: metrics]})

      :telemetry.execute(event, %{}, %{})
      :telemetry.execute(event, %{value: 3}, %{})

      assert Process.alive?(exporter)
      assert :telemetry.list_handlers(event) != []

      assert get_in(OtelMetricExporter.MetricStore.get_metrics(@name), [
               {:counter, "test.counter"},
               %{}
             ]) == 1

      assert get_in(OtelMetricExporter.MetricStore.get_metrics(@name), [
               {:distribution, "test.distribution"},
               %{},
               1
             ]) == {1, 3}
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

      exporter = start_supervised!({OtelMetricExporter, @base_config ++ [metrics: metrics]})

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
      start_supervised!({OtelMetricExporter, @base_config ++ [metrics: []]})

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

      start_supervised!({OtelMetricExporter, @base_config ++ [metrics: metrics]})

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

    test "handles detaching of handlers if ETS table missing" do
      test_event = :"event_#{inspect(self())}"

      metrics = [
        Telemetry.Metrics.sum("test.event.value", event_name: [:test, test_event])
      ]

      start_supervised!({OtelMetricExporter, @base_config ++ [metrics: metrics]})

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
