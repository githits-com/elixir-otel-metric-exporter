defmodule OtelMetricExporter.MetricStoreBench.Loopback do
  @moduledoc false

  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest

  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.{
    Gauge,
    Histogram,
    Metric,
    NumberDataPoint,
    Sum
  }

  @type request :: %{
          required(:sequence) => pos_integer(),
          required(:status) => pos_integer(),
          required(:data_points) => non_neg_integer(),
          required(:distinct_series) => non_neg_integer(),
          optional(:decode_error) => String.t()
        }

  @spec start([pos_integer()]) :: %{
          pid: pid(),
          port: :inet.port_number()
        }
  def start(statuses) when is_list(statuses) and statuses != [] do
    {:ok, listen_socket} =
      :gen_tcp.listen(0, [
        :binary,
        {:active, false},
        {:ip, {127, 0, 0, 1}},
        {:reuseaddr, true}
      ])

    {:ok, {_address, port}} = :inet.sockname(listen_socket)
    pid = spawn(fn -> loop(listen_socket, statuses, []) end)
    %{pid: pid, port: port}
  end

  @spec stop(%{pid: pid()}) :: [request()]
  def stop(%{pid: pid}) do
    ref = make_ref()
    send(pid, {:stop, self(), ref})

    receive do
      {:stopped, ^ref, requests} -> requests
    after
      5_000 -> raise "loopback responder did not stop"
    end
  end

  defp loop(listen_socket, statuses, requests) do
    receive do
      {:request, connection, body} ->
        {status, statuses} = next_status(statuses)
        request = decode_request(body, length(requests) + 1, status)
        send(connection, {:response, response(status)})
        loop(listen_socket, statuses, [request | requests])

      {:stop, caller, ref} ->
        :ok = :gen_tcp.close(listen_socket)
        send(caller, {:stopped, ref, Enum.reverse(requests)})
    after
      0 ->
        case :gen_tcp.accept(listen_socket, 1) do
          {:ok, connection} ->
            server = self()
            spawn(fn -> serve_connection(connection, server) end)
            loop(listen_socket, statuses, requests)

          {:error, :timeout} ->
            loop(listen_socket, statuses, requests)

          {:error, :closed} ->
            :ok
        end
    end
  end

  defp next_status([status | rest]), do: {status, rest}
  defp next_status([]), do: {500, []}

  defp serve_connection(connection, server) do
    case read_request(connection, <<>>) do
      {:ok, body} ->
        send(server, {:request, self(), body})

        receive do
          {:response, response} ->
            :ok = :gen_tcp.send(connection, response)
            :ok = :gen_tcp.close(connection)
        after
          5_000 -> :gen_tcp.close(connection)
        end

      {:error, _reason} ->
        :gen_tcp.close(connection)
    end
  end

  defp read_request(connection, buffer) do
    case :binary.match(buffer, "\r\n\r\n") do
      {header_end, 4} ->
        header = binary_part(buffer, 0, header_end)
        body_start = header_end + 4
        body = binary_part(buffer, body_start, byte_size(buffer) - body_start)

        with {:ok, content_length} <- content_length(header),
             {:ok, body} <- read_body(connection, body, content_length) do
          {:ok, body}
        end

      :nomatch ->
        case :gen_tcp.recv(connection, 0, 5_000) do
          {:ok, chunk} -> read_request(connection, buffer <> chunk)
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp content_length(header) do
    header
    |> :binary.split("\r\n", [:global])
    |> Enum.find_value(fn line ->
      case String.split(line, ":", parts: 2) do
        [name, value] when name == "content-length" ->
          case Integer.parse(String.trim(value)) do
            {length, ""} when length >= 0 -> {:ok, length}
            _ -> nil
          end

        _ ->
          nil
      end
    end)
    |> case do
      nil -> {:error, :missing_content_length}
      result -> result
    end
  end

  defp read_body(_connection, body, content_length) when byte_size(body) >= content_length,
    do: {:ok, binary_part(body, 0, content_length)}

  defp read_body(connection, body, content_length) do
    case :gen_tcp.recv(connection, content_length - byte_size(body), 5_000) do
      {:ok, chunk} -> read_body(connection, body <> chunk, content_length)
      {:error, reason} -> {:error, reason}
    end
  end

  defp response(200), do: http_response(200, "OK", <<>>)
  defp response(503), do: http_response(503, "Service Unavailable", <<>>)
  defp response(500), do: http_response(500, "Internal Server Error", <<>>)

  defp http_response(status, reason, body) do
    [
      "HTTP/1.1 ",
      Integer.to_string(status),
      " ",
      reason,
      "\r\ncontent-type: application/x-protobuf\r\ncontent-length: ",
      Integer.to_string(byte_size(body)),
      "\r\nconnection: close\r\n\r\n",
      body
    ]
  end

  defp decode_request(body, sequence, status) do
    case decode_metrics(body) do
      {:ok, data_points, distinct_series} ->
        %{
          sequence: sequence,
          status: status,
          data_points: data_points,
          distinct_series: distinct_series
        }

      {:error, reason} ->
        %{
          sequence: sequence,
          status: status,
          data_points: 0,
          distinct_series: 0,
          decode_error: reason
        }
    end
  end

  defp decode_metrics(body) do
    request = Protobuf.decode(body, ExportMetricsServiceRequest)

    points =
      for resource_metrics <- request.resource_metrics,
          scope_metrics <- resource_metrics.scope_metrics,
          metric <- scope_metrics.metrics,
          point <- metric_points(metric),
          do: {metric.name, point}

    {:ok, length(points), points |> Enum.map(&series_key/1) |> MapSet.new() |> MapSet.size()}
  rescue
    error -> {:error, error.__struct__ |> Module.split() |> Enum.join(".")}
  catch
    _kind, reason -> {:error, inspect(reason)}
  end

  defp metric_points(%Metric{data: {:sum, %Sum{data_points: points}}}), do: points
  defp metric_points(%Metric{data: {:gauge, %Gauge{data_points: points}}}), do: points
  defp metric_points(%Metric{data: {:histogram, %Histogram{data_points: points}}}), do: points
  defp metric_points(_metric), do: []

  defp series_key({metric_name, %NumberDataPoint{attributes: attributes}}),
    do: {metric_name, attributes}

  defp series_key({metric_name, %{attributes: attributes}}), do: {metric_name, attributes}
end

defmodule OtelMetricExporter.MetricStoreBench do
  @moduledoc false

  alias OtelMetricExporter.MetricStore
  alias OtelMetricExporter.MetricStoreBench.Loopback
  alias Telemetry.Metrics

  @baseline_revision "1a35ce558607b60921a03ffd98923d416b46bfdb"
  @baseline_metric_store_source_sha "bc215141d51b73fac6e535558700303e474dac77"
  @production_source_paths ["lib", "config", "mix.exs", "mix.lock"]
  @multi_series 500
  @operations_per_writer 500
  @collection_series [500, 5_000]
  @warmup_repetitions 2
  @measured_repetitions 21
  @retryable_intervals 10
  @nanoseconds_per_second 1_000_000_000

  def run do
    Logger.configure(level: :none)
    production_source_unchanged? = print_metadata()

    write_scenarios()
    float_sum_failure(production_source_unchanged?)

    for series <- @collection_series do
      normal_collection(series)
      recovery_collection(series, production_source_unchanged?)
    end

    :ok
  end

  defp print_metadata do
    {revision, 0} = System.cmd("git", ["rev-parse", "HEAD"], stderr_to_stdout: true)
    revision = String.trim(revision)

    {metric_store_source_sha, 0} =
      System.cmd("git", ["hash-object", "lib/otel_metric_exporter/metric_store.ex"],
        stderr_to_stdout: true
      )

    metric_store_source_sha = String.trim(metric_store_source_sha)
    production_source_unchanged? = production_source_unchanged?()
    {:ok, hostname} = :inet.gethostname()

    IO.puts("BENCHMARK metric_store")
    IO.puts("metadata production_code_revision=#{revision}")
    IO.puts("metadata baseline_reference_revision=#{@baseline_revision}")
    IO.puts("metadata baseline_revision_match=#{revision == @baseline_revision}")
    IO.puts("metadata metric_store_source_sha=#{metric_store_source_sha}")
    IO.puts("metadata baseline_metric_store_source_sha=#{@baseline_metric_store_source_sha}")
    IO.puts("metadata production_source_paths=#{Enum.join(@production_source_paths, ",")}")
    IO.puts("metadata production_source_predicate=tracked_diff_quiet_and_no_untracked_files")
    IO.puts("metadata production_source_unchanged=#{production_source_unchanged?}")
    IO.puts("metadata host=#{hostname}")
    IO.puts("metadata elixir_version=#{System.version()}")
    IO.puts("metadata otp_release=#{:erlang.system_info(:otp_release)}")
    IO.puts("metadata schedulers_online=#{System.schedulers_online()}")
    IO.puts("metadata multi_series_cardinality=#{@multi_series}")
    IO.puts("metadata operations_per_writer=#{@operations_per_writer}")
    IO.puts("metadata scheduler_contention_writer_count=#{System.schedulers_online()}")
    IO.puts("metadata write_matrix=4_metric_types_x_2_modes_x_2_tag_shapes")
    IO.puts("metadata contention_start_boundary=all_workers_ready_then_single_release")
    IO.puts("metadata collection_series=#{inspect(@collection_series)}")
    IO.puts("metadata warmup_repetitions=#{@warmup_repetitions}")
    IO.puts("metadata measured_repetitions=#{@measured_repetitions}")
    IO.puts("metadata retryable_intervals=#{@retryable_intervals}")
    IO.puts("metadata write_units=throughput_writes_per_second,write_latency_microseconds")
    IO.puts("metadata collection_units=throughput_points_per_second,latency_milliseconds")

    IO.puts(
      "metadata throughput_timing_method=wall_clock_around_write_loop_no_per_write_timestamps"
    )

    IO.puts("metadata latency_timing_method=per_write_monotonic_timestamps")
    IO.puts("metadata paired_write_sample_sets=separate_throughput_and_latency_stores")
    IO.puts("metadata temporality_for_collection_and_recovery=delta")
    IO.puts("metadata responder=gen_tcp_loopback_uncompressed_deterministic_http")
    IO.puts("metadata responder_decode=generated_export_metrics_service_request")
    production_source_unchanged?
  end

  defp production_source_unchanged? do
    {_output, tracked_status} =
      System.cmd("git", ["diff", "--quiet", @baseline_revision, "--" | @production_source_paths])

    tracked_clean? =
      case tracked_status do
        0 -> true
        1 -> false
        status -> raise "production source comparison failed with status #{status}"
      end

    {untracked_sources, untracked_status} =
      System.cmd("git", [
        "ls-files",
        "--others",
        "--exclude-standard",
        "--" | @production_source_paths
      ])

    case untracked_status do
      0 -> tracked_clean? and String.trim(untracked_sources) == ""
      status -> raise "untracked production source comparison failed with status #{status}"
    end
  end

  defp write_scenarios do
    scenarios = [
      {"counter", Metrics.counter("bench.counter"), fn _index -> 1 end},
      {"integer_sum", Metrics.sum("bench.integer_sum"), fn _index -> 7 end},
      {"integer_distribution", Metrics.distribution("bench.integer_distribution"),
       fn index -> rem(index, 17) end},
      {"fractional_distribution", Metrics.distribution("bench.fractional_distribution"),
       fn index -> (rem(index, 17) - 8) / 4 end}
    ]

    modes = [
      {"single_writer", 1},
      {"scheduler_count_contention", System.schedulers_online()}
    ]

    tag_shapes = [
      {"hot_one_series", 1, :hot},
      {"bounded_multi_series", @multi_series, :multi}
    ]

    Enum.each(scenarios, fn {label, metric, value_for} ->
      Enum.each(modes, fn {mode, writer_count} ->
        Enum.each(tag_shapes, fn {tag_shape, cardinality, shape} ->
          run_write_case(
            label,
            metric,
            value_for,
            mode,
            writer_count,
            tag_shape,
            cardinality,
            shape
          )
        end)
      end)
    end)
  end

  defp run_write_case(
         label,
         metric,
         value_for,
         mode,
         writer_count,
         tag_shape,
         cardinality,
         shape
       ) do
    {throughput_samples, latency_samples, operations_per_writer} =
      measure_write_case(label, metric, value_for, writer_count, tag_shape, cardinality, shape)

    print_write_result(
      label,
      mode,
      writer_count,
      tag_shape,
      cardinality,
      throughput_samples,
      latency_samples,
      operations_per_writer
    )
  end

  defp measure_write_case(label, metric, value_for, writer_count, tag_shape, cardinality, shape) do
    writes = writes_for(cardinality, value_for, shape)

    throughput_store =
      start_store("write_#{label}_#{tag_shape}_throughput", metric, "http://127.0.0.1:1")

    latency_store =
      start_store("write_#{label}_#{tag_shape}_latency", metric, "http://127.0.0.1:1")

    Enum.each(1..@warmup_repetitions, fn _ ->
      write_sample(throughput_store, metric, writes, writer_count, :throughput)
      write_sample(latency_store, metric, writes, writer_count, :latency)
    end)

    throughput_samples =
      Enum.map(1..@measured_repetitions, fn _ ->
        write_sample(throughput_store, metric, writes, writer_count, :throughput)
      end)

    latency_samples =
      Enum.map(1..@measured_repetitions, fn _ ->
        write_sample(latency_store, metric, writes, writer_count, :latency)
      end)

    stop_store(throughput_store)
    stop_store(latency_store)
    {throughput_samples, latency_samples, length(writes)}
  end

  defp float_sum_failure(production_source_unchanged?) do
    metric = Metrics.sum("bench.float_sum")
    probe_store = start_store("float_sum_probe", metric, "http://127.0.0.1:1")
    probe_result = probe_float_sum(probe_store, metric)
    stop_store(probe_store)

    case {production_source_unchanged?, probe_result} do
      {true, {:crash, :badarg}} ->
        IO.puts("RESULT write scenario=float_sum mode=pre_change_failure")
        IO.puts("result float_sum_production_source_unchanged=#{production_source_unchanged?}")
        IO.puts("result float_sum_throughput_baseline=not_recorded_until_correction")
        IO.puts("result float_sum_failure_observed=true")
        IO.puts("result float_sum_failure_classification=badarg")

        IO.puts("result float_sum_planned_pre_change_failure_confirmed=true")

      {false, :ok} ->
        {throughput_samples, latency_samples, operations_per_writer} =
          measure_write_case(
            "float_sum_absolute",
            metric,
            fn index -> (rem(index, 17) - 8) / 4 end,
            1,
            "bounded_multi_series",
            @multi_series,
            :multi
          )

        IO.puts("RESULT write scenario=float_sum mode=absolute_corrected")
        IO.puts("result float_sum_production_source_unchanged=#{production_source_unchanged?}")
        IO.puts("result float_sum_throughput_baseline=absolute_not_paired_or_gated")
        IO.puts("result float_sum_failure_observed=false")
        IO.puts("result float_sum_failure_classification=none")
        IO.puts("result series_cardinality=#{@multi_series}")
        IO.puts("result tag_shape=bounded_multi_series")
        IO.puts("result mode=single_writer")
        IO.puts("result writer_count=1")
        IO.puts("result operations_per_sample=#{operations_per_writer}")
        IO.puts("result warmup_repetitions=#{@warmup_repetitions}")
        IO.puts("result repetitions=#{length(throughput_samples)}")
        print_write_statistics(throughput_samples, latency_samples, operations_per_writer, 1)
        IO.puts("result float_sum_planned_pre_change_failure_confirmed=false")

      {source_unchanged?, probe_result} ->
        print_float_sum_probe_result(source_unchanged?, probe_result)

        raise "float sum gate failed: source_unchanged=#{source_unchanged?} probe=#{inspect(probe_result)}"
    end
  end

  defp print_float_sum_probe_result(source_unchanged?, probe_result) do
    {observed, classification} =
      case probe_result do
        {:crash, value} -> {true, value}
        :ok -> {false, :none}
        {:error, value} -> {false, value}
      end

    IO.puts("RESULT write scenario=float_sum mode=invalid_probe")
    IO.puts("result float_sum_production_source_unchanged=#{source_unchanged?}")
    IO.puts("result float_sum_throughput_baseline=not_recorded_until_correction")
    IO.puts("result float_sum_failure_observed=#{observed}")
    IO.puts("result float_sum_failure_classification=#{inspect(classification)}")
    IO.puts("result float_sum_planned_pre_change_failure_confirmed=false")
  end

  defp probe_float_sum(store, metric) do
    {pid, ref} = spawn_monitor(fn -> MetricStore.write_metric(store.name, metric, 0.5, %{}) end)

    receive do
      {:DOWN, ^ref, :process, ^pid, :normal} -> :ok
      {:DOWN, ^ref, :process, ^pid, reason} -> {:crash, classify_exit(reason)}
    after
      5_000 -> {:error, :timeout}
    end
  end

  defp normal_collection(series) do
    statuses = List.duplicate(200, @warmup_repetitions + @measured_repetitions)
    responder = Loopback.start(statuses)
    metric = Metrics.sum("bench.normal_#{series}")
    store = start_store("normal_#{series}", metric, "http://127.0.0.1:#{responder.port}")
    writes = collection_writes_for(series, fn _index -> 1 end)

    Enum.each(1..@warmup_repetitions, fn _ ->
      Enum.each(writes, &write(store, metric, &1))
      :ok = MetricStore.export_sync(store.name)
    end)

    samples =
      Enum.map(1..@measured_repetitions, fn _ ->
        Enum.each(writes, &write(store, metric, &1))
        started = monotonic_ns()
        :ok = MetricStore.export_sync(store.name)
        monotonic_ns() - started
      end)

    stop_store(store)
    requests = Loopback.stop(responder)
    assert_requests!(requests, statuses, series)
    final = List.last(requests)

    print_collection_result("normal_delta", series, samples, final, length(writes))
  end

  defp recovery_collection(series, production_source_unchanged?) do
    statuses = List.duplicate(503, @retryable_intervals) ++ [200]
    responder = Loopback.start(statuses)
    metric = Metrics.sum("bench.recovery_#{series}")
    store = start_store("recovery_#{series}", metric, "http://127.0.0.1:#{responder.port}")
    writes = collection_writes_for(series, fn _index -> 1 end)
    started = monotonic_ns()

    retryable_results =
      Enum.map(1..@retryable_intervals, fn _interval ->
        Enum.each(writes, &write(store, metric, &1))
        result_started = monotonic_ns()
        result = MetricStore.export_sync(store.name)
        elapsed = monotonic_ns() - result_started
        {:error, :retryable, {:http_status, 503}} = result
        elapsed
      end)

    final_started = monotonic_ns()
    :ok = MetricStore.export_sync(store.name)
    final_elapsed = monotonic_ns() - final_started
    total_elapsed = monotonic_ns() - started

    stop_store(store)
    requests = Loopback.stop(responder)
    assert_requests!(requests, statuses, series)
    final = List.last(requests)

    target_met = final.data_points == series and final.distinct_series == series

    if not production_source_unchanged? and not target_met do
      raise "recovery wire point mismatch after MetricStore change: expected=#{series} observed=#{final.data_points}"
    end

    print_recovery_result(
      series,
      retryable_results,
      final_elapsed,
      total_elapsed,
      final,
      length(writes),
      production_source_unchanged?
    )
  end

  defp write_sample(store, metric, writes, writer_count, timing) do
    parent = self()
    release_ref = make_ref()

    workers =
      Enum.map(1..writer_count, fn _ ->
        {pid, monitor_ref} =
          spawn_monitor(fn ->
            send(parent, {:writer_ready, self(), release_ref})

            receive do
              {:writer_release, ^release_ref} ->
                result = write_sample_body(store, metric, writes, timing)
                send(parent, {:writer_done, self(), release_ref, result})

                receive do
                  {:writer_stop, ^release_ref} -> :ok
                end
            end
          end)

        {pid, monitor_ref}
      end)

    worker_refs = Map.new(workers)
    await_writers_ready(release_ref, worker_refs, MapSet.new())
    started = monotonic_ns()
    Enum.each(workers, fn {pid, _monitor_ref} -> send(pid, {:writer_release, release_ref}) end)
    results = await_writers_done(release_ref, worker_refs, [])
    elapsed = monotonic_ns() - started
    stop_writers(release_ref, worker_refs)

    result =
      case timing do
        :throughput -> []
        :latency -> List.flatten(results)
      end

    {elapsed, result}
  end

  defp await_writers_ready(release_ref, worker_refs, ready) do
    if MapSet.size(ready) == map_size(worker_refs) do
      :ok
    else
      receive do
        {:writer_ready, pid, ^release_ref} ->
          if Map.has_key?(worker_refs, pid) and not MapSet.member?(ready, pid) do
            await_writers_ready(release_ref, worker_refs, MapSet.put(ready, pid))
          else
            raise "unexpected or duplicate contention worker ready message"
          end

        {:DOWN, monitor_ref, :process, pid, reason} ->
          if Map.get(worker_refs, pid) == monitor_ref do
            raise "contention worker exited before release: #{inspect(reason)}"
          else
            raise "unexpected contention worker monitor message"
          end
      end
    end
  end

  defp await_writers_done(_release_ref, pending, latencies) when map_size(pending) == 0,
    do: latencies

  defp await_writers_done(release_ref, pending, latencies) do
    receive do
      {:writer_done, pid, ^release_ref, writer_latencies} ->
        case Map.pop(pending, pid) do
          {monitor_ref, pending} when is_reference(monitor_ref) ->
            await_writers_done(release_ref, pending, [writer_latencies | latencies])

          {nil, _pending} ->
            raise "unexpected or duplicate contention worker completion"
        end

      {:DOWN, monitor_ref, :process, pid, reason} ->
        if Map.get(pending, pid) == monitor_ref do
          raise "contention worker exited before completion: #{inspect(reason)}"
        else
          raise "unexpected contention worker monitor message"
        end
    end
  end

  defp stop_writers(release_ref, worker_refs) do
    Enum.each(Map.keys(worker_refs), fn pid -> send(pid, {:writer_stop, release_ref}) end)
    await_writers_stopped(worker_refs)
  end

  defp await_writers_stopped(pending) when map_size(pending) == 0, do: :ok

  defp await_writers_stopped(pending) do
    receive do
      {:DOWN, monitor_ref, :process, pid, :normal} ->
        case Map.pop(pending, pid) do
          {^monitor_ref, pending} ->
            Process.demonitor(monitor_ref, [:flush])
            await_writers_stopped(pending)

          {nil, _pending} ->
            raise "unexpected contention worker monitor message"
        end

      {:DOWN, monitor_ref, :process, pid, reason} ->
        if Map.get(pending, pid) == monitor_ref do
          raise "contention worker exited abnormally during cleanup: #{inspect(reason)}"
        else
          raise "unexpected contention worker monitor message"
        end
    end
  end

  defp write_sample_body(store, metric, writes, :throughput) do
    Enum.each(writes, &write(store, metric, &1))
    :ok
  end

  defp write_sample_body(store, metric, writes, :latency),
    do: measured_write_sample(store, metric, writes)

  defp measured_write_sample(store, metric, writes) do
    Enum.map(writes, fn {value, tags} ->
      write_started = monotonic_ns()
      MetricStore.write_metric(store.name, metric, value, tags)
      monotonic_ns() - write_started
    end)
  end

  defp print_write_result(
         label,
         mode,
         writer_count,
         tag_shape,
         cardinality,
         throughput_samples,
         latency_samples,
         operations_per_writer
       ) do
    elapsed = Enum.map(throughput_samples, &elem(&1, 0))
    latencies = latency_samples |> Enum.flat_map(&elem(&1, 1)) |> Enum.map(&(&1 / 1_000))
    operations_per_sample = operations_per_writer * writer_count
    throughput = Enum.map(elapsed, &(operations_per_sample * @nanoseconds_per_second / &1))

    IO.puts("RESULT write scenario=#{label} mode=#{mode} tag_shape=#{tag_shape}")
    IO.puts("result writer_count=#{writer_count}")
    IO.puts("result series_cardinality=#{cardinality}")
    IO.puts("result operations_per_writer=#{operations_per_writer}")
    IO.puts("result operations_per_sample=#{operations_per_sample}")
    IO.puts("result throughput_samples=#{length(throughput_samples)}")
    IO.puts("result latency_samples=#{length(latency_samples)}")

    IO.puts(
      "result throughput_timing_method=wall_clock_around_write_loop_no_per_write_timestamps"
    )

    IO.puts("result latency_timing_method=per_write_monotonic_timestamps")
    IO.puts("result warmup_repetitions=#{@warmup_repetitions}")
    IO.puts("result repetitions=#{length(throughput_samples)}")
    IO.puts("result measured_writes=#{operations_per_sample * length(throughput_samples)}")
    IO.puts("result median_throughput_writes_per_second=#{format_number(median(throughput))}")
    IO.puts("result median_write_latency_microseconds=#{format_number(median(latencies))}")
    IO.puts("result p99_write_latency_microseconds=#{format_number(percentile(latencies, 0.99))}")
    IO.puts("result maximum_write_latency_microseconds=#{format_number(Enum.max(latencies))}")
  end

  defp print_write_statistics(
         throughput_samples,
         latency_samples,
         operations_per_writer,
         writer_count
       ) do
    elapsed = Enum.map(throughput_samples, &elem(&1, 0))
    latencies = latency_samples |> Enum.flat_map(&elem(&1, 1)) |> Enum.map(&(&1 / 1_000))
    operations_per_sample = operations_per_writer * writer_count
    throughput = Enum.map(elapsed, &(operations_per_sample * @nanoseconds_per_second / &1))

    IO.puts("result throughput_samples=#{length(throughput_samples)}")
    IO.puts("result latency_samples=#{length(latency_samples)}")

    IO.puts(
      "result throughput_timing_method=wall_clock_around_write_loop_no_per_write_timestamps"
    )

    IO.puts("result latency_timing_method=per_write_monotonic_timestamps")
    IO.puts("result measured_writes=#{operations_per_sample * length(throughput_samples)}")
    IO.puts("result median_throughput_writes_per_second=#{format_number(median(throughput))}")
    IO.puts("result median_write_latency_microseconds=#{format_number(median(latencies))}")
    IO.puts("result p99_write_latency_microseconds=#{format_number(percentile(latencies, 0.99))}")
    IO.puts("result maximum_write_latency_microseconds=#{format_number(Enum.max(latencies))}")
  end

  defp print_collection_result(label, series, samples, final, points_per_sample) do
    throughput = Enum.map(samples, &(points_per_sample * @nanoseconds_per_second / &1))
    latencies = Enum.map(samples, &(&1 / 1_000_000))

    IO.puts("RESULT collection scenario=#{label} series=#{series} temporality=delta")
    IO.puts("result warmup_repetitions=#{@warmup_repetitions}")
    IO.puts("result repetitions=#{length(samples)}")
    IO.puts("result points_per_sample=#{points_per_sample}")

    IO.puts(
      "result median_collection_export_throughput_points_per_second=#{format_number(median(throughput))}"
    )

    IO.puts(
      "result p99_collection_export_latency_milliseconds=#{format_number(percentile(latencies, 0.99))}"
    )

    IO.puts(
      "result maximum_collection_export_latency_milliseconds=#{format_number(Enum.max(latencies))}"
    )

    IO.puts("result final_decoded_data_points=#{final.data_points}")
    IO.puts("result final_decoded_distinct_series=#{final.distinct_series}")
    IO.puts("result target_wire_points_per_series=#{series}")

    IO.puts(
      "result one_wire_point_per_series_target_met=#{final.data_points == series and final.distinct_series == series}"
    )
  end

  defp print_recovery_result(
         series,
         retryable_results,
         final_elapsed,
         total_elapsed,
         final,
         points_per_sample,
         production_source_unchanged?
       ) do
    retryable_latency_ms = Enum.map(retryable_results, &(&1 / 1_000_000))

    retryable_throughput =
      Enum.map(retryable_results, &(points_per_sample * @nanoseconds_per_second / &1))

    IO.puts(
      "RESULT collection scenario=recovery_after_ten_retryable_delta_intervals series=#{series} temporality=delta"
    )

    IO.puts("result retryable_intervals=#{length(retryable_results)}")
    IO.puts("result retryable_interval_points=#{points_per_sample}")
    IO.puts("result retryable_interval_status=503")
    IO.puts("result warmup_repetitions=0")
    IO.puts("result samples=#{length(retryable_results)}")

    IO.puts(
      "result median_retryable_collection_export_throughput_points_per_second=#{format_number(median(retryable_throughput))}"
    )

    IO.puts(
      "result median_retryable_collection_export_latency_milliseconds=#{format_number(median(retryable_latency_ms))}"
    )

    IO.puts(
      "result p99_retryable_collection_export_latency_milliseconds=#{format_number(percentile(retryable_latency_ms, 0.99))}"
    )

    IO.puts(
      "result maximum_retryable_collection_export_latency_milliseconds=#{format_number(Enum.max(retryable_latency_ms))}"
    )

    IO.puts(
      "result recovery_final_export_latency_milliseconds=#{format_number(final_elapsed / 1_000_000)}"
    )

    IO.puts(
      "result recovery_total_latency_milliseconds=#{format_number(total_elapsed / 1_000_000)}"
    )

    IO.puts("result final_response_status=#{final.status}")
    IO.puts("result final_decoded_data_points=#{final.data_points}")
    IO.puts("result final_decoded_distinct_series=#{final.distinct_series}")
    IO.puts("result target_wire_points_per_series=#{series}")
    IO.puts("result production_source_unchanged=#{production_source_unchanged?}")

    IO.puts(
      "result recovery_mismatch_allowed_for_unchanged_source=#{production_source_unchanged?}"
    )

    IO.puts(
      "result one_wire_point_per_series_target_met=#{final.data_points == series and final.distinct_series == series}"
    )
  end

  defp assert_requests!(requests, expected_statuses, series) do
    if length(requests) != length(expected_statuses) do
      raise "loopback request count mismatch: expected=#{length(expected_statuses)} observed=#{length(requests)}"
    end

    if Enum.map(requests, & &1.status) != expected_statuses do
      raise "loopback response sequence mismatch: expected=#{inspect(expected_statuses)} observed=#{inspect(Enum.map(requests, & &1.status))}"
    end

    Enum.each(requests, fn request ->
      if Map.get(request, :decode_error) do
        raise "loopback protobuf decode failed: #{inspect(request)}"
      end

      if request.status == 200 and Enum.all?(expected_statuses, &(&1 == 200)) and
           request.data_points != series do
        raise "normal wire point mismatch: expected=#{series} observed=#{request.data_points}"
      end
    end)
  end

  defp writes_for(_cardinality, value_for, :hot) do
    Enum.map(0..(@operations_per_writer - 1), fn index ->
      {value_for.(index), %{series: "hot"}}
    end)
  end

  defp writes_for(cardinality, value_for, :multi) do
    Enum.map(0..(@operations_per_writer - 1), fn index ->
      series_index = rem(index, cardinality)

      {value_for.(index),
       %{series: "series_#{String.pad_leading(Integer.to_string(series_index), 5, "0")}"}}
    end)
  end

  defp collection_writes_for(series, value_for) do
    Enum.map(0..(series - 1), fn index ->
      {value_for.(index),
       %{series: "series_#{String.pad_leading(Integer.to_string(index), 5, "0")}"}}
    end)
  end

  defp write(store, metric, {value, tags}),
    do: MetricStore.write_metric(store.name, metric, value, tags)

  defp start_store(label, metric, endpoint) do
    name = String.to_atom("metric_store_bench_#{label}_#{System.unique_integer([:positive])}")

    config = %{
      name: name,
      metrics: [metric],
      export_period: :timer.hours(1),
      otlp_protocol: :http_protobuf,
      otlp_endpoint: endpoint,
      otlp_headers: %{},
      otlp_compression: nil,
      resource: %{},
      finch_pool: OtelMetricExporter.Finch,
      retry: false,
      aggregation_temporality: :delta
    }

    {:ok, pid} = MetricStore.start_link(config)
    %{name: name, pid: pid}
  end

  defp stop_store(%{pid: pid}), do: GenServer.stop(pid, :normal)

  defp classify_exit(:badarg), do: :badarg
  defp classify_exit({:badarg, _}), do: :badarg
  defp classify_exit(reason) when is_tuple(reason), do: elem(reason, 0)
  defp classify_exit(reason), do: reason

  defp monotonic_ns, do: System.monotonic_time(:nanosecond)

  defp median(values) do
    values = Enum.sort(values)
    middle = div(length(values), 2)

    if rem(length(values), 2) == 1 do
      Enum.at(values, middle)
    else
      (Enum.at(values, middle - 1) + Enum.at(values, middle)) / 2
    end
  end

  defp percentile(values, percentile) do
    values = Enum.sort(values)
    index = max(1, ceil(length(values) * percentile)) - 1
    Enum.at(values, index)
  end

  defp format_number(number) when is_integer(number), do: Integer.to_string(number)
  defp format_number(number), do: :erlang.float_to_binary(number * 1.0, decimals: 3)
end

OtelMetricExporter.MetricStoreBench.run()
