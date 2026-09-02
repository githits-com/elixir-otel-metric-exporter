defmodule OtelMetricExporter.MetricStore do
  @moduledoc false

  use GenServer

  require Logger

  alias Telemetry.Metrics

  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.{
    Metric,
    NumberDataPoint,
    HistogramDataPoint,
    Sum,
    Gauge,
    Histogram
  }

  alias OtelMetricExporter.OtelApi

  import OtelMetricExporter.OtlpUtils, only: [build_kv: 1]

  @default_buckets [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]

  defmodule State do
    @moduledoc false
    defstruct [:config, :api, :metrics, :metrics_table, :last_export, :generations_table]

    @type t :: %__MODULE__{
            config: map(),
            api: struct(),
            metrics: list(),
            metrics_table: atom(),
            generations_table: :ets.tid(),
            last_export: nil | DateTime.t()
          }
  end

  @doc false
  def default_buckets, do: @default_buckets

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: config.name)
  end

  def get_metrics(metrics_table, generation \\ nil) do
    generation = generation || :persistent_term.get(generation_key(metrics_table))

    :ets.match_object(metrics_table, {{generation, :_, :_, :_, :_}, :_, :_})
    |> Enum.reduce(%{}, fn
      {{_, name, :distribution, tags, bucket}, count, sum}, acc ->
        Map.update(
          acc,
          {:distribution, name},
          %{tags => %{bucket => {count, sum}}},
          fn all_tags ->
            Map.update(all_tags, tags, %{bucket => {count, sum}}, fn all_buckets ->
              Map.put(all_buckets, bucket, {count, sum})
            end)
          end
        )

      {{_, name, type, tags, _}, value, _}, acc ->
        Map.update(acc, {type, name}, %{tags => value}, fn all_tags ->
          Map.put(all_tags, tags, value)
        end)
    end)
  end

  @spec export_sync(GenServer.name()) :: OtelApi.export_result()
  def export_sync(name) do
    GenServer.call(name, :export_sync, :infinity)
  end

  defp metric_type(%Metrics.Counter{}), do: :counter
  defp metric_type(%Metrics.Sum{}), do: :sum
  defp metric_type(%Metrics.LastValue{}), do: :last_value
  defp metric_type(%Metrics.Distribution{}), do: :distribution

  def write_metric(metrics_table, metric, value, tags),
    do: write_metric(metrics_table, metric, Enum.join(metric.name, "."), value, tags)

  def write_metric(metrics_table, %Metrics.Counter{} = metric, string_name, _, tags) do
    generation = :persistent_term.get(generation_key(metrics_table))
    ets_key = {generation, string_name, metric_type(metric), tags, nil}

    :ets.update_counter(metrics_table, ets_key, 1, {ets_key, 0, nil})
  end

  def write_metric(metrics_table, %Metrics.Sum{} = metric, string_name, value, tags) do
    generation = :persistent_term.get(generation_key(metrics_table))
    ets_key = {generation, string_name, metric_type(metric), tags, nil}

    :ets.update_counter(metrics_table, ets_key, value, {ets_key, 0, nil})
  end

  def write_metric(metrics_table, %Metrics.LastValue{} = metric, string_name, value, tags) do
    generation = :persistent_term.get(generation_key(metrics_table))
    ets_key = {generation, string_name, metric_type(metric), tags, nil}
    :ets.update_element(metrics_table, ets_key, {2, value}, {ets_key, value, nil})
  end

  def write_metric(metrics_table, %Metrics.Distribution{} = metric, string_name, value, tags) do
    bucket = find_bucket(metric, value)
    generation = :persistent_term.get(generation_key(metrics_table))
    ets_key = {generation, string_name, metric_type(metric), tags, bucket}
    update_counter_op = {2, 1}
    update_sum_op = {3, round(value)}

    :ets.update_counter(
      metrics_table,
      ets_key,
      [update_counter_op, update_sum_op],
      {ets_key, 0, 0}
    )

    update_min_max(metrics_table, {generation, string_name, metric_type(metric), tags}, value)
  end

  def table_exists?(metrics_table) do
    case :ets.whereis(metrics_table) do
      :undefined -> false
      tid when is_reference(tid) -> true
    end
  end

  defp find_bucket(%Metrics.Distribution{reporter_options: opts}, value) do
    bucket_bounds = Keyword.get(opts, :buckets, @default_buckets)

    case Enum.find_index(bucket_bounds, &(value <= &1)) do
      # Overflow bucket
      nil -> length(bucket_bounds)
      idx -> idx
    end
  end

  defp update_min_max(metrics_table, base_key, value) do
    min_key = Tuple.insert_at(base_key, tuple_size(base_key), :min)

    unless :ets.insert_new(metrics_table, {min_key, value, nil}) do
      case :ets.lookup(metrics_table, min_key) do
        [{_, current, _}] when value < current ->
          :ets.insert(metrics_table, {min_key, value, nil})

        _ ->
          :ok
      end
    end

    max_key = Tuple.insert_at(base_key, tuple_size(base_key), :max)

    unless :ets.insert_new(metrics_table, {max_key, value, nil}) do
      case :ets.lookup(metrics_table, max_key) do
        [{_, current, _}] when value > current ->
          :ets.insert(metrics_table, {max_key, value, nil})

        _ ->
          :ok
      end
    end
  end

  @impl true
  def init(config) do
    metrics = Map.get(config, :metrics, [])
    metrics_table = config.name
    finch_pool = Map.get(config, :finch_pool, OtelMetricExporter.Finch)
    Process.send_after(self(), :export, config.export_period)

    # Create ETS table for metrics
    :ets.new(metrics_table, [:ordered_set, :public, :named_table, {:write_concurrency, true}])

    generations_table = :ets.new(:generations, [:ordered_set, :private])
    :ets.insert(generations_table, {0, System.system_time(:nanosecond), 0})
    :persistent_term.put(generation_key(metrics_table), 0)

    with {:ok, api, config} <- OtelApi.new(Map.put(config, :finch, finch_pool), :metrics) do
      {:ok,
       %State{
         config: config,
         api: api,
         metrics: metrics,
         metrics_table: metrics_table,
         generations_table: generations_table
       }}
    end
  end

  @impl true
  def handle_call(:export_sync, _from, state) do
    {:reply, export_metrics(state), state}
  end

  @impl true
  def handle_info(:export, state) do
    {duration, _} = :timer.tc(fn -> export_metrics(state) end, :millisecond)

    # schedule after we've sent to avoid problems when there's some kind of
    # problem sending and we get into a retry loop but take into account
    # time taken to send so we keep flushing the data on a regular interval
    Process.send_after(self(), :export, max(state.config.export_period - duration, 100))

    {:noreply, state}
  end

  defp rotate_generation(%State{} = state) do
    current_gen = :persistent_term.get(generation_key(state.metrics_table))
    :persistent_term.put(generation_key(state.metrics_table), current_gen + 1)

    :ets.update_element(
      state.generations_table,
      current_gen,
      {3, System.system_time(:nanosecond)}
    )

    :ets.insert(state.generations_table, {current_gen + 1, System.system_time(:nanosecond), nil})

    current_gen
  end

  defp export_metrics(%State{} = state) do
    current_gen = rotate_generation(state)

    earliest_gen =
      case :ets.first(state.generations_table) do
        :"$end_of_table" -> 0
        x -> x
      end

    earliest_gen..current_gen//1
    |> Enum.reduce(%{}, fn gen, acc ->
      {_, start, finish} = List.first(:ets.lookup(state.generations_table, gen), {nil, nil, nil})

      get_metrics(state.metrics_table, gen)
      |> Map.new(fn {metric_key, values} ->
        {metric_key, Enum.map(values, fn {tags, value} -> {{start, finish}, tags, value} end)}
      end)
      |> Map.merge(acc, fn _k, v1, v2 -> v2 ++ v1 end)
    end)
    |> Enum.map(fn {{type, name}, tagged_values} ->
      metric =
        Enum.find(state.metrics, &(Enum.join(&1.name, ".") == name and metric_type(&1) == type))

      convert_metric(metric, tagged_values)
    end)
    |> then(fn payload ->
      deadline = OtelApi.new_deadline(state.api)

      {worker_pid, monitor_ref, result_ref} =
        spawn_export_worker(state.api, payload, deadline)

      await_export_task(result_ref, worker_pid, monitor_ref, deadline)
    end)
    |> case do
      :ok ->
        clear_exported_metrics(state, earliest_gen, current_gen)
        :ok

      {:partial_success, _rejected_count} = result ->
        clear_exported_metrics(state, earliest_gen, current_gen)
        result

      {:error, :terminal, reason} = result ->
        log_export_failure(:terminal, reason)
        clear_exported_metrics(state, earliest_gen, current_gen)
        result

      {:error, :retryable, reason} = result ->
        log_export_failure(:retryable, reason)
        result
    end
  end

  @spec spawn_export_worker(%OtelApi{}, list(), OtelApi.deadline()) ::
          {pid(), reference(), reference()}
  defp spawn_export_worker(api, payload, deadline) do
    parent = self()
    result_ref = make_ref()

    {worker_pid, monitor_ref} =
      :erlang.spawn_opt(
        fn ->
          result = OtelApi.send_metrics(api, payload, deadline)
          send(parent, {result_ref, result})
        end,
        [:monitor]
      )

    {worker_pid, monitor_ref, result_ref}
  end

  @spec await_export_task(reference(), pid(), reference(), OtelApi.deadline()) ::
          OtelApi.export_result()
  defp await_export_task(result_ref, worker_pid, monitor_ref, deadline) do
    case OtelApi.remaining_timeout(deadline) do
      timeout when is_integer(timeout) ->
        receive do
          {^result_ref, result} ->
            Process.demonitor(monitor_ref, [:flush])
            result

          {:DOWN, ^monitor_ref, :process, ^worker_pid, _reason} ->
            Process.demonitor(monitor_ref, [:flush])
            {:error, :retryable, :export_task_failed}
        after
          timeout ->
            await_export_at_deadline(result_ref, worker_pid, monitor_ref)
        end

      :expired ->
        await_export_at_deadline(result_ref, worker_pid, monitor_ref)
    end
  end

  @spec await_export_at_deadline(reference(), pid(), reference()) :: OtelApi.export_result()
  defp await_export_at_deadline(result_ref, worker_pid, monitor_ref) do
    receive do
      {^result_ref, result} ->
        Process.demonitor(monitor_ref, [:flush])
        result

      {:DOWN, ^monitor_ref, :process, ^worker_pid, _reason} ->
        Process.demonitor(monitor_ref, [:flush])
        {:error, :retryable, :export_task_failed}
    after
      0 ->
        case stop_export_worker(result_ref, worker_pid, monitor_ref) do
          {:result, result} -> result
          :none -> {:error, :retryable, :deadline_exceeded}
        end
    end
  end

  @spec stop_export_worker(reference(), pid(), reference()) ::
          {:result, OtelApi.export_result()} | :none
  defp stop_export_worker(result_ref, worker_pid, monitor_ref) do
    Process.exit(worker_pid, :kill)

    receive do
      {:DOWN, ^monitor_ref, :process, ^worker_pid, _reason} ->
        Process.demonitor(monitor_ref, [:flush])
        take_export_result(result_ref)
    end
  end

  @spec take_export_result(reference()) :: {:result, OtelApi.export_result()} | :none
  defp take_export_result(result_ref) do
    receive do
      {^result_ref, result} -> {:result, result}
    after
      0 -> :none
    end
  end

  @spec log_export_failure(OtelApi.failure_disposition(), OtelApi.failure_reason()) :: :ok
  defp log_export_failure(disposition, reason) do
    Logger.error(
      "Failed to export metrics: disposition=#{format_disposition(disposition)} reason=#{format_failure_reason(reason)}",
      disposition: disposition,
      reason: reason
    )
  end

  @spec format_disposition(OtelApi.failure_disposition()) :: String.t()
  defp format_disposition(:terminal), do: "terminal"
  defp format_disposition(:retryable), do: "retryable"

  @spec format_failure_reason(OtelApi.failure_reason()) :: String.t()
  defp format_failure_reason(:invalid_response), do: "invalid_response"
  defp format_failure_reason(:encoding_failed), do: "encoding_failed"
  defp format_failure_reason(:deadline_exceeded), do: "deadline_exceeded"
  defp format_failure_reason(:pool_timeout), do: "pool_timeout"
  defp format_failure_reason(:transport_failure), do: "transport_failure"
  defp format_failure_reason(:request_failed), do: "request_failed"
  defp format_failure_reason(:export_task_failed), do: "export_task_failed"
  defp format_failure_reason(:response_too_large), do: "response_too_large"

  defp format_failure_reason({:http_status, status}),
    do: "http_status=#{Integer.to_string(status)}"

  @spec clear_exported_metrics(%State{}, non_neg_integer(), non_neg_integer()) :: :ok
  defp clear_exported_metrics(state, earliest_gen, current_gen) do
    for x <- earliest_gen..current_gen//1 do
      :ets.match_delete(state.metrics_table, {{x, :_, :_, :_, :_}, :_, :_})
      :ets.delete(state.generations_table, x)
    end

    :ok
  end

  defp convert_metric(
         %{name: name, description: description, unit: unit} = metric,
         values
       ) do
    %Metric{
      name: Enum.join(name, "."),
      description: description,
      unit: convert_unit(unit),
      data: convert_data(metric, values)
    }
  end

  defp convert_data(%Metrics.Counter{}, values) do
    {:sum,
     %Sum{
       data_points:
         Enum.map(values, fn {{from, to}, tags, value} ->
           %NumberDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             value: convert_value(value, :int)
           }
         end),
       aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
       is_monotonic: true
     }}
  end

  defp convert_data(%Metrics.Sum{}, values) do
    {:sum,
     %Sum{
       data_points:
         Enum.map(values, fn {{from, to}, tags, value} ->
           %NumberDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             value: convert_value(value, :int)
           }
         end),
       aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
       is_monotonic: false
     }}
  end

  defp convert_data(%Metrics.LastValue{}, values) do
    {:gauge,
     %Gauge{
       data_points:
         Enum.map(values, fn {{from, to}, tags, value} ->
           %NumberDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             value: convert_value(value, :double)
           }
         end)
     }}
  end

  defp convert_data(%Metrics.Distribution{reporter_options: opts}, values) do
    bucket_bounds = Keyword.get(opts, :buckets, @default_buckets)
    total_bucket_bounds = length(bucket_bounds)

    {:histogram,
     %Histogram{
       data_points:
         Enum.map(values, fn {{from, to}, tags, bucket_values} ->
           {min_value, _} = Map.get(bucket_values, :min, {nil, nil})
           {max_value, _} = Map.get(bucket_values, :max, {nil, nil})
           bucket_values = Map.drop(bucket_values, [:min, :max])

           {total_count, total_sum} =
             Enum.reduce(bucket_values, {0, 0.0}, fn {_, {count, sum}},
                                                     {total_count, total_sum} ->
               {total_count + count, total_sum + sum}
             end)

           bucket_counts =
             Enum.map(0..total_bucket_bounds//1, &elem(Map.get(bucket_values, &1, {0, 0}), 0))

           %HistogramDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             count: total_count,
             sum: total_sum,
             bucket_counts: bucket_counts,
             explicit_bounds: bucket_bounds,
             min: min_value && min_value / 1,
             max: max_value && max_value / 1
           }
         end),
       aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE
     }}
  end

  defp convert_unit(:unit), do: nil
  defp convert_unit(:second), do: "s"
  defp convert_unit(:millisecond), do: "ms"
  defp convert_unit(:microsecond), do: "us"
  defp convert_unit(:nanosecond), do: "ns"
  defp convert_unit(:byte), do: "By"
  defp convert_unit(:kilobyte), do: "kBy"
  defp convert_unit(:megabyte), do: "MBy"
  defp convert_unit(:gigabyte), do: "GBy"
  defp convert_unit(:terabyte), do: "TBy"
  defp convert_unit(x) when is_atom(x), do: Atom.to_string(x)

  # These clauses are here to preserve the current behaviour of the library and avoid
  # introducing unexpected errors. Ideally, we would filter these nil/:undefined values higher
  # up in the call stack and stop short of exporting metrics with nil values.
  #
  # `:telemetry` emits `:undefined` for uninitialised values, so we treat it the same as `nil`.
  defp convert_value(nil, :int), do: {:as_int, nil}
  defp convert_value(nil, :double), do: {:as_double, nil}
  defp convert_value(:undefined, :int), do: {:as_int, nil}
  defp convert_value(:undefined, :double), do: {:as_double, nil}

  @signed_int64_max 2 ** 63 - 1
  @signed_int64_min -2 ** 63
  defp convert_value(int, _preferred_type)
       when is_integer(int) and int >= @signed_int64_min and int <= @signed_int64_max,
       do: {:as_int, int}

  # The OpenTelemetry protocol has no supporrt for bigint, so the best we can do is convert to
  # double at the cost of losing some precision.
  defp convert_value(bigint_or_float, _preferred_type)
       when is_integer(bigint_or_float) or is_float(bigint_or_float),
       do: {:as_double, bigint_or_float / 1}

  defp generation_key(metrics_table) do
    {__MODULE__, metrics_table, :generation}
  end
end
