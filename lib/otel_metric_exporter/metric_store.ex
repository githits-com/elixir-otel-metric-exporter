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

  # Maximum number of generations to retain on export failure.
  # Prevents unbounded ETS growth when the backend is unreachable.
  @max_retained_generations 10

  defmodule State do
    @moduledoc false
    defstruct [:config, :api, :metrics, :metric_lookup, :metrics_table, :last_export, :generations_table, :aggregation_temporality]

    @type t :: %__MODULE__{
            config: map(),
            api: struct(),
            metrics: list(),
            metrics_table: atom(),
            generations_table: :ets.tid(),
            last_export: nil | DateTime.t(),
            aggregation_temporality: :cumulative | :delta
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

  def write_metric(metrics_table, %Metrics.Distribution{} = metric, string_name, value, tags)
      when is_number(value) do
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
  end

  def write_metric(_metrics_table, %Metrics.Distribution{}, _string_name, value, _tags) do
    Logger.debug("OtelMetricExporter: dropping distribution metric with non-numeric value: #{inspect(value)}")
    :ok
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

  @impl true
  def init(config) do
    metrics = Map.get(config, :metrics, [])
    # Precompute {type, name_string} -> metric lookup map to replace O(n²) scan during export
    metric_lookup =
      Map.new(metrics, fn metric ->
        key = {metric_type(metric), Enum.join(metric.name, ".")}
        {key, metric}
      end)
    metrics_table = config.name
    finch_pool = Map.get(config, :finch_pool, OtelMetricExporter.Finch)
    Process.send_after(self(), :export, config.export_period)

    # Create ETS table for metrics
    :ets.new(metrics_table, [:ordered_set, :public, :named_table, {:write_concurrency, true}])

    generations_table = :ets.new(:generations, [:ordered_set, :private])
    :ets.insert(generations_table, {0, System.system_time(:nanosecond), 0})
    :persistent_term.put(generation_key(metrics_table), 0)

    aggregation_temporality = Map.get(config, :aggregation_temporality, :cumulative)

    with {:ok, api, config} <- OtelApi.new(Map.put(config, :finch, finch_pool), :metrics) do
      {:ok,
       %State{
         config: config,
         api: api,
         metrics: metrics,
         metric_lookup: metric_lookup,
         metrics_table: metrics_table,
         generations_table: generations_table,
         aggregation_temporality: aggregation_temporality
       }}
    end
  end

  @impl true
  def handle_call(:export_sync, _from, state) do
    case export_metrics(state) do
      :ok ->
        {:reply, :ok, state}

      error ->
        {:reply, error, state}
    end
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

    # Drop oldest generations if we've accumulated too many (backend unreachable)
    {earliest_gen, dropped} = maybe_drop_old_generations(state, earliest_gen, current_gen)

    if dropped > 0 do
      Logger.warning(
        "OtelMetricExporter dropped #{dropped} old metric generation(s) to prevent unbounded growth"
      )
    end

    now = System.system_time(:nanosecond)

    earliest_gen..current_gen//1
    |> Enum.reduce(%{}, fn gen, acc ->
      {_, start, finish} = List.first(:ets.lookup(state.generations_table, gen), {nil, nil, nil})
      # Use current time when generation end timestamp is nil (not yet rotated)
      finish = finish || now

      get_metrics(state.metrics_table, gen)
      |> Map.new(fn {metric_key, values} ->
        {metric_key, Enum.map(values, fn {tags, value} -> {{start, finish}, tags, value} end)}
      end)
      |> Map.merge(acc, fn _k, v1, v2 -> v2 ++ v1 end)
    end)
    |> Enum.flat_map(fn {{type, name} = key, tagged_values} ->
      case Map.fetch(state.metric_lookup, key) do
        :error ->
          Logger.warning("OtelMetricExporter: unknown metric #{inspect({type, name})}, skipping")
          []

        {:ok, metric} ->
          [convert_metric(metric, tagged_values, state.aggregation_temporality)]
      end
    end)
    |> then(fn payload ->
      task = Task.async(fn -> OtelApi.send_metrics(state.api, payload) end)

      case Task.yield(task, 20_000) || Task.shutdown(task) do
        {:ok, result} -> result
        {:exit, reason} -> {:error, reason}
        nil -> {:error, :timeout}
      end
    end)
    |> case do
      :ok ->
        # Clear exported metrics
        for x <- earliest_gen..current_gen//1 do
          :ets.match_delete(state.metrics_table, {{x, :_, :_, :_, :_}, :_, :_})
          :ets.delete(state.generations_table, x)
        end

        :ok

      {:error, reason} ->
        Logger.error("Failed to export metrics: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp maybe_drop_old_generations(state, earliest_gen, current_gen) do
    total = current_gen - earliest_gen + 1

    if total > @max_retained_generations do
      drop_until = current_gen - @max_retained_generations + 1

      for gen <- earliest_gen..(drop_until - 1)//1 do
        :ets.match_delete(state.metrics_table, {{gen, :_, :_, :_, :_}, :_, :_})
        :ets.delete(state.generations_table, gen)
      end

      {drop_until, drop_until - earliest_gen}
    else
      {earliest_gen, 0}
    end
  end

  defp convert_metric(
         %{name: name, description: description, unit: unit} = metric,
         values,
         aggregation_temporality
       ) do
    %Metric{
      name: Enum.join(name, "."),
      description: description,
      unit: convert_unit(unit),
      data: convert_data(metric, values, aggregation_temporality)
    }
  end

  defp convert_data(%Metrics.Counter{}, values, temporality) do
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
       aggregation_temporality: otlp_temporality(temporality),
       is_monotonic: true
     }}
  end

  defp convert_data(%Metrics.Sum{}, values, temporality) do
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
       aggregation_temporality: otlp_temporality(temporality),
       is_monotonic: false
     }}
  end

  defp convert_data(%Metrics.LastValue{}, values, _temporality) do
    {:gauge,
     %Gauge{
       data_points:
         Enum.map(values, fn {{_from, to}, tags, value} ->
           %NumberDataPoint{
             attributes: build_kv(tags),
             # Gauge data points must NOT set start_time_unix_nano.
             # The Datadog Agent interprets a non-zero start time as a
             # rate denominator, dividing the value by (time - start),
             # which turns an absolute count into a per-second rate.
             start_time_unix_nano: 0,
             time_unix_nano: to,
             value: convert_value(value, :double)
           }
         end)
     }}
  end

  defp convert_data(%Metrics.Distribution{reporter_options: opts}, values, temporality) do
    bucket_bounds = Keyword.get(opts, :buckets, @default_buckets)
    total_bucket_bounds = length(bucket_bounds)

    {:histogram,
     %Histogram{
       data_points:
         Enum.map(values, fn {{from, to}, tags, bucket_values} ->
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
             explicit_bounds: bucket_bounds
           }
         end),
       aggregation_temporality: otlp_temporality(temporality)
     }}
  end

  defp otlp_temporality(:delta), do: :AGGREGATION_TEMPORALITY_DELTA
  defp otlp_temporality(:cumulative), do: :AGGREGATION_TEMPORALITY_CUMULATIVE

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

  # These two clauses are here to preserve the current behaviour of the library and avoid
  # introducing unexpected errors. Ideally, we would filter these nil values higher up in the
  # call stack and stop short of exporting metrics with nil values.
  defp convert_value(nil, :int), do: {:as_int, nil}
  defp convert_value(nil, :double), do: {:as_double, nil}

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
