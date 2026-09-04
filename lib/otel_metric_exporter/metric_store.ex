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
  alias OtelMetricExporter.ExportTelemetry
  alias OtelMetricExporter.MetricStore.Aggregate

  import OtelMetricExporter.OtlpUtils, only: [build_kv: 1]

  @default_buckets [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]
  @max_pending_intervals 10
  @otp_cleanup_grace 1_000

  defmodule State do
    @moduledoc false

    defstruct [
      :config,
      :api,
      :metric_lookup,
      :metrics_table,
      :aggregation_temporality,
      :started_at,
      :last_collection,
      pending_intervals: []
    ]

    @type interval :: %{
            start: non_neg_integer(),
            finish: non_neg_integer(),
            aggregates: %{optional(tuple()) => non_neg_integer() | number() | tuple()}
          }

    @type t :: %__MODULE__{
            config: map(),
            api: %OtelApi{},
            metric_lookup: %{optional({atom(), String.t()}) => Metrics.t()},
            metrics_table: atom(),
            aggregation_temporality: :cumulative | :delta,
            started_at: non_neg_integer(),
            last_collection: non_neg_integer(),
            pending_intervals: [interval()]
          }
  end

  @type raw_config :: %{
          required(:name) => atom(),
          required(:export_period) => pos_integer(),
          optional(atom()) => term()
        }

  @type prepared_config :: %{
          required(:config) => map(),
          required(:api) => %OtelApi{}
        }

  @type prepared_start :: {:prepared, prepared_config()}
  @type start_arg :: raw_config() | prepared_start()

  @doc false
  def default_buckets, do: @default_buckets

  @doc false
  # Resolves the metrics API once before enabled MetricStore state is created.
  @spec prepare_config(raw_config()) :: {:ok, prepared_config()} | {:error, term()}
  def prepare_config(config) do
    finch_pool = Map.get(config, :finch_pool, OtelMetricExporter.Finch)

    with {:ok, api, config} <- OtelApi.new(Map.put(config, :finch, finch_pool), :metrics) do
      {:ok, %{config: config, api: api}}
    end
  end

  @spec start_link(start_arg()) :: GenServer.on_start()
  def start_link({:prepared, %{config: config, api: %OtelApi{}}} = prepared_arg) do
    GenServer.start_link(__MODULE__, prepared_arg, name: config.name)
  end

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: config.name)
  end

  @doc false
  @spec shutdown_timeout(%OtelApi{}) :: pos_integer()
  def shutdown_timeout(%OtelApi{config: %{otlp_timeout: timeout}}),
    do: 2 * timeout + @otp_cleanup_grace

  @doc false
  @spec child_spec(start_arg()) :: Supervisor.child_spec()
  def child_spec({:prepared, %{api: api}} = prepared_arg) do
    super(prepared_arg)
    |> Map.put(:shutdown, shutdown_timeout(api))
  end

  def child_spec(config) do
    child_spec = super(config)

    case prepare_config(config) do
      {:ok, %{api: api}} -> Map.put(child_spec, :shutdown, shutdown_timeout(api))
      {:error, _reason} -> child_spec
    end
  end

  @doc false
  @spec get_metrics(atom()) :: map()
  def get_metrics(metrics_table) do
    metrics_table
    |> :ets.tab2list()
    |> Enum.reduce(%{}, fn object, acc ->
      {type, name, encoded_tags} = elem(object, 0)
      tags = decode_tags(encoded_tags)
      value = object_value(type, object)
      value = if type == :distribution, do: Aggregate.to_map(value), else: value

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

  def write_metric(metrics_table, %Metrics.Counter{}, string_name, _value, tags) do
    key = logical_key(:counter, string_name, tags)
    :ets.update_counter(metrics_table, key, 1, {key, 0})
  end

  def write_metric(metrics_table, %Metrics.Sum{}, string_name, value, tags)
      when is_number(value) do
    key = logical_key(:sum, string_name, tags)

    if is_integer(value) do
      add_integer_sum(metrics_table, key, value)
    else
      cas_update(metrics_table, key, value, &Aggregate.add(:sum, &1, value))
    end
  end

  def write_metric(_metrics_table, %Metrics.Sum{}, _string_name, _value, _tags), do: :ok

  def write_metric(metrics_table, %Metrics.LastValue{}, string_name, value, tags)
      when is_number(value) do
    key = logical_key(:last_value, string_name, tags)
    :ets.insert(metrics_table, {key, value})
    :ok
  end

  def write_metric(_metrics_table, %Metrics.LastValue{}, _string_name, _value, _tags), do: :ok

  def write_metric(metrics_table, %Metrics.Distribution{} = metric, string_name, value, tags)
      when is_number(value) do
    bucket_bounds = Keyword.get(metric.reporter_options, :buckets, @default_buckets)
    bucket = find_bucket(bucket_bounds, value)
    key = logical_key(:distribution, string_name, tags)
    scaled = Aggregate.encode(value)
    operations = Aggregate.distribution_update_ops(scaled, bucket)
    add_distribution(metrics_table, key, operations, scaled, bucket, length(bucket_bounds) + 1)
  end

  def write_metric(_metrics_table, %Metrics.Distribution{}, _string_name, _value, _tags),
    do: :ok

  @doc false
  def table_exists?(metrics_table) do
    case :ets.whereis(metrics_table) do
      :undefined -> false
      tid when is_reference(tid) -> true
    end
  end

  defp cas_update(metrics_table, key, initial, update_fun) do
    # `select_replace` compares the exact object read above. A failed insert or
    # replacement means collection or another writer won the race, so retry
    # against the current table state without a bounded-loss fallback.
    case :ets.lookup(metrics_table, key) do
      [] ->
        if :ets.insert_new(metrics_table, {key, initial}) do
          :ok
        else
          cas_update(metrics_table, key, initial, update_fun)
        end

      [{^key, current}] ->
        updated = update_fun.(current)
        old_object = {key, current}
        new_object = {key, updated}

        match_spec = [{old_object, [], [{:const, new_object}]}]

        case :ets.select_replace(metrics_table, match_spec) do
          1 -> :ok
          0 -> cas_update(metrics_table, key, initial, update_fun)
        end
    end
  end

  defp add_integer_sum(metrics_table, key, value) do
    try do
      :ets.update_counter(metrics_table, key, {2, value}, {key, 0})
      :ok
    rescue
      ArgumentError ->
        cas_update(metrics_table, key, value, &Aggregate.add(:sum, &1, value))
    end
  end

  defp add_distribution(metrics_table, key, operations, scaled, bucket, bucket_count) do
    try do
      :ets.update_counter(metrics_table, key, operations)
      :ok
    rescue
      ArgumentError ->
        initial = Aggregate.new_scaled(scaled, bucket, bucket_count)
        add_distribution_after_missing(metrics_table, key, operations, initial)
    end
  end

  defp add_distribution_after_missing(metrics_table, key, operations, initial) do
    object = Aggregate.object(key, initial)

    if :ets.insert_new(metrics_table, object) do
      :ok
    else
      # A failed insert proves another writer has created the row; retry the
      # atomic update against that competing row without a bounded fallback.
      try do
        :ets.update_counter(metrics_table, key, operations)
        :ok
      rescue
        error in ArgumentError ->
          case :ets.lookup(metrics_table, key) do
            [] ->
              # The competing row was removed between the update and this
              # lookup; retry only after verifying that absence.
              add_distribution_after_missing(metrics_table, key, operations, initial)

            [_object] ->
              # ETS :set lookup proves a present object owns this key; any
              # update error therefore indicates an incompatible row.
              reraise error, __STACKTRACE__
          end
      end
    end
  end

  defp object_value(:distribution, object), do: Aggregate.row(object)
  defp object_value(_type, object), do: elem(object, 1)

  defp find_bucket(bucket_bounds, value) do
    case Enum.find_index(bucket_bounds, &(value <= &1)) do
      # Overflow bucket
      nil -> length(bucket_bounds)
      idx -> idx
    end
  end

  # Direct-key operations preserve raw tag maps; only sums need map-free keys
  # because their CAS match specs embed the key literally.
  defp logical_key(type, name, tags) when type in [:counter, :last_value, :distribution],
    do: {type, name, tags}

  defp logical_key(type, name, tags), do: {type, name, canonical_tags(tags)}

  # Keep the internal key component free of maps so it can be embedded as a
  # literal in the bound CAS match head; deterministic encoding preserves the
  # exact tag map while remaining separate from external identifiers.
  defp canonical_tags(tags), do: :erlang.term_to_binary(tags, [:deterministic])

  defp decode_tags(tags) when is_map(tags), do: tags
  defp decode_tags(encoded_tags), do: :erlang.binary_to_term(encoded_tags)

  @impl true
  def init({:prepared, %{config: config, api: api}}) do
    init_enabled(config, api)
  end

  def init(config) do
    case prepare_config(config) do
      {:ok, %{config: config, api: api}} ->
        case api.config.exporter do
          :none -> :ignore
          :otlp -> init_enabled(config, api)
        end

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp init_enabled(config, api) do
    Process.flag(:trap_exit, true)

    metrics = Map.get(config, :metrics, [])

    # Precompute {type, name_string} -> metric lookup map to keep export lookup
    # independent from the number of configured metric definitions.
    metric_lookup =
      Enum.reduce(metrics, %{}, fn metric, lookup ->
        key = {metric_type(metric), OtelMetricExporter.metric_name_string(metric)}
        Map.put_new(lookup, key, metric)
      end)

    metrics_table = config.name
    now = System.system_time(:nanosecond)
    Process.send_after(self(), :export, config.export_period)

    :ets.new(metrics_table, [:set, :public, :named_table, {:write_concurrency, true}])

    {:ok,
     %State{
       config: config,
       api: api,
       metric_lookup: metric_lookup,
       metrics_table: metrics_table,
       aggregation_temporality: Map.get(config, :aggregation_temporality, :cumulative),
       started_at: now,
       last_collection: now
     }}
  end

  @impl true
  def handle_call(:export_sync, _from, state) do
    {result, state} = export_metrics(state)
    {:reply, result, state}
  end

  @impl true
  def handle_info(:export, state) do
    {duration, {result, state}} = :timer.tc(fn -> export_metrics(state) end, :millisecond)

    _ = result
    # Schedule after sending so a slow or retrying request does not overlap the
    # next timer tick, while preserving the configured regular export period.
    Process.send_after(self(), :export, max(state.config.export_period - duration, 100))
    {:noreply, state}
  end

  @impl true
  def terminate(reason, state) do
    # Abnormal exits do not have a reliable transport lifetime, so only the
    # graceful shutdown reasons run a final drain.
    # The parent supervisor terminates this child after TelemetryHandlers, so
    # no new callbacks are attached while this final drain runs. A callback
    # already executing at detach may still linearize after its series take.
    if shutdown_reason?(reason) and table_exists?(state.metrics_table) do
      deadline = OtelApi.new_deadline(state.api)
      _ = export_metrics(state, deadline, true)
    end

    :ok
  end

  defp shutdown_reason?(:shutdown), do: true
  defp shutdown_reason?({:shutdown, _reason}), do: true
  defp shutdown_reason?(_reason), do: false

  defp export_metrics(%State{} = state) do
    export_metrics(state, nil, false)
  end

  defp export_metrics(%State{} = state, deadline, final?) do
    {interval, state} = collect(state)

    {pending, retained_drops, merged} =
      case state.aggregation_temporality do
        :delta ->
          {pending, retained_drops} = append_pending(state.pending_intervals, interval)
          {pending, retained_drops, merge_pending(pending)}

        :cumulative ->
          {[], 0, merge_pending([interval])}
      end

    payload = build_payload(state, merged)
    batch_size = count_data_points(payload)

    if batch_size == 0 do
      {:ok, %{state | pending_intervals: pending}}
    else
      telemetry_start = ExportTelemetry.start()
      result = send_payload(state, payload, deadline)

      dropped_items =
        retained_drops +
          if terminal?(result) or (final? and retryable?(result)), do: batch_size, else: 0

      ExportTelemetry.stop(telemetry_start, :metrics, batch_size, result, dropped_items)

      state =
        case result do
          {:error, :retryable, reason} ->
            log_export_failure(:retryable, reason)
            %{state | pending_intervals: pending}

          :ok ->
            %{state | pending_intervals: []}

          {:partial_success, _} ->
            %{state | pending_intervals: []}

          {:error, :terminal, reason} ->
            log_export_failure(:terminal, reason)
            %{state | pending_intervals: []}
        end

      {result, state}
    end
  end

  defp retryable?({:error, :retryable, _reason}), do: true
  defp retryable?(_result), do: false

  defp send_payload(state, payload, nil) do
    send_payload(state, payload, OtelApi.new_deadline(state.api))
  end

  defp send_payload(state, payload, deadline) do
    {worker_pid, monitor_ref, result_ref} = spawn_export_worker(state.api, payload, deadline)
    await_export_task(result_ref, worker_pid, monitor_ref, deadline)
  end

  defp collect(%State{aggregation_temporality: :delta} = state) do
    finish = System.system_time(:nanosecond)

    keys = :ets.select(state.metrics_table, [{:_, [], [{:element, 1, :"$_"}]}])

    aggregates =
      Enum.reduce(keys, %{}, fn key, acc ->
        case :ets.take(state.metrics_table, key) do
          [object] -> Map.put(acc, key, object_value(elem(key, 0), object))
          [] -> acc
        end
      end)

    interval = %{start: state.last_collection, finish: finish, aggregates: aggregates}
    {interval, %{state | last_collection: finish}}
  end

  defp collect(%State{aggregation_temporality: :cumulative} = state) do
    finish = System.system_time(:nanosecond)

    aggregates =
      state.metrics_table
      |> :ets.tab2list()
      |> Map.new(fn object ->
        key = elem(object, 0)
        {key, object_value(elem(key, 0), object)}
      end)

    interval = %{start: state.started_at, finish: finish, aggregates: aggregates}
    {interval, %{state | last_collection: finish}}
  end

  defp append_pending(pending, %{aggregates: aggregates} = _interval)
       when map_size(aggregates) == 0,
       do: {pending, 0}

  defp append_pending(pending, interval) do
    intervals = pending ++ [interval]
    overflow = max(length(intervals) - @max_pending_intervals, 0)
    evicted = Enum.take(intervals, overflow)
    retained = Enum.drop(intervals, overflow)

    {retained, Enum.reduce(evicted, 0, &(map_size(&1.aggregates) + &2))}
  end

  defp merge_pending(intervals) do
    Enum.reduce(intervals, %{}, fn interval, merged ->
      Enum.reduce(interval.aggregates, merged, fn {key, row}, acc ->
        type = elem(key, 0)

        case Map.fetch(acc, key) do
          :error ->
            tags = decode_tags(elem(key, 2))
            Map.put(acc, key, {interval.start, interval.finish, tags, row})

          {:ok, {start, _finish, tags, current}} ->
            Map.put(acc, key, {
              start,
              interval.finish,
              tags,
              Aggregate.merge(type, current, row)
            })
        end
      end)
    end)
  end

  defp build_payload(state, merged) do
    merged
    |> Enum.group_by(fn {{type, name, _tags}, _value} -> {type, name} end)
    |> Enum.map(fn {key, values} ->
      metric = Map.fetch!(state.metric_lookup, key)

      tagged_values =
        Enum.map(values, fn {{_type, _name, _tags}, {from, to, tags, value}} ->
          {{from, to}, tags, value}
        end)

      convert_metric(metric, tagged_values, state.aggregation_temporality)
    end)
  end

  @spec count_data_points(list(Metric.t())) :: non_neg_integer()
  defp count_data_points(metrics) do
    Enum.reduce(metrics, 0, fn
      %Metric{data: {:sum, %Sum{data_points: points}}}, count ->
        count + length(points)

      %Metric{data: {:gauge, %Gauge{data_points: points}}}, count ->
        count + length(points)

      %Metric{data: {:histogram, %Histogram{data_points: points}}}, count ->
        count + length(points)
    end)
  end

  defp terminal?({:error, :terminal, _}), do: true
  defp terminal?(_), do: false

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

  defp convert_metric(
         %{description: description, unit: unit} = metric,
         values,
         aggregation_temporality
       ) do
    %Metric{
      name: OtelMetricExporter.metric_name_string(metric),
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
             # Gauges must not set start_time_unix_nano; Datadog interprets a
             # non-zero start time as a rate denominator.
             start_time_unix_nano: 0,
             time_unix_nano: to,
             value: convert_value(value, :double)
           }
         end)
     }}
  end

  defp convert_data(%Metrics.Distribution{reporter_options: opts}, values, temporality) do
    bucket_bounds = Keyword.get(opts, :buckets, @default_buckets)

    {:histogram,
     %Histogram{
       data_points:
         Enum.map(values, fn {{from, to}, tags, aggregate} ->
           %HistogramDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             count: Aggregate.count(aggregate),
             sum: Aggregate.sum(aggregate),
             bucket_counts: Aggregate.buckets(aggregate),
             explicit_bounds: bucket_bounds,
             min: Aggregate.min_value(aggregate),
             max: Aggregate.max_value(aggregate)
           }
         end),
       aggregation_temporality: otlp_temporality(temporality)
     }}
  end

  @spec otlp_temporality(:cumulative | :delta) ::
          :AGGREGATION_TEMPORALITY_DELTA | :AGGREGATION_TEMPORALITY_CUMULATIVE
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

  @signed_int64_max 2 ** 63 - 1
  @signed_int64_min -2 ** 63

  # Preserve integer encoding when the value fits OTLP's signed int64 range;
  # larger integers and floating-point values use the double representation.
  defp convert_value(int, _preferred_type)
       when is_integer(int) and int >= @signed_int64_min and int <= @signed_int64_max,
       do: {:as_int, int}

  # OTLP has no bigint representation, so larger integers are converted to
  # double at the cost of precision.
  defp convert_value(bigint_or_float, _preferred_type)
       when is_integer(bigint_or_float) or is_float(bigint_or_float),
       do: {:as_double, bigint_or_float / 1}
end
