defmodule OtelMetricExporter.LogHandler do
  @moduledoc """
  A Logger handler that forwards log events to OTel collector via OTLP protocol.

  This can be installed as a handler for the `:logger` application.

  Example:

      # Add and configure the handler
      config :my_app, :logger, [
        {:handler, OtelMetricExporter.LogHandler, :logger_std_h, %{
          config: %{
            metadata_map: %{
              request_id: "http.request.id"
            }
          },
        }}
      ]

      # Configure the resource and endpoint
      config :otel_metric_exporter,
        otlp_protocol: :http_protobuf,
        otlp_endpoint: otlp_endpoint,
        resource: %{
          name: "metrics",
          service: %{name: service_name, version: version},
          instance: %{id: instance_id}
        }

  It should then be explicitly attached by executing `Logger.add_handlers(:my_app)` in `Application.start/2` callback
  of your application.

  ## Options

  Options starting with `otlp_` and the `resource` option will be take automatically from the `:otel_metric_exporter`
  app configuration, but can be overridden when adding the handler.

  #{OtelMetricExporter.LogAccumulator.options_schema() |> NimbleOptions.docs()}
  """

  alias OtelMetricExporter.LogAccumulator
  alias OtelMetricExporter.LogHandlerFailureTelemetry
  alias OtelMetricExporter.LogHandlerSupervisor

  @behaviour :logger_handler

  @olp_config_keys [
    :drop_mode_qlen,
    :flush_qlen,
    :burst_limit_enable,
    :burst_limit_max_count,
    :burst_limit_window_time,
    :overload_kill_enable,
    :overload_kill_qlen,
    :overload_kill_mem_size,
    :overload_kill_restart_after
  ]

  @accumulator_config_keys [:metadata, :metadata_map, :debounce_ms, :max_buffer_size]
  @api_config_keys [
    :otlp_endpoint,
    :otlp_protocol,
    :otlp_headers,
    :otlp_timeout,
    :resource,
    :otlp_compression,
    :otlp_concurrent_requests
  ]
  @internal_config_keys [:api, :olp, :task_supervisor, :sync_mode_qlen, :otlp_endpoint_kind]
  @redacted_header "[REDACTED]"

  @impl true
  def adding_handler(%{config: config} = handler_config) do
    {olp_config, accumulator_config} = Map.split(Map.new(config), @olp_config_keys)
    base_name = reg_name(handler_config)

    with {:ok, olp_config} <- prevalidate_olp(olp_config),
         {:ok, config} <- check_config(accumulator_config, base_name) do
      case config.api.config.exporter do
        :none ->
          {:ok, %{handler_config | config: store_disabled_config(config, olp_config)}}

        :otlp ->
          with {:ok, sup_pid, olp} <- start_supervisor(handler_config, config, olp_config) do
            olp_opts = :logger_olp.get_opts(olp)

            # Register the handler with the logger handler watcher, which detaches the handler
            # if it crashes for any reason to avoid taking down the entire logger process.
            :ok = :logger_handler_watcher.register_handler(handler_config.id, sup_pid)

            {:ok, %{handler_config | config: config |> Map.merge(olp_opts) |> Map.put(:olp, olp)}}
          end
      end
    end
  end

  @spec check_config(map(), atom()) :: {:ok, map()} | {:error, term()}
  defp check_config(config, base_name) do
    LogAccumulator.check_config(config, base_name)
  end

  @spec check_effective_config(map(), atom()) :: {:ok, map()} | {:error, term()}
  defp check_effective_config(config, base_name) do
    LogAccumulator.check_effective_config(config, base_name)
  end

  defp prevalidate_olp(olp_config) do
    olp_config =
      Map.put(olp_config, :sync_mode_qlen, Map.get(olp_config, :drop_mode_qlen, 200))

    effective = Map.merge(:logger_olp.get_default_opts(), olp_config)

    with :ok <- validate_olp_types(effective),
         :ok <- validate_olp_levels(effective) do
      {:ok, effective}
    end
  end

  @spec validate_olp_types(map()) :: :ok | {:error, term()}
  defp validate_olp_types(config) do
    integer_keys = [
      :sync_mode_qlen,
      :drop_mode_qlen,
      :flush_qlen,
      :burst_limit_max_count,
      :burst_limit_window_time,
      :overload_kill_qlen,
      :overload_kill_mem_size
    ]

    boolean_keys = [:burst_limit_enable, :overload_kill_enable]

    case Enum.find(integer_keys, &(not is_integer(Map.fetch!(config, &1)))) do
      nil ->
        case Enum.find(boolean_keys, &(not is_boolean(Map.fetch!(config, &1)))) do
          nil ->
            case Map.fetch!(config, :overload_kill_restart_after) do
              value when is_integer(value) or value == :infinity -> :ok
              value -> {:error, {:invalid_olp_config, %{overload_kill_restart_after: value}}}
            end

          key ->
            {:error, {:invalid_olp_config, %{key => Map.fetch!(config, key)}}}
        end

      key ->
        {:error, {:invalid_olp_config, %{key => Map.fetch!(config, key)}}}
    end
  end

  @spec validate_olp_levels(map()) :: :ok | {:error, term()}
  defp validate_olp_levels(
         %{
           sync_mode_qlen: sync_mode_qlen,
           drop_mode_qlen: drop_mode_qlen,
           flush_qlen: flush_qlen
         } = config
       ) do
    if drop_mode_qlen > 1 and sync_mode_qlen <= drop_mode_qlen and
         drop_mode_qlen <= flush_qlen do
      :ok
    else
      {:error,
       {:invalid_olp_levels, Map.take(config, [:sync_mode_qlen, :drop_mode_qlen, :flush_qlen])}}
    end
  end

  defp start_supervisor(handler_config, accumulator_config, olp_config) do
    Supervisor.start_child(
      :logger_sup,
      %{
        LogHandlerSupervisor.child_spec(
          name: reg_name(handler_config),
          accumulator_config: accumulator_config,
          olp_config: olp_config
        )
        | id: handler_config.id
      }
    )
  end

  @impl true
  def changing_config(
        set_or_update,
        %{config: old_handler_config} = old_config,
        new_config
      ) do
    old_public_config = public_config(old_handler_config)

    new_public_config =
      new_config
      |> Map.get(:config, %{})
      |> Map.new()
      |> Map.drop(@internal_config_keys)

    # `:set` resolves defaults as a fresh public configuration. `:update` starts from the
    # installed effective state so it cannot retarget a signal when application defaults change.
    user_config_to_validate =
      case set_or_update do
        :set -> new_public_config
        :update -> effective_update(old_handler_config, old_public_config, new_public_config)
      end

    {olp_config, accumulator_config_to_validate} =
      Map.split(user_config_to_validate, @olp_config_keys)

    with :ok <-
           ensure_requested_exporter_unchanged(
             set_or_update,
             old_handler_config,
             new_public_config
           ),
         :ok <- reject_nested_signal_config(new_public_config),
         {:ok, olp_config} <- prevalidate_olp(olp_config),
         {:ok, acc_config} <-
           check_changed_config(
             set_or_update,
             accumulator_config_to_validate,
             reg_name(old_config)
           ),
         :ok <- ensure_exporter_unchanged(old_handler_config, acc_config),
         :ok <- ensure_otlp_concurrent_requests_unchanged(old_handler_config, acc_config),
         :ok <- ensure_otlp_timeout_unchanged(old_handler_config, acc_config),
         {:ok, config} <- apply_config_change(old_handler_config, acc_config, olp_config) do
      {:ok, Map.put(new_config, :config, config)}
    end
  end

  @spec ensure_requested_exporter_unchanged(:set | :update, map(), map()) ::
          :ok | {:error, tuple()}
  defp ensure_requested_exporter_unchanged(
         _set_or_update,
         %{api: %{config: %{exporter: current_exporter}}},
         %{logs: logs_config}
       )
       when is_map(logs_config) or is_list(logs_config) do
    case requested_exporter(logs_config) do
      {:ok, ^current_exporter} ->
        :ok

      {:ok, requested_exporter} ->
        {:error, {:unsupported_live_exporter_change, current_exporter, requested_exporter}}

      :error ->
        :ok
    end
  end

  defp ensure_requested_exporter_unchanged(_set_or_update, _old_handler_config, _new_config),
    do: :ok

  defp requested_exporter(logs_config) when is_map(logs_config),
    do: Map.fetch(logs_config, :exporter)

  defp requested_exporter(logs_config) when is_list(logs_config) do
    if Keyword.keyword?(logs_config), do: Keyword.fetch(logs_config, :exporter), else: :error
  end

  defp reject_nested_signal_config(config) do
    nested_keys = config |> Map.take([:logs, :metrics]) |> Map.keys()

    case nested_keys do
      [] -> :ok
      keys -> {:error, {:unsupported_nested_signal_config, keys}}
    end
  end

  defp check_changed_config(:set, config, base_name), do: check_config(config, base_name)

  defp check_changed_config(:update, config, base_name),
    do: check_effective_config(config, base_name)

  @impl true
  def filter_config(%{config: config} = handler_config) do
    filtered_config =
      config
      |> public_config()
      |> redact_headers()
      |> redact_endpoint_credentials()

    %{handler_config | config: filtered_config}
  end

  @impl true
  def log(_event, %{config: %{api: %{config: %{exporter: :none}}}}), do: :ok

  def log(event, %{config: %{olp: olp} = config}) do
    olp_pid = :logger_olp.get_pid(olp)

    try do
      true = Process.alive?(olp_pid)
      :logger_olp.load(olp, LogAccumulator.prepare_log_event(event, config))
    catch
      kind, reason ->
        stacktrace = __STACKTRACE__
        LogHandlerFailureTelemetry.emit(event, olp_pid, kind, reason, stacktrace)
        :erlang.raise(kind, reason, stacktrace)
    end
  end

  @impl true
  def removing_handler(handler_config) do
    case Process.whereis(reg_name(handler_config)) do
      nil ->
        :ok

      _ ->
        config = handler_config.config
        olp = config.olp

        :gen_server.stop(
          :logger_olp.get_pid(olp),
          :normal,
          LogHandlerSupervisor.shutdown_timeout(config)
        )
    end
  end

  @spec apply_config_change(map(), map(), map()) :: {:ok, map()} | {:error, term()}
  defp apply_config_change(
         %{api: %{config: %{exporter: :none}}},
         %{api: %{config: %{exporter: :none}}} = config,
         olp_config
       ),
       do: {:ok, store_disabled_config(config, olp_config)}

  defp apply_config_change(%{olp: olp}, config, olp_config) do
    with :ok <- :logger_olp.set_opts(olp, olp_config),
         :ok <- :logger_olp.call(olp, {:config_changed, config}) do
      olp_opts = :logger_olp.get_opts(olp)
      {:ok, config |> Map.merge(olp_opts) |> Map.put(:olp, olp)}
    end
  end

  @spec store_disabled_config(map(), map()) :: map()
  defp store_disabled_config(config, olp_config) do
    Map.merge(config, Map.take(olp_config, @olp_config_keys))
  end

  @spec merge_public_update(map(), map()) :: map()
  defp merge_public_update(old_config, new_config) do
    Map.merge(old_config, new_config, fn
      key, old_value, new_value
      when key in [:logs, :resource] and is_map(old_value) and is_map(new_value) ->
        Map.merge(old_value, new_value)

      _key, _old_value, new_value ->
        new_value
    end)
  end

  @spec effective_update(map(), map(), map()) :: map()
  defp effective_update(%{api: %{config: api_config} = api}, old_public_config, new_config) do
    effective_api_config =
      api_config
      |> Map.from_struct()
      |> Map.take(@api_config_keys)
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
      |> Map.new()
      |> Map.put(:exporter, api_config.exporter)
      |> Map.put(:otlp_endpoint_kind, api_config.otlp_endpoint_kind)
      |> Map.put(:otlp_compression, api_config.otlp_compression)
      |> Map.put(:retry, api.retry)

    merged =
      old_public_config
      |> Map.drop([:logs, :metrics])
      |> Map.merge(effective_api_config)
      |> merge_public_update(new_config)

    if Map.has_key?(new_config, :otlp_endpoint) do
      Map.put(merged, :otlp_endpoint_kind, :generic)
    else
      merged
    end
  end

  @spec public_config(map()) :: map()
  defp public_config(%{api: %{config: api_config} = api} = config) do
    config
    |> Map.take(@accumulator_config_keys ++ @olp_config_keys)
    |> Map.merge(public_api_config(api_config))
    |> Map.put(:retry, api.retry)
  end

  @spec public_api_config(%OtelMetricExporter.OtelApi.Config{}) :: map()
  defp public_api_config(api_config) do
    api_config
    |> Map.from_struct()
    |> Map.take(@api_config_keys)
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
    |> put_endpoint(api_config)
    |> put_exporter(api_config.exporter)
  end

  @spec put_endpoint(map(), %OtelMetricExporter.OtelApi.Config{}) :: map()
  defp put_endpoint(config, %{otlp_endpoint_kind: :signal, otlp_endpoint: endpoint})
       when is_binary(endpoint) do
    config
    |> Map.delete(:otlp_endpoint)
    |> Map.put(:logs, %{otlp_endpoint: endpoint})
  end

  defp put_endpoint(config, _api_config), do: config

  @spec put_exporter(map(), :otlp | :none | nil) :: map()
  defp put_exporter(config, nil), do: config

  defp put_exporter(config, exporter) do
    Map.update(config, :logs, %{exporter: exporter}, &Map.put(&1, :exporter, exporter))
  end

  @spec redact_headers(map()) :: map()
  defp redact_headers(%{otlp_headers: headers} = config) when is_map(headers) do
    Map.put(config, :otlp_headers, redact_header_values(headers))
  end

  defp redact_headers(config), do: config

  @spec redact_endpoint_credentials(map()) :: map()
  defp redact_endpoint_credentials(config) do
    config
    |> redact_endpoint_at(:otlp_endpoint)
    |> Map.update(:logs, %{}, fn logs -> redact_endpoint_at(logs, :otlp_endpoint) end)
  end

  defp redact_endpoint_at(config, key) do
    case Map.fetch(config, key) do
      {:ok, endpoint} ->
        uri = URI.parse(endpoint)

        case uri.userinfo do
          nil -> config
          _userinfo -> Map.put(config, key, URI.to_string(%{uri | userinfo: @redacted_header}))
        end

      :error ->
        config
    end
  end

  @spec redact_header_values(map()) :: map()
  defp redact_header_values(headers) do
    Map.new(headers, fn {key, _value} -> {key, @redacted_header} end)
  end

  @spec ensure_exporter_unchanged(map(), map()) :: :ok | {:error, tuple()}
  defp ensure_exporter_unchanged(
         %{api: %{config: %{exporter: exporter}}},
         %{api: %{config: %{exporter: exporter}}}
       ),
       do: :ok

  defp ensure_exporter_unchanged(
         %{api: %{config: %{exporter: current_exporter}}},
         %{api: %{config: %{exporter: requested_exporter}}}
       ) do
    {:error, {:unsupported_live_exporter_change, current_exporter, requested_exporter}}
  end

  @spec ensure_otlp_concurrent_requests_unchanged(map(), map()) :: :ok | {:error, tuple()}
  defp ensure_otlp_concurrent_requests_unchanged(
         %{api: %{config: %{otlp_concurrent_requests: concurrency}}},
         %{api: %{config: %{otlp_concurrent_requests: concurrency}}}
       ),
       do: :ok

  defp ensure_otlp_concurrent_requests_unchanged(
         %{api: %{config: %{otlp_concurrent_requests: current_concurrency}}},
         %{api: %{config: %{otlp_concurrent_requests: requested_concurrency}}}
       ) do
    {
      :error,
      {
        :unsupported_live_otlp_concurrent_requests_change,
        current_concurrency,
        requested_concurrency
      }
    }
  end

  defp ensure_otlp_timeout_unchanged(
         %{api: %{config: %{otlp_timeout: timeout}}},
         %{api: %{config: %{otlp_timeout: timeout}}}
       ),
       do: :ok

  defp ensure_otlp_timeout_unchanged(
         %{api: %{config: %{otlp_timeout: current_timeout}}},
         %{api: %{config: %{otlp_timeout: requested_timeout}}}
       ) do
    # Supervisor/OLP shutdown allowances are captured at handler startup.
    {:error, {:unsupported_live_otlp_timeout_change, current_timeout, requested_timeout}}
  end

  defp reg_name(%{module: module, id: id}), do: :"#{module}_#{id}"
end
