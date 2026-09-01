defmodule OtelMetricExporter.OtelApi do
  @moduledoc false

  alias OtelMetricExporter.OtelApi.Config
  alias OtelMetricExporter.Protocol

  @opaque deadline :: integer()

  @retry_initial_delay 1_000
  @transient_statuses [408, 429, 500, 502, 503, 504]
  @finch_checkout_timeout_prefix "Finch was unable to provide a connection within the timeout"

  @schema NimbleOptions.new!(
            finch: [
              type: {:or, [:atom, :pid]},
              required: true,
              doc: "Finch process pid or registered name to use for sending requests."
            ],
            retry: [
              type: :boolean,
              default: true,
              doc: "Retry HTTP requests when receiving a transient error"
            ]
          )

  defstruct [:finch, :retry, :config, :scope]

  @spec new_deadline(%__MODULE__{}) :: deadline()
  def new_deadline(%__MODULE__{config: %Config{otlp_timeout: timeout}}),
    do: System.monotonic_time(:millisecond) + timeout

  @spec remaining_timeout(deadline()) :: pos_integer() | :expired
  def remaining_timeout(deadline) do
    case deadline - System.monotonic_time(:millisecond) do
      remaining when remaining > 0 -> remaining
      _ -> :expired
    end
  end

  def public_options, do: Config.public_options()

  def new(opts, scope) do
    with {:ok, config, rest} <- Config.validate_for_scope(opts, scope),
         {own_opts, rest} <- Map.split(rest, [:finch, :retry]),
         {:ok, validated} <- NimbleOptions.validate(own_opts, @schema) do
      {:ok,
       %__MODULE__{
         config: config,
         scope: scope,
         finch: validated.finch,
         retry: validated.retry
       }, rest}
    end
  end

  @spec send_log_events(%__MODULE__{}, list()) :: :ok | {:error, term()}
  def send_log_events(%__MODULE__{} = api, events),
    do: send_log_events(api, events, new_deadline(api))

  @spec send_log_events(%__MODULE__{}, list(), deadline()) :: :ok | {:error, term()}
  def send_log_events(%__MODULE__{config: config} = api, events, deadline) do
    events
    |> Protocol.build_log_service_request(config.resource)
    |> send_proto("/v1/logs", api, deadline)
  end

  @spec send_metrics(%__MODULE__{}, list()) :: :ok | {:error, term()}
  def send_metrics(%__MODULE__{} = api, metrics),
    do: send_metrics(api, metrics, new_deadline(api))

  @spec send_metrics(%__MODULE__{}, list(), deadline()) :: :ok | {:error, term()}
  def send_metrics(%__MODULE__{config: config} = api, metrics, deadline) do
    metrics
    |> Protocol.build_metric_service_request(config.resource)
    |> send_proto("/v1/metrics", api, deadline)
  end

  @spec send_proto(struct(), String.t(), %__MODULE__{}, deadline()) :: :ok | {:error, term()}
  defp send_proto(body, path, %__MODULE__{} = api, deadline) do
    body
    |> encode_to_iodata()
    |> build_finch_request(path, api)
    |> make_finch_request(api.finch, deadline, with_retry?: api.retry)
  end

  def encode_to_iodata(body) do
    Protobuf.encode_to_iodata(body)
  rescue
    e in Protobuf.EncodeError ->
      raise Protobuf.EncodeError,
        message: """
        Failed to encode body: #{e.message}

        Body:

        #{inspect(body, pretty: true, limit: :infinity, printable_limit: :infinity)}
        """
  end

  defp build_finch_request(body, path, %__MODULE__{} = api) do
    Finch.build(
      :post,
      url(api, path),
      Map.to_list(headers(api)),
      maybe_compress(body, api)
    )
  end

  defp make_finch_request(request, finch_pool, deadline, with_retry?: true) do
    retry_delays =
      Retry.DelayStreams.exponential_backoff(@retry_initial_delay)
      |> Retry.DelayStreams.randomize()

    request_with_retry(request, finch_pool, deadline, retry_delays)
  end

  defp make_finch_request(request, finch_pool, deadline, with_retry?: false) do
    finch_request(request, finch_pool, deadline)
  end

  defp request_with_retry(request, finch_pool, deadline, retry_delays) do
    case finch_request(request, finch_pool, deadline) do
      :ok ->
        :ok

      {:error, {:unexpected_status, %{status: status}} = reason}
      when status not in @transient_statuses ->
        {:error, reason}

      {:error, reason} ->
        retry_after_error(request, finch_pool, deadline, retry_delays, {:error, reason})
    end
  end

  defp retry_after_error(request, finch_pool, deadline, retry_delays, last_error) do
    [delay] = Enum.take(retry_delays, 1)

    case remaining_timeout(deadline) do
      remaining when is_integer(remaining) and delay < remaining ->
        Process.sleep(delay)

        request_with_retry(
          request,
          finch_pool,
          deadline,
          Stream.drop(retry_delays, 1)
        )

      _ ->
        last_error
    end
  end

  defp finch_request(request, finch_pool, deadline) do
    case remaining_timeout(deadline) do
      remaining when is_integer(remaining) ->
        parent = self()
        request_ref = make_ref()

        {worker_pid, monitor_ref} =
          spawn_monitor(fn ->
            result = finch_request_in_worker(request, finch_pool, deadline)
            send(parent, {request_ref, result})
          end)

        await_finch_request(request_ref, worker_pid, monitor_ref, deadline)

      :expired ->
        timeout_error()
    end
  end

  defp finch_request_in_worker(request, finch_pool, deadline) do
    case remaining_timeout(deadline) do
      timeout when is_integer(timeout) ->
        try do
          Finch.request(
            request,
            finch_pool,
            pool_timeout: timeout,
            receive_timeout: timeout,
            request_timeout: timeout
          )
        rescue
          error in RuntimeError ->
            if checkout_timeout?(error) do
              {:error, :pool_timeout}
            else
              reraise(error, __STACKTRACE__)
            end
        end

      :expired ->
        timeout_error()
    end
  end

  defp await_finch_request(request_ref, worker_pid, monitor_ref, deadline) do
    case remaining_timeout(deadline) do
      timeout when is_integer(timeout) ->
        receive do
          {^request_ref, result} ->
            Process.demonitor(monitor_ref, [:flush])
            normalize_finch_response(result)

          {:DOWN, ^monitor_ref, :process, ^worker_pid, _reason} ->
            {:error, :request_failed}
        after
          timeout ->
            stop_finch_worker(request_ref, worker_pid, monitor_ref)
            timeout_error()
        end

      :expired ->
        stop_finch_worker(request_ref, worker_pid, monitor_ref)
        timeout_error()
    end
  end

  defp stop_finch_worker(request_ref, worker_pid, monitor_ref) do
    Process.exit(worker_pid, :kill)

    receive do
      {:DOWN, ^monitor_ref, :process, ^worker_pid, _reason} ->
        flush_finch_result(request_ref)
    end
  end

  defp flush_finch_result(request_ref) do
    receive do
      {^request_ref, _result} -> :ok
    after
      0 -> :ok
    end
  end

  defp normalize_finch_response({:ok, %{status: 200}}), do: :ok

  defp normalize_finch_response({:ok, %{status: status}}),
    do: {:error, {:unexpected_status, %{status: status}}}

  defp normalize_finch_response({:error, _reason} = error), do: error

  defp checkout_timeout?(%RuntimeError{message: message}),
    do: String.starts_with?(message, @finch_checkout_timeout_prefix)

  defp timeout_error, do: {:error, %Mint.TransportError{reason: :timeout}}

  defp url(%__MODULE__{config: config}, path), do: config.otlp_endpoint <> path

  defp headers(%__MODULE__{config: %Config{otlp_compression: compression} = config}) do
    [:content_type, :accept, :compression]
    |> Enum.reduce(%{}, fn
      :content_type, acc -> Map.put(acc, "content-type", "application/x-protobuf")
      :accept, acc -> Map.put(acc, "accept", "application/x-protobuf")
      :compression, acc when compression == :gzip -> Map.put(acc, "content-encoding", "gzip")
      _, acc -> acc
    end)
    |> Map.merge(config.otlp_headers)
  end

  defp maybe_compress(body, %__MODULE__{config: %Config{otlp_compression: :gzip}}),
    do: :zlib.gzip(body)

  defp maybe_compress(body, _), do: body
end
