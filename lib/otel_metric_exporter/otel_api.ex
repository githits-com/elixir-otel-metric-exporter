defmodule OtelMetricExporter.OtelApi do
  @moduledoc false

  alias OtelMetricExporter.OtelApi.Config

  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Logs.V1.{
    ExportLogsPartialSuccess,
    ExportLogsServiceResponse
  }

  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.{
    ExportMetricsPartialSuccess,
    ExportMetricsServiceResponse
  }

  alias OtelMetricExporter.Protocol
  alias OtelMetricExporter.OtelApi.RetryAfter

  @opaque deadline :: integer()
  @type failure_reason ::
          :invalid_response
          | :encoding_failed
          | :deadline_exceeded
          | :pool_timeout
          | :transport_failure
          | :request_failed
          | :export_task_failed
          | :response_too_large
          | {:http_status, non_neg_integer()}
  @type failure_disposition :: :terminal | :retryable
  @type export_result ::
          :ok
          | {:partial_success, non_neg_integer()}
          | {:error, failure_disposition(), failure_reason()}
  @typep retry_after_hint :: :none | non_neg_integer()
  @typep retry_after_header_state ::
           :missing | :duplicate | {:present, binary()}
  @typep response_accumulator :: %{
           status: non_neg_integer() | nil,
           body: [binary()],
           bytes: non_neg_integer(),
           overflow: boolean(),
           retry_after: retry_after_header_state()
         }
  @typep internal_retry_result ::
           {:error, :retryable, failure_reason(), retry_after_hint()}
  @typep finch_result ::
           {:ok, binary()}
           | {:error, failure_disposition(), failure_reason()}
           | internal_retry_result()
  @typep public_finch_result ::
           {:ok, binary()} | {:error, failure_disposition(), failure_reason()}

  @retry_initial_delay 1_000
  @max_response_body_bytes 4_194_304
  @transient_statuses [429, 502, 503, 504]
  @finch_checkout_timeout_prefix "Finch was unable to provide a connection within the timeout"

  @schema NimbleOptions.new!(
            finch: [
              type: :atom,
              required: true,
              doc: "Registered Finch process name to use for sending requests."
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

  @spec send_log_events(%__MODULE__{}, list()) :: export_result()
  def send_log_events(%__MODULE__{} = api, events),
    do: send_log_events(api, events, new_deadline(api))

  @spec send_log_events(%__MODULE__{}, list(), deadline()) :: export_result()
  def send_log_events(%__MODULE__{config: config} = api, events, deadline) do
    events
    |> Protocol.build_log_service_request(config.resource)
    |> send_proto("/v1/logs", api, deadline)
  end

  @spec send_metrics(%__MODULE__{}, list()) :: export_result()
  def send_metrics(%__MODULE__{} = api, metrics),
    do: send_metrics(api, metrics, new_deadline(api))

  @spec send_metrics(%__MODULE__{}, list(), deadline()) :: export_result()
  def send_metrics(%__MODULE__{config: config} = api, metrics, deadline) do
    metrics
    |> Protocol.build_metric_service_request(config.resource)
    |> send_proto("/v1/metrics", api, deadline)
  end

  @spec send_proto(struct(), String.t(), %__MODULE__{}, deadline()) :: export_result()
  defp send_proto(body, path, %__MODULE__{} = api, deadline) do
    try do
      request =
        body
        |> encode_to_iodata()
        |> build_finch_request(path, api)

      with {:ok, response_body} <-
             make_finch_request(request, api.finch, deadline, with_retry?: api.retry) do
        decode_response(response_body, api.scope)
      end
    rescue
      Protobuf.EncodeError -> {:error, :terminal, :encoding_failed}
    end
  end

  def encode_to_iodata(body) do
    Protobuf.encode_to_iodata(body)
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
    request
    |> finch_request(finch_pool, deadline)
    |> public_result()
  end

  defp request_with_retry(request, finch_pool, deadline, retry_delays) do
    case finch_request(request, finch_pool, deadline) do
      {:ok, _response_body} = success ->
        success

      {:error, :terminal, _reason} = error ->
        error

      {:error, :retryable, reason, retry_after} ->
        retry_after_error(
          request,
          finch_pool,
          deadline,
          retry_delays,
          {:error, :retryable, reason},
          retry_after
        )

      {:error, :retryable, _reason} = error ->
        retry_after_error(request, finch_pool, deadline, retry_delays, error, :none)
    end
  end

  @spec retry_after_error(
          Finch.Request.t(),
          Finch.name(),
          deadline(),
          Enumerable.t(),
          {:error, :retryable, failure_reason()},
          retry_after_hint()
        ) :: public_finch_result()
  defp retry_after_error(request, finch_pool, deadline, retry_delays, last_error, retry_after) do
    [delay] = Enum.take(retry_delays, 1)
    delay = if is_integer(retry_after), do: retry_after, else: delay

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
          :erlang.spawn_opt(
            fn ->
              result = finch_request_in_worker(request, finch_pool, deadline)
              send(parent, {request_ref, result})
            end,
            [:link, :monitor]
          )

        await_finch_request(request_ref, worker_pid, monitor_ref, deadline)

      :expired ->
        timeout_error()
    end
  end

  defp finch_request_in_worker(request, finch_pool, deadline) do
    case remaining_timeout(deadline) do
      timeout when is_integer(timeout) ->
        try do
          Finch.stream_while(
            request,
            finch_pool,
            response_accumulator(),
            &stream_response/2,
            pool_timeout: timeout,
            receive_timeout: timeout,
            request_timeout: timeout
          )
          |> normalize_stream_response()
        rescue
          error in RuntimeError ->
            if checkout_timeout?(error) do
              {:error, :retryable, :pool_timeout}
            else
              {:error, :retryable, :request_failed}
            end
        catch
          _kind, _reason ->
            {:error, :retryable, :request_failed}
        end

      :expired ->
        timeout_error()
    end
  end

  @spec response_accumulator() :: response_accumulator()
  defp response_accumulator do
    %{status: nil, body: [], bytes: 0, overflow: false, retry_after: :missing}
  end

  defp stream_response({:status, status}, response),
    do: {:cont, %{response | status: status, retry_after: :missing}}

  defp stream_response({:headers, headers}, response) do
    {:cont, %{response | retry_after: retry_after_header(headers, response.retry_after)}}
  end

  defp stream_response({:data, data}, %{status: 200, bytes: bytes} = response) do
    total_bytes = bytes + byte_size(data)

    if total_bytes > @max_response_body_bytes do
      {:halt, %{response | body: [], bytes: 0, overflow: true}}
    else
      {:cont, %{response | body: [data | response.body], bytes: total_bytes}}
    end
  end

  # Non-200 responses are classified from their status only. Their bodies are
  # consumed by Finch without retaining receiver content in the exporter.
  defp stream_response({:data, _data}, response), do: {:cont, response}

  defp stream_response(_entry, response), do: {:cont, response}

  @spec normalize_stream_response(
          {:ok, response_accumulator()}
          | {:error, term(), response_accumulator()}
        ) :: finch_result()
  defp normalize_stream_response({:ok, %{status: 200, overflow: true}}),
    do: {:error, :terminal, :response_too_large}

  defp normalize_stream_response({:ok, %{status: 200, body: body}}),
    do: {:ok, body |> Enum.reverse() |> IO.iodata_to_binary()}

  defp normalize_stream_response({:ok, %{status: status, retry_after: retry_after}})
       when status in @transient_statuses do
    {:error, :retryable, {:http_status, status}, parse_retry_after(retry_after)}
  end

  defp normalize_stream_response({:ok, %{status: status}}),
    do: {:error, :terminal, {:http_status, status}}

  defp normalize_stream_response({:error, _reason, _response}),
    do: {:error, :retryable, :transport_failure, :none}

  @spec retry_after_header(Mint.Types.headers(), retry_after_header_state()) ::
          retry_after_header_state()
  defp retry_after_header(headers, initial_state) do
    Enum.reduce(headers, initial_state, fn
      {name, value}, :missing ->
        if retry_after_header?(name), do: {:present, value}, else: :missing

      {name, _other_value}, {:present, _present_value} = present ->
        if retry_after_header?(name), do: :duplicate, else: present

      {_name, _value}, :duplicate ->
        :duplicate
    end)
  end

  @spec retry_after_header?(binary()) :: boolean()
  defp retry_after_header?(name) when is_binary(name),
    do: String.downcase(name) == "retry-after"

  @spec parse_retry_after(retry_after_header_state()) :: retry_after_hint()
  defp parse_retry_after({:present, value}) do
    case RetryAfter.parse(value) do
      {:ok, delay} -> delay
      :error -> :none
    end
  end

  defp parse_retry_after(_header), do: :none

  @spec public_result(finch_result()) :: public_finch_result()
  defp public_result({:error, disposition, reason, _retry_after}),
    do: {:error, disposition, reason}

  defp public_result(result), do: result

  defp await_finch_request(request_ref, worker_pid, monitor_ref, deadline) do
    case remaining_timeout(deadline) do
      timeout when is_integer(timeout) ->
        receive do
          {^request_ref, result} ->
            Process.demonitor(monitor_ref, [:flush])
            result

          {:DOWN, ^monitor_ref, :process, ^worker_pid, _reason} ->
            {:error, :retryable, :request_failed}
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
    # Owner-controlled cancellation must not propagate :killed back through the link.
    Process.unlink(worker_pid)
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

  defp checkout_timeout?(%RuntimeError{message: message}),
    do: String.starts_with?(message, @finch_checkout_timeout_prefix)

  defp timeout_error, do: {:error, :retryable, :deadline_exceeded}

  # The pinned protobuf decoder leaks MatchError while matching truncated fixed-width
  # or length-delimited fields, so keep it inside the invalid-response boundary.
  defp decode_response(body, :logs) do
    case Protobuf.decode(body, ExportLogsServiceResponse) do
      %ExportLogsServiceResponse{partial_success: nil} ->
        :ok

      %ExportLogsServiceResponse{
        partial_success: %ExportLogsPartialSuccess{
          rejected_log_records: rejected_count
        }
      } ->
        normalize_partial_success(rejected_count)
    end
  rescue
    Protobuf.DecodeError -> {:error, :terminal, :invalid_response}
    MatchError -> {:error, :terminal, :invalid_response}
  end

  defp decode_response(body, :metrics) do
    case Protobuf.decode(body, ExportMetricsServiceResponse) do
      %ExportMetricsServiceResponse{partial_success: nil} ->
        :ok

      %ExportMetricsServiceResponse{
        partial_success: %ExportMetricsPartialSuccess{
          rejected_data_points: rejected_count
        }
      } ->
        normalize_partial_success(rejected_count)
    end
  rescue
    Protobuf.DecodeError -> {:error, :terminal, :invalid_response}
    MatchError -> {:error, :terminal, :invalid_response}
  end

  defp normalize_partial_success(rejected_count) when rejected_count >= 0,
    do: {:partial_success, rejected_count}

  defp normalize_partial_success(_rejected_count),
    do: {:error, :terminal, :invalid_response}

  @spec url(%__MODULE__{}, String.t()) :: String.t()
  defp url(
         %__MODULE__{
           config: %Config{otlp_endpoint: endpoint, otlp_endpoint_kind: :signal}
         },
         _path
       ),
       do: endpoint

  defp url(
         %__MODULE__{
           config: %Config{otlp_endpoint: endpoint, otlp_endpoint_kind: :generic}
         },
         path
       ),
       do: endpoint |> URI.parse() |> URI.append_path(path) |> URI.to_string()

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
