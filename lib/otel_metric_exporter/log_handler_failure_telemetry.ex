defmodule OtelMetricExporter.LogHandlerFailureTelemetry do
  @moduledoc """
  Emits bounded diagnostics when the Logger callback fails.

  Event metadata contains classifications only. It never contains the log
  event, Logger metadata values, the exception reason or stacktrace, or OTLP
  configuration and responses.
  """

  alias OtelMetricExporter.LogHandler
  alias OtelMetricExporter.Protocol

  @event [:otel_metric_exporter, :log_handler, :exception]
  @emitting_key {__MODULE__, :emitting}

  @type stage :: :olp_liveness | :prepare | :load | :handler
  @type failure_source ::
          :trace_context | :body | :attributes | :protocol | :olp | :handler | :unknown
  @type exception ::
          :argument_error
          | :function_clause
          | :match_error
          | :protocol_undefined
          | :undefined_function
          | :key_error
          | :case_clause
          | :bad_map
          | :other_error
          | :exit
          | :throw
  @type message_shape ::
          :string | :report_map | :report_struct | :report_list | :report_other | :format | :other
  @type trace_context :: :valid | :missing | :partial | :invalid
  @type metadata :: %{
          required(:stage) => stage(),
          required(:failure_source) => failure_source(),
          required(:exception) => exception(),
          required(:message_shape) => message_shape(),
          required(:trace_context) => trace_context(),
          required(:olp_alive) => boolean()
        }

  @spec emit(:logger.log_event(), pid(), :error | :exit | :throw, term(), list()) :: :ok
  def emit(event, olp_pid, kind, reason, stacktrace) do
    if Process.get(@emitting_key) do
      :ok
    else
      Process.put(@emitting_key, true)

      try do
        execute(event, olp_pid, kind, reason, stacktrace)
      after
        Process.delete(@emitting_key)
      end
    end
  end

  defp execute(event, olp_pid, kind, reason, stacktrace) do
    olp_alive = Process.alive?(olp_pid)

    :telemetry.execute(@event, %{count: 1}, %{
      stage: stage(stacktrace, olp_alive),
      failure_source: failure_source(stacktrace),
      exception: exception(kind, reason),
      message_shape: message_shape(event),
      trace_context: trace_context(event),
      olp_alive: olp_alive
    })
  end

  defp stage(stacktrace, olp_alive) do
    cond do
      stacktrace_from?(stacktrace, Protocol) -> :prepare
      stacktrace_from?(stacktrace, :logger_olp) -> :load
      not olp_alive -> :olp_liveness
      true -> :handler
    end
  end

  defp failure_source(stacktrace) do
    cond do
      stacktrace_from?(stacktrace, Protocol, :hex_to_bytes) -> :trace_context
      stacktrace_from?(stacktrace, Base, :decode16!) -> :trace_context
      stacktrace_from?(stacktrace, Protocol, :encode_body) -> :body
      stacktrace_from?(stacktrace, Protocol, :prepare_attributes) -> :attributes
      stacktrace_from?(stacktrace, Protocol) -> :protocol
      stacktrace_from?(stacktrace, :logger_olp) -> :olp
      stacktrace_from?(stacktrace, LogHandler) -> :handler
      true -> :unknown
    end
  end

  defp stacktrace_from?(stacktrace, module, function \\ nil) do
    Enum.any?(stacktrace, fn
      {^module, _function, _arity_or_args, _location} when is_nil(function) -> true
      {^module, ^function, _arity_or_args, _location} -> true
      _frame -> false
    end)
  end

  defp exception(:exit, _reason), do: :exit
  defp exception(:throw, _reason), do: :throw
  defp exception(:error, %ArgumentError{}), do: :argument_error
  defp exception(:error, :badarg), do: :argument_error
  defp exception(:error, %FunctionClauseError{}), do: :function_clause
  defp exception(:error, :function_clause), do: :function_clause
  defp exception(:error, %MatchError{}), do: :match_error
  defp exception(:error, {:badmatch, _value}), do: :match_error
  defp exception(:error, %Elixir.Protocol.UndefinedError{}), do: :protocol_undefined
  defp exception(:error, %UndefinedFunctionError{}), do: :undefined_function
  defp exception(:error, :undef), do: :undefined_function
  defp exception(:error, %KeyError{}), do: :key_error
  defp exception(:error, {:badkey, _key}), do: :key_error
  defp exception(:error, %CaseClauseError{}), do: :case_clause
  defp exception(:error, {:case_clause, _value}), do: :case_clause
  defp exception(:error, %BadMapError{}), do: :bad_map
  defp exception(:error, {:badmap, _value}), do: :bad_map
  defp exception(:error, _reason), do: :other_error

  defp message_shape(%{msg: {:string, _chardata}}), do: :string
  defp message_shape(%{msg: {:report, %_struct{}}}), do: :report_struct
  defp message_shape(%{msg: {:report, report}}) when is_map(report), do: :report_map
  defp message_shape(%{msg: {:report, report}}) when is_list(report), do: :report_list
  defp message_shape(%{msg: {:report, _report}}), do: :report_other
  defp message_shape(%{msg: {_format, _args}}), do: :format
  defp message_shape(_event), do: :other

  defp trace_context(%{meta: metadata}) when is_map(metadata) do
    trace_id = Map.get(metadata, :otel_trace_id)
    span_id = Map.get(metadata, :otel_span_id)

    cond do
      is_nil(trace_id) and is_nil(span_id) ->
        :missing

      valid_hex_id?(trace_id, 32) and valid_hex_id?(span_id, 16) ->
        :valid

      (is_nil(trace_id) and valid_hex_id?(span_id, 16)) or
          (valid_hex_id?(trace_id, 32) and is_nil(span_id)) ->
        :partial

      true ->
        :invalid
    end
  end

  defp trace_context(_event), do: :invalid

  defp valid_hex_id?(value, length) when is_binary(value) and byte_size(value) == length do
    match?({:ok, _bytes}, Base.decode16(value, case: :mixed))
  end

  defp valid_hex_id?(value, length) when is_list(value), do: valid_hex_charlist?(value, length)
  defp valid_hex_id?(_value, _length), do: false

  defp valid_hex_charlist?([], 0), do: true

  defp valid_hex_charlist?([char | rest], remaining)
       when remaining > 0 and
              (char in ?0..?9 or char in ?A..?F or char in ?a..?f) do
    valid_hex_charlist?(rest, remaining - 1)
  end

  defp valid_hex_charlist?(_value, _remaining), do: false
end
