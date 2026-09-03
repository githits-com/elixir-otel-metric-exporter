defmodule OtelMetricExporter.OtlpUtils do
  @moduledoc false

  alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.{
    AnyValue,
    KeyValue,
    KeyValueList,
    ArrayValue
  }

  @type kv_value ::
          {:string_value, String.t()}
          | {:bool_value, boolean()}
          | {:int_value, integer()}
          | {:double_value, float()}
          | {:kvlist_value, %KeyValueList{}}
          | {:array_value, %ArrayValue{}}

  def build_kv(tags, key_prefix \\ "") do
    Enum.flat_map(tags, fn
      {key, value} when is_map(value) and not is_struct(value) ->
        build_kv(value, key_prefix <> key_to_string(key) <> ".")

      {key, value} ->
        [
          %KeyValue{
            key: key_prefix <> key_to_string(key),
            value: %AnyValue{value: to_kv_value(value)}
          }
        ]
    end)
  end

  @spec to_kv_value(term()) :: kv_value()
  def to_kv_value(value) when is_binary(value), do: {:string_value, value}
  def to_kv_value(value) when is_boolean(value), do: {:bool_value, value}
  def to_kv_value(value) when is_atom(value), do: {:string_value, to_string(value)}
  def to_kv_value(value) when is_integer(value), do: {:int_value, value}
  def to_kv_value(value) when is_float(value), do: {:double_value, value}
  def to_kv_value(value) when is_struct(value), do: {:string_value, inspect(value)}

  def to_kv_value([{k, _} | _] = value) when is_atom(k),
    do: {:kvlist_value, %KeyValueList{values: build_kv(value)}}

  def to_kv_value(value) when is_list(value),
    do: {:array_value, %ArrayValue{values: Enum.map(value, &%AnyValue{value: to_kv_value(&1)})}}

  def to_kv_value(value) when is_tuple(value), do: to_kv_value(Tuple.to_list(value))
  def to_kv_value(value) when is_pid(value), do: to_kv_value(inspect(value))
  def to_kv_value(any), do: to_kv_value(inspect(any))

  @spec key_to_string(term()) :: String.t()
  defp key_to_string(key) do
    case String.Chars.impl_for(key) do
      nil -> inspect(key)
      _implementation -> to_string(key)
    end
  end
end
