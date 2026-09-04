defmodule OtelMetricExporter.MetricStore.Aggregate do
  @moduledoc false

  import Bitwise

  @scale_bits 1_074
  @fraction_bits 52
  @exponent_bias 1_023
  @max_exponent 1_023
  @max_float_exponent 2_047

  @type distribution :: tuple()
  @type row :: number() | distribution()

  @spec new(:distribution, number(), non_neg_integer(), pos_integer()) :: distribution()
  def new(:distribution, value, bucket, bucket_count) do
    new_scaled(encode(value), bucket, bucket_count)
  end

  @spec new_scaled(integer(), non_neg_integer(), pos_integer()) :: distribution()
  def new_scaled(scaled, bucket, bucket_count) do
    buckets = List.duplicate(0, bucket_count) |> List.replace_at(bucket, 1)
    List.to_tuple([1, scaled, scaled, -scaled | buckets])
  end

  @spec add(:sum, number(), number()) :: number()
  def add(:sum, current, value), do: current + value

  @spec distribution_update_ops(integer(), non_neg_integer()) :: [tuple()]
  def distribution_update_ops(scaled, bucket) do
    [{2, 1}, {3, scaled}, {4, 0, scaled, scaled}, {5, 0, -scaled, -scaled}, {6 + bucket, 1}]
  end

  @spec merge(:counter | :sum | :last_value | :distribution, row(), row()) :: row()
  def merge(:counter, oldest, newest), do: oldest + newest
  def merge(:sum, oldest, newest), do: oldest + newest
  def merge(:last_value, _oldest, newest), do: newest

  def merge(:distribution, oldest, newest) do
    buckets = Enum.zip_with(buckets(oldest), buckets(newest), &Kernel.+/2)

    List.to_tuple([
      count(oldest) + count(newest),
      scaled_sum(oldest) + scaled_sum(newest),
      min(elem(oldest, 2), elem(newest, 2)),
      min(elem(oldest, 3), elem(newest, 3)) | buckets
    ])
  end

  @spec encode(number()) :: integer()
  def encode(value) when is_integer(value), do: value <<< @scale_bits

  def encode(value) when is_float(value) do
    <<sign::1, exponent::11, fraction::52>> = <<value::float-big-64>>

    cond do
      exponent == 0 and fraction == 0 ->
        0

      exponent == 0 ->
        signed(fraction, sign)

      exponent == @max_float_exponent ->
        raise ArithmeticError, "non-finite histogram value"

      true ->
        significand = (1 <<< @fraction_bits) + fraction
        signed(significand <<< (exponent - 1), sign)
    end
  end

  @spec decode(integer()) :: float()
  def decode(0), do: 0.0

  def decode(scaled) when is_integer(scaled) do
    sign = if scaled < 0, do: 1, else: 0
    magnitude = abs(scaled)
    highest_bit = bit_length(magnitude) - 1
    exponent = highest_bit - @scale_bits

    cond do
      exponent < -1_022 ->
        float_from_bits(sign, 0, magnitude)

      exponent > @max_exponent ->
        raise ArithmeticError, "histogram value out of range"

      true ->
        shift = highest_bit - @fraction_bits
        {significand, _remainder} = round_significand(magnitude, shift)

        {significand, exponent} =
          if significand == 1 <<< (@fraction_bits + 1) do
            {significand >>> 1, exponent + 1}
          else
            {significand, exponent}
          end

        if exponent > @max_exponent do
          raise ArithmeticError, "histogram value out of range"
        end

        float_from_bits(sign, exponent + @exponent_bias, significand - (1 <<< @fraction_bits))
    end
  end

  @spec count(distribution()) :: non_neg_integer()
  def count(row), do: elem(row, 0)

  @spec scaled_sum(distribution()) :: integer()
  def scaled_sum(row), do: elem(row, 1)

  @spec sum(distribution()) :: float()
  def sum(row), do: decode(scaled_sum(row))

  @spec min_value(distribution()) :: float()
  def min_value(row), do: decode(elem(row, 2))

  @spec max_value(distribution()) :: float()
  def max_value(row), do: decode(-elem(row, 3))

  @spec buckets(distribution()) :: [non_neg_integer()]
  def buckets(row), do: row |> Tuple.to_list() |> Enum.drop(4)

  @spec object(term(), row()) :: tuple()
  def object(key, row), do: List.to_tuple([key | Tuple.to_list(row)])

  @spec row(tuple()) :: row()
  def row(object), do: object |> Tuple.to_list() |> tl() |> List.to_tuple()

  @spec to_map(distribution()) :: map()
  def to_map(row) do
    %{
      count: count(row),
      sum: sum(row),
      min: min_value(row),
      max: max_value(row),
      buckets: buckets(row)
    }
  end

  defp signed(value, 0), do: value
  defp signed(value, 1), do: -value

  defp round_significand(magnitude, shift) when shift <= 0,
    do: {magnitude <<< -shift, 0}

  defp round_significand(magnitude, shift) do
    mask = (1 <<< shift) - 1
    significand = magnitude >>> shift
    remainder = magnitude &&& mask
    halfway = 1 <<< (shift - 1)

    rounded? = remainder > halfway or (remainder == halfway and odd?(significand))
    {if(rounded?, do: significand + 1, else: significand), remainder}
  end

  defp odd?(value), do: (value &&& 1) == 1

  defp bit_length(value) do
    <<first, rest::binary>> = :binary.encode_unsigned(value)
    byte_size(rest) * 8 + top_byte_bit_length(first)
  end

  defp top_byte_bit_length(byte) when byte >= 128, do: 8
  defp top_byte_bit_length(byte) when byte >= 64, do: 7
  defp top_byte_bit_length(byte) when byte >= 32, do: 6
  defp top_byte_bit_length(byte) when byte >= 16, do: 5
  defp top_byte_bit_length(byte) when byte >= 8, do: 4
  defp top_byte_bit_length(byte) when byte >= 4, do: 3
  defp top_byte_bit_length(byte) when byte >= 2, do: 2
  defp top_byte_bit_length(_byte), do: 1

  defp float_from_bits(sign, exponent, fraction) do
    bits = sign <<< 63 ||| exponent <<< @fraction_bits ||| fraction
    <<value::float-big-64>> = <<bits::unsigned-big-64>>
    value
  end
end
