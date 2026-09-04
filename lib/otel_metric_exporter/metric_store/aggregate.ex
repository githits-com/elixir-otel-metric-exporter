defmodule OtelMetricExporter.MetricStore.Aggregate do
  @moduledoc false

  @type distribution :: %{
          count: non_neg_integer(),
          sum: number(),
          min: number(),
          max: number(),
          buckets: [non_neg_integer()]
        }

  @type row :: number() | distribution()

  @spec new(:distribution, number(), non_neg_integer(), non_neg_integer()) :: distribution()
  def new(:distribution, value, bucket, bucket_count) do
    buckets = List.duplicate(0, bucket_count)

    %{
      count: 1,
      sum: value,
      min: value,
      max: value,
      buckets: List.replace_at(buckets, bucket, 1)
    }
  end

  @spec add(:sum, number(), number()) :: number()
  def add(:sum, current, value), do: current + value

  @spec add(:distribution, distribution(), number(), non_neg_integer()) :: distribution()
  def add(:distribution, current, value, bucket) do
    %{
      count: current.count + 1,
      sum: current.sum + value,
      min: min(current.min, value),
      max: max(current.max, value),
      buckets: List.update_at(current.buckets, bucket, &(&1 + 1))
    }
  end

  @spec merge(:counter | :sum | :last_value | :distribution, row(), row()) :: row()
  def merge(:counter, oldest, newest), do: oldest + newest
  def merge(:sum, oldest, newest), do: oldest + newest
  def merge(:last_value, _oldest, newest), do: newest

  def merge(:distribution, oldest, newest) do
    %{
      count: oldest.count + newest.count,
      sum: oldest.sum + newest.sum,
      min: min(oldest.min, newest.min),
      max: max(oldest.max, newest.max),
      buckets: Enum.zip_with(oldest.buckets, newest.buckets, &(&1 + &2))
    }
  end
end
