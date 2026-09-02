defmodule OtelMetricExporter.OtelApi.RetryAfter do
  @moduledoc """
  Parses the `Retry-After` response header into a non-negative millisecond delay.

  HTTP-date values are interpreted against the current UTC wall clock. The
  two-argument form accepts an injected millisecond timestamp so callers can
  test absolute dates without depending on the machine clock.
  """

  @type result :: {:ok, non_neg_integer()} | :error
  @typep date_components ::
           {integer(), integer(), integer(), integer(), integer(), integer()}
  @typep parsed_date :: {:ok, date_components()} | :error
  @typep time_components :: {integer(), integer(), integer()}

  @weekday_short ~w(Mon Tue Wed Thu Fri Sat Sun)
  @weekday_long ~w(Monday Tuesday Wednesday Thursday Friday Saturday Sunday)
  @months ~w(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec)
  @epoch_seconds :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

  @spec parse(binary()) :: result()
  def parse(value) when is_binary(value),
    do: parse(value, System.system_time(:millisecond))

  @spec parse(binary(), integer()) :: result()
  def parse(value, now_ms) when is_binary(value) and is_integer(now_ms) do
    value = trim_ows(value)

    case parse_delta_seconds(value) do
      {:ok, seconds} ->
        {:ok, seconds * 1_000}

      :error ->
        parse_http_date(value, now_ms)
    end
  end

  @spec parse_delta_seconds(binary()) :: {:ok, non_neg_integer()} | :error
  defp parse_delta_seconds(<<>>), do: :error

  defp parse_delta_seconds(value) do
    if ascii_digits?(value) do
      case Integer.parse(value, 10) do
        {seconds, ""} when seconds >= 0 -> {:ok, seconds}
        _ -> :error
      end
    else
      :error
    end
  end

  @spec parse_http_date(binary(), integer()) :: result()
  defp parse_http_date(value, now_ms) do
    value
    |> parse_date_components(now_ms)
    |> date_delay(now_ms)
  end

  @spec parse_date_components(binary(), integer()) :: parsed_date()
  defp parse_date_components(value, now_ms) do
    case parse_imf_fixdate(value) do
      {:ok, _components} = parsed ->
        parsed

      :error ->
        case parse_rfc850(value, now_ms) do
          {:ok, _components} = parsed -> parsed
          :error -> parse_asctime(value)
        end
    end
  end

  @spec date_delay(parsed_date(), integer()) :: result()
  defp date_delay({:ok, {year, month, day, hour, minute, second}}, now_ms) do
    with {:ok, date_ms} <- date_milliseconds(year, month, day, hour, minute, second) do
      {:ok, max(date_ms - now_ms, 0)}
    end
  end

  defp date_delay(:error, _now_ms), do: :error

  @spec parse_imf_fixdate(binary()) :: parsed_date()
  defp parse_imf_fixdate(
         <<weekday::binary-size(3), ", ", day::binary-size(2), " ", month::binary-size(3), " ",
           year::binary-size(4), " ", time::binary-size(8), " GMT">>
       ) do
    with true <- weekday in @weekday_short,
         {:ok, day} <- decimal(day),
         {:ok, month} <- month_number(month),
         {:ok, year} <- decimal(year),
         {:ok, {hour, minute, second}} <- parse_time(time) do
      {:ok, {year, month, day, hour, minute, second}}
    else
      _ -> :error
    end
  end

  defp parse_imf_fixdate(_value), do: :error

  @spec parse_rfc850(binary(), integer()) :: parsed_date()
  defp parse_rfc850(value, now_ms) do
    case :binary.split(value, ", ", [:global]) do
      [weekday, rest] when weekday in @weekday_long ->
        parse_rfc850_rest(rest, now_ms)

      _ ->
        :error
    end
  end

  @spec parse_rfc850_rest(binary(), integer()) :: parsed_date()
  defp parse_rfc850_rest(
         <<day::binary-size(2), "-", month::binary-size(3), "-", year::binary-size(2), " ",
           time::binary-size(8), " GMT">>,
         now_ms
       ) do
    with {:ok, day} <- decimal(day),
         {:ok, month} <- month_number(month),
         {:ok, year} <- decimal(year),
         {:ok, {hour, minute, second}} <- parse_time(time) do
      {:ok, {rollover_year(year, now_ms), month, day, hour, minute, second}}
    else
      _ -> :error
    end
  end

  defp parse_rfc850_rest(_value, _now_ms), do: :error

  @spec parse_asctime(binary()) :: parsed_date()
  defp parse_asctime(
         <<weekday::binary-size(3), " ", month::binary-size(3), " ", day::binary-size(2), " ",
           time::binary-size(8), " ", year::binary-size(4)>>
       ) do
    with true <- weekday in @weekday_short,
         {:ok, month} <- month_number(month),
         {:ok, day} <- asctime_day(day),
         {:ok, {hour, minute, second}} <- parse_time(time),
         {:ok, year} <- decimal(year) do
      {:ok, {year, month, day, hour, minute, second}}
    else
      _ -> :error
    end
  end

  defp parse_asctime(_value), do: :error

  @spec asctime_day(binary()) :: {:ok, non_neg_integer()} | :error
  defp asctime_day(<<" ", digit>>) when digit in ?0..?9, do: {:ok, digit - ?0}
  defp asctime_day(day), do: decimal(day)

  @spec parse_time(binary()) :: {:ok, time_components()} | :error
  defp parse_time(
         <<hour::binary-size(2), ":", minute::binary-size(2), ":", second::binary-size(2)>>
       ) do
    with {:ok, hour} <- decimal(hour),
         {:ok, minute} <- decimal(minute),
         {:ok, second} <- decimal(second),
         true <- hour in 0..23,
         true <- minute in 0..59,
         true <- second in 0..60 do
      {:ok, {hour, minute, second}}
    else
      _ -> :error
    end
  end

  defp parse_time(_value), do: :error

  @spec month_number(binary()) :: {:ok, 1..12} | :error
  defp month_number(month) do
    case Enum.find_index(@months, &(&1 == month)) do
      nil -> :error
      index -> {:ok, index + 1}
    end
  end

  @spec decimal(binary()) :: {:ok, non_neg_integer()} | :error
  defp decimal(value) do
    if ascii_digits?(value) do
      case Integer.parse(value, 10) do
        {number, ""} -> {:ok, number}
        _ -> :error
      end
    else
      :error
    end
  end

  @spec ascii_digits?(binary()) :: boolean()
  defp ascii_digits?(<<>>), do: true
  defp ascii_digits?(<<digit, rest::binary>>) when digit in ?0..?9, do: ascii_digits?(rest)
  defp ascii_digits?(_value), do: false

  @spec date_milliseconds(integer(), integer(), integer(), integer(), integer(), integer()) ::
          {:ok, integer()} | :error
  defp date_milliseconds(year, month, day, hour, minute, second) do
    if :calendar.valid_date({year, month, day}) do
      calendar_second = min(second, 59)

      seconds =
        :calendar.datetime_to_gregorian_seconds(
          {{year, month, day}, {hour, minute, calendar_second}}
        )

      leap_second_ms = if second == 60, do: 1_000, else: 0
      {:ok, (seconds - @epoch_seconds) * 1_000 + leap_second_ms}
    else
      :error
    end
  end

  @spec rollover_year(non_neg_integer(), integer()) :: integer()
  defp rollover_year(two_digit_year, now_ms) do
    now_year =
      now_ms
      |> div(1_000)
      |> Kernel.+(@epoch_seconds)
      |> :calendar.gregorian_seconds_to_datetime()
      |> elem(0)
      |> elem(0)

    initially_selected = div(now_year, 100) * 100 + two_digit_year

    cond do
      initially_selected - now_year > 50 -> initially_selected - 100
      now_year - initially_selected > 50 -> initially_selected + 100
      true -> initially_selected
    end
  end

  @spec trim_ows(binary()) :: binary()
  defp trim_ows(value), do: value |> trim_leading_ows() |> trim_trailing_ows()

  @spec trim_leading_ows(binary()) :: binary()
  defp trim_leading_ows(<<char, rest::binary>>) when char in [32, 9],
    do: trim_leading_ows(rest)

  defp trim_leading_ows(value), do: value

  @spec trim_trailing_ows(binary()) :: binary()
  defp trim_trailing_ows(<<>>), do: <<>>

  defp trim_trailing_ows(value) do
    size = byte_size(value)
    body = :binary.part(value, 0, size - 1)
    char = :binary.last(value)

    if char in [32, 9], do: trim_trailing_ows(body), else: value
  end
end
