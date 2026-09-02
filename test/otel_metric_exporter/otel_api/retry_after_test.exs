defmodule OtelMetricExporter.OtelApi.RetryAfterTest do
  use ExUnit.Case, async: true

  alias OtelMetricExporter.OtelApi.RetryAfter

  @epoch_seconds :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

  test "parses delta-seconds with OWS, zero, and leading zeros" do
    assert {:ok, 0} = RetryAfter.parse("0", now_ms())
    assert {:ok, 12_000} = RetryAfter.parse(" \t0012\t ", now_ms())
  end

  test "rejects invalid delta-seconds values" do
    for value <- ["", "+1", "-1", "1.0", "1x", "s1", "1s"] do
      assert :error = RetryAfter.parse(value, now_ms())
    end
  end

  test "parses IMF-fixdate and clamps past dates to zero" do
    future = "Sun, 06 Nov 2094 08:49:37 GMT"
    past = "Sun, 06 Nov 1994 08:49:37 GMT"

    assert {:ok, timestamp({{2094, 11, 6}, {8, 49, 37}}) - now_ms()} ==
             RetryAfter.parse(future, now_ms())

    assert {:ok, 0} = RetryAfter.parse(past, now_ms())
  end

  test "parses every valid RFC850 weekday token" do
    for weekday <- [
          "Monday",
          "Tuesday",
          "Wednesday",
          "Thursday",
          "Friday",
          "Saturday",
          "Sunday"
        ] do
      value = "#{weekday}, 06-Nov-74 08:49:37 GMT"

      assert {:ok, timestamp({{2074, 11, 6}, {8, 49, 37}}) - now_ms()} ==
               RetryAfter.parse(value, now_ms())
    end
  end

  test "applies RFC850 rollover at exactly 50 years but not 51" do
    now_ms = timestamp({{2020, 1, 1}, {0, 0, 0}})

    assert {:ok, timestamp({{2070, 11, 6}, {8, 49, 37}}) - now_ms} ==
             RetryAfter.parse("Sunday, 06-Nov-70 08:49:37 GMT", now_ms)

    assert {:ok, 0} = RetryAfter.parse("Sunday, 06-Nov-71 08:49:37 GMT", now_ms)
  end

  test "selects the next century for a two-digit year after mid-century" do
    now_ms = timestamp({{2076, 1, 1}, {0, 0, 0}})

    assert {:ok, timestamp({{2100, 11, 6}, {8, 49, 37}}) - now_ms} ==
             RetryAfter.parse("Sunday, 06-Nov-00 08:49:37 GMT", now_ms)
  end

  test "parses asctime dates with a space-padded day and clamps past dates" do
    future = "Sun Nov  6 08:49:37 2094"
    past = "Sun Nov  6 08:49:37 1994"

    assert {:ok, timestamp({{2094, 11, 6}, {8, 49, 37}}) - now_ms()} ==
             RetryAfter.parse(future, now_ms())

    assert {:ok, 0} = RetryAfter.parse(past, now_ms())
  end

  test "honors a standards-valid leap second" do
    now_ms = timestamp({{2024, 6, 30}, {23, 59, 59}})

    assert {:ok, 1_000} = RetryAfter.parse("Sun, 30 Jun 2024 23:59:60 GMT", now_ms)
  end

  test "rejects invalid weekday, month, calendar, time, and GMT tokens" do
    invalid = [
      "Funday, 06 Nov 2094 08:49:37 GMT",
      "Sundaay, 06-Nov-74 08:49:37 GMT",
      "Sun, 06 Foo 2094 08:49:37 GMT",
      "Sunday, 31-Feb-74 08:49:37 GMT",
      "Sun Nov  6 24:49:37 2094",
      "Sun, 06 Nov 2094 08:60:37 GMT",
      "Sun, 06 Nov 2094 08:49:61 GMT",
      "Sun, 06 Nov 2094 08:49:37 UTC",
      "Sun, 06 Nov 2094 08:49:37 GMT trailing",
      "Sun Nov 6 08:49:37 2094"
    ]

    for value <- invalid do
      assert :error = RetryAfter.parse(value, now_ms())
    end
  end

  defp timestamp({date, time}) do
    (:calendar.datetime_to_gregorian_seconds({date, time}) - @epoch_seconds) * 1_000
  end

  defp now_ms, do: timestamp({{2024, 1, 1}, {0, 0, 0}})
end
