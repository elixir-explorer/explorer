defmodule Explorer.Series.DateTimeTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Explorer.Series

  describe "ns" do
    setup do
      # This is a dataframe with a single column called datetime with 3 values [~N[2023-04-19 16:14:35.474487],~N[2023-04-20 16:14:35.474487], ~N[2023-04-21 16:14:35.474487]] with datetime[ns] precession
      df = Explorer.DataFrame.from_parquet!("test/support/datetime_with_ns_res.parquet")
      series = Explorer.DataFrame.to_series(df)
      [series: series["datetime"]]
    end

    test "min", %{series: series} do
      assert Series.min(series) == ~N[2023-04-19 16:14:35.474487]
    end

    test "max", %{series: series} do
      assert Series.max(series) == ~N[2023-04-21 16:14:35.474487]
    end

    test "quantile", %{series: series} do
      assert Series.quantile(series, 0.5) == ~N[2023-04-20 16:14:35.474487]
    end

    test "day_of_week", %{series: series} do
      assert Series.day_of_week(series) |> Series.to_list() == [4, 3, 5]
    end

    test "month", %{series: series} do
      assert Series.month(series) |> Series.to_list() == [4, 4, 4]
    end

    test "year", %{series: series} do
      assert Series.year(series) |> Series.to_list() == [2023, 2023, 2023]
    end

    test "hour", %{series: series} do
      assert Series.hour(series) |> Series.to_list() == [16, 16, 16]
    end

    test "minute", %{series: series} do
      assert Series.minute(series) |> Series.to_list() == [14, 14, 14]
    end

    test "second", %{series: series} do
      assert Series.second(series) |> Series.to_list() == [35, 35, 35]
    end
  end

  describe "timezones" do
    test "UTC" do
      datetimes_in = [
        ~U[2024-01-01T12:00:00.000000Z],
        ~U[2024-01-01T13:00:00.000000Z],
        ~U[2024-01-01T14:00:00.000000Z]
      ]

      datetimes_out =
        datetimes_in
        |> Series.from_list()
        |> Series.to_list()

      assert datetimes_out == datetimes_in
    end

    test "America/New_York" do
      datetimes_in =
        [
          ~U[2024-01-01T12:00:00.000000Z],
          ~U[2024-01-01T13:00:00.000000Z],
          ~U[2024-01-01T14:00:00.000000Z]
        ]
        |> Enum.map(&DateTime.shift_zone!(&1, "America/New_York"))

      datetimes_out =
        datetimes_in
        |> Series.from_list()
        |> Series.to_list()

      assert datetimes_out == datetimes_in
    end

    test "can't build a series from datetimes with non-matching timezones" do
      datetimes_in =
        [
          ~U[2024-01-01T12:00:00.000000Z],
          ~U[2024-01-01T13:00:00.000000Z] |> DateTime.shift_zone!("America/New_York")
        ]

      assert_raise(
        ArgumentError,
        "the value #DateTime<2024-01-01 08:00:00.000000-05:00 EST America/New_York> does not match the inferred dtype {:datetime, :microsecond, \"Etc/UTC\"}",
        fn -> Series.from_list(datetimes_in) end
      )
    end
  end

  test "can't encode (naive) datetimes with nanosecond precision outside representable range" do
    # i64 datetimes with nanosecond precision can span a range from
    # 1677-09-21T00:12:43.145224192 to 2262-04-11T23:47:16.854775807.
    below_min_ndt = ~N[1666-01-01 00:00:00]
    below_min_utc = ~U[1666-01-01 00:00:00Z]

    assert_raise(
      RuntimeError,
      "Timestamp Conversion Error: cannot represent naive datetime(ns) with `i64`",
      fn -> Series.from_list([below_min_ndt], dtype: {:naive_datetime, :nanosecond}) end
    )

    assert_raise(
      RuntimeError,
      "Timestamp Conversion Error: cannot represent datetime(ns) with `i64`",
      fn -> Series.from_list([below_min_utc], dtype: {:datetime, :nanosecond, "Etc/UTC"}) end
    )
  end

  property "naive datetimes survive encoding" do
    check all(
            time_unit <- Explorer.Generator.time_unit(),
            timestamp <- Explorer.Generator.naive_datetime(time_unit),
            max_runs: 10_000
          ) do
      [timestamp_after] =
        [timestamp]
        |> Explorer.Series.from_list(dtype: {:naive_datetime, time_unit})
        |> Explorer.Series.to_list()

      timestamp_trunc =
        case time_unit do
          :nanosecond -> timestamp
          _ -> NaiveDateTime.truncate(timestamp, time_unit)
        end

      assert NaiveDateTime.compare(timestamp_trunc, timestamp_after) == :eq
    end
  end

  property "datetimes survive encoding" do
    check all(
            time_unit <- Explorer.Generator.time_unit(),
            timestamp <- Explorer.Generator.datetime(time_unit, "Etc/UTC"),
            max_runs: 10_000
          ) do
      [timestamp_after] =
        [timestamp]
        |> Explorer.Series.from_list(dtype: {:datetime, time_unit, "Etc/UTC"})
        |> Explorer.Series.to_list()

      timestamp_trunc =
        case time_unit do
          :nanosecond -> timestamp
          _ -> DateTime.truncate(timestamp, time_unit)
        end

      assert DateTime.compare(timestamp_trunc, timestamp_after) == :eq
    end
  end
end
