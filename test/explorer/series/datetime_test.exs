defmodule Explorer.Series.DateTimeTest do
  use ExUnit.Case, async: true

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
end
