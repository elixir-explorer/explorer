defmodule Explorer.PolarsBackend.SeriesTest do
  use ExUnit.Case, async: true
  alias Explorer.PolarsBackend.Series

  test "from_list/2 of dates" do
    dates = [~D[1643-01-04], ~D[-0030-08-12], ~D[1994-05-01]]

    assert Series.from_list(dates, :date) |> Series.to_list() == dates

    today_in_days = Date.utc_today() |> Date.to_gregorian_days()

    dates =
      for _i <- 0..:rand.uniform(100) do
        days = :rand.uniform(today_in_days)

        Date.from_gregorian_days(days)
      end

    assert Series.from_list(dates, :date) |> Series.to_list() == dates

    dates = Enum.intersperse(dates, nil)
    assert Series.from_list(dates, :date) |> Series.to_list() == dates
  end

  test "from_list/2 of naive datetime" do
    dates = [~N[1022-01-04 21:18:31.224], ~N[1988-11-23 06:36:16.158], ~N[2353-03-07 00:39:35.702]]

    assert Series.from_list(dates, :datetime) |> Series.to_list() == dates

    today_in_days = Date.utc_today() |> Date.to_gregorian_days()
    day_in_seconds = 86_400

    dates =
      for _i <- 0..:rand.uniform(100) do
        days = :rand.uniform(today_in_days)
        seconds = days * day_in_seconds

        seconds
        |> NaiveDateTime.from_gregorian_seconds({:rand.uniform(100) * 1_000, 3})
        |> NaiveDateTime.add(:rand.uniform(24) * 60 * 60, :second)
        |> NaiveDateTime.add(:rand.uniform(60) * 60, :second)
        |> NaiveDateTime.add(:rand.uniform(60), :second)
      end

    assert Series.from_list(dates, :datetime) |> Series.to_list() == dates
  end
end
