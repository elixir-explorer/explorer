defmodule Explorer.Backend.LazySeriesTest do
  use ExUnit.Case, async: true

  alias Explorer.Backend
  alias Explorer.Backend.LazySeries

  test "inspect/2 gives a basic hint of lazy series" do
    data = LazySeries.new(:column, ["col_a"])
    opaque_series = Backend.Series.new(data, :integer)

    assert inspect(opaque_series) ==
             """
             #Explorer.Series<
               LazySeries[???]
               integer (column("col_a"))
             >
             """
             |> String.trim_trailing()
  end

  test "inspect/2 with nested operations" do
    col = LazySeries.new(:column, ["col_a"])
    equal = LazySeries.new(:equal, [col, 5])

    series = Backend.Series.new(equal, :boolean)

    assert inspect(series) ==
             """
             #Explorer.Series<
               LazySeries[???]
               boolean (column("col_a") == 5)
             >
             """
             |> String.trim_trailing()
  end

  test "inspect/2 with single-element series" do
    col = LazySeries.new(:column, ["col_a"])
    equal = LazySeries.new(:equal, [col, Explorer.Series.from_list([5]).data])

    series = Backend.Series.new(equal, :boolean)

    assert inspect(series) ==
             """
             #Explorer.Series<
               LazySeries[???]
               boolean (column("col_a") == 5)
             >
             """
             |> String.trim_trailing()
  end

  test "inspect/2 with nested series" do
    col = LazySeries.new(:column, ["col_a"])
    equal = LazySeries.new(:equal, [col, Explorer.Series.from_list([1, 2, 3]).data])

    series = Backend.Series.new(equal, :boolean)

    assert inspect(series) ==
             """
             #Explorer.Series<
               LazySeries[???]
               boolean (column("col_a") == #Explorer.Series<
               Polars[3]
               integer [1, 2, 3]
             >)
             >
             """
             |> String.trim_trailing()
  end
end
