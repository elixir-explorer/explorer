defmodule Explorer.Backend.LazySeriesTest do
  use ExUnit.Case, async: true

  alias Explorer.Backend
  alias Explorer.Backend.LazySeries

  test "inspect/2 gives a basic hint of lazy series" do
    data = LazySeries.new(:column, ["col_a"], :s64)
    opaque_series = Backend.Series.new(data, :s64)

    assert inspect(opaque_series) ==
             """
             #Explorer.Series<
               LazySeries[???]
               s64 (column("col_a"))
             >\
             """
  end

  test "inspect/2 with nested operations" do
    col = LazySeries.new(:column, ["col_a"], :s64)
    equal = LazySeries.new(:equal, [col, 5], :boolean)

    series = Backend.Series.new(equal, :boolean)

    assert inspect(series) ==
             """
             #Explorer.Series<
               LazySeries[???]
               boolean (column("col_a") == 5)
             >\
             """
  end

  test "inspect/2 with single-element series" do
    col = LazySeries.new(:column, ["col_a"], :u32)
    equal = LazySeries.new(:equal, [col, Explorer.Series.from_list([5]).data], :boolean)

    series = Backend.Series.new(equal, :boolean)

    assert inspect(series) ==
             """
             #Explorer.Series<
               LazySeries[???]
               boolean (column("col_a") == 5)
             >\
             """
  end

  test "inspect/2 with nested series" do
    col = LazySeries.new(:column, ["col_a"], :u32)
    equal = LazySeries.new(:equal, [col, Explorer.Series.from_list([1, 2, 3]).data], :boolean)

    series = Backend.Series.new(equal, :boolean)

    assert inspect(series) ==
             """
             #Explorer.Series<
               LazySeries[???]
               boolean (column("col_a") == #Explorer.Series<
               Polars[3]
               s64 [1, 2, 3]
             >)
             >\
             """
  end
end
