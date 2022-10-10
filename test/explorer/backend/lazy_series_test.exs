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
               LazySeries integer
               [???]
               column("col_a")
             >
             """
             |> String.trim_trailing()
  end

  test "inspect/2 with nested operations" do
    col = LazySeries.new(:column, ["col_a"])
    eq = LazySeries.new(:eq, [col, 5])

    series = Backend.Series.new(eq, :boolean)

    assert inspect(series) ==
             """
             #Explorer.Series<
               LazySeries boolean
               [???]
               column("col_a") == 5
             >
             """
             |> String.trim_trailing()
  end

  test "validate lazy series on both sides without arithmetic operations" do
    col = LazySeries.new(:column, ["col_a"])
    add = LazySeries.new(:add, [col, 5])

    series = Backend.Series.new(add, :integer)

    polars_series = Explorer.Series.from_list([1, 2, 3])

    msg =
      "expecting a LazySeries, but instead got #Explorer.Series<\n  integer[3]\n  [1, 2, 3]\n>"

    assert_raise ArgumentError, msg, fn ->
      LazySeries.eq(series, polars_series)
    end
  end
end
