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
               Operation: column
               Args: ["col_a"]
             >
             """
             |> String.trim_trailing()
  end
end
