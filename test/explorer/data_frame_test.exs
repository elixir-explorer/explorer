defmodule Explorer.DataFrameTest do
  use ExUnit.Case, async: true
  doctest Explorer.DataFrame

  alias Explorer.DataFrame, as: DF

  setup do
    {:ok, df: Explorer.Datasets.fossil_fuels()}
  end

  describe "filter/2" do
    test "raises with mask of invalid length", %{df: df} do
      assert_raise ArgumentError,
                   "Length of the mask (3) must match number of rows in the dataframe (1094).",
                   fn -> DF.filter(df, [true, false, true]) end
    end
  end

  describe "mutate/2" do
    test "raises with series of invalid length", %{df: df} do
      assert_raise ArgumentError,
                   "Length of new column test (3) must match number of rows in the dataframe (1094).",
                   fn -> DF.mutate(df, test: [1, 2, 3]) end
    end
  end

  describe "arrange/3" do
    test "raises with invalid column names", %{df: df} do
      assert_raise ArgumentError,
                   "Could not find column name \"test\"",
                   fn -> DF.arrange(df, ["test"]) end
    end
  end

  describe "take/2" do
    test "raises with index out of bounds", %{df: df} do
      assert_raise ArgumentError,
                   "Requested row index (2000) out of bounds (-1094:1094).",
                   fn -> DF.take(df, [1, 2, 3, 2000]) end
    end
  end

  describe "join/3" do
    test "raises if no overlapping columns" do
      assert_raise ArgumentError,
                   "Could not find any overlapping columns.",
                   fn ->
                     left = DF.from_map(%{a: [1, 2, 3]})
                     right = DF.from_map(%{b: [1, 2, 3]})
                     DF.join(left, right)
                   end
    end

    test "doesn't raise if no overlapping columns on cross join" do
      left = DF.from_map(%{a: [1, 2, 3]})
      right = DF.from_map(%{b: [1, 2, 3]})
      joined = DF.join(left, right, how: :cross)
      assert %DF{} = joined
    end
  end
end
