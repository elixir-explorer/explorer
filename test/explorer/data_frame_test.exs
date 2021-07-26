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
end
