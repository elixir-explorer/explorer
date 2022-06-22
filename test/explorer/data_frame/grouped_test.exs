defmodule Explorer.DataFrame.GroupedTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame, as: DF
  alias Explorer.Datasets
  alias Explorer.Series

  setup do
    df = Datasets.fossil_fuels()
    {:ok, df: df}
  end

  test "grouped arrange sorts by group", %{df: df} do
    df = DF.arrange(df, "total")
    grouped_df = df |> DF.group_by("country") |> DF.arrange("total")

    assert df["total"][0] == Series.min(df["total"])

    assert grouped_df
           |> DF.ungroup()
           |> DF.filter(&Series.equal(&1["country"], "HONDURAS"))
           |> DF.pull("total")
           |> Series.first() == 2175
  end

  describe "distinct/2" do
    test "with one group", %{df: df} do
      df1 = DF.group_by(df, "year")

      df2 = DF.distinct(df1, columns: [:country])
      assert DF.names(df2) == ["year", "country"]
      assert DF.groups(df2) == ["year"]
      assert DF.shape(df2) == {1094, 2}
    end

    test "with one group and distinct as the same", %{df: df} do
      df1 = DF.group_by(df, "country")
      df2 = DF.distinct(df1, columns: [:country])

      assert DF.names(df2) == ["country"]
      assert DF.groups(df2) == ["country"]
      assert DF.shape(df2) == {222, 1}
    end

    test "multiple groups and different distinct", %{df: df} do
      df1 = DF.group_by(df, ["country", "year"])

      df2 = DF.distinct(df1, columns: [:bunker_fuels])
      assert DF.names(df2) == ["country", "year", "bunker_fuels"]
      assert DF.groups(df2) == ["country", "year"]
      assert DF.shape(df2) == {1094, 3}
    end

    test "with groups and keeping all", %{df: df} do
      df1 = DF.group_by(df, "year")

      df2 = DF.distinct(df1, columns: [:country], keep_all?: true)

      assert DF.names(df2) == [
               "year",
               "country",
               "total",
               "solid_fuel",
               "liquid_fuel",
               "gas_fuel",
               "cement",
               "gas_flaring",
               "per_capita",
               "bunker_fuels"
             ]

      assert DF.groups(df2) == ["year"]

      assert DF.shape(df2) == {1094, 10}
    end
  end
end
