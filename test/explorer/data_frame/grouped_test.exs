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
end
