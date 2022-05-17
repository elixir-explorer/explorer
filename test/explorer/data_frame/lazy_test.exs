defmodule Explorer.DataFrame.LazyTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame, as: DF
  alias Explorer.Datasets

  setup do
    df = Datasets.fossil_fuels()
    ldf = DF.to_lazy(df)
    {:ok, ldf: ldf, df: df}
  end

  test "names/1", %{ldf: ldf} do
    assert DF.names(ldf) == [
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
  end

  test "dtypes/1", %{ldf: ldf} do
    assert DF.dtypes(ldf) == [
             :integer,
             :string,
             :integer,
             :integer,
             :integer,
             :integer,
             :integer,
             :integer,
             :float,
             :integer
           ]
  end

  test "n_columns/1", %{ldf: ldf} do
    assert DF.n_columns(ldf) == 10
  end

  test "select/3 :keep", %{ldf: ldf} do
    assert ldf |> DF.select(["total", "cement"]) |> DF.names() == ["total", "cement"]
  end

  test "select/3 :drop", %{ldf: ldf} do
    assert ldf |> DF.select(["total", "cement"], :drop) |> DF.names() == [
             "year",
             "country",
             "solid_fuel",
             "liquid_fuel",
             "gas_fuel",
             "gas_flaring",
             "per_capita",
             "bunker_fuels"
           ]
  end

  test "collect/1", %{ldf: ldf, df: df} do
    assert ldf |> DF.collect() |> DF.to_columns() == DF.to_columns(df)
  end

  test "to_lazy/1 is no-op", %{ldf: ldf} do
    assert DF.to_lazy(ldf) == ldf
  end
end
