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
    assert DF.dtypes(ldf) == %{
             "bunker_fuels" => :integer,
             "cement" => :integer,
             "country" => :string,
             "gas_flaring" => :integer,
             "gas_fuel" => :integer,
             "liquid_fuel" => :integer,
             "per_capita" => :float,
             "solid_fuel" => :integer,
             "total" => :integer,
             "year" => :integer
           }
  end

  test "n_columns/1", %{ldf: ldf} do
    assert DF.n_columns(ldf) == 10
  end

  test "select/2", %{ldf: ldf} do
    assert ldf |> DF.select(["total", "cement"]) |> DF.names() == ["total", "cement"]
  end

  test "discard/2", %{ldf: ldf} do
    assert ldf |> DF.discard(["total", "cement"]) |> DF.names() == [
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

  test "inspect/1", %{ldf: ldf} do
    assert inspect(ldf) == ~s(#Explorer.DataFrame<
  LazyPolars[??? x 10]
  year integer [2010, 2010, 2010, 2010, 2010, ...]
  country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
  total integer [2308, 1254, 32500, 141, 7924, ...]
  solid_fuel integer [627, 117, 332, 0, 0, ...]
  liquid_fuel integer [1601, 953, 12381, 141, 3649, ...]
  gas_fuel integer [74, 7, 14565, 0, 374, ...]
  cement integer [5, 177, 2598, 0, 204, ...]
  gas_flaring integer [0, 0, 2623, 0, 3697, ...]
  per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, ...]
  bunker_fuels integer [9, 7, 663, 0, 321, ...]
>)
  end
end
