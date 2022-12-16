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

  test "slice/3", %{ldf: ldf} do
    assert ldf |> DF.slice(0, 5) |> inspect() == ~s(#Explorer.DataFrame<
  LazyPolars[??? x 10]
  year integer [2010, 2010, 2010, 2010, 2010]
  country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA"]
  total integer [2308, 1254, 32500, 141, 7924]
  solid_fuel integer [627, 117, 332, 0, 0]
  liquid_fuel integer [1601, 953, 12381, 141, 3649]
  gas_fuel integer [74, 7, 14565, 0, 374]
  cement integer [5, 177, 2598, 0, 204]
  gas_flaring integer [0, 0, 2623, 0, 3697]
  per_capita float [0.08, 0.43, 0.9, 1.68, 0.37]
  bunker_fuels integer [9, 7, 663, 0, 321]
>)
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

  @tag :tmp_dir
  test "from_csv/2 - with defaults", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.csv"])
    df = DF.slice(df, 0, 10)
    DF.to_csv!(df, path)

    ldf = DF.from_csv!(path, lazy: true)

    # no-op 
    assert DF.to_lazy(ldf) == ldf

    df1 = DF.collect(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  @tag :tmp_dir
  test "from_csv/2 - passing columns", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.csv"])
    df = DF.slice(df, 0, 10)
    DF.to_csv!(df, path)

    assert_raise ArgumentError,
                 "`columns` is not supported by Polars' lazy backend. Consider using `select/2` after reading the CSV",
                 fn ->
                   DF.from_csv!(path, lazy: true, columns: ["country", "year", "total"])
                 end
  end

  @tag :tmp_dir
  test "from_parquet/2 - with defaults", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.parquet"])
    df = DF.slice(df, 0, 10)
    DF.to_parquet!(df, path)

    ldf = DF.from_parquet!(path, lazy: true)

    # no-op 
    assert DF.to_lazy(ldf) == ldf

    df1 = DF.collect(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  @tag :tmp_dir
  test "from_ndjson/2 - with defaults", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.ndjson"])
    df = DF.slice(df, 0, 10)
    DF.to_ndjson!(df, path)

    ldf = DF.from_ndjson!(path, lazy: true)

    # no-op 
    assert DF.to_lazy(ldf) == ldf

    df1 = DF.collect(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  @tag :tmp_dir
  test "from_ipc/2 - with defaults", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.ipc"])
    df = DF.slice(df, 0, 10)
    DF.to_ipc!(df, path)

    ldf = DF.from_ipc!(path, lazy: true)

    # no-op 
    assert DF.to_lazy(ldf) == ldf

    df1 = DF.collect(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  @tag :tmp_dir
  test "from_ipc/2 - passing columns", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.ipc"])
    df = DF.slice(df, 0, 10)
    DF.to_ipc!(df, path)

    assert_raise ArgumentError,
                 "`columns` is not supported by Polars' lazy backend. Consider using `select/2` after reading the IPC file",
                 fn ->
                   DF.from_ipc!(path, lazy: true, columns: ["country", "year", "total"])
                 end
  end

  test "load_csv/2 - with defaults", %{df: df} do
    df = DF.slice(df, 0, 10)
    contents = DF.dump_csv!(df)

    ldf = DF.load_csv!(contents, lazy: true)

    # no-op 
    assert DF.to_lazy(ldf) == ldf

    df1 = DF.collect(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  test "load_parquet/2 - with defaults", %{df: df} do
    df = DF.slice(df, 0, 10)
    contents = DF.dump_parquet!(df)

    ldf = DF.load_parquet!(contents, lazy: true)

    # no-op 
    assert DF.to_lazy(ldf) == ldf

    df1 = DF.collect(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  test "load_ndjson/2 - with defaults", %{df: df} do
    df = DF.slice(df, 0, 10)
    contents = DF.dump_ndjson!(df)

    ldf = DF.load_ndjson!(contents, lazy: true)

    # no-op 
    assert DF.to_lazy(ldf) == ldf

    df1 = DF.collect(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  test "load_ipc/2 - with defaults", %{df: df} do
    df = DF.slice(df, 0, 10)
    contents = DF.dump_ipc!(df)

    ldf = DF.load_ipc!(contents, lazy: true)

    # no-op 
    assert DF.to_lazy(ldf) == ldf

    df1 = DF.collect(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end
end
