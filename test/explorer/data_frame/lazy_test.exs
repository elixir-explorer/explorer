defmodule Explorer.DataFrame.LazyTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame, as: DF
  alias Explorer.Datasets
  alias Explorer.Series

  setup do
    df = Datasets.fossil_fuels()
    ldf = DF.to_lazy(df)
    {:ok, ldf: ldf, df: df}
  end

  test "new/1" do
    ldf = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)
    assert DF.to_lazy(ldf) == ldf

    ldf1 = DF.new([%{a: 42, b: "a"}, %{a: 51, b: "c"}], lazy: true)
    assert DF.to_lazy(ldf1) == ldf1

    ldf2 =
      DF.new(
        [a: Series.from_list([1, 2, 3]), b: Series.from_list(["a", "b", "c"])],
        lazy: true
      )

    assert DF.to_lazy(ldf2) == ldf2
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

  describe "from_parquet/2 - with options" do
    @tag :tmp_dir
    test "max_rows", %{df: df, tmp_dir: tmp_dir} do
      path = Path.join([tmp_dir, "fossil_fuels.parquet"])
      DF.to_parquet!(df, path)

      ldf = DF.from_parquet!(path, lazy: true, max_rows: 1)

      df1 = DF.collect(ldf)

      assert DF.n_rows(df1) == 1
    end

    @tag :tmp_dir
    test "columns", %{df: df, tmp_dir: tmp_dir} do
      path = Path.join([tmp_dir, "fossil_fuels.parquet"])
      DF.to_parquet!(df, path)

      assert_raise ArgumentError,
                   "`columns` is not supported by Polars' lazy backend. Consider using `select/2` after reading the parquet file",
                   fn ->
                     DF.from_parquet!(path, lazy: true, columns: ["country", "year", "total"])
                   end
    end
  end

  @tag :tmp_dir
  test "to_ipc/2 - with defaults", %{ldf: ldf, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.ipc"])

    ldf = DF.head(ldf, 15)
    DF.to_ipc!(ldf, path)

    df = DF.collect(ldf)
    df1 = DF.from_ipc!(path)

    assert DF.to_rows(df1) |> Enum.sort() == DF.to_rows(df) |> Enum.sort()
  end

  @tag :tmp_dir
  test "to_ipc/2 - without streaming", %{ldf: ldf, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.ipc"])

    ldf = DF.head(ldf, 15)
    DF.to_ipc!(ldf, path, streaming: false)

    df = DF.collect(ldf)
    df1 = DF.from_ipc!(path)

    assert DF.to_rows(df1) |> Enum.sort() == DF.to_rows(df) |> Enum.sort()
  end

  @tag :tmp_dir
  test "to_parquet/2 - with defaults", %{ldf: ldf, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.parquet"])

    ldf = DF.head(ldf, 15)
    DF.to_parquet!(ldf, path)

    df = DF.collect(ldf)
    df1 = DF.from_parquet!(path)

    assert DF.to_rows(df1) |> Enum.sort() == DF.to_rows(df) |> Enum.sort()
  end

  @tag :tmp_dir
  test "to_parquet/2 - with streaming disabled", %{ldf: ldf, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.parquet"])

    ldf = DF.head(ldf, 15)
    DF.to_parquet!(ldf, path, streaming: false)

    df = DF.collect(ldf)
    df1 = DF.from_parquet!(path)

    assert DF.to_rows(df1) |> Enum.sort() == DF.to_rows(df) |> Enum.sort()
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

  @tag :tmp_dir
  test "from_ipc_stream/2 - with defaults", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.ipc"])
    df = DF.slice(df, 0, 10)
    DF.to_ipc_stream!(df, path)

    ldf = DF.from_ipc_stream!(path, lazy: true)

    # no-op
    assert DF.to_lazy(ldf) == ldf

    df1 = DF.collect(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
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

  test "load_ipc_stream/2 - with defaults", %{df: df} do
    df = DF.slice(df, 0, 10)
    contents = DF.dump_ipc_stream!(df)

    ldf = DF.load_ipc_stream!(contents, lazy: true)

    # no-op
    assert DF.to_lazy(ldf) == ldf

    df1 = DF.collect(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  describe "filter_with/2" do
    test "filters by a simple selector" do
      ldf = DF.new([a: [1, 2, 3, 4], b: [150, 50, 250, 0]], lazy: true)

      ldf1 = DF.filter_with(ldf, fn ldf -> Series.greater(ldf["a"], 2) end)
      df1 = DF.collect(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [3, 4],
               b: [250, 0]
             }
    end

    test "filters with a group and without aggregation" do
      ldf =
        DF.new([col1: ["a", "b", "a", "b"], col2: [1, 2, 3, 4], col3: ["z", "y", "w", "k"]],
          lazy: true
        )

      ldf_grouped = DF.group_by(ldf, "col1")

      ldf1 =
        DF.filter_with(ldf_grouped, fn ldf ->
          Series.greater(ldf["col2"], 2)
        end)

      df1 = DF.collect(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               col1: ["a", "b"],
               col2: [3, 4],
               col3: ["w", "k"]
             }

      assert DF.groups(df1) == ["col1"]
    end

    test "filters with a group and with aggregation" do
      ldf =
        DF.new(
          [
            col1: ["a", "b", "a", "b", "a", "b"],
            col2: [1, 2, 3, 4, 5, 6],
            col3: ["z", "y", "w", "k", "d", "j"]
          ],
          lazy: true
        )

      ldf_grouped = DF.group_by(ldf, "col1")

      assert_raise RuntimeError,
                   "filter_with/2 with groups and aggregations is not supported yet for lazy frames",
                   fn ->
                     DF.filter_with(ldf_grouped, fn ldf ->
                       Series.greater(ldf["col2"], Series.mean(ldf["col2"]))
                     end)
                   end
    end
  end

  describe "arrange_with/2" do
    test "with a simple df and asc order" do
      ldf = DF.new([a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"]], lazy: true)
      ldf1 = DF.arrange_with(ldf, fn ldf -> [asc: ldf["a"]] end)

      df1 = DF.collect(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5, 6],
               b: ["a", "b", "c", "d", "e", "f"]
             }
    end

    test "with a simple df one column and without order" do
      ldf = DF.new([a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"]], lazy: true)
      ldf1 = DF.arrange_with(ldf, fn ldf -> ldf["a"] end)

      df1 = DF.collect(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5, 6],
               b: ["a", "b", "c", "d", "e", "f"]
             }
    end

    test "with a simple df and desc order" do
      ldf = DF.new([a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"]], lazy: true)
      ldf1 = DF.arrange_with(ldf, fn ldf -> [desc: ldf["a"]] end)

      df1 = DF.collect(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [6, 5, 4, 3, 2, 1],
               b: ["f", "e", "d", "c", "b", "a"]
             }
    end

    test "with a simple df and just the lazy series" do
      ldf = DF.new([a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"]], lazy: true)
      ldf1 = DF.arrange_with(ldf, fn ldf -> [ldf["a"]] end)

      df1 = DF.collect(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5, 6],
               b: ["a", "b", "c", "d", "e", "f"]
             }
    end

    test "with a simple df and arrange by two columns" do
      ldf = DF.new([a: [1, 2, 2, 3, 6, 5], b: [1.1, 2.5, 2.2, 3.3, 4.0, 5.1]], lazy: true)
      ldf1 = DF.arrange_with(ldf, fn ldf -> [asc: ldf["a"], asc: ldf["b"]] end)

      df1 = DF.collect(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 2, 3, 5, 6],
               b: [1.1, 2.2, 2.5, 3.3, 5.1, 4.0]
             }
    end

    test "with a simple df and window function" do
      ldf = DF.new([a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"]], lazy: true)
      ldf1 = DF.arrange_with(ldf, fn ldf -> [desc: Series.window_mean(ldf["a"], 2)] end)

      df1 = DF.collect(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [5, 6, 3, 4, 2, 1],
               b: ["e", "f", "c", "d", "b", "a"]
             }
    end

    test "raise when groups is in use" do
      ldf = DF.new([a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"]], lazy: true)
      ldf = DF.group_by(ldf, "b")

      assert_raise RuntimeError,
                   "arrange_with/2 with groups is not supported yet for lazy frames",
                   fn ->
                     DF.arrange_with(ldf, fn ldf -> [asc: ldf["a"], asc: ldf["b"]] end)
                   end
    end
  end

  describe "head/2" do
    test "selects the first 5 rows by default", %{ldf: ldf} do
      ldf1 = DF.head(ldf)
      df = DF.collect(ldf1)

      assert DF.shape(df) == {5, 10}
    end

    test "selects the first 2 rows", %{ldf: ldf} do
      ldf1 = DF.head(ldf, 2)
      df = DF.collect(ldf1)

      assert DF.shape(df) == {2, 10}
    end
  end

  describe "distinct/2" do
    test "with lists of strings", %{ldf: ldf} do
      ldf1 = DF.distinct(ldf, [:year, :country])
      assert DF.names(ldf1) == ["year", "country"]

      assert DF.n_columns(ldf1) == 2

      ldf2 = DF.head(ldf1, 1)
      df = DF.collect(ldf2)

      assert DF.to_columns(df, atom_keys: true) == %{country: ["AFGHANISTAN"], year: [2010]}
    end

    test "keeping all columns", %{ldf: ldf} do
      ldf2 = DF.distinct(ldf, [:year, :country], keep_all: true)
      assert DF.names(ldf2) == DF.names(ldf)
    end

    test "with lists of indices", %{ldf: ldf} do
      ldf1 = DF.distinct(ldf, [0, 2, 4])
      assert DF.names(ldf1) == ["year", "total", "liquid_fuel"]

      assert DF.n_columns(ldf1) == 3
    end

    test "with ranges", %{df: df} do
      df1 = DF.distinct(df, 0..1)
      assert DF.names(df1) == ["year", "country"]

      df2 = DF.distinct(df)
      assert DF.names(df2) == DF.names(df)

      df3 = DF.distinct(df, 0..-1)
      assert DF.names(df3) == DF.names(df)

      assert df == DF.distinct(df, 100..200)
    end
  end

  describe "mutate_with/2" do
    test "adds new columns" do
      ldf = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)

      ldf1 =
        DF.mutate_with(ldf, fn ldf ->
          [c: Series.add(ldf["a"], 5), d: Series.add(2, ldf["a"])]
        end)

      assert ldf1.names == ["a", "b", "c", "d"]
      assert ldf1.dtypes == %{"a" => :integer, "b" => :string, "c" => :integer, "d" => :integer}

      df = DF.collect(ldf1)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 3],
               b: ["a", "b", "c"],
               c: [6, 7, 8],
               d: [3, 4, 5]
             }

      assert ldf1.names == df.names
      assert ldf1.dtypes == df.dtypes
    end

    test "adds literal columns" do
      ldf = DF.new([d: ~w(a b c)], lazy: true)
      ldf1 = DF.mutate_with(ldf, fn _ -> [a: 1, b: 2.0, c: true] end)

      assert ldf1.names == ["d", "a", "b", "c"]
      assert ldf1.dtypes == %{"a" => :integer, "b" => :float, "c" => :boolean, "d" => :string}

      df = DF.collect(ldf1)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 1, 1],
               b: [2.0, 2.0, 2.0],
               c: [true, true, true],
               d: ["a", "b", "c"]
             }

      assert ldf1.names == df.names
      assert ldf1.dtypes == df.dtypes
    end

    test "changes a column" do
      ldf = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)

      ldf1 =
        DF.mutate_with(ldf, fn ldf ->
          [a: Series.cast(ldf["a"], :float)]
        end)

      assert ldf1.names == ["a", "b"]
      assert ldf1.dtypes == %{"a" => :float, "b" => :string}

      df = DF.collect(ldf1)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1.0, 2.0, 3.0],
               b: ["a", "b", "c"]
             }

      assert ldf1.names == df.names
      assert ldf1.dtypes == df.dtypes
    end

    test "raise when groups is in use" do
      ldf = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)
      ldf1 = DF.group_by(ldf, "b")

      assert_raise RuntimeError,
                   "mutate_with/2 with groups is not supported yet for lazy frames",
                   fn ->
                     DF.mutate_with(ldf1, fn ldf -> [a: Series.cast(ldf["a"], :float)] end)
                   end
    end
  end

  describe "summarise_with/2" do
    test "with one group and one column with aggregations", %{ldf: ldf} do
      ldf1 =
        ldf
        |> DF.group_by("year")
        |> DF.summarise_with(fn ldf ->
          total = ldf["total"]

          [total_min: Series.min(total), total_max: Series.max(total)]
        end)

      df = DF.collect(ldf1)

      assert DF.to_columns(df, atom_keys: true) == %{
               year: [2010, 2011, 2012, 2013, 2014],
               total_min: [1, 2, 2, 2, 3],
               total_max: [2_393_248, 2_654_360, 2_734_817, 2_797_384, 2_806_634]
             }
    end

    test "with one group and two columns with aggregations", %{ldf: ldf} do
      ldf1 =
        ldf
        |> DF.group_by("year")
        |> DF.summarise_with(fn ldf ->
          total = ldf["total"]
          liquid_fuel = ldf["liquid_fuel"]

          [
            total_min: Series.min(total),
            total_max: Series.max(total),
            median_liquid_fuel: Series.median(liquid_fuel)
          ]
        end)

      df = DF.collect(ldf1)

      assert DF.to_columns(df, atom_keys: true) == %{
               year: [2010, 2011, 2012, 2013, 2014],
               total_min: [1, 2, 2, 2, 3],
               total_max: [2_393_248, 2_654_360, 2_734_817, 2_797_384, 2_806_634],
               median_liquid_fuel: [1193.0, 1236.0, 1199.0, 1260.0, 1255.0]
             }
    end

    test "without a group and with one column with aggregations", %{ldf: ldf} do
      ldf1 =
        DF.summarise_with(ldf, fn ldf ->
          total = ldf["total"]

          [total_min: Series.min(total), total_max: Series.max(total)]
        end)

      df = DF.collect(ldf1)

      assert DF.to_columns(df, atom_keys: true) == %{
               total_min: [1],
               total_max: [2_806_634]
             }
    end
  end

  describe "relocate/3" do
    test "with multiple columns" do
      ldf =
        DF.new(
          [
            first: ["a", "b", "a"],
            second: ["x", "y", "z"],
            third: [2.2, 3.3, nil],
            last: [1, 3, 1]
          ],
          lazy: true
        )

      ldf1 = DF.relocate(ldf, ["third", "second"], before: "first")
      assert ldf1.names == ["third", "second", "first", "last"]

      df = DF.collect(ldf1)
      assert DF.dump_csv(df) == {:ok, "third,second,first,last\n2.2,x,a,1\n3.3,y,b,3\n,z,a,1\n"}
    end
  end

  describe "rename/2" do
    test "renames a column" do
      ldf = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)

      ldf1 = DF.rename(ldf, %{"a" => "ids"})

      assert ldf1.names == ["ids", "b"]
      assert ldf1.dtypes == %{"ids" => :integer, "b" => :string}

      df = DF.collect(ldf1)

      assert DF.to_columns(df, atom_keys: true) == %{
               ids: [1, 2, 3],
               b: ["a", "b", "c"]
             }

      assert ldf1.names == df.names
      assert ldf1.dtypes == df.dtypes
    end
  end

  describe "drop_nils/2" do
    test "considering all columns" do
      ldf = DF.new([a: [1, 2, nil], b: [1, nil, 3]], lazy: true)

      ldf1 = DF.drop_nil(ldf)

      df = DF.collect(ldf1)
      assert DF.to_columns(df) == %{"a" => [1], "b" => [1]}
    end

    test "considering one column" do
      ldf = DF.new([a: [1, 2, nil], b: [1, nil, 3]], lazy: true)

      ldf1 = DF.drop_nil(ldf, :a)

      df = DF.collect(ldf1)
      assert DF.to_columns(df) == %{"a" => [1, 2], "b" => [1, nil]}
    end

    test "selecting none" do
      ldf = DF.new([a: [1, 2, nil], b: [1, nil, 3]], lazy: true)

      ldf1 = DF.drop_nil(ldf, [])

      df = DF.collect(ldf1)
      assert DF.to_columns(df) == %{"a" => [1, 2, nil], "b" => [1, nil, 3]}
    end
  end

  describe "pivot_longer/3" do
    test "without selecting columns", %{ldf: ldf} do
      ldf = DF.pivot_longer(ldf, &String.ends_with?(&1, "fuel"), select: [])

      assert ldf.names == ["variable", "value"]
      assert ldf.dtypes == %{"variable" => :string, "value" => :integer}

      df = DF.collect(ldf)
      assert DF.shape(df) == {3282, 2}
      assert ldf.names == df.names
      assert ldf.dtypes == df.dtypes
    end

    test "selecting some columns", %{ldf: ldf} do
      ldf = DF.pivot_longer(ldf, &String.ends_with?(&1, "fuel"), select: ["year", "country"])

      assert ldf.names == ["year", "country", "variable", "value"]

      assert ldf.dtypes == %{
               "year" => :integer,
               "country" => :string,
               "variable" => :string,
               "value" => :integer
             }

      df = DF.collect(ldf)

      assert DF.shape(df) == {3282, 4}
      assert ldf.names == df.names
      assert ldf.dtypes == df.dtypes
    end

    test "selecting all the columns (not passing select option)", %{ldf: ldf} do
      ldf = DF.pivot_longer(ldf, &String.ends_with?(&1, ["fuel", "fuels"]))

      assert ldf.names == [
               "year",
               "country",
               "total",
               "cement",
               "gas_flaring",
               "per_capita",
               "variable",
               "value"
             ]

      df = DF.collect(ldf)

      assert DF.shape(df) == {4376, 8}
      assert ldf.names == df.names
      assert ldf.dtypes == df.dtypes
    end

    test "dropping some columns", %{ldf: ldf} do
      ldf =
        DF.pivot_longer(ldf, &String.ends_with?(&1, ["fuel", "fuels"]),
          discard: ["gas_flaring", "cement"]
        )

      assert ldf.names == [
               "year",
               "country",
               "total",
               "per_capita",
               "variable",
               "value"
             ]

      df = DF.collect(ldf)

      assert ldf.names == df.names
      assert ldf.dtypes == df.dtypes
    end

    test "select and discard with the same columns discards the columns", %{ldf: ldf} do
      ldf =
        DF.pivot_longer(ldf, &String.ends_with?(&1, ["fuel", "fuels"]),
          select: ["gas_flaring", "cement"],
          discard: fn name -> name == "cement" end
        )

      assert ldf.names == [
               "gas_flaring",
               "variable",
               "value"
             ]

      df = DF.collect(ldf)

      assert ldf.names == df.names
      assert ldf.dtypes == df.dtypes
    end
  end

  describe "join/3" do
    test "raises if no overlapping columns" do
      assert_raise ArgumentError,
                   ~r"could not find any overlapping columns",
                   fn ->
                     left = DF.new([a: [1, 2, 3]], lazy: true)
                     right = DF.new([b: [1, 2, 3]], lazy: true)
                     DF.join(left, right)
                   end
    end

    test "doesn't raise if no overlapping columns on cross join" do
      left = DF.new([a: [1, 2, 3]], lazy: true)
      right = DF.new([b: [1, 2, 3]], lazy: true)
      joined = DF.join(left, right, how: :cross)

      assert DF.to_lazy(joined) == joined
      assert %DF{} = joined

      assert DF.names(joined) == ["a", "b"]
    end

    test "with a custom 'on'" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"]], lazy: true)

      ldf = DF.join(left, right, on: [{"a", "d"}])
      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"]
             }
    end

    test "with a custom 'on' but with repeated column on right side - inner join" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"], a: [5, 6, 7]], lazy: true)

      ldf = DF.join(left, right, on: [{"a", "d"}])
      assert ldf.names == ["a", "b", "c", "a_right"]

      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"],
               a_right: [5, 6, 7]
             }
    end

    test "with a custom 'on' but with repeated column on right side - left join" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"], a: [5, 6, 7]], lazy: true)

      ldf = DF.join(left, right, on: [{"a", "d"}], how: :left)

      assert ldf.names == ["a", "b", "c", "a_right"]
      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               a_right: [5, 6, 7, nil]
             }
    end

    test "with a custom 'on' but with repeated column on right side - outer join" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"], a: [5, 6, 7]], lazy: true)

      ldf = DF.join(left, right, on: [{"a", "d"}], how: :outer)
      assert ldf.names == ["a", "b", "c", "a_right"]

      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               a_right: [5, 6, 7, nil]
             }
    end

    test "with a custom 'on' but with repeated column on right side - cross join" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"], a: [5, 6, 7]], lazy: true)

      ldf = DF.join(left, right, how: :cross)
      assert ldf.names == ["a", "b", "d", "c", "a_right"]

      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 1, 1, 2, 2, 2, 3, 3, 3],
               a_right: [5, 6, 7, 5, 6, 7, 5, 6, 7],
               b: ["a", "a", "a", "b", "b", "b", "c", "c", "c"],
               c: ["d", "e", "f", "d", "e", "f", "d", "e", "f"],
               d: [1, 2, 2, 1, 2, 2, 1, 2, 2]
             }
    end

    test "with a custom 'on' but with repeated column on right side - right join" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"], a: [5, 6, 7]], lazy: true)

      ldf = DF.join(left, right, on: [{"a", "d"}], how: :right)
      assert ldf.names == ["d", "c", "a", "b"]

      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [5, 6, 7],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"],
               d: [1, 2, 2]
             }
    end

    test "with a custom 'on' but with repeated column on left side - inner join" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"], d: [5, 6, 7]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"]], lazy: true)

      ldf = DF.join(left, right, on: [{"a", "d"}])
      assert ldf.names == ["a", "b", "d", "c"]

      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"],
               d: [5, 6, 6]
             }
    end

    test "with a custom 'on' but with repeated column on left side - left join" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"], d: [5, 6, 7]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"]], lazy: true)

      ldf = DF.join(left, right, on: [{"a", "d"}], how: :left)
      assert ldf.names == ["a", "b", "d", "c"]

      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               d: [5, 6, 6, 7]
             }
    end

    test "with a custom 'on' but with repeated column on left side - outer join" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"], d: [5, 6, 7]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"]], lazy: true)

      ldf = DF.join(left, right, on: [{"a", "d"}], how: :outer)
      assert ldf.names == ["a", "b", "d", "c"]

      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               d: [5, 6, 6, 7]
             }
    end

    test "with a custom 'on' but with repeated column on left side - cross join" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"], d: [5, 6, 7]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"]], lazy: true)

      ldf = DF.join(left, right, how: :cross)
      assert ldf.names == ["a", "b", "d", "d_right", "c"]

      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 1, 1, 2, 2, 2, 3, 3, 3],
               b: ["a", "a", "a", "b", "b", "b", "c", "c", "c"],
               c: ["d", "e", "f", "d", "e", "f", "d", "e", "f"],
               d: [5, 5, 5, 6, 6, 6, 7, 7, 7],
               d_right: [1, 2, 2, 1, 2, 2, 1, 2, 2]
             }
    end

    test "with a custom 'on' but with repeated column on left side - right join" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"], d: [5, 6, 7]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"]], lazy: true)

      ldf = DF.join(left, right, on: [{"a", "d"}], how: :right)
      assert ldf.names == ["d", "c", "b", "d_left"]

      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               d: [1, 2, 2],
               c: ["d", "e", "f"],
               b: ["a", "b", "b"],
               d_left: [5, 6, 6]
             }
    end
  end

  describe "concat_rows/2" do
    test "two simple DFs of the same dtypes" do
      ldf1 = DF.new([x: [1, 2, 3], y: ["a", "b", "c"]], lazy: true)
      ldf2 = DF.new([x: [4, 5, 6], y: ["d", "e", "f"]], lazy: true)

      ldf3 = DF.concat_rows(ldf1, ldf2)

      assert ldf3.names == ["x", "y"]

      df = DF.collect(ldf3)

      assert DF.to_columns(df, atom_keys: true) == %{
               x: [1, 2, 3, 4, 5, 6],
               y: ~w(a b c d e f)
             }
    end

    test "two DFs with different numeric types" do
      ldf1 = DF.new([x: [1, 2, 3], y: ["a", "b", "c"]], lazy: true)
      ldf2 = DF.new([x: [4.1, 5.2, 6.3, nil], y: ["d", "e", "f", "g"]], lazy: true)
      ldf3 = DF.concat_rows(ldf1, ldf2)

      df = DF.collect(ldf3)

      assert DF.to_columns(df, atom_keys: true) == %{
               x: [1.0, 2.0, 3.0, 4.1, 5.2, 6.3, nil],
               y: ~w(a b c d e f g)
             }
    end

    test "errors" do
      df1 = DF.new([x: [7, 8, 9], y: ["g", "h", nil]], lazy: true)

      assert_raise ArgumentError,
                   "dataframes must have the same columns",
                   fn -> DF.concat_rows(df1, DF.new([z: [7, 8, 9]], lazy: true)) end

      assert_raise ArgumentError,
                   "dataframes must have the same columns",
                   fn -> DF.concat_rows(df1, DF.new([x: [7, 8, 9], z: [7, 8, 9]], lazy: true)) end

      assert_raise ArgumentError,
                   "columns and dtypes must be identical for all dataframes",
                   fn ->
                     DF.concat_rows(df1, DF.new([x: [7, 8, 9], y: [10, 11, 12]], lazy: true))
                   end
    end
  end

  describe "concat_columns/1" do
    test "combine columns of both data frames" do
      ldf1 = DF.new([x: [1, 2, 3], y: ["a", "b", "c"]], lazy: true)
      ldf2 = DF.new([z: [4, 5, 6], a: ["d", "e", "f"]], lazy: true)

      ldf = DF.concat_columns([ldf1, ldf2])

      assert ldf.names == ["x", "y", "z", "a"]

      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               x: [1, 2, 3],
               y: ["a", "b", "c"],
               z: [4, 5, 6],
               a: ["d", "e", "f"]
             }
    end

    test "with conflicting names add number suffix" do
      ldf1 = DF.new([x: [1, 2, 3], y: ["a", "b", "c"]], lazy: true)
      ldf2 = DF.new([x: [4, 5, 6], a: ["d", "e", "f"]], lazy: true)

      ldf = DF.concat_columns([ldf1, ldf2])

      assert ldf.names == ["x", "y", "x_1", "a"]

      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               x: [1, 2, 3],
               y: ["a", "b", "c"],
               x_1: [4, 5, 6],
               a: ["d", "e", "f"]
             }
    end

    test "with a bigger df in the right side removes the last row" do
      ldf1 = DF.new([x: [1, 2, 3], y: ["a", "b", "c"]], lazy: true)
      ldf2 = DF.new([z: [4, 5, 6, 7], a: ["d", "e", "f", "g"]], lazy: true)

      ldf = DF.concat_columns([ldf1, ldf2])

      assert ldf.names == ["x", "y", "z", "a"]

      df = DF.collect(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               x: [1, 2, 3],
               y: ["a", "b", "c"],
               z: [4, 5, 6],
               a: ["d", "e", "f"]
             }
    end
  end
end
