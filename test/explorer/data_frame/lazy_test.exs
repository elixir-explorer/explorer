defmodule Explorer.DataFrame.LazyTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame, as: DF
  alias Explorer.Datasets
  alias Explorer.Series

  setup do
    df = Datasets.fossil_fuels()
    ldf = DF.lazy(df)
    {:ok, ldf: ldf, df: df}
  end

  test "new/1" do
    ldf = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)
    assert DF.lazy(ldf) == ldf

    ldf1 = DF.new([%{a: 42, b: "a"}, %{a: 51, b: "c"}], lazy: true)
    assert DF.lazy(ldf1) == ldf1

    ldf2 =
      DF.new(
        [a: Series.from_list([1, 2, 3]), b: Series.from_list(["a", "b", "c"])],
        lazy: true
      )

    assert DF.lazy(ldf2) == ldf2
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
             "bunker_fuels" => {:s, 64},
             "cement" => {:s, 64},
             "country" => :string,
             "gas_flaring" => {:s, 64},
             "gas_fuel" => {:s, 64},
             "liquid_fuel" => {:s, 64},
             "per_capita" => {:f, 64},
             "solid_fuel" => {:s, 64},
             "total" => {:s, 64},
             "year" => {:s, 64}
           }
  end

  test "n_columns/1", %{ldf: ldf} do
    assert DF.n_columns(ldf) == 10
  end

  test "select/2", %{ldf: ldf} do
    assert ldf |> DF.select(["total", "cement"]) |> DF.names() == ["total", "cement"]
  end

  test "slice/3", %{ldf: ldf} do
    new_ldf = ldf |> DF.slice(0, 5)
    assert new_ldf |> DF.names() == DF.names(ldf)

    df = DF.compute(new_ldf)

    assert DF.n_rows(df) == 5
  end

  test "slice/3 with groups", %{ldf: ldf} do
    new_ldf = ldf |> DF.group_by("country") |> DF.slice(0, 2)
    assert new_ldf |> DF.names() == DF.names(ldf)

    df = DF.compute(new_ldf)

    # Just like the head with 2 items per group.
    assert DF.n_rows(df) == 444
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
    assert ldf |> DF.compute() |> DF.to_columns() == DF.to_columns(df)
  end

  test "lazy/1 is no-op", %{ldf: ldf} do
    assert DF.lazy(ldf) == ldf
  end

  test "inspect/1 without operations", %{ldf: ldf} do
    assert inspect(ldf) ==
             """
             #Explorer.DataFrame<
               LazyPolars[??? x 10]
               year s64 [2010, 2010, 2010, 2010, 2010, ...]
               country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
               total s64 [2308, 1254, 32500, 141, 7924, ...]
               solid_fuel s64 [627, 117, 332, 0, 0, ...]
               liquid_fuel s64 [1601, 953, 12381, 141, 3649, ...]
               gas_fuel s64 [74, 7, 14565, 0, 374, ...]
               cement s64 [5, 177, 2598, 0, 204, ...]
               gas_flaring s64 [0, 0, 2623, 0, 3697, ...]
               per_capita f64 [0.08, 0.43, 0.9, 1.68, 0.37, ...]
               bunker_fuels s64 [9, 7, 663, 0, 321, ...]
             >\
             """
  end

  test "inspect/1 after one operation", %{ldf: ldf} do
    assert inspect(DF.head(ldf, 12)) ==
             """
             #Explorer.DataFrame<
               LazyPolars[??? x 10]
               year s64 [2010, 2010, 2010, 2010, 2010, ...]
               country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
               total s64 [2308, 1254, 32500, 141, 7924, ...]
               solid_fuel s64 [627, 117, 332, 0, 0, ...]
               liquid_fuel s64 [1601, 953, 12381, 141, 3649, ...]
               gas_fuel s64 [74, 7, 14565, 0, 374, ...]
               cement s64 [5, 177, 2598, 0, 204, ...]
               gas_flaring s64 [0, 0, 2623, 0, 3697, ...]
               per_capita f64 [0.08, 0.43, 0.9, 1.68, 0.37, ...]
               bunker_fuels s64 [9, 7, 663, 0, 321, ...]
             >\
             """
  end

  @tag :tmp_dir
  test "from_csv/2 - with defaults", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.csv"])
    df = DF.slice(df, 0, 10)
    DF.to_csv!(df, path)

    ldf = DF.from_csv!(path, lazy: true)

    # no-op
    assert DF.lazy(ldf) == ldf

    df1 = DF.compute(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  @tag :tmp_dir
  test "from_csv/2 - passing columns", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.csv"])
    df = DF.slice(df, 0, 10)
    DF.to_csv!(df, path)

    assert {:error, error} = DF.from_csv(path, lazy: true, columns: ["country", "year", "total"])

    assert error ==
             ArgumentError.exception(
               "`columns` is not supported by Polars' lazy backend. Consider using `select/2` after reading the CSV"
             )
  end

  @tag :tmp_dir
  test "from_parquet/2 - with defaults", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.parquet"])
    df = DF.slice(df, 0, 10)
    DF.to_parquet!(df, path)

    ldf = DF.from_parquet!(path, lazy: true)

    # no-op
    assert DF.lazy(ldf) == ldf

    df1 = DF.compute(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  describe "from_parquet/2 - with options" do
    @tag :tmp_dir
    test "max_rows", %{df: df, tmp_dir: tmp_dir} do
      path = Path.join([tmp_dir, "fossil_fuels.parquet"])
      DF.to_parquet!(df, path)

      ldf = DF.from_parquet!(path, lazy: true, max_rows: 1)

      df1 = DF.compute(ldf)

      assert DF.n_rows(df1) == 1
    end

    @tag :tmp_dir
    test "columns", %{df: df, tmp_dir: tmp_dir} do
      path = Path.join([tmp_dir, "fossil_fuels.parquet"])
      DF.to_parquet!(df, path)

      df1 = DF.from_parquet!(path, lazy: true, columns: ["country", "year", "total"])

      assert DF.n_columns(df1) == 3
      assert DF.names(df1) == ["country", "year", "total"]
    end
  end

  describe "from_parquet/2 - from S3" do
    setup do
      config = %FSS.S3.Config{
        access_key_id: "test",
        secret_access_key: "test",
        endpoint: "http://localhost:4566",
        region: "us-east-1"
      }

      [config: config]
    end

    @tag :cloud_integration
    test "reads a parquet file from S3", %{config: config} do
      assert {:ok, ldf} =
               DF.from_parquet("s3://test-bucket/wine.parquet",
                 config: config,
                 lazy: true
               )

      df = DF.compute(ldf)

      assert DF.to_columns(df) == DF.to_columns(Explorer.Datasets.wine())
    end

    @tag :cloud_integration
    test "reads a parquet file from S3 using the *.parquet syntax", %{config: config} do
      assert {:ok, ldf} =
               DF.from_parquet("s3://test-bucket/*.parquet",
                 config: config,
                 lazy: true
               )

      df = DF.compute(ldf)

      assert DF.to_columns(df) == DF.to_columns(Explorer.Datasets.wine())
    end

    @tag :cloud_integration
    test "returns an error when file is not found", %{config: config} do
      assert {:error, error} =
               DF.from_parquet("s3://test-bucket/oranges.parquet",
                 config: config,
                 lazy: true
               )

      assert RuntimeError.message(error) =~
               "Polars Error: expected at least 1 path: 'parquet scan' failed: 'select' input failed to resolve"
    end
  end

  @tag :tmp_dir
  test "to_csv/2 - with defaults", %{ldf: ldf, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.csv"])

    ldf = DF.head(ldf, 15)
    assert :ok = DF.to_csv(ldf, path)

    df = DF.compute(ldf)
    df1 = DF.from_csv!(path)

    assert DF.to_rows(df1) |> Enum.sort() == DF.to_rows(df) |> Enum.sort()
  end

  @tag :tmp_dir
  test "to_csv/2 - with streaming disabled", %{ldf: ldf, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.csv"])

    ldf = DF.head(ldf, 15)
    assert :ok = DF.to_csv(ldf, path, streaming: false)

    df = DF.compute(ldf)
    df1 = DF.from_csv!(path)

    assert DF.to_rows(df1) |> Enum.sort() == DF.to_rows(df) |> Enum.sort()
  end

  @tag :cloud_integration
  test "to_csv/3 - cloud with streaming enabled - ignores streaming option", %{ldf: ldf} do
    config = %FSS.S3.Config{
      access_key_id: "test",
      secret_access_key: "test",
      endpoint: "http://localhost:4566",
      region: "us-east-1"
    }

    path = "s3://test-bucket/test-lazy-writes/wine-#{System.monotonic_time()}.csv"

    ldf = DF.head(ldf, 15)
    assert :ok = DF.to_csv(ldf, path, streaming: true, config: config)

    df = DF.compute(ldf)
    df1 = DF.from_csv!(path, config: config)

    assert DF.to_rows(df1) |> Enum.sort() == DF.to_rows(df) |> Enum.sort()
  end

  @tag :tmp_dir
  test "to_ipc/2 - with defaults", %{ldf: ldf, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.ipc"])

    ldf = DF.head(ldf, 15)
    DF.to_ipc!(ldf, path)

    df = DF.compute(ldf)
    df1 = DF.from_ipc!(path)

    assert DF.to_rows(df1) |> Enum.sort() == DF.to_rows(df) |> Enum.sort()
  end

  @tag :tmp_dir
  test "to_ipc/2 - without streaming", %{ldf: ldf, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.ipc"])

    ldf = DF.head(ldf, 15)
    DF.to_ipc!(ldf, path, streaming: false)

    df = DF.compute(ldf)
    df1 = DF.from_ipc!(path)

    assert DF.to_rows(df1) |> Enum.sort() == DF.to_rows(df) |> Enum.sort()
  end

  test "to_ipc/3 - cloud with streaming enabled", %{ldf: ldf} do
    config = %FSS.S3.Config{
      access_key_id: "test",
      secret_access_key: "test",
      endpoint: "http://localhost:4566",
      region: "us-east-1"
    }

    path = "s3://test-bucket/test-lazy-writes/wine-#{System.monotonic_time()}.ipc"

    ldf = DF.head(ldf, 15)
    assert {:error, error} = DF.to_ipc(ldf, path, streaming: true, config: config)

    assert error == ArgumentError.exception("streaming is not supported for writes to AWS S3")
  end

  @tag :cloud_integration
  test "to_ipc/2 - cloud with streaming disabled", %{ldf: ldf} do
    config = %FSS.S3.Config{
      access_key_id: "test",
      secret_access_key: "test",
      endpoint: "http://localhost:4566",
      region: "us-east-1"
    }

    path = "s3://test-bucket/test-lazy-writes/wine-#{System.monotonic_time()}.ipc"

    ldf = DF.head(ldf, 15)
    assert :ok = DF.to_ipc(ldf, path, streaming: false, config: config)

    saved_df = DF.from_ipc!(path, config: config)
    df = DF.compute(ldf)

    assert DF.to_columns(df) == DF.to_columns(saved_df)
  end

  @tag :tmp_dir
  test "to_parquet/2 - with defaults", %{ldf: ldf, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.parquet"])

    ldf = DF.head(ldf, 15)
    DF.to_parquet!(ldf, path)

    df = DF.compute(ldf)
    df1 = DF.from_parquet!(path)

    assert DF.to_rows(df1) |> Enum.sort() == DF.to_rows(df) |> Enum.sort()
  end

  @tag :tmp_dir
  test "to_parquet/2 - with streaming disabled", %{ldf: ldf, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.parquet"])

    ldf = DF.head(ldf, 15)
    DF.to_parquet!(ldf, path, streaming: false)

    df = DF.compute(ldf)
    df1 = DF.from_parquet!(path)

    assert DF.to_rows(df1) |> Enum.sort() == DF.to_rows(df) |> Enum.sort()
  end

  @tag :cloud_integration
  test "to_parquet/2 - cloud with streaming enabled", %{ldf: ldf} do
    config = %FSS.S3.Config{
      access_key_id: "test",
      secret_access_key: "test",
      endpoint: "http://localhost:4566",
      region: "us-east-1"
    }

    path = "s3://test-bucket/test-lazy-writes/wine-#{System.monotonic_time()}.parquet"

    ldf = DF.head(ldf, 15)
    # assert :ok = DF.to_parquet(ldf, path, streaming: true, config: config)

    # df = DF.compute(ldf)
    # df1 = DF.from_parquet!(path, config: config)

    # assert DF.to_rows(df) |> Enum.sort() == DF.to_rows(df1) |> Enum.sort()

    message =
      "streaming of a lazy frame to the cloud using parquet is currently unavailable. Please try again disabling the `:streaming` option."

    assert_raise RuntimeError, message, fn ->
      DF.to_parquet(ldf, path, streaming: true, config: config)
    end
  end

  @tag :cloud_integration
  test "to_parquet/2 - cloud with streaming disabled", %{ldf: ldf} do
    config = %FSS.S3.Config{
      access_key_id: "test",
      secret_access_key: "test",
      endpoint: "http://localhost:4566",
      region: "us-east-1"
    }

    path = "s3://test-bucket/test-lazy-writes/wine-#{System.monotonic_time()}.parquet"

    ldf = DF.head(ldf, 15)
    assert :ok = DF.to_parquet(ldf, path, streaming: false, config: config)

    saved_ldf = DF.from_parquet!(path, config: config)

    df = DF.compute(ldf)
    saved_df = DF.compute(saved_ldf)

    assert DF.to_columns(df) == DF.to_columns(saved_df)
  end

  @tag :tmp_dir
  test "from_ndjson/2 - with defaults", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.ndjson"])
    df = DF.slice(df, 0, 10)
    DF.to_ndjson!(df, path)

    ldf = DF.from_ndjson!(path, lazy: true)

    # no-op
    assert DF.lazy(ldf) == ldf

    df1 = DF.compute(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  @tag :tmp_dir
  test "from_ipc/2 - with defaults", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.ipc"])
    df = DF.slice(df, 0, 10)
    DF.to_ipc!(df, path)

    ldf = DF.from_ipc!(path, lazy: true)

    # no-op
    assert DF.lazy(ldf) == ldf

    df1 = DF.compute(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  @tag :tmp_dir
  test "from_ipc/2 - passing columns", %{df: df, tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "fossil_fuels.ipc"])
    df = DF.slice(df, 0, 10)
    DF.to_ipc!(df, path)

    assert_raise ArgumentError,
                 "from_ipc failed: `columns` is not supported by Polars' lazy backend. " <>
                   "Consider using `select/2` after reading the IPC file",
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
    assert DF.lazy(ldf) == ldf

    df1 = DF.compute(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  test "load_csv/2 - with defaults", %{df: df} do
    df = DF.slice(df, 0, 10)
    contents = DF.dump_csv!(df)

    ldf = DF.load_csv!(contents, lazy: true)

    # no-op
    assert DF.lazy(ldf) == ldf

    df1 = DF.compute(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  test "load_parquet/2 - with defaults", %{df: df} do
    df = DF.slice(df, 0, 10)
    contents = DF.dump_parquet!(df)

    ldf = DF.load_parquet!(contents, lazy: true)

    # no-op
    assert DF.lazy(ldf) == ldf

    df1 = DF.compute(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  test "load_ndjson/2 - with defaults", %{df: df} do
    df = DF.slice(df, 0, 10)
    contents = DF.dump_ndjson!(df)

    ldf = DF.load_ndjson!(contents, lazy: true)

    # no-op
    assert DF.lazy(ldf) == ldf

    df1 = DF.compute(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  test "load_ipc/2 - with defaults", %{df: df} do
    df = DF.slice(df, 0, 10)
    contents = DF.dump_ipc!(df)

    ldf = DF.load_ipc!(contents, lazy: true)

    # no-op
    assert DF.lazy(ldf) == ldf

    df1 = DF.compute(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  test "load_ipc_stream/2 - with defaults", %{df: df} do
    df = DF.slice(df, 0, 10)
    contents = DF.dump_ipc_stream!(df)

    ldf = DF.load_ipc_stream!(contents, lazy: true)

    # no-op
    assert DF.lazy(ldf) == ldf

    df1 = DF.compute(ldf)

    assert DF.to_columns(df1) == DF.to_columns(df)
  end

  describe "readers that are not cloud supported" do
    setup do
      config = %FSS.S3.Config{
        access_key_id: "test",
        secret_access_key: "test",
        endpoint: "http://localhost:4566",
        region: "us-east-1"
      }

      [config: config]
    end

    test "from_ipc/2", %{config: config} do
      path = "s3://test-bucket/test-lazy-writes/wine.ipc"

      assert {:error, error} = DF.from_ipc(path, config: config, lazy: true)

      assert error ==
               ArgumentError.exception(
                 "reading IPC from AWS S3 is not supported for Lazy dataframes"
               )
    end

    test "from_ipc_stream/2", %{config: config} do
      path = "s3://test-bucket/test-lazy-writes/wine.ipcstream"

      assert {:error, error} = DF.from_ipc_stream(path, config: config, lazy: true)

      assert error ==
               ArgumentError.exception(
                 "reading IPC Stream from AWS S3 is not supported for Lazy dataframes"
               )
    end

    test "from_ndjson/2", %{config: config} do
      path = "s3://test-bucket/test-lazy-writes/wine.ndjson"

      assert {:error, error} = DF.from_ndjson(path, config: config, lazy: true)

      assert error ==
               ArgumentError.exception(
                 "reading NDJSON from AWS S3 is not supported for Lazy dataframes"
               )
    end

    test "from_csv/2", %{config: config} do
      path = "s3://test-bucket/test-lazy-writes/wine.csv"

      assert {:error, error} = DF.from_csv(path, config: config, lazy: true)

      assert error ==
               ArgumentError.exception(
                 "reading CSV from AWS S3 is not supported for Lazy dataframes"
               )
    end
  end

  describe "filter_with/2" do
    test "filters by a simple selector" do
      ldf = DF.new([a: [1, 2, 3, 4], b: [150, 50, 250, 0]], lazy: true)

      ldf1 = DF.filter_with(ldf, fn ldf -> Series.greater(ldf["a"], 2) end)
      df1 = DF.compute(ldf1)

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

      df1 = DF.compute(ldf1)

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

      ldf_grouped1 =
        DF.filter_with(ldf_grouped, fn ldf ->
          Series.greater(ldf["col2"], Series.mean(ldf["col2"]))
        end)

      df1 = DF.compute(ldf_grouped1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               col1: ["a", "b"],
               col2: [5, 6],
               col3: ["d", "j"]
             }

      assert DF.groups(df1) == ["col1"]
    end
  end

  describe "sort_with/2" do
    test "with a simple df and asc order" do
      ldf = DF.new([a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"]], lazy: true)
      ldf1 = DF.sort_with(ldf, fn ldf -> [asc: ldf["a"]] end)

      df1 = DF.compute(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5, 6],
               b: ["a", "b", "c", "d", "e", "f"]
             }
    end

    test "with a simple df one column and without order" do
      ldf = DF.new([a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"]], lazy: true)
      ldf1 = DF.sort_with(ldf, fn ldf -> ldf["a"] end)

      df1 = DF.compute(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5, 6],
               b: ["a", "b", "c", "d", "e", "f"]
             }
    end

    test "with a simple df and desc order" do
      ldf = DF.new([a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"]], lazy: true)
      ldf1 = DF.sort_with(ldf, fn ldf -> [desc: ldf["a"]] end)

      df1 = DF.compute(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [6, 5, 4, 3, 2, 1],
               b: ["f", "e", "d", "c", "b", "a"]
             }
    end

    test "with a simple df and just the lazy series" do
      ldf = DF.new([a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"]], lazy: true)
      ldf1 = DF.sort_with(ldf, fn ldf -> [ldf["a"]] end)

      df1 = DF.compute(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5, 6],
               b: ["a", "b", "c", "d", "e", "f"]
             }
    end

    test "with a simple df and sort_by by two columns" do
      ldf = DF.new([a: [1, 2, 2, 3, 6, 5], b: [1.1, 2.5, 2.2, 3.3, 4.0, 5.1]], lazy: true)
      ldf1 = DF.sort_with(ldf, fn ldf -> [asc: ldf["a"], asc: ldf["b"]] end)

      df1 = DF.compute(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 2, 3, 5, 6],
               b: [1.1, 2.2, 2.5, 3.3, 5.1, 4.0]
             }
    end

    test "with a simple df and window function" do
      ldf = DF.new([a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"]], lazy: true)
      ldf1 = DF.sort_with(ldf, fn ldf -> [desc: Series.window_mean(ldf["a"], 2)] end)

      df1 = DF.compute(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [5, 6, 3, 4, 2, 1],
               b: ["e", "f", "c", "d", "b", "a"]
             }
    end

    test "sort considering groups" do
      # CSV from the Window Function section of the Polars guide.
      # https://docs.pola.rs/user-guide/expressions/window/
      psychic_pokemons = """
      name,type 1,speed
      Slowpoke,Water,15
      Slowbro, Water,30
      SlowbroMega Slowbro,Water,30
      Exeggcute,Grass,40
      Exeggutor,Grass,55
      Starmie,Water,115
      Jynx,Ice,95
      """

      ldf = DF.load_csv!(psychic_pokemons, lazy: true)

      assert DF.dtypes(ldf) == %{"name" => :string, "type 1" => :string, "speed" => {:s, 64}}

      grouped_ldf = DF.group_by(ldf, "type 1")

      # Some options are not available for sorting with groups.
      # How to warn about them? (nils_last and maintain_order)
      sorted_ldf = DF.sort_with(grouped_ldf, fn ldf -> [desc: ldf["speed"]] end)

      df = DF.compute(sorted_ldf)

      assert DF.to_rows(df, atom_keys: true) == [
               %{name: "Starmie", speed: 115, "type 1": "Water"},
               %{name: "Slowbro", speed: 30, "type 1": " Water"},
               %{name: "SlowbroMega Slowbro", speed: 30, "type 1": "Water"},
               %{name: "Exeggutor", speed: 55, "type 1": "Grass"},
               %{name: "Exeggcute", speed: 40, "type 1": "Grass"},
               %{name: "Slowpoke", speed: 15, "type 1": "Water"},
               %{name: "Jynx", speed: 95, "type 1": "Ice"}
             ]
    end
  end

  describe "head/2" do
    test "selects the first 5 rows by default", %{ldf: ldf} do
      ldf1 = DF.head(ldf)
      df = DF.compute(ldf1)

      assert DF.shape(df) == {5, 10}
    end

    test "selects the first 2 rows", %{ldf: ldf} do
      ldf1 = DF.head(ldf, 2)
      df = DF.compute(ldf1)

      assert DF.shape(df) == {2, 10}
    end

    test "selects the first 2 rows of each group", %{ldf: ldf} do
      ldf1 =
        ldf
        |> DF.group_by("country")
        |> DF.head(2)

      df = DF.compute(ldf1)

      assert DF.shape(df) == {444, 10}
    end
  end

  describe "tail/2" do
    test "selects the last 5 rows by default", %{ldf: ldf} do
      ldf1 = DF.tail(ldf)
      df = DF.compute(ldf1)

      assert DF.shape(df) == {5, 10}
    end

    test "selects the last 2 rows", %{ldf: ldf} do
      ldf1 = DF.tail(ldf, 2)
      df = DF.compute(ldf1)

      assert DF.shape(df) == {2, 10}
    end

    test "selects the last 2 rows of each group", %{ldf: ldf} do
      ldf1 =
        ldf
        |> DF.group_by("country")
        |> DF.tail(2)

      df = DF.compute(ldf1)

      assert DF.shape(df) == {444, 10}
    end
  end

  describe "distinct/2" do
    test "with lists of strings", %{ldf: ldf} do
      ldf1 = DF.distinct(ldf, [:year, :country])
      assert DF.names(ldf1) == ["year", "country"]

      assert DF.n_columns(ldf1) == 2

      ldf2 = DF.head(ldf1, 1)
      df = DF.compute(ldf2)

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

      df3 = DF.distinct(df, ..)
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
      assert ldf1.dtypes == %{"a" => {:s, 64}, "b" => :string, "c" => {:s, 64}, "d" => {:s, 64}}

      df = DF.compute(ldf1)

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
      assert ldf1.dtypes == %{"a" => {:s, 64}, "b" => {:f, 64}, "c" => :boolean, "d" => :string}

      df = DF.compute(ldf1)

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
          [a: Series.cast(ldf["a"], {:f, 64})]
        end)

      assert ldf1.names == ["a", "b"]
      assert ldf1.dtypes == %{"a" => {:f, 64}, "b" => :string}

      df = DF.compute(ldf1)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1.0, 2.0, 3.0],
               b: ["a", "b", "c"]
             }

      assert ldf1.names == df.names
      assert ldf1.dtypes == df.dtypes
    end

    test "calculates aggregations over groups" do
      ldf = DF.new([a: [1, 16, 2, 3], b: ["a", "a", "b", "c"]], lazy: true)
      ldf1 = DF.group_by(ldf, "b")

      ldf2 = DF.mutate_with(ldf1, fn ldf -> [c: Series.mean(ldf["a"])] end)

      df = DF.compute(ldf2)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 16, 2, 3],
               b: ["a", "a", "b", "c"],
               c: [8.5, 8.5, 2, 3]
             }
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

      df = DF.compute(ldf1)

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

      df = DF.compute(ldf1)

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

      df = DF.compute(ldf1)

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

      df = DF.compute(ldf1)
      assert DF.dump_csv(df) == {:ok, "third,second,first,last\n2.2,x,a,1\n3.3,y,b,3\n,z,a,1\n"}
    end
  end

  describe "rename/2" do
    test "renames a column" do
      ldf = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)

      ldf1 = DF.rename(ldf, %{"a" => "ids"})

      assert ldf1.names == ["ids", "b"]
      assert ldf1.dtypes == %{"ids" => {:s, 64}, "b" => :string}

      df = DF.compute(ldf1)

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

      df = DF.compute(ldf1)
      assert DF.to_columns(df) == %{"a" => [1], "b" => [1]}
    end

    test "considering one column" do
      ldf = DF.new([a: [1, 2, nil], b: [1, nil, 3]], lazy: true)

      ldf1 = DF.drop_nil(ldf, :a)

      df = DF.compute(ldf1)
      assert DF.to_columns(df) == %{"a" => [1, 2], "b" => [1, nil]}
    end

    test "selecting none" do
      ldf = DF.new([a: [1, 2, nil], b: [1, nil, 3]], lazy: true)

      ldf1 = DF.drop_nil(ldf, [])

      df = DF.compute(ldf1)
      assert DF.to_columns(df) == %{"a" => [1, 2, nil], "b" => [1, nil, 3]}
    end
  end

  describe "pivot_longer/3" do
    test "without selecting columns", %{ldf: ldf} do
      ldf = DF.pivot_longer(ldf, &String.ends_with?(&1, "fuel"), select: [])

      assert ldf.names == ["variable", "value"]
      assert ldf.dtypes == %{"variable" => :string, "value" => {:s, 64}}

      df = DF.compute(ldf)
      assert DF.shape(df) == {3282, 2}
      assert ldf.names == df.names
      assert ldf.dtypes == df.dtypes
    end

    test "selecting some columns", %{ldf: ldf} do
      ldf = DF.pivot_longer(ldf, &String.ends_with?(&1, "fuel"), select: ["year", "country"])

      assert ldf.names == ["year", "country", "variable", "value"]

      assert ldf.dtypes == %{
               "year" => {:s, 64},
               "country" => :string,
               "variable" => :string,
               "value" => {:s, 64}
             }

      df = DF.compute(ldf)

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

      df = DF.compute(ldf)

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

      df = DF.compute(ldf)

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

      df = DF.compute(ldf)

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

      assert DF.lazy(joined) == joined
      assert %DF{} = joined

      assert DF.names(joined) == ["a", "b"]
    end

    test "with a custom 'on'" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"]], lazy: true)

      ldf = DF.join(left, right, on: [{"a", "d"}])
      df = DF.compute(ldf)

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

      df = DF.compute(ldf)

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
      df = DF.compute(ldf)

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
      assert ldf.names == ["a", "b", "d", "c", "a_right"]

      df = DF.compute(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               d: [1, 2, 2, nil],
               a_right: [5, 6, 7, nil]
             }
    end

    test "with a custom 'on' but with repeated column on right side - cross join" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"], a: [5, 6, 7]], lazy: true)

      ldf = DF.join(left, right, how: :cross)
      assert ldf.names == ["a", "b", "d", "c", "a_right"]

      df = DF.compute(ldf)

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

      df = DF.compute(ldf)

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

      df = DF.compute(ldf)

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

      df = DF.compute(ldf)

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
      assert ldf.names == ["a", "b", "d", "d_right", "c"]

      df = DF.compute(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               d: [5, 6, 6, 7],
               d_right: [1, 2, 2, nil]
             }
    end

    test "with a custom 'on' but with repeated column on left side - cross join" do
      left = DF.new([a: [1, 2, 3], b: ["a", "b", "c"], d: [5, 6, 7]], lazy: true)
      right = DF.new([d: [1, 2, 2], c: ["d", "e", "f"]], lazy: true)

      ldf = DF.join(left, right, how: :cross)
      assert ldf.names == ["a", "b", "d", "d_right", "c"]

      df = DF.compute(ldf)

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

      df = DF.compute(ldf)

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

      df = DF.compute(ldf3)

      assert DF.to_columns(df, atom_keys: true) == %{
               x: [1, 2, 3, 4, 5, 6],
               y: ~w(a b c d e f)
             }
    end

    test "two simple DFs of the same dtypes and an operation in between" do
      ldf1 = DF.new([x: [1, 2, 3], y: ["a", "b", "c"]], lazy: true)
      ldf2 = DF.new([x: [4, 5, 6], y: ["d", "e", "f"]], lazy: true)

      ldf3 = DF.concat_rows(DF.head(ldf1, 2), ldf2)

      assert ldf3.names == ["x", "y"]

      df = DF.compute(ldf3)

      assert DF.to_columns(df, atom_keys: true) == %{
               x: [1, 2, 4, 5, 6],
               y: ~w(a b d e f)
             }
    end

    test "two DFs with different numeric types" do
      ldf1 = DF.new([x: [1, 2, 3], y: ["a", "b", "c"]], lazy: true)
      ldf2 = DF.new([x: [4.1, 5.2, 6.3, nil], y: ["d", "e", "f", "g"]], lazy: true)
      ldf3 = DF.concat_rows(ldf1, ldf2)

      df = DF.compute(ldf3)

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

      df = DF.compute(ldf)

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

      df = DF.compute(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               x: [1, 2, 3],
               y: ["a", "b", "c"],
               x_1: [4, 5, 6],
               a: ["d", "e", "f"]
             }
    end

    test "with a bigger df in the right side add nils for smaller columns" do
      ldf1 = DF.new([x: [1, 2, 3], y: ["a", "b", "c"]], lazy: true)
      ldf2 = DF.new([z: [4, 5, 6, 7], a: ["d", "e", "f", "g"]], lazy: true)

      ldf = DF.concat_columns([ldf1, ldf2])

      assert ldf.names == ["x", "y", "z", "a"]

      df = DF.compute(ldf)

      assert DF.to_columns(df, atom_keys: true) == %{
               x: [1, 2, 3, nil],
               y: ["a", "b", "c", nil],
               z: [4, 5, 6, 7],
               a: ["d", "e", "f", "g"]
             }
    end
  end

  describe "from_query/3" do
    alias Adbc.{Database, Connection}

    setup do
      db = start_supervised!({Database, driver: :sqlite})
      conn = start_supervised!({Connection, database: db})
      [conn: conn]
    end

    test "queries database", %{conn: conn} do
      {:ok, %DF{} = df} =
        Explorer.DataFrame.from_query(conn, "SELECT 123 as num, 'abc' as str", [], lazy: true)

      assert DF.lazy(df) == df
    end

    test "returns error", %{conn: conn} do
      assert {:error, %Adbc.Error{} = error} =
               Explorer.DataFrame.from_query(conn, "INVALID SQL", [], lazy: true)

      assert Exception.message(error) =~ "syntax error"
    end
  end

  describe "explode/2" do
    test "explodes a list column" do
      ldf = DF.new([letters: [~w(a e), ~w(b c d)], is_vowel: [true, false]], lazy: true)

      ldf1 = DF.explode(ldf, :letters)
      df1 = DF.compute(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               letters: ["a", "e", "b", "c", "d"],
               is_vowel: [true, true, false, false, false]
             }
    end
  end

  describe "unnest/2" do
    test "unnests a struct column" do
      ldf = DF.new([data: [%{x: 1, y: 2}, %{x: 3, y: 4}]], lazy: true)

      ldf1 = DF.unnest(ldf, :data)
      df1 = DF.compute(ldf1)

      assert DF.to_columns(df1, atom_keys: true) == %{
               x: [1, 3],
               y: [2, 4]
             }
    end
  end
end
