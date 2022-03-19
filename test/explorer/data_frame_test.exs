defmodule Explorer.DataFrameTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO

  doctest Explorer.DataFrame

  alias Explorer.DataFrame, as: DF
  alias Explorer.Datasets
  alias Explorer.Series

  setup do
    {:ok, df: Datasets.fossil_fuels()}
  end

  defp tmp_csv(tmp_dir, contents) do
    path = Path.join(tmp_dir, "tmp.csv")
    :ok = File.write!(path, contents)
    path
  end

  describe "filter/2" do
    test "raises with mask of invalid length", %{df: df} do
      assert_raise ArgumentError,
                   "length of the mask (3) must match number of rows in the dataframe (1094)",
                   fn -> DF.filter(df, [true, false, true]) end
    end
  end

  describe "mutate/2" do
    test "raises with series of invalid length", %{df: df} do
      assert_raise ArgumentError,
                   "length of new column test (3) must match number of rows in the dataframe (1094)",
                   fn -> DF.mutate(df, test: [1, 2, 3]) end
    end
  end

  describe "arrange/3" do
    test "raises with invalid column names", %{df: df} do
      assert_raise ArgumentError,
                   "could not find column name \"test\"",
                   fn -> DF.arrange(df, ["test"]) end
    end
  end

  describe "take/2" do
    test "raises with index out of bounds", %{df: df} do
      assert_raise ArgumentError,
                   "requested row index (2000) out of bounds (-1094:1094)",
                   fn -> DF.take(df, [1, 2, 3, 2000]) end
    end
  end

  describe "join/3" do
    test "raises if no overlapping columns" do
      assert_raise ArgumentError,
                   "could not find any overlapping columns",
                   fn ->
                     left = DF.from_columns(a: [1, 2, 3])
                     right = DF.from_columns(b: [1, 2, 3])
                     DF.join(left, right)
                   end
    end

    test "doesn't raise if no overlapping columns on cross join" do
      left = DF.from_columns(a: [1, 2, 3])
      right = DF.from_columns(b: [1, 2, 3])
      joined = DF.join(left, right, how: :cross)
      assert %DF{} = joined
    end
  end

  describe "read_csv/2 options" do
    @tag :tmp_dir
    test "delimiter", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a*b
        c*d
        e*f
        """)

      df = DF.read_csv!(csv, delimiter: "*")

      assert DF.to_map(df) == %{
               a: ["c", "e"],
               b: ["d", "f"]
             }
    end

    @tag :tmp_dir
    test "dtypes", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        1,2
        3,4
        """)

      df = DF.read_csv!(csv, dtypes: [{"a", :string}])

      assert DF.to_map(df) == %{
               a: ["1", "3"],
               b: [2, 4]
             }
    end

    @tag :tmp_dir
    test "dtypes - parse datetime", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b,c
        1,2,"2020-10-15 00:00:01",
        3,4,2020-10-15 00:00:18
        """)

      df = DF.read_csv!(csv, parse_dates: true)
      assert [:datetime] = DF.select(df, ["c"]) |> Explorer.DataFrame.dtypes()

      assert DF.to_map(df) == %{
               a: [1, 3],
               b: [2, 4],
               c: [~N[2020-10-15 00:00:01.000], ~N[2020-10-15 00:00:18.000]]
             }
    end

    @tag :tmp_dir
    test "dtypes - do not parse datetime(default)", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b,c
        1,2,"2020-10-15 00:00:01",
        3,4,2020-10-15 00:00:18
        """)

      df = DF.read_csv!(csv, parse_dates: false)
      assert [:string] = DF.select(df, ["c"]) |> Explorer.DataFrame.dtypes()

      assert DF.to_map(df) == %{
               a: [1, 3],
               b: [2, 4],
               c: ["2020-10-15 00:00:01", "2020-10-15 00:00:18"]
             }
    end

    @tag :tmp_dir
    test "header?", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.read_csv!(csv, header?: false)

      assert DF.to_map(df) == %{
               column_1: ["a", "c", "e"],
               column_2: ["b", "d", "f"]
             }
    end

    @tag :tmp_dir
    test "max_rows", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.read_csv!(csv, max_rows: 1)

      assert DF.to_map(df) == %{
               a: ["c"],
               b: ["d"]
             }
    end

    @tag :tmp_dir
    test "null_character", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        n/a,NA
        nil,
        c,d
        """)

      df = DF.read_csv!(csv, null_character: "n/a")

      assert DF.to_map(df) == %{
               a: [nil, "nil", "c"],
               b: ["NA", nil, "d"]
             }
    end

    @tag :tmp_dir
    test "skip_rows", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.read_csv!(csv, skip_rows: 1)

      assert DF.to_map(df) == %{
               c: ["e"],
               d: ["f"]
             }
    end

    @tag :tmp_dir
    test "with_columns", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.read_csv!(csv, with_columns: ["b"])

      assert DF.to_map(df) == %{
               b: ["d", "f"]
             }
    end

    @tag :tmp_dir
    test "names", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.read_csv!(csv, names: ["a2", "b2"])

      assert DF.to_map(df) == %{
               a2: ["c", "e"],
               b2: ["d", "f"]
             }
    end

    @tag :tmp_dir
    test "names raises exception on invalid length", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        """)

      assert_raise(
        ArgumentError,
        "Expected length of provided names (1) to match number of columns in dataframe (2).",
        fn -> DF.read_csv(csv, names: ["a2"]) end
      )
    end

    @tag :tmp_dir
    test "names and dtypes options work together", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        1,2
        3,4
        """)

      df = DF.read_csv!(csv, dtypes: [{"a", :string}], names: ["a2", "b2"])

      assert DF.to_map(df) == %{
               a2: ["1", "3"],
               b2: [2, 4]
             }
    end

    @tag :tmp_dir
    test "automatically detects gz and uncompresses", config do
      csv = Path.join(config.tmp_dir, "tmp.csv.gz")

      :ok =
        File.write!(
          csv,
          :zlib.gzip("""
          a,b
          1,2
          3,4
          """)
        )

      df = DF.read_csv!(csv)

      assert DF.to_map(df) == %{
               a: [1, 3],
               b: [2, 4]
             }
    end
  end

  describe "parquet read and write" do
    @tag :tmp_dir
    test "can write parquet to file", %{df: df, tmp_dir: tmp_dir} do
      parquet_path = Path.join(tmp_dir, "test.parquet")

      assert {:ok, ^parquet_path} = DF.write_parquet(df, parquet_path)
      assert {:ok, parquet_df} = DF.read_parquet(parquet_path)

      assert DF.names(df) == DF.names(parquet_df)
      assert DF.dtypes(df) == DF.dtypes(parquet_df)
      assert DF.to_map(df) == DF.to_map(parquet_df)
    end
  end

  describe "table/1" do
    test "prints what we expect" do
      df = Datasets.iris()

      assert capture_io(fn -> DF.table(df) end) == """
             +-----------------------------------------------------------------------+
             |              Explorer DataFrame: [rows: 150, columns: 5]              |
             +--------------+-------------+--------------+-------------+-------------+
             | sepal_length | sepal_width | petal_length | petal_width |   species   |
             |   <float>    |   <float>   |   <float>    |   <float>   |  <string>   |
             +==============+=============+==============+=============+=============+
             | 5.1          | 3.5         | 1.4          | 0.2         | Iris-setosa |
             +--------------+-------------+--------------+-------------+-------------+
             | 4.9          | 3.0         | 1.4          | 0.2         | Iris-setosa |
             +--------------+-------------+--------------+-------------+-------------+
             | 4.7          | 3.2         | 1.3          | 0.2         | Iris-setosa |
             +--------------+-------------+--------------+-------------+-------------+
             | 4.6          | 3.1         | 1.5          | 0.2         | Iris-setosa |
             +--------------+-------------+--------------+-------------+-------------+
             | 5.0          | 3.6         | 1.4          | 0.2         | Iris-setosa |
             +--------------+-------------+--------------+-------------+-------------+

             """
    end
  end

  test "fetch/2" do
    df = DF.from_columns(a: [1, 2, 3], b: ["a", "b", "c"])
    assert Series.to_list(df["a"]) == [1, 2, 3]
    assert DF.to_map(df[["a"]]) == %{a: [1, 2, 3]}
  end

  test "pop/2" do
    df1 = DF.from_columns(a: [1, 2, 3], b: ["a", "b", "c"])

    {s1, df2} = Access.pop(df1, "a")
    assert Series.to_list(s1) == [1, 2, 3]
    assert DF.to_map(df2) == %{b: ["a", "b", "c"]}

    {df3, df4} = Access.pop(df1, ["a"])
    assert DF.to_map(df3) == %{a: [1, 2, 3]}
    assert DF.to_map(df4) == %{b: ["a", "b", "c"]}
  end

  test "get_and_update/3" do
    df1 = DF.from_columns(a: [1, 2, 3], b: ["a", "b", "c"])

    {s, df2} =
      Access.get_and_update(df1, "a", fn current_value ->
        {current_value, [0, 0, 0]}
      end)

    assert Series.to_list(s) == [1, 2, 3]
    assert DF.to_map(df2) == %{a: [0, 0, 0], b: ["a", "b", "c"]}
  end

  test "pivot_wider/2" do
    df1 = DF.from_columns(id: [1, 1], variable: ["a", "b"], value: [1, 2])
    assert DF.to_map(DF.pivot_wider(df1, "variable", "value")) == %{id: [1], a: [1], b: [2]}

    df2 = DF.from_columns(id: [1, 1], variable: ["a", "b"], value: [1.0, 2.0])

    assert DF.to_map(
             DF.pivot_wider(df2, "variable", "value",
               id_cols: ["id"],
               names_prefix: "col"
             )
           ) == %{id: [1], cola: [1.0], colb: [2.0]}
  end

  test "concat_rows/2" do
    df1 = DF.from_columns(x: [1, 2, 3], y: ["a", "b", "c"])
    df2 = DF.from_columns(x: [4, 5, 6], y: ["d", "e", "f"])
    df3 = DF.concat_rows(df1, df2)

    assert Series.to_list(df3["x"]) == [1, 2, 3, 4, 5, 6]
    assert Series.to_list(df3["y"]) == ~w(a b c d e f)

    df2 = DF.from_columns(x: [4.0, 5.0, 6.0], y: ["d", "e", "f"])
    df3 = DF.concat_rows(df1, df2)

    assert Series.to_list(df3["x"]) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]

    df4 = DF.from_columns(x: [7, 8, 9], y: ["g", "h", nil])
    df5 = DF.concat_rows(df3, df4)

    assert Series.to_list(df5["x"]) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    assert Series.to_list(df5["y"]) == ~w(a b c d e f g h) ++ [nil]

    df6 = DF.concat_rows([df1, df2, df4])

    assert Series.to_list(df6["x"]) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    assert Series.to_list(df6["y"]) == ~w(a b c d e f g h) ++ [nil]

    assert_raise ArgumentError,
                 "dataframes must have the same columns",
                 fn -> DF.concat_rows(df1, DF.from_columns(z: [7, 8, 9])) end

    assert_raise ArgumentError,
                 "dataframes must have the same columns",
                 fn -> DF.concat_rows(df1, DF.from_columns(x: [7, 8, 9], z: [7, 8, 9])) end

    assert_raise ArgumentError,
                 "columns and dtypes must be identical for all dataframes",
                 fn -> DF.concat_rows(df1, DF.from_columns(x: [7, 8, 9], y: [10, 11, 12])) end
  end
end
