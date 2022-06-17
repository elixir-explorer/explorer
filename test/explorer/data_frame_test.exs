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
    test "raises with mask of invalid size", %{df: df} do
      assert_raise ArgumentError,
                   "size of the mask (3) must match number of rows in the dataframe (1094)",
                   fn -> DF.filter(df, [true, false, true]) end
    end
  end

  describe "mutate/2" do
    test "adds a new column" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      df1 = DF.mutate(df, c: [true, false, true])

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3],
               b: ["a", "b", "c"],
               c: [true, false, true]
             }

      assert df1.names == ["a", "b", "c"]
      assert df1.dtypes == %{"a" => :integer, "b" => :string, "c" => :boolean}
    end

    test "adds a new column when there is a group" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"], c: [1, 1, 2])

      df1 = DF.group_by(df, :c)
      df2 = DF.mutate(df1, d: &Series.add(&1["a"], -7.1))

      # We need to sort again because it breaks ordering in the process.
      df3 = df2 |> DF.ungroup() |> DF.arrange(:a) |> DF.group_by(:c)

      assert DF.to_columns(df3, atom_keys: true) == %{
               a: [1, 2, 3],
               b: ["a", "b", "c"],
               c: [1, 1, 2],
               d: [-6.1, -5.1, -4.1]
             }

      assert df2.names == ["a", "b", "c", "d"]
      assert df2.dtypes == %{"a" => :integer, "b" => :string, "c" => :integer, "d" => :float}
      assert df2.groups == ["c"]
    end

    test "raises with series of invalid size", %{df: df} do
      assert_raise ArgumentError,
                   "size of new column test (3) must match number of rows in the dataframe (1094)",
                   fn -> DF.mutate(df, test: [1, 2, 3]) end
    end

    test "keeps the column order" do
      df = DF.new(e: [1, 2, 3], c: ["a", "b", "c"], a: [1.2, 2.3, 4.5])

      df1 = DF.mutate(df, d: 1, b: 2)

      assert df1.names == ["e", "c", "a", "d", "b"]
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
                     left = DF.new(a: [1, 2, 3])
                     right = DF.new(b: [1, 2, 3])
                     DF.join(left, right)
                   end
    end

    test "doesn't raise if no overlapping columns on cross join" do
      left = DF.new(a: [1, 2, 3])
      right = DF.new(b: [1, 2, 3])
      joined = DF.join(left, right, how: :cross)
      assert %DF{} = joined
    end

    test "with a custom 'on'" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(d: [1, 2, 2], c: ["d", "e", "f"])

      df = DF.join(left, right, on: [{"a", "d"}])

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"]
             }
    end

    test "with a custom 'on' but with repeated column on right side" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(d: [1, 2, 2], c: ["d", "e", "f"], a: [5, 6, 7])

      df = DF.join(left, right, on: [{"a", "d"}])

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"],
               a_right: [5, 6, 7]
             }

      assert df.names == ["a", "b", "c", "a_right"]

      df1 = DF.join(left, right, on: [{"a", "d"}], how: :left)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               a_right: [5, 6, 7, nil]
             }

      assert df1.names == ["a", "b", "c", "a_right"]

      df2 = DF.join(left, right, on: [{"a", "d"}], how: :outer)

      assert DF.to_columns(df2, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               a_right: [5, 6, 7, nil]
             }

      assert df2.names == ["a", "b", "c", "a_right"]

      df3 = DF.join(left, right, how: :cross)

      assert DF.to_columns(df3, atom_keys: true) == %{
               a: [1, 1, 1, 2, 2, 2, 3, 3, 3],
               a_right: [5, 6, 7, 5, 6, 7, 5, 6, 7],
               b: ["a", "a", "a", "b", "b", "b", "c", "c", "c"],
               c: ["d", "e", "f", "d", "e", "f", "d", "e", "f"],
               d: [1, 2, 2, 1, 2, 2, 1, 2, 2]
             }

      assert df3.names == ["a", "b", "d", "c", "a_right"]

      df4 = DF.join(left, right, on: [{"a", "d"}], how: :right)

      assert DF.to_columns(df4, atom_keys: true) == %{
               a: [5, 6, 7],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"],
               d: [1, 2, 2]
             }

      assert df4.names == ["d", "c", "a", "b"]
    end

    test "with a custom 'on' but with repeated column on left side" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"], d: [5, 6, 7])
      right = DF.new(d: [1, 2, 2], c: ["d", "e", "f"])

      df = DF.join(left, right, on: [{"a", "d"}])

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"],
               d: [5, 6, 6]
             }

      assert df.names == ["a", "b", "d", "c"]

      df1 = DF.join(left, right, on: [{"a", "d"}], how: :left)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               d: [5, 6, 6, 7]
             }

      assert df1.names == ["a", "b", "d", "c"]

      df2 = DF.join(left, right, on: [{"a", "d"}], how: :outer)

      assert DF.to_columns(df2, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               d: [5, 6, 6, 7]
             }

      assert df2.names == ["a", "b", "d", "c"]

      df3 = DF.join(left, right, how: :cross)

      assert DF.to_columns(df3, atom_keys: true) == %{
               a: [1, 1, 1, 2, 2, 2, 3, 3, 3],
               b: ["a", "a", "a", "b", "b", "b", "c", "c", "c"],
               c: ["d", "e", "f", "d", "e", "f", "d", "e", "f"],
               d: [5, 5, 5, 6, 6, 6, 7, 7, 7],
               d_right: [1, 2, 2, 1, 2, 2, 1, 2, 2]
             }

      assert df3.names == ["a", "b", "d", "d_right", "c"]

      df4 = DF.join(left, right, on: [{"a", "d"}], how: :right)

      assert DF.to_columns(df4, atom_keys: true) == %{
               b: ["a", "b", "b"],
               c: ["d", "e", "f"],
               d: [1, 2, 2],
               d_left: [5, 6, 6]
             }

      assert df4.names == ["d", "c", "b", "d_left"]
    end

    test "with invalid join strategy" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(a: [1, 2, 2], c: ["d", "e", "f"])

      msg =
        "join type is not valid: :inner_join. Valid options are: :inner, :left, :right, :outer, :cross"

      assert_raise ArgumentError, msg, fn -> DF.join(left, right, how: :inner_join) end
    end

    test "with matching column indexes" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(a: [1, 2, 2], c: ["d", "e", "f"])

      df = DF.join(left, right, on: [0])

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"]
             }
    end

    test "with no matching column indexes" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(c: ["d", "e", "f"], a: [1, 2, 2])

      msg = "the column given to option `:on` is not the same for both dataframes"

      assert_raise ArgumentError, msg, fn -> DF.join(left, right, on: [0]) end
    end
  end

  describe "from_csv/2 options" do
    @tag :tmp_dir
    test "delimiter", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a*b
        c*d
        e*f
        """)

      df = DF.from_csv!(csv, delimiter: "*")

      assert DF.to_columns(df, atom_keys: true) == %{
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

      df = DF.from_csv!(csv, dtypes: [{"a", :string}])

      assert DF.to_columns(df, atom_keys: true) == %{
               a: ["1", "3"],
               b: [2, 4]
             }

      df = DF.from_csv!(csv, dtypes: %{a: :string})

      assert DF.to_columns(df, atom_keys: true) == %{
               a: ["1", "3"],
               b: [2, 4]
             }
    end

    @tag :tmp_dir
    test "dtypes - parse datetime", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b,c
        1,2,2020-10-15 00:00:01,
        3,4,2020-10-15 00:00:18
        """)

      df = DF.from_csv!(csv, parse_dates: true)
      assert [:datetime] = DF.select(df, ["c"]) |> Explorer.DataFrame.dtypes()

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 3],
               b: [2, 4],
               c: [~N[2020-10-15 00:00:01.000000], ~N[2020-10-15 00:00:18.000000]]
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

      df = DF.from_csv!(csv, parse_dates: false)
      assert [:string] = DF.select(df, ["c"]) |> Explorer.DataFrame.dtypes()

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 3],
               b: [2, 4],
               c: ["2020-10-15 00:00:01", "2020-10-15 00:00:18"]
             }
    end

    @tag :tmp_dir
    test "header", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.from_csv!(csv, header: false)

      assert DF.to_columns(df, atom_keys: true) == %{
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

      df = DF.from_csv!(csv, max_rows: 1)

      assert DF.to_columns(df, atom_keys: true) == %{
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

      df = DF.from_csv!(csv, null_character: "n/a")

      assert DF.to_columns(df, atom_keys: true) == %{
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

      df = DF.from_csv!(csv, skip_rows: 1)

      assert DF.to_columns(df, atom_keys: true) == %{
               c: ["e"],
               d: ["f"]
             }
    end

    @tag :tmp_dir
    test "columns - str", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.from_csv!(csv, columns: ["b"])

      assert DF.to_columns(df, atom_keys: true) == %{
               b: ["d", "f"]
             }
    end

    @tag :tmp_dir
    test "columns - atom", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.from_csv!(csv, columns: [:b])

      assert DF.to_columns(df, atom_keys: true) == %{
               b: ["d", "f"]
             }
    end

    @tag :tmp_dir
    test "columns - integer", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.from_csv!(csv, columns: [1])

      assert DF.to_columns(df, atom_keys: true) == %{
               b: ["d", "f"]
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

      df = DF.from_csv!(csv)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 3],
               b: [2, 4]
             }
    end
  end

  describe "parquet read and write" do
    @tag :tmp_dir
    test "can write parquet to file", %{df: df, tmp_dir: tmp_dir} do
      parquet_path = Path.join(tmp_dir, "test.parquet")

      assert {:ok, ^parquet_path} = DF.to_parquet(df, parquet_path)
      assert {:ok, parquet_df} = DF.from_parquet(parquet_path)

      assert DF.names(df) == DF.names(parquet_df)
      assert DF.dtypes(df) == DF.dtypes(parquet_df)
      assert DF.to_columns(df) == DF.to_columns(parquet_df)
    end
  end

  describe "from_ndjson/2" do
    @tag :tmp_dir
    test "reads from file with default options", %{tmp_dir: tmp_dir} do
      ndjson_path = to_ndjson(tmp_dir)

      assert {:ok, df} = DF.from_ndjson(ndjson_path)

      assert DF.names(df) == ~w[a b c d]
      assert DF.dtypes(df) == [:integer, :float, :boolean, :string]

      assert take_five(df["a"]) == [1, -10, 2, 1, 7]
      assert take_five(df["b"]) == [2.0, -3.5, 0.6, 2.0, -3.5]
      assert take_five(df["c"]) == [false, true, false, false, true]
      assert take_five(df["d"]) == ["4", "4", "text", "4", "4"]

      assert {:error, _message} = DF.from_ndjson(Path.join(tmp_dir, "idontexist.ndjson"))
    end

    @tag :tmp_dir
    test "reads from file with options", %{tmp_dir: tmp_dir} do
      ndjson_path = to_ndjson(tmp_dir)

      assert {:ok, df} = DF.from_ndjson(ndjson_path, infer_schema_length: 3, batch_size: 3)

      assert DF.names(df) == ~w[a b c d]
      assert DF.dtypes(df) == [:integer, :float, :boolean, :string]
    end

    defp to_ndjson(tmp_dir) do
      ndjson_path = Path.join(tmp_dir, "test.ndjson")

      contents = """
      {"a":1, "b":2.0, "c":false, "d":"4"}
      {"a":-10, "b":-3.5, "c":true, "d":"4"}
      {"a":2, "b":0.6, "c":false, "d":"text"}
      {"a":1, "b":2.0, "c":false, "d":"4"}
      {"a":7, "b":-3.5, "c":true, "d":"4"}
      {"a":1, "b":0.6, "c":false, "d":"text"}
      {"a":1, "b":2.0, "c":false, "d":"4"}
      {"a":5, "b":-3.5, "c":true, "d":"4"}
      {"a":1, "b":0.6, "c":false, "d":"text"}
      {"a":1, "b":2.0, "c":false, "d":"4"}
      {"a":1, "b":-3.5, "c":true, "d":"4"}
      {"a":100000000000000, "b":0.6, "c":false, "d":"text"}
      """

      :ok = File.write!(ndjson_path, contents)
      ndjson_path
    end

    defp take_five(series) do
      series |> Series.to_list() |> Enum.take(5)
    end
  end

  describe "to_ndjson" do
    @tag :tmp_dir
    test "writes to a file", %{tmp_dir: tmp_dir} do
      df =
        DF.new(
          a: [1, -10, 2, 1, 7, 1, 1, 5, 1, 1, 1, 100_000_000_000_000],
          b: [2.0, -3.5, 0.6, 2.0, -3.5, 0.6, 2.0, -3.5, 0.6, 2.0, -3.5, 0.6],
          c: [false, true, false, false, true, false, false, true, false, false, true, false],
          d: ["4", "4", "text", "4", "4", "text", "4", "4", "text", "4", "4", "text"]
        )

      ndjson_path = Path.join(tmp_dir, "test-write.ndjson")

      assert {:ok, ^ndjson_path} = DF.to_ndjson(df, ndjson_path)
      assert {:ok, ndjson_df} = DF.from_ndjson(ndjson_path)

      assert DF.names(df) == DF.names(ndjson_df)
      assert DF.dtypes(df) == DF.dtypes(ndjson_df)
      assert DF.to_columns(df) == DF.to_columns(ndjson_df)
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
    df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"], c: [4.0, 5.1, 6.2])

    assert Series.to_list(df[:a]) == [1, 2, 3]
    assert Series.to_list(df["a"]) == [1, 2, 3]
    assert DF.to_columns(df[["a"]]) == %{"a" => [1, 2, 3]}
    assert DF.to_columns(df[[:a, :c]]) == %{"a" => [1, 2, 3], "c" => [4.0, 5.1, 6.2]}
    assert DF.to_columns(df[0..-2]) == %{"a" => [1, 2, 3], "b" => ["a", "b", "c"]}
    assert DF.to_columns(df[-3..-1]) == DF.to_columns(df)
    assert DF.to_columns(df[0..-1]) == DF.to_columns(df)

    assert %Series{} = s1 = df[0]
    assert Series.to_list(s1) == [1, 2, 3]

    assert %Series{} = s2 = df[2]
    assert Series.to_list(s2) == [4.0, 5.1, 6.2]

    assert %Series{} = s3 = df[-1]
    assert Series.to_list(s3) == [4.0, 5.1, 6.2]

    assert %DF{} = df2 = df[1..2]
    assert DF.names(df2) == ["b", "c"]

    assert %DF{} = df3 = df[-2..-1]
    assert DF.names(df3) == ["b", "c"]

    assert_raise ArgumentError,
                 "no column exists at index 100",
                 fn -> df[100] end

    assert_raise ArgumentError,
                 "could not find column name \"class\"",
                 fn -> df[:class] end

    assert DF.to_columns(df[0..100]) == DF.to_columns(df)
  end

  test "pop/2" do
    df1 = DF.new(a: [1, 2, 3], b: ["a", "b", "c"], c: [4.0, 5.1, 6.2])

    {s1, df2} = Access.pop(df1, "a")
    assert Series.to_list(s1) == [1, 2, 3]
    assert DF.to_columns(df2) == %{"b" => ["a", "b", "c"], "c" => [4.0, 5.1, 6.2]}

    {s1, df2} = Access.pop(df1, :a)
    assert Series.to_list(s1) == [1, 2, 3]
    assert DF.to_columns(df2) == %{"b" => ["a", "b", "c"], "c" => [4.0, 5.1, 6.2]}

    {s1, df2} = Access.pop(df1, 0)
    assert Series.to_list(s1) == [1, 2, 3]
    assert DF.to_columns(df2) == %{"b" => ["a", "b", "c"], "c" => [4.0, 5.1, 6.2]}

    {s1, df2} = Access.pop(df1, -3)
    assert Series.to_list(s1) == [1, 2, 3]
    assert DF.to_columns(df2) == %{"b" => ["a", "b", "c"], "c" => [4.0, 5.1, 6.2]}

    {df3, df4} = Access.pop(df1, ["a", "c"])
    assert DF.to_columns(df3) == %{"a" => [1, 2, 3], "c" => [4.0, 5.1, 6.2]}
    assert DF.to_columns(df4) == %{"b" => ["a", "b", "c"]}

    {df3, df4} = Access.pop(df1, 0..1)
    assert DF.to_columns(df3) == %{"a" => [1, 2, 3], "b" => ["a", "b", "c"]}
    assert DF.to_columns(df4) == %{"c" => [4.0, 5.1, 6.2]}

    {df3, df4} = Access.pop(df1, 0..-2)
    assert DF.to_columns(df3) == %{"a" => [1, 2, 3], "b" => ["a", "b", "c"]}
    assert DF.to_columns(df4) == %{"c" => [4.0, 5.1, 6.2]}

    assert {%Series{} = s2, %DF{} = df5} = Access.pop(df1, :a)
    assert Series.to_list(s2) == Series.to_list(df1[:a])
    assert DF.names(df1) -- DF.names(df5) == ["a"]

    assert {%Series{} = s3, %DF{} = df6} = Access.pop(df1, 0)
    assert Series.to_list(s3) == Series.to_list(df1[:a])
    assert DF.names(df1) -- DF.names(df6) == ["a"]
  end

  test "get_and_update/3" do
    df1 = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

    {s, df2} =
      Access.get_and_update(df1, "a", fn current_value ->
        {current_value, [0, 0, 0]}
      end)

    assert Series.to_list(s) == [1, 2, 3]
    assert DF.to_columns(df2, atom_keys: true) == %{a: [0, 0, 0], b: ["a", "b", "c"]}
  end

  test "concat_rows/2" do
    df1 = DF.new(x: [1, 2, 3], y: ["a", "b", "c"])
    df2 = DF.new(x: [4, 5, 6], y: ["d", "e", "f"])
    df3 = DF.concat_rows(df1, df2)

    assert Series.to_list(df3["x"]) == [1, 2, 3, 4, 5, 6]
    assert Series.to_list(df3["y"]) == ~w(a b c d e f)

    df2 = DF.new(x: [4.0, 5.0, 6.0], y: ["d", "e", "f"])
    df3 = DF.concat_rows(df1, df2)

    assert Series.to_list(df3["x"]) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]

    df4 = DF.new(x: [7, 8, 9], y: ["g", "h", nil])
    df5 = DF.concat_rows(df3, df4)

    assert Series.to_list(df5["x"]) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    assert Series.to_list(df5["y"]) == ~w(a b c d e f g h) ++ [nil]

    df6 = DF.concat_rows([df1, df2, df4])

    assert Series.to_list(df6["x"]) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    assert Series.to_list(df6["y"]) == ~w(a b c d e f g h) ++ [nil]

    assert_raise ArgumentError,
                 "dataframes must have the same columns",
                 fn -> DF.concat_rows(df1, DF.new(z: [7, 8, 9])) end

    assert_raise ArgumentError,
                 "dataframes must have the same columns",
                 fn -> DF.concat_rows(df1, DF.new(x: [7, 8, 9], z: [7, 8, 9])) end

    assert_raise ArgumentError,
                 "columns and dtypes must be identical for all dataframes",
                 fn -> DF.concat_rows(df1, DF.new(x: [7, 8, 9], y: [10, 11, 12])) end
  end

  describe "distinct/2" do
    test "with lists", %{df: df} do
      df1 = DF.distinct(df, columns: [:year, :country])
      assert DF.names(df1) == ["year", "country"]

      df1 = DF.distinct(df, columns: [0, 1])
      assert DF.names(df1) == ["year", "country"]

      assert df == DF.distinct(df, columns: [])

      df2 = DF.distinct(df, columns: [:year, :country], keep_all?: true)
      assert DF.names(df2) == DF.names(df)
    end

    test "with ranges", %{df: df} do
      df1 = DF.distinct(df, columns: 0..1)
      assert DF.names(df1) == ["year", "country"]

      df2 = DF.distinct(df)
      assert DF.names(df2) == DF.names(df)

      df3 = DF.distinct(df, columns: 0..-1)
      assert DF.names(df3) == DF.names(df)

      assert df == DF.distinct(df, columns: 100..200)
    end
  end

  test "drop_nil/2" do
    df = DF.new(a: [1, 2, nil], b: [1, nil, 3])

    df1 = DF.drop_nil(df)
    assert DF.to_columns(df1) == %{"a" => [1], "b" => [1]}

    df2 = DF.drop_nil(df, :a)
    assert DF.to_columns(df2) == %{"a" => [1, 2], "b" => [1, nil]}

    # Empty list do nothing.
    df3 = DF.drop_nil(df, [])
    assert DF.to_columns(df3) == %{"a" => [1, 2, nil], "b" => [1, nil, 3]}

    assert_raise ArgumentError,
                 "no column exists at index 3",
                 fn -> DF.drop_nil(df, [3, 4, 5]) end

    # It takes the slice of columns in the range
    df4 = DF.drop_nil(df, 0..200)
    assert DF.to_columns(df4) == %{"a" => [1], "b" => [1]}
  end

  describe "rename/2" do
    test "with lists" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      df1 = DF.rename(df, ["c", "d"])

      assert DF.names(df1) == ["c", "d"]
      assert df1.names == ["c", "d"]
      assert Series.to_list(df1["c"]) == Series.to_list(df["a"])
    end

    test "with keyword" do
      df = DF.new(a: ["a", "b", "a"], b: [1, 3, 1])
      df1 = DF.rename(df, a: "first")

      assert df1.names == ["first", "b"]
      assert Series.to_list(df1["first"]) == Series.to_list(df["a"])
    end

    test "with a map" do
      df = DF.new(a: ["a", "b", "a"], b: [1, 3, 1])
      df1 = DF.rename(df, %{"a" => "first", "b" => "second"})

      assert df1.names == ["first", "second"]
      assert Series.to_list(df1["first"]) == Series.to_list(df["a"])
      assert Series.to_list(df1["second"]) == Series.to_list(df["b"])
    end

    test "with keyword and a column that doesn't exist" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      assert_raise ArgumentError, "could not find column name \"g\"", fn ->
        DF.rename(df, g: "first")
      end
    end

    test "with a map and a column that doesn't exist" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      assert_raise ArgumentError, "could not find column name \"i\"", fn ->
        DF.rename(df, %{"a" => "first", "i" => "foo"})
      end
    end

    test "with a mismatch size of columns" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      assert_raise ArgumentError,
                   "list of new names must match the number of columns in the dataframe; found 3 new name(s), but the supplied dataframe has 2 column(s)",
                   fn ->
                     DF.rename(df, ["first", "second", "third"])
                   end
    end
  end

  describe "rename_with/2" do
    test "with lists", %{df: df} do
      df_names = DF.names(df)

      df1 = DF.rename_with(df, ["total", "cement"], &String.upcase/1)
      df1_names = DF.names(df1)

      assert df_names -- df1_names == ["total", "cement"]
      assert df1_names -- df_names == ["TOTAL", "CEMENT"]

      assert df1.names == [
               "year",
               "country",
               "TOTAL",
               "solid_fuel",
               "liquid_fuel",
               "gas_fuel",
               "CEMENT",
               "gas_flaring",
               "per_capita",
               "bunker_fuels"
             ]
    end

    test "with ranges", %{df: df} do
      df_names = DF.names(df)

      df1 = DF.rename_with(df, 0..1, &String.upcase/1)
      df1_names = DF.names(df1)

      assert df_names -- df1_names == ["year", "country"]
      assert df1_names -- df_names == ["YEAR", "COUNTRY"]

      df2 = DF.rename_with(df, &String.upcase/1)

      assert Enum.all?(DF.names(df2), &String.match?(&1, ~r/[A-Z]+/))
    end

    test "with a filter function", %{df: df} do
      df_names = DF.names(df)

      df1 = DF.rename_with(df, &String.starts_with?(&1, "tot"), &String.upcase/1)
      df1_names = DF.names(df1)

      assert df_names -- df1_names == ["total"]
      assert df1_names -- df_names == ["TOTAL"]

      df2 = DF.rename_with(df, &String.starts_with?(&1, "non-existent"), &String.upcase/1)

      assert df2 == df
    end
  end

  describe "pivot_wider/4" do
    test "with a single id" do
      df1 = DF.new(id: [1, 1], variable: ["a", "b"], value: [1, 2])

      df2 = DF.pivot_wider(df1, "variable", "value")

      assert DF.to_columns(df2, atom_keys: true) == %{
               id: [1],
               a: [1],
               b: [2]
             }

      df3 = DF.new(id: [1, 1], variable: ["1", "2"], value: [1.0, 2.0])

      df4 =
        DF.pivot_wider(df3, "variable", "value",
          id_columns: ["id"],
          names_prefix: "column_"
        )

      assert DF.to_columns(
               df4,
               atom_keys: true
             ) == %{id: [1], column_1: [1.0], column_2: [2.0]}

      assert df4.names == ["id", "column_1", "column_2"]
    end

    test "with multiple id columns" do
      df = DF.new(id: [1, 1], variable: ["a", "b"], value: [1, 2], other_id: [4, 5])
      df1 = DF.pivot_wider(df, "variable", "value")

      assert DF.names(df1) == ["id", "other_id", "a", "b"]
      assert df1.names == ["id", "other_id", "a", "b"]

      assert DF.to_columns(df1, atom_keys: true) == %{
               id: [1, 1],
               other_id: [4, 5],
               a: [1, nil],
               b: [nil, 2]
             }
    end

    test "with a single id column ignoring other columns" do
      df = DF.new(id: [1, 1], variable: ["a", "b"], value: [1, 2], other: [4, 5])

      df2 = DF.pivot_wider(df, "variable", "value", id_columns: [:id])
      assert DF.names(df2) == ["id", "a", "b"]

      df2 = DF.pivot_wider(df, "variable", "value", id_columns: [0])
      assert DF.names(df2) == ["id", "a", "b"]
      assert df2.names == ["id", "a", "b"]

      assert DF.to_columns(df2, atom_keys: true) == %{
               id: [1],
               a: [1],
               b: [2]
             }
    end

    test "with a single id column and repeated values" do
      df = DF.new(id: [1, 1, 2, 2], variable: ["a", "b", "a", "b"], value: [1, 2, 3, 4])

      df2 = DF.pivot_wider(df, "variable", "value", id_columns: [:id])
      assert DF.names(df2) == ["id", "a", "b"]

      df2 = DF.pivot_wider(df, "variable", "value", id_columns: [0])
      assert DF.names(df2) == ["id", "a", "b"]

      assert DF.to_columns(df2, atom_keys: true) == %{
               id: [1, 2],
               a: [1, 3],
               b: [2, 4]
             }
    end

    test "with a filter function for id columns" do
      df = DF.new(id_main: [1, 1], variable: ["a", "b"], value: [1, 2], other: [4, 5])

      df1 = DF.pivot_wider(df, "variable", "value", id_columns: &String.starts_with?(&1, "id"))
      assert DF.names(df1) == ["id_main", "a", "b"]

      assert DF.to_columns(df1, atom_keys: true) == %{
               id_main: [1],
               a: [1],
               b: [2]
             }
    end

    test "without an id column" do
      df = DF.new(id: [1, 1], variable: ["a", "b"], value: [1, 2], other_id: [4, 5])

      assert_raise ArgumentError,
                   "id_columns must select at least one existing column, but [] selects none",
                   fn ->
                     DF.pivot_wider(df, "variable", "value", id_columns: [])
                   end

      assert_raise ArgumentError,
                   ~r/id_columns must select at least one existing column, but/,
                   fn ->
                     DF.pivot_wider(df, "variable", "value",
                       id_columns: &String.starts_with?(&1, "none")
                     )
                   end
    end
  end

  test "table reader integration" do
    df = DF.new(x: [1, 2, 3], y: ["a", "b", "c"])

    assert df |> Table.to_rows() |> Enum.to_list() == [
             %{"x" => 1, "y" => "a"},
             %{"x" => 2, "y" => "b"},
             %{"x" => 3, "y" => "c"}
           ]

    columns = Table.to_columns(df)
    assert Enum.to_list(columns["x"]) == [1, 2, 3]
    assert Enum.to_list(columns["y"]) == ["a", "b", "c"]
  end

  test "collect/1 is no-op", %{df: df} do
    assert DF.collect(df) == df
  end

  test "to_lazy/1", %{df: df} do
    assert %Explorer.PolarsBackend.LazyDataFrame{} = DF.to_lazy(df).data
  end

  describe "select/3" do
    test "keep column names" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])
      df = DF.select(df, ["a"])

      assert DF.names(df) == ["a"]
      assert df.names == ["a"]
    end

    test "keep column positions" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])
      df = DF.select(df, [1])

      assert DF.names(df) == ["b"]
      assert df.names == ["b"]
    end

    test "keep column range" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3], c: [42.0, 42.1, 42.2])
      df = DF.select(df, 1..2)

      assert DF.names(df) == ["b", "c"]
      assert df.names == ["b", "c"]
    end

    test "keep columns matching callback" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3], c: [42.0, 42.1, 42.2])
      df = DF.select(df, fn name -> name in ~w(a c) end)

      assert DF.names(df) == ["a", "c"]
      assert df.names == ["a", "c"]
    end

    test "keep column raises error with non-existent column" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])

      assert_raise ArgumentError, "could not find column name \"g\"", fn ->
        DF.select(df, ["g"])
      end
    end

    test "drop column names" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])
      df = DF.select(df, ["a"], :drop)

      assert DF.names(df) == ["b"]
      assert df.names == ["b"]
    end

    test "drop column positions" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])
      df = DF.select(df, [1], :drop)

      assert DF.names(df) == ["a"]
      assert df.names == ["a"]
    end

    test "drop column range" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3], c: [42.0, 42.1, 42.2])
      df = DF.select(df, 1..2, :drop)

      assert DF.names(df) == ["a"]
      assert df.names == ["a"]
    end

    test "drop columns matching callback" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3], c: [42.0, 42.1, 42.2])
      df = DF.select(df, fn name -> name in ~w(a c) end, :drop)

      assert DF.names(df) == ["b"]
      assert df.names == ["b"]
    end

    test "drop column raises error with non-existent column" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])

      assert_raise ArgumentError, "could not find column name \"g\"", fn ->
        DF.select(df, ["g"], :drop)
      end
    end
  end

  describe "group_by/2" do
    test "groups a dataframe by one column", %{df: df} do
      assert df.groups == []
      df1 = DF.group_by(df, "country")

      assert df1.groups == ["country"]
      assert DF.groups(df1) == ["country"]
    end

    test "groups a dataframe by two columns", %{df: df} do
      df1 = DF.group_by(df, ["country", "year"])

      assert df1.groups == ["country", "year"]
      assert DF.groups(df1) == ["country", "year"]
    end

    test "adds a group for an already grouped dataframe", %{df: df} do
      df1 = DF.group_by(df, ["country"])
      df2 = DF.group_by(df1, "year")

      assert df2.groups == ["country", "year"]
      assert DF.groups(df2) == ["country", "year"]
    end

    test "raise error for unknown columns", %{df: df} do
      assert_raise ArgumentError, "could not find column name \"something_else\"", fn ->
        DF.group_by(df, "something_else")
      end
    end
  end

  describe "ungroup/2" do
    test "removes one group", %{df: df} do
      df1 = DF.group_by(df, "country")
      df2 = DF.ungroup(df1, "country")

      assert df2.groups == []
      assert DF.groups(df2) == []
    end

    test "remove one group for a dataframe that is grouped by two groups", %{df: df} do
      df1 = DF.group_by(df, ["country", "year"])
      df2 = DF.ungroup(df1, "country")

      assert df2.groups == ["year"]
      assert DF.groups(df2) == ["year"]
    end

    test "remove two groups of a dataframe", %{df: df} do
      df1 = DF.group_by(df, ["country", "year"])
      df2 = DF.ungroup(df1, ["year", "country"])

      assert df2.groups == []
      assert DF.groups(df2) == []
    end

    test "raise error for unknown groups", %{df: df} do
      df1 = DF.group_by(df, ["country", "year"])

      assert_raise ArgumentError, "could not find column name \"something_else\"", fn ->
        DF.ungroup(df1, ["something_else"])
      end
    end
  end

  describe "summarise/2" do
    test "with one group and one column with aggregations", %{df: df} do
      df1 = df |> DF.group_by("year") |> DF.summarise(total: [:max, :min])

      assert DF.to_columns(df1, atom_keys: true) == %{
               year: [2010, 2011, 2012, 2013, 2014],
               total_min: [1, 2, 2, 2, 3],
               total_max: [2_393_248, 2_654_360, 2_734_817, 2_797_384, 2_806_634]
             }
    end

    test "with one group and two columns with aggregations", %{df: df} do
      df1 = df |> DF.group_by("year") |> DF.summarise(total: [:max, :min], country: [:n_unique])

      assert DF.to_columns(df1, atom_keys: true) == %{
               year: [2010, 2011, 2012, 2013, 2014],
               total_min: [1, 2, 2, 2, 3],
               total_max: [2_393_248, 2_654_360, 2_734_817, 2_797_384, 2_806_634],
               country_n_unique: [217, 217, 220, 220, 220]
             }
    end

    test "with two groups and one column with aggregations", %{df: df} do
      df1 =
        df |> DF.head(5) |> DF.group_by(["country", "year"]) |> DF.summarise(total: [:max, :min])

      assert DF.to_columns(df1, atom_keys: true) == %{
               year: [2010, 2010, 2010, 2010, 2010],
               country: ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA"],
               total_max: [2308, 1254, 32500, 141, 7924],
               total_min: [2308, 1254, 32500, 141, 7924]
             }
    end

    test "with two groups and two columns with aggregations", %{df: df} do
      equal_filters =
        for country <- ["BRAZIL", "AUSTRALIA", "POLAND"], do: Series.equal(df["country"], country)

      filters = Enum.reduce(equal_filters, fn filter, acc -> Series.or(acc, filter) end)

      df1 =
        df
        |> DF.filter(filters)
        |> DF.group_by(["country", "year"])
        |> DF.summarise(total: [:max, :min], cement: [:median])
        |> DF.arrange(:country)

      assert DF.to_columns(df1, atom_keys: true) == %{
               country: [
                 "AUSTRALIA",
                 "AUSTRALIA",
                 "AUSTRALIA",
                 "AUSTRALIA",
                 "AUSTRALIA",
                 "BRAZIL",
                 "BRAZIL",
                 "BRAZIL",
                 "BRAZIL",
                 "BRAZIL",
                 "POLAND",
                 "POLAND",
                 "POLAND",
                 "POLAND",
                 "POLAND"
               ],
               year: [
                 2010,
                 2011,
                 2012,
                 2013,
                 2014,
                 2010,
                 2011,
                 2012,
                 2013,
                 2014,
                 2010,
                 2011,
                 2012,
                 2013,
                 2014
               ],
               total_min: [
                 106_589,
                 106_850,
                 105_843,
                 101_518,
                 98517,
                 114_468,
                 119_829,
                 128_178,
                 137_354,
                 144_480,
                 86246,
                 86446,
                 81792,
                 82432,
                 77922
               ],
               total_max: [
                 106_589,
                 106_850,
                 105_843,
                 101_518,
                 98517,
                 114_468,
                 119_829,
                 128_178,
                 137_354,
                 144_480,
                 86246,
                 86446,
                 81792,
                 82432,
                 77922
               ],
               cement_median: [
                 1129.0,
                 1170.0,
                 1156.0,
                 1142.0,
                 1224.0,
                 8040.0,
                 8717.0,
                 9428.0,
                 9517.0,
                 9691.0,
                 2111.0,
                 2523.0,
                 2165.0,
                 1977.0,
                 2089.0
               ]
             }
    end
  end
end
