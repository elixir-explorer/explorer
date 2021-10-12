defmodule Explorer.DataFrameTest do
  use ExUnit.Case, async: true
  doctest Explorer.DataFrame

  alias Explorer.DataFrame, as: DF

  setup do
    {:ok, df: Explorer.Datasets.fossil_fuels()}
  end

  defp tmp_csv(tmp_dir, contents) do
    path = Path.join(tmp_dir, "tmp.csv")
    :ok = File.write!(path, contents)
    path
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

  describe "arrange/3" do
    test "raises with invalid column names", %{df: df} do
      assert_raise ArgumentError,
                   "Could not find column name \"test\"",
                   fn -> DF.arrange(df, ["test"]) end
    end
  end

  describe "take/2" do
    test "raises with index out of bounds", %{df: df} do
      assert_raise ArgumentError,
                   "Requested row index (2000) out of bounds (-1094:1094).",
                   fn -> DF.take(df, [1, 2, 3, 2000]) end
    end
  end

  describe "join/3" do
    test "raises if no overlapping columns" do
      assert_raise ArgumentError,
                   "Could not find any overlapping columns.",
                   fn ->
                     left = DF.from_map(%{a: [1, 2, 3]})
                     right = DF.from_map(%{b: [1, 2, 3]})
                     DF.join(left, right)
                   end
    end

    test "doesn't raise if no overlapping columns on cross join" do
      left = DF.from_map(%{a: [1, 2, 3]})
      right = DF.from_map(%{b: [1, 2, 3]})
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
               b: ["NA", "", "d"]
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
  end

  describe "parquet read and write" do
    @tag :tmp_dir
    test "can write parquet to file", %{df: df} do
      {:ok, "test.parquet"} = DF.write_parquet(df, "test.parquet")
      {:ok, parquet_df} = DF.read_parquet("test.parquet")
      assert DF.names(df) == DF.names(parquet_df)
      assert DF.dtypes(df) == DF.dtypes(parquet_df)
      assert DF.to_map(df) == DF.to_map(parquet_df)
    end
  end
end
