defmodule Explorer.DataFrameTest do
  use ExUnit.Case, async: true
  doctest Explorer.DataFrame

  alias Explorer.DataFrame, as: DF

  setup do
    {:ok, df: Explorer.Datasets.fossil_fuels()}
  end

  setup do
    :ok = File.touch!("tmp_test.csv")

    on_exit(fn ->
      File.rm!("tmp_test.csv")
    end)
  end

  def tmp_csv(contents) do
    :ok = File.write!("tmp_test.csv", contents)
    "tmp_test.csv"
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
    test "delimiter" do
      csv =
        tmp_csv("""
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

    test "dtypes" do
      csv =
        tmp_csv("""
        a,b
        1,2
        3,4
        """)

      df = DF.read_csv!(csv, dtypes: [{"a", "str"}])

      assert DF.to_map(df) == %{
               a: ["1", "3"],
               b: [2, 4]
             }
    end

    test "header?" do
      csv =
        tmp_csv("""
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

    test "max_rows" do
      csv =
        tmp_csv("""
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

    test "null_character" do
      csv =
        tmp_csv("""
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

    test "skip_rows" do
      csv =
        tmp_csv("""
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

    test "with_columns" do
      csv =
        tmp_csv("""
        a,b
        c,d
        e,f
        """)

      df = DF.read_csv!(csv, with_columns: ["b"])

      assert DF.to_map(df) == %{
               b: ["d", "f"]
             }
    end
  end
end
