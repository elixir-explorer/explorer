defmodule Explorer.DataFrame.ParquetTest do
  @moduledoc """
  Integration tests, based on `Explorer.DataFrame.CsvTest`.
  """
  use ExUnit.Case, async: true
  alias Explorer.DataFrame, as: DF
  import Explorer.IOHelpers

  test "read" do
    parquet = tmp_parquet_file!(Explorer.Datasets.iris())

    {:ok, frame} = DF.from_parquet(parquet)

    assert DF.n_rows(frame) == 150
    assert DF.n_columns(frame) == 5

    assert frame.dtypes == %{
             "sepal_length" => :float,
             "sepal_width" => :float,
             "petal_length" => :float,
             "petal_width" => :float,
             "species" => :string
           }

    assert_in_delta(5.1, frame["sepal_length"][0], f64_epsilon())

    species = frame["species"]

    assert species[0] == "Iris-setosa"
    assert species[149] == "Iris-virginica"
  end

  def assert_parquet(type, value, parsed_value) do
    assert_from_with_correct_type(type, value, parsed_value, fn df ->
      assert {:ok, df} = DF.from_parquet(tmp_parquet_file!(df))
      df
    end)
  end

  describe "dtypes" do
    test "integer" do
      assert_parquet(:integer, "100", 100)
      assert_parquet(:integer, "-101", -101)
    end

    test "float" do
      assert_parquet(:float, "2.3", 2.3)
      assert_parquet(:float, "57.653484", 57.653484)
      assert_parquet(:float, "-1.772232", -1.772232)
    end

    # cast not used as it is not implemented for boolean values
    test "boolean" do
      assert_parquet(:boolean, true, true)
      assert_parquet(:boolean, false, false)
    end

    test "string" do
      assert_parquet(:string, "some string", "some string")
      assert_parquet(:string, "éphémère", "éphémère")
    end

    test "date" do
      assert_parquet(:date, "19327", ~D[2022-12-01])
      assert_parquet(:date, "-3623", ~D[1960-01-31])
    end

    test "datetime" do
      assert_parquet(:datetime, "1664624050123456", ~N[2022-10-01 11:34:10.123456])
    end
  end
end
