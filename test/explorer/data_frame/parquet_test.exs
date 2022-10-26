defmodule Explorer.DataFrame.ParquetTest do
  @moduledoc """
  Integration tests, based on `Explorer.DataFrame.CsvTest`.
  """
  use ExUnit.Case, async: true
  alias Explorer.DataFrame, as: DF
  alias Explorer.Series

  # https://doc.rust-lang.org/std/primitive.f64.html#associatedconstant.EPSILON
  @f64_epsilon 2.2204460492503131e-16

  def tmp_file!(df) do
    System.tmp_dir!()
    |> Path.join("data.parquet")
    |> tap(fn filename ->
      :ok = DF.to_parquet(df, filename)
    end)
  end

  test "read" do
    parquet = tmp_file!(Explorer.Datasets.iris())

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

    assert_in_delta(5.1, frame["sepal_length"][0], @f64_epsilon)

    species = frame["species"]

    assert species[0] == "Iris-setosa"
    assert species[149] == "Iris-virginica"
  end

  def assert_parquet(type, value, parsed_value) do
    df = [value] |> Series.from_list() |> Series.cast(type) |> then(&DF.new(column: &1))
    # parsing should work as expected
    {:ok, frame} = DF.from_parquet(tmp_file!(df))
    assert frame[0][0] == parsed_value
    assert frame[0].dtype == type
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
