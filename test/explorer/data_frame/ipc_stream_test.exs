defmodule Explorer.DataFrame.IPCStreamTest do
  # Integration tests for IPC Stream reader.

  use ExUnit.Case, async: true
  alias Explorer.DataFrame, as: DF
  import Explorer.IOHelpers

  test "read" do
    ipc = tmp_ipc_stream_file!(Explorer.Datasets.iris())

    assert {:ok, df} = DF.from_ipc_stream(ipc)

    assert DF.n_rows(df) == 150
    assert DF.n_columns(df) == 5

    assert df.dtypes == %{
             "sepal_length" => :float,
             "sepal_width" => :float,
             "petal_length" => :float,
             "petal_width" => :float,
             "species" => :string
           }

    assert_in_delta(5.1, df["sepal_length"][0], f64_epsilon())

    species = df["species"]

    assert species[0] == "Iris-setosa"
    assert species[149] == "Iris-virginica"
  end

  def assert_ipc_stream(type, value, parsed_value) do
    assert_from_with_correct_type(type, value, parsed_value, fn df ->
      assert {:ok, df} = DF.from_ipc_stream(tmp_ipc_stream_file!(df))
      df
    end)
  end

  describe "dtypes" do
    test "integer" do
      assert_ipc_stream(:integer, "100", 100)
      assert_ipc_stream(:integer, "-101", -101)
    end

    test "float" do
      assert_ipc_stream(:float, "2.3", 2.3)
      assert_ipc_stream(:float, "57.653484", 57.653484)
      assert_ipc_stream(:float, "-1.772232", -1.772232)
    end

    # cast not used as it is not implemented for boolean values
    test "boolean" do
      assert_ipc_stream(:boolean, true, true)
      assert_ipc_stream(:boolean, false, false)
    end

    test "string" do
      assert_ipc_stream(:string, "some string", "some string")
      assert_ipc_stream(:string, "éphémère", "éphémère")
    end

    test "date" do
      assert_ipc_stream(:date, "19327", ~D[2022-12-01])
      assert_ipc_stream(:date, "-3623", ~D[1960-01-31])
    end

    test "datetime" do
      assert_ipc_stream(:datetime, "1664624050123456", ~N[2022-10-01 11:34:10.123456])
    end
  end
end
