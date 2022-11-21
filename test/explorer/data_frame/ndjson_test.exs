defmodule Explorer.DataFrame.NDJSONTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame, as: DF
  import Explorer.IOHelpers

  test "from" do
    filename = tmp_ndjson_file!(Explorer.Datasets.iris())

    df = DF.from_ndjson!(filename)

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

  test "dump" do
    df = Explorer.Datasets.iris() |> DF.slice(0, 10)

    assert {:ok, ndjson} = DF.dump_ndjson(df)

    assert ndjson == """
           {"sepal_length":5.1,"sepal_width":3.5,"petal_length":1.4,"petal_width":0.2,"species":"Iris-setosa"}
           {"sepal_length":4.9,"sepal_width":3.0,"petal_length":1.4,"petal_width":0.2,"species":"Iris-setosa"}
           {"sepal_length":4.7,"sepal_width":3.2,"petal_length":1.3,"petal_width":0.2,"species":"Iris-setosa"}
           {"sepal_length":4.6,"sepal_width":3.1,"petal_length":1.5,"petal_width":0.2,"species":"Iris-setosa"}
           {"sepal_length":5.0,"sepal_width":3.6,"petal_length":1.4,"petal_width":0.2,"species":"Iris-setosa"}
           {"sepal_length":5.4,"sepal_width":3.9,"petal_length":1.7,"petal_width":0.4,"species":"Iris-setosa"}
           {"sepal_length":4.6,"sepal_width":3.4,"petal_length":1.4,"petal_width":0.3,"species":"Iris-setosa"}
           {"sepal_length":5.0,"sepal_width":3.4,"petal_length":1.5,"petal_width":0.2,"species":"Iris-setosa"}
           {"sepal_length":4.4,"sepal_width":2.9,"petal_length":1.4,"petal_width":0.2,"species":"Iris-setosa"}
           {"sepal_length":4.9,"sepal_width":3.1,"petal_length":1.5,"petal_width":0.1,"species":"Iris-setosa"}
           """
  end

  def assert_ndjson(type, value, parsed_value) do
    assert_from_with_correct_type(type, value, parsed_value, fn df ->
      assert {:ok, df} = DF.from_ndjson(tmp_ndjson_file!(df))
      df
    end)
  end

  describe "dtypes" do
    test "integer" do
      assert_ndjson(:integer, "100", 100)
      assert_ndjson(:integer, "-101", -101)
    end

    test "float" do
      assert_ndjson(:float, "2.3", 2.3)
      assert_ndjson(:float, "57.653484", 57.653484)
      assert_ndjson(:float, "-1.772232", -1.772232)
    end

    # cast not used as it is not implemented for boolean values
    test "boolean" do
      assert_ndjson(:boolean, true, true)
      assert_ndjson(:boolean, false, false)
    end

    test "string" do
      assert_ndjson(:string, "some string", "some string")
      assert_ndjson(:string, "éphémère", "éphémère")
    end

    # test "date" do
    #   assert_ndjson(:date, "19327", ~D[2022-12-01])
    #   assert_ndjson(:date, "-3623", ~D[1960-01-31])
    # end

    # test "datetime" do
    #   assert_ndjson(:datetime, "1664624050123456", ~N[2022-10-01 11:34:10.123456])
    # end
  end
end
