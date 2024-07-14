defmodule Explorer.DataFrame.NDJSONTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame, as: DF
  import Explorer.IOHelpers

  test "from_ndjson/2" do
    filename = tmp_ndjson_file!(Explorer.Datasets.iris())

    df = DF.from_ndjson!(filename)

    assert DF.n_rows(df) == 150
    assert DF.n_columns(df) == 5

    assert df.dtypes == %{
             "sepal_length" => {:f, 64},
             "sepal_width" => {:f, 64},
             "petal_length" => {:f, 64},
             "petal_width" => {:f, 64},
             "species" => :string
           }

    assert_in_delta(5.1, df["sepal_length"][0], f64_epsilon())

    species = df["species"]

    assert species[0] == "Iris-setosa"
    assert species[149] == "Iris-virginica"
  end

  test "dump_ndjson/1" do
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

  test "load_ndjson/1" do
    ndjson = """
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

    assert {:ok, df} = DF.load_ndjson(ndjson)

    assert DF.n_rows(df) == 10
    assert DF.n_columns(df) == 5

    assert df.dtypes == %{
             "sepal_length" => {:f, 64},
             "sepal_width" => {:f, 64},
             "petal_length" => {:f, 64},
             "petal_width" => {:f, 64},
             "species" => :string
           }

    assert_in_delta(5.1, df["sepal_length"][0], f64_epsilon())

    species = df["species"]

    assert species[0] == "Iris-setosa"
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
      assert_ndjson({:f, 64}, "2.3", 2.3)
      assert_ndjson({:f, 64}, "57.653484", 57.653484)
      assert_ndjson({:f, 64}, "-1.772232", -1.772232)
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

    test "list of integer" do
      assert_ndjson({:list, :integer}, [["100"]], [100])
      assert_ndjson({:list, :integer}, [["-101"]], [-101])
    end

    test "list of floats" do
      assert_ndjson({:list, {:f, 64}}, [["100.42"]], [100.42])
      assert_ndjson({:list, {:f, 64}}, [["-101.51"]], [-101.51])
    end

    test "structs" do
      assert_ndjson({:struct, %{"a" => {:s, 64}}}, [%{a: 1}], %{"a" => 1})
    end

    test "infers correctly ordered dtype from ordered source" do
      df =
        """
        {"col": {"b": "b", "a": "a"}}
        """
        |> DF.load_ndjson!()

      assert df["col"].dtype == {:struct, [{"b", :string}, {"a", :string}]}

      df1 =
        """
        {"col": {"a": "a", "b": "b"}}
        """
        |> DF.load_ndjson!()

      assert df1["col"].dtype == {:struct, [{"a", :string}, {"b", :string}]}
    end

    # test "date" do
    #   assert_ndjson(:date, "19327", ~D[2022-12-01])
    #   assert_ndjson(:date, "-3623", ~D[1960-01-31])
    # end

    # test "datetime" do
    #   assert_ndjson(:datetime, "1664624050123456", ~N[2022-10-01 11:34:10.123456])
    # end
  end

  describe "nulls" do
    test "column with a null and a non-null" do
      df = DF.new(a: [nil, 1]) |> tmp_ndjson_file!() |> DF.from_ndjson!()
      assert DF.dtypes(df) == %{"a" => {:s, 64}}
      assert inspect(df) == ~s(#Explorer.DataFrame<
  Polars[2 x 1]
  a s64 [nil, 1]
>)
    end

    test "column with only nulls" do
      df = DF.new(a: [nil, nil]) |> tmp_ndjson_file!() |> DF.from_ndjson!()
      assert DF.dtypes(df) == %{"a" => :null}
      assert inspect(df) == ~s(#Explorer.DataFrame<
  Polars[2 x 1]
  a null [nil, nil]
>)
    end
  end

  describe "from_ndjson/2 options" do
    @tag :tmp_dir
    test "reads from file with default options", %{tmp_dir: tmp_dir} do
      ndjson_path = to_ndjson(tmp_dir)

      assert {:ok, df} = DF.from_ndjson(ndjson_path)

      assert DF.names(df) == ~w[a b c d]
      assert DF.dtypes(df) == %{"a" => {:s, 64}, "b" => {:f, 64}, "c" => :boolean, "d" => :string}

      sliced = DF.slice(df, 0, 5)

      assert DF.to_columns(sliced, atom_keys: true) == %{
               a: [1, -10, 2, 1, 7],
               b: [2.0, -3.5, 0.6, 2.0, -3.5],
               c: [false, true, false, false, true],
               d: ["4", "4", "text", "4", "4"]
             }

      assert {:error, _message} = DF.from_ndjson(Path.join(tmp_dir, "idontexist.ndjson"))
    end

    @tag :tmp_dir
    test "reads from file with options", %{tmp_dir: tmp_dir} do
      ndjson_path = to_ndjson(tmp_dir)

      assert {:ok, df} = DF.from_ndjson(ndjson_path, infer_schema_length: 3, batch_size: 3)

      assert DF.names(df) == ~w[a b c d]
      assert DF.dtypes(df) == %{"a" => {:s, 64}, "b" => {:f, 64}, "c" => :boolean, "d" => :string}
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
  end

  describe "to_ndjson/2" do
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

      assert :ok = DF.to_ndjson(df, ndjson_path)

      contents = File.read!(ndjson_path)

      assert contents == """
             {"a":1,"b":2.0,"c":false,"d":"4"}
             {"a":-10,"b":-3.5,"c":true,"d":"4"}
             {"a":2,"b":0.6,"c":false,"d":"text"}
             {"a":1,"b":2.0,"c":false,"d":"4"}
             {"a":7,"b":-3.5,"c":true,"d":"4"}
             {"a":1,"b":0.6,"c":false,"d":"text"}
             {"a":1,"b":2.0,"c":false,"d":"4"}
             {"a":5,"b":-3.5,"c":true,"d":"4"}
             {"a":1,"b":0.6,"c":false,"d":"text"}
             {"a":1,"b":2.0,"c":false,"d":"4"}
             {"a":1,"b":-3.5,"c":true,"d":"4"}
             {"a":100000000000000,"b":0.6,"c":false,"d":"text"}
             """
    end
  end

  describe "cloud reads and writes" do
    setup do
      s3_config = %FSS.S3.Config{
        access_key_id: "test",
        secret_access_key: "test",
        endpoint: "http://localhost:4566",
        region: "us-east-1"
      }

      [df: Explorer.Datasets.wine(), s3_config: s3_config]
    end

    @tag :cloud_integration
    test "writes a NDJSON file to S3", %{df: df, s3_config: s3_config} do
      path = "s3://test-bucket/test-writes/wine-#{System.monotonic_time()}.ndjson"

      assert :ok = DF.to_ndjson(df, path, config: s3_config)

      saved_df = DF.from_ndjson!(path, config: s3_config)
      assert DF.to_columns(saved_df) == DF.to_columns(Explorer.Datasets.wine())
    end

    @tag :cloud_integration
    test "returns an error in case file is not found in S3 bucket", %{s3_config: s3_config} do
      path = "s3://test-bucket/test-writes/file-does-not-exist.ndjson"

      assert {:error, %ArgumentError{message: "resource not found (404)"}} =
               DF.from_ndjson(path, config: s3_config)
    end
  end

  describe "from_ndjson/2 - HTTP" do
    setup do
      [bypass: Bypass.open(), df: Explorer.Datasets.wine()]
    end

    test "reads a NDJSON file from an HTTP server", %{bypass: bypass, df: df} do
      Bypass.expect(bypass, "GET", "/path/to/file.ndjson", fn conn ->
        bytes = Explorer.DataFrame.dump_ndjson!(df)
        Plug.Conn.resp(conn, 200, bytes)
      end)

      url = http_endpoint(bypass) <> "/path/to/file.ndjson"

      assert {:ok, df1} = DF.from_ndjson(url)

      assert DF.to_columns(df1) == DF.to_columns(df)
    end

    test "reads a NDJSON file from an HTTP server using headers", %{bypass: bypass, df: df} do
      Bypass.expect(bypass, "GET", "/path/to/file.ndjson", fn conn ->
        assert ["Bearer my-token"] = Plug.Conn.get_req_header(conn, "authorization")
        bytes = Explorer.DataFrame.dump_ndjson!(df)
        Plug.Conn.resp(conn, 200, bytes)
      end)

      url = http_endpoint(bypass) <> "/path/to/file.ndjson"

      assert {:ok, df1} =
               DF.from_ndjson(url,
                 config: [headers: [{"authorization", "Bearer my-token"}]]
               )

      assert DF.to_columns(df1) == DF.to_columns(df)
    end

    test "cannot find a NDJSON file", %{bypass: bypass} do
      Bypass.expect(bypass, "GET", "/path/to/file.ndjson", fn conn ->
        Plug.Conn.resp(conn, 404, "not found")
      end)

      url = http_endpoint(bypass) <> "/path/to/file.ndjson"

      assert {:error, %ArgumentError{message: "resource not found (404)"}} = DF.from_ndjson(url)
    end
  end

  defp http_endpoint(bypass), do: "http://localhost:#{bypass.port}"
end
