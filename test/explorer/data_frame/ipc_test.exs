defmodule Explorer.DataFrame.IPCTest do
  # Integration tests for IPC reader

  use ExUnit.Case, async: true
  alias Explorer.DataFrame, as: DF
  import Explorer.IOHelpers

  test "from_ipc/2" do
    ipc = tmp_ipc_file!(Explorer.Datasets.iris())

    assert {:ok, df} = DF.from_ipc(ipc)

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

  test "dump_ipc/2 without compression" do
    df = Explorer.Datasets.iris() |> DF.slice(0, 10)

    assert {:ok, ipc} = DF.dump_ipc(df)

    assert is_binary(ipc)
  end

  test "dump_ipc/2 with compression" do
    df = Explorer.Datasets.iris() |> DF.slice(0, 10)

    assert {:ok, ipc} = DF.dump_ipc(df, compression: :lz4)

    assert is_binary(ipc)
  end

  test "load_ipc/2 without compression" do
    df = Explorer.Datasets.iris() |> DF.slice(0, 10)
    ipc = DF.dump_ipc!(df)

    assert {:ok, df1} = DF.load_ipc(ipc)

    assert DF.to_columns(df) == DF.to_columns(df1)
  end

  test "load_ipc/2 with compression" do
    df = Explorer.Datasets.iris() |> DF.slice(0, 10)
    ipc = DF.dump_ipc!(df, compression: :lz4)

    assert {:ok, df1} = DF.load_ipc(ipc)

    assert DF.to_columns(df) == DF.to_columns(df1)
  end

  def assert_ipc(type, value, parsed_value) do
    assert_from_with_correct_type(type, value, parsed_value, fn df ->
      assert {:ok, df} = DF.from_ipc(tmp_ipc_file!(df))
      df
    end)
  end

  describe "dtypes" do
    test "integer" do
      assert_ipc(:integer, "100", 100)
      assert_ipc(:integer, "-101", -101)
    end

    test "float" do
      assert_ipc(:float, "2.3", 2.3)
      assert_ipc(:float, "57.653484", 57.653484)
      assert_ipc(:float, "-1.772232", -1.772232)
    end

    # cast not used as it is not implemented for boolean values
    test "boolean" do
      assert_ipc(:boolean, true, true)
      assert_ipc(:boolean, false, false)
    end

    test "string" do
      assert_ipc(:string, "some string", "some string")
      assert_ipc(:string, "éphémère", "éphémère")
    end

    test "date" do
      assert_ipc(:date, "19327", ~D[2022-12-01])
      assert_ipc(:date, "-3623", ~D[1960-01-31])
    end

    test "datetime" do
      assert_ipc({:datetime, :microsecond}, "1664624050123456", ~N[2022-10-01 11:34:10.123456])
    end

    test "list of integer" do
      assert_ipc({:list, :integer}, [["100"]], [100])
      assert_ipc({:list, :integer}, [["-101"]], [-101])
    end

    test "list of floats" do
      assert_ipc({:list, :float}, [["100.42"]], [100.42])
      assert_ipc({:list, :float}, [["-101.51"]], [-101.51])
    end
  end

  describe "to_ipc/3" do
    setup do
      [df: Explorer.Datasets.wine()]
    end

    @tag :tmp_dir
    test "can write a CSV to file", %{df: df, tmp_dir: tmp_dir} do
      ipc_path = Path.join(tmp_dir, "test.ipc")

      assert :ok = DF.to_ipc(df, ipc_path)
      assert {:ok, ipc_df} = DF.from_ipc(ipc_path)

      assert DF.names(df) == DF.names(ipc_df)
      assert DF.dtypes(df) == DF.dtypes(ipc_df)
      assert DF.to_columns(df) == DF.to_columns(ipc_df)
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
    test "writes an IPC file to S3", %{df: df, s3_config: s3_config} do
      path = "s3://test-bucket/test-writes/wine-#{System.monotonic_time()}.ipc"

      assert :ok = DF.to_ipc(df, path, config: s3_config)

      saved_df = DF.from_ipc!(path, config: s3_config)
      assert DF.to_columns(saved_df) == DF.to_columns(Explorer.Datasets.wine())
    end

    @tag :cloud_integration
    test "returns an error in case file is not found in S3 bucket", %{s3_config: s3_config} do
      path = "s3://test-bucket/test-writes/file-does-not-exist.ipc"

      assert {:error, %ArgumentError{message: "resource not found (404)"}} =
               DF.from_ipc(path, config: s3_config)
    end

    @tag :cloud_integration
    test "cannot write an IPC file to S3 if bucket does not exist", %{
      df: df,
      s3_config: s3_config
    } do
      key = "test-writes/wine-#{System.monotonic_time()}.ipc"
      path = "s3://test-bucket-not-found/" <> key

      assert {:error, error} = DF.to_ipc(df, path, config: s3_config)

      assert error ==
               RuntimeError.exception("Generic Error: Could not put multipart to path " <> key)
    end
  end

  describe "from_ipc/2 - HTTP" do
    setup do
      [bypass: Bypass.open(), df: Explorer.Datasets.wine()]
    end

    test "reads a IPC file from an HTTP server", %{bypass: bypass, df: df} do
      Bypass.expect(bypass, "GET", "/path/to/file.ipc", fn conn ->
        bytes = Explorer.DataFrame.dump_ipc!(df)
        Plug.Conn.resp(conn, 200, bytes)
      end)

      url = http_endpoint(bypass) <> "/path/to/file.ipc"

      assert {:ok, df1} = DF.from_ipc(url)

      assert DF.to_columns(df1) == DF.to_columns(df)
    end

    test "reads a IPC file from an HTTP server using headers", %{bypass: bypass, df: df} do
      Bypass.expect(bypass, "GET", "/path/to/file.ipc", fn conn ->
        assert ["Bearer my-token"] = Plug.Conn.get_req_header(conn, "authorization")
        bytes = Explorer.DataFrame.dump_ipc!(df)
        Plug.Conn.resp(conn, 200, bytes)
      end)

      url = http_endpoint(bypass) <> "/path/to/file.ipc"

      assert {:ok, df1} =
               DF.from_ipc(url,
                 config: [headers: [{"authorization", "Bearer my-token"}]]
               )

      assert DF.to_columns(df1) == DF.to_columns(df)
    end

    test "cannot find a IPC file", %{bypass: bypass} do
      Bypass.expect(bypass, "GET", "/path/to/file.ipc", fn conn ->
        Plug.Conn.resp(conn, 404, "not found")
      end)

      url = http_endpoint(bypass) <> "/path/to/file.ipc"

      assert {:error, %ArgumentError{message: "resource not found (404)"}} = DF.from_ipc(url)
    end
  end

  defp http_endpoint(bypass), do: "http://localhost:#{bypass.port}"
end
