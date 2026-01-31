defmodule Explorer.DataFrame.IPCStreamTest do
  # Integration tests for IPC Stream reader.

  use ExUnit.Case, async: true
  alias Explorer.DataFrame, as: DF
  import Explorer.IOHelpers

  test "from_ipc_stream/2" do
    ipc = tmp_ipc_stream_file!(Explorer.Datasets.iris())

    assert {:ok, df} = DF.from_ipc_stream(ipc)

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

  test "dump_ipc_stream/2 without compression" do
    df = Explorer.Datasets.iris() |> DF.slice(0, 10)

    assert {:ok, ipc} = DF.dump_ipc_stream(df)

    assert is_binary(ipc)
  end

  test "dump_ipc_stream/2 with compression" do
    df = Explorer.Datasets.iris() |> DF.slice(0, 10)

    assert {:ok, ipc} = DF.dump_ipc_stream(df, compression: :lz4)

    assert is_binary(ipc)
  end

  test "load_ipc_stream/2 without compression" do
    df = Explorer.Datasets.iris() |> DF.slice(0, 10)
    ipc = DF.dump_ipc_stream!(df)

    assert {:ok, df1} = DF.load_ipc_stream(ipc)

    assert DF.to_columns(df) == DF.to_columns(df1)
  end

  test "load_ipc_stream/2 with compression" do
    df = Explorer.Datasets.iris() |> DF.slice(0, 10)
    ipc = DF.dump_ipc_stream!(df, compression: :lz4)

    assert {:ok, df1} = DF.load_ipc_stream(ipc)

    assert DF.to_columns(df) == DF.to_columns(df1)
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
      assert_ipc_stream({:f, 64}, "2.3", 2.3)
      assert_ipc_stream({:f, 64}, "57.653484", 57.653484)
      assert_ipc_stream({:f, 64}, "-1.772232", -1.772232)
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
      assert_ipc_stream(:date, "2022-12-01", ~D[2022-12-01])
      assert_ipc_stream(:date, "1960-01-31", ~D[1960-01-31])

      assert_ipc_stream(:date, 0, ~D[1970-01-01])
    end

    test "naive datetime" do
      assert_ipc_stream(
        {:naive_datetime, :microsecond},
        ~N[2022-10-01 11:34:10.123456],
        ~N[2022-10-01 11:34:10.123456]
      )

      assert_ipc_stream(
        {:naive_datetime, :microsecond},
        0,
        ~N[1970-01-01 00:00:00.000000]
      )
    end

    test "list of integer" do
      assert_ipc_stream({:list, :integer}, [["100"]], [100])
      assert_ipc_stream({:list, :integer}, [["-101"]], [-101])
    end

    test "list of floats" do
      assert_ipc_stream({:list, {:f, 64}}, [["100.42"]], [100.42])
      assert_ipc_stream({:list, {:f, 64}}, [["-101.51"]], [-101.51])
    end

    test "struct" do
      assert_ipc_stream({:struct, %{"a" => :integer}}, [%{a: 1}], %{"a" => 1})
    end
  end

  describe "to_ipc_stream/3" do
    setup do
      [df: Explorer.Datasets.wine()]
    end

    @tag :tmp_dir
    test "can write an IPC stream to file", %{df: df, tmp_dir: tmp_dir} do
      ipc_path = Path.join(tmp_dir, "test.ipcstream")

      assert :ok = DF.to_ipc_stream(df, ipc_path)
      assert {:ok, ipc_df} = DF.from_ipc_stream(ipc_path)

      assert DF.names(df) == DF.names(ipc_df)
      assert DF.dtypes(df) == DF.dtypes(ipc_df)
      assert DF.to_columns(df) == DF.to_columns(ipc_df)
    end
  end

  describe "cloud reads and writes" do
    setup do
      s3_config = %{
        access_key_id: "test",
        secret_access_key: "test",
        endpoint: "http://localhost:4566",
        region: "us-east-1"
      }

      [df: Explorer.Datasets.wine(), s3_config: s3_config]
    end

    @tag :cloud_integration
    test "writes an IPC stream to S3", %{df: df, s3_config: s3_config} do
      path = "s3://test-bucket/test-writes/wine-#{System.monotonic_time()}.ipcstream"

      assert :ok = DF.to_ipc_stream(df, path, config: s3_config)

      saved_df = DF.from_ipc_stream!(path, config: s3_config)
      assert DF.to_columns(saved_df) == DF.to_columns(Explorer.Datasets.wine())
    end

    @tag :cloud_integration
    test "returns an error in case file is not found in S3 bucket", %{s3_config: s3_config} do
      path = "s3://test-bucket/test-writes/file-does-not-exist.ipcstream"

      assert {:error, %ArgumentError{message: "resource not found (404)"}} =
               DF.from_ipc_stream(path, config: s3_config)
    end
  end

  describe "from_ipc_stream/2 - HTTP" do
    setup do
      [bypass: Bypass.open(), df: Explorer.Datasets.wine()]
    end

    test "reads a IPC stream from an HTTP server", %{bypass: bypass, df: df} do
      Bypass.expect(bypass, "GET", "/path/to/file.ipcstream", fn conn ->
        bytes = Explorer.DataFrame.dump_ipc_stream!(df)
        Plug.Conn.resp(conn, 200, bytes)
      end)

      url = http_endpoint(bypass) <> "/path/to/file.ipcstream"

      assert {:ok, df1} = DF.from_ipc_stream(url)

      assert DF.to_columns(df1) == DF.to_columns(df)
    end

    test "reads a IPC stream from an HTTP server using headers", %{bypass: bypass, df: df} do
      Bypass.expect(bypass, "GET", "/path/to/file.ipcstream", fn conn ->
        assert ["Bearer my-token"] = Plug.Conn.get_req_header(conn, "authorization")
        bytes = Explorer.DataFrame.dump_ipc_stream!(df)
        Plug.Conn.resp(conn, 200, bytes)
      end)

      url = http_endpoint(bypass) <> "/path/to/file.ipcstream"

      assert {:ok, df1} =
               DF.from_ipc_stream(url,
                 config: [headers: [{"authorization", "Bearer my-token"}]]
               )

      assert DF.to_columns(df1) == DF.to_columns(df)
    end

    test "cannot find a IPC stream", %{bypass: bypass} do
      Bypass.expect(bypass, "GET", "/path/to/file.ipcstream", fn conn ->
        Plug.Conn.resp(conn, 404, "not found")
      end)

      url = http_endpoint(bypass) <> "/path/to/file.ipcstream"

      assert {:error, %ArgumentError{message: "resource not found (404)"}} =
               DF.from_ipc_stream(url)
    end
  end

  defp http_endpoint(bypass), do: "http://localhost:#{bypass.port}"
end
