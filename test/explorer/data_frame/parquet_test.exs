defmodule Explorer.DataFrame.ParquetTest do
  # Integration tests, based on `Explorer.DataFrame.CsvTest`.

  use ExUnit.Case, async: true
  alias Explorer.DataFrame, as: DF
  import Explorer.IOHelpers

  test "from_parquet/2" do
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

    file_path =
      tmp_filename(fn filename ->
        :ok = DF.to_parquet!(frame, filename)
      end)

    assert File.read!(file_path) == File.read!(parquet)
  end

  describe "from_parquet/2 options" do
    test "max_rows" do
      parquet = tmp_parquet_file!(Explorer.Datasets.iris())

      {:ok, frame} = DF.from_parquet(parquet, max_rows: 1)

      assert DF.n_rows(frame) == 1
      assert DF.n_columns(frame) == 5
    end

    test "columns - str" do
      parquet = tmp_parquet_file!(Explorer.Datasets.iris())
      {:ok, frame} = DF.from_parquet(parquet, columns: ["sepal_length"])

      assert DF.n_columns(frame) == 1
      assert DF.names(frame) == ["sepal_length"]
    end

    test "columns - atom" do
      parquet = tmp_parquet_file!(Explorer.Datasets.iris())
      {:ok, frame} = DF.from_parquet(parquet, columns: [:sepal_width, :petal_length])

      assert DF.n_columns(frame) == 2
      assert DF.names(frame) == ["sepal_width", "petal_length"]
    end

    test "columns - integer 0 indexed" do
      parquet = tmp_parquet_file!(Explorer.Datasets.iris())
      {:ok, frame} = DF.from_parquet(parquet, columns: [2, 3, 4])

      assert DF.n_columns(frame) == 3
      assert DF.names(frame) == ["petal_length", "petal_width", "species"]
    end
  end

  test "load_parquet/2" do
    parquet = tmp_parquet_file!(Explorer.Datasets.iris())
    contents = File.read!(parquet)

    assert {:ok, frame} = DF.load_parquet(contents)

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

  test "dump_parquet/1 without compression" do
    df = Explorer.Datasets.iris() |> DF.slice(0, 10)

    assert {:ok, parquet} = DF.dump_parquet(df)

    assert is_binary(parquet)
  end

  test "dump_parquet/1 with compression" do
    df = Explorer.Datasets.iris() |> DF.slice(0, 10)

    assert {:ok, parquet} = DF.dump_parquet(df, compression: {:brotli, 5})

    assert is_binary(parquet)
  end

  describe "to_parquet/2" do
    setup do
      [df: Explorer.Datasets.iris()]
    end

    @tag :tmp_dir
    test "can write parquet to file", %{df: df, tmp_dir: tmp_dir} do
      parquet_path = Path.join(tmp_dir, "test.parquet")

      assert :ok = DF.to_parquet(df, parquet_path)
      assert {:ok, parquet_df} = DF.from_parquet(parquet_path)

      assert DF.names(df) == DF.names(parquet_df)
      assert DF.dtypes(df) == DF.dtypes(parquet_df)
      assert DF.to_columns(df) == DF.to_columns(parquet_df)
    end

    # Note that we are disabling gzip for now
    for compression <- [:snappy, :brotli, :zstd, :lz4raw] do
      @tag :tmp_dir
      test "can write parquet to file with compression #{compression}", %{
        df: df,
        tmp_dir: tmp_dir
      } do
        parquet_path = Path.join(tmp_dir, "test.parquet")

        assert :ok = DF.to_parquet(df, parquet_path, compression: unquote(compression))

        assert_equal_from_path(df, parquet_path)
      end
    end

    defp assert_equal_from_path(df, path) do
      assert {:ok, parquet_df} = DF.from_parquet(path)

      assert DF.names(df) == DF.names(parquet_df)
      assert DF.dtypes(df) == DF.dtypes(parquet_df)
      assert DF.to_columns(df) == DF.to_columns(parquet_df)
    end

    # Note that we are disabling gzip for now
    for compression <- [:brotli, :zstd], level <- [1, 2, 3] do
      @tag :tmp_dir
      test "can write parquet to file with compression #{compression} and level #{level}", %{
        df: df,
        tmp_dir: tmp_dir
      } do
        parquet_path = Path.join(tmp_dir, "test.parquet")

        assert :ok =
                 DF.to_parquet(df, parquet_path,
                   compression: {unquote(compression), unquote(level)}
                 )

        assert_equal_from_path(df, parquet_path)
      end
    end
  end
end
