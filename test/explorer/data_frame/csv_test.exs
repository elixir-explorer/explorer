defmodule Explorer.DataFrame.CSVTest do
  use ExUnit.Case, async: true
  alias Explorer.DataFrame, as: DF
  import Explorer.IOHelpers

  @data """
  city,lat,lng
  "Elgin, Scotland, the UK",57.653484,-3.335724
  "Stoke-on-Trent, Staffordshire, the UK",53.002666,-2.179404
  "Solihull, Birmingham, UK",52.412811,-1.778197
  "Cardiff, Cardiff county, UK",51.481583,-3.17909
  "Eastbourne, East Sussex, UK",50.768036,0.290472
  "Oxford, Oxfordshire, UK",51.752022,-1.257677
  "London, UK",51.509865,-0.118092
  "Swindon, Swindon, UK",51.568535,-1.772232
  "Gravesend, Kent, UK",51.441883,0.370759
  "Northampton, Northamptonshire, UK",52.240479,-0.902656
  "Rugby, Warwickshire, UK",52.370876,-1.265032
  "Sutton Coldfield, West Midlands, UK",52.570385,-1.824042
  "Harlow, Essex, UK",51.772938,0.10231
  "Aberdeen, Aberdeen City, UK",57.149651,-2.099075
  """

  # Integration tests, based on:
  # https://github.com/jorgecarleitao/arrow2/blob/0ba4f8e21547ed08b446828bd921787e8c00e3d3/tests/it/io/csv/read.rs
  test "from_csv/2" do
    frame = DF.from_csv!(tmp_file!(@data))

    assert DF.n_rows(frame) == 14
    assert DF.n_columns(frame) == 3

    assert frame.dtypes == %{
             "city" => :string,
             "lat" => :float,
             "lng" => :float
           }

    assert_in_delta(57.653484, frame["lat"][0], f64_epsilon())

    city = frame["city"]

    assert city[0] == "Elgin, Scotland, the UK"
    assert city[13] == "Aberdeen, Aberdeen City, UK"

    file_path =
      tmp_filename(fn filename ->
        :ok = DF.to_csv!(frame, filename)
      end)

    assert File.read!(file_path) == @data
  end

  test "from_csv/2 error" do
    assert_raise RuntimeError,
                 ~r/No such file or directory/,
                 fn ->
                   DF.from_csv!("unknown")
                 end
  end

  test "load_csv/2" do
    frame = DF.load_csv!(@data)

    assert DF.n_rows(frame) == 14
    assert DF.n_columns(frame) == 3

    assert frame.dtypes == %{
             "city" => :string,
             "lat" => :float,
             "lng" => :float
           }

    assert_in_delta(57.653484, frame["lat"][0], f64_epsilon())

    city = frame["city"]

    assert city[0] == "Elgin, Scotland, the UK"
    assert city[13] == "Aberdeen, Aberdeen City, UK"
  end

  def assert_csv(type, csv_value, parsed_value, from_csv_options) do
    data = "column\n#{csv_value}\n"
    # parsing should work as expected
    frame = DF.from_csv!(tmp_file!(data), from_csv_options)
    assert frame[0][0] == parsed_value
    assert frame[0].dtype == type
    # but we also re-encode to make a full tested round-trip
    assert DF.dump_csv!(frame) == data
  end

  def assert_csv(type, csv_value, parsed_value) do
    # with explicit dtype
    assert_csv(type, csv_value, parsed_value, dtypes: [{"column", type}])
    # with inferred dtype (date/datetime support is opt-in)
    assert_csv(type, csv_value, parsed_value, parse_dates: true)
  end

  describe "dtypes" do
    test "integer" do
      assert_csv(:integer, "100", 100)
      assert_csv(:integer, "-101", -101)
    end

    test "float" do
      assert_csv(:float, "2.3", 2.3)
      assert_csv(:float, "57.653484", 57.653484)
      assert_csv(:float, "-1.772232", -1.772232)
    end

    test "boolean" do
      assert_csv(:boolean, "true", true)
      assert_csv(:boolean, "false", false)
    end

    test "string" do
      assert_csv(:string, nil, nil)
      assert_csv(:string, "some string", "some string")
      assert_csv(:string, "éphémère", "éphémère")
    end

    test "date" do
      assert_csv(:date, "2022-12-01", ~D[2022-12-01])
      assert_csv(:date, "1960-01-31", ~D[1960-01-31])
    end

    test "datetime" do
      assert_csv(
        {:datetime, :microsecond},
        "2022-10-01T11:34:10.123456",
        ~N[2022-10-01 11:34:10.123456]
      )
    end
  end

  defp tmp_csv(tmp_dir, contents) do
    path = Path.join(tmp_dir, "tmp.csv")
    :ok = File.write!(path, contents)
    path
  end

  describe "from_csv/2 options" do
    @default_infer_schema_length 1_000

    @tag :tmp_dir
    test "delimiter", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a*b
        c*d
        e*f
        """)

      df = DF.from_csv!(csv, delimiter: "*")

      assert DF.to_columns(df, atom_keys: true) == %{
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

      df = DF.from_csv!(csv, dtypes: [{"a", :string}])

      assert DF.to_columns(df, atom_keys: true) == %{
               a: ["1", "3"],
               b: [2, 4]
             }

      df = DF.from_csv!(csv, dtypes: %{a: :string})

      assert DF.to_columns(df, atom_keys: true) == %{
               a: ["1", "3"],
               b: [2, 4]
             }
    end

    @tag :tmp_dir
    test "dtypes - parse datetime", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b,c
        1,2,2020-10-15 00:00:01,
        3,4,2020-10-15 00:00:18
        """)

      df = DF.from_csv!(csv, parse_dates: true)
      assert %{"c" => {:datetime, :microsecond}} = Explorer.DataFrame.dtypes(df)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 3],
               b: [2, 4],
               c: [~N[2020-10-15 00:00:01.000000], ~N[2020-10-15 00:00:18.000000]]
             }
    end

    @tag :tmp_dir
    test "dtypes - do not parse datetime(default)", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b,c
        1,2,"2020-10-15 00:00:01",
        3,4,2020-10-15 00:00:18
        """)

      df = DF.from_csv!(csv, parse_dates: false)
      assert %{"c" => :string} = Explorer.DataFrame.dtypes(df)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 3],
               b: [2, 4],
               c: ["2020-10-15 00:00:01", "2020-10-15 00:00:18"]
             }
    end

    @tag :tmp_dir
    test "infer_schema_length - when not set, use default number of rows for schema inference",
         config do
      csv =
        tmp_csv(config.tmp_dir, """
        a
        #{1..(@default_infer_schema_length - 1) |> Enum.join("\n")}
        1.0
        """)

      df = DF.from_csv!(csv)
      assert %{"a" => :float} = Explorer.DataFrame.dtypes(df)

      csv =
        tmp_csv(config.tmp_dir, """
        a
        #{1..@default_infer_schema_length |> Enum.join("\n")}
        1.0
        """)

      assert_raise RuntimeError, ~r/from_csv failed:/, fn ->
        DF.from_csv!(csv)
      end
    end

    @tag :tmp_dir
    test "infer_schema_length - when set to n, use n rows for schema inference",
         config do
      csv =
        tmp_csv(config.tmp_dir, """
        a
        #{1..@default_infer_schema_length |> Enum.join("\n")}
        1.0
        """)

      df = DF.from_csv!(csv, infer_schema_length: @default_infer_schema_length + 1)
      assert %{"a" => :float} = Explorer.DataFrame.dtypes(df)
    end

    @tag :tmp_dir
    test "infer_schema_length - when set to `nil`, use all rows for schema inference",
         config do
      csv =
        tmp_csv(config.tmp_dir, """
        a
        #{1..@default_infer_schema_length |> Enum.join("\n")}
        1.0
        """)

      df = DF.from_csv!(csv, infer_schema_length: nil)
      assert %{"a" => :float} = Explorer.DataFrame.dtypes(df)
    end

    @tag :tmp_dir
    test "infer_schema_length - when set to `nil` and max_rows is set, use max_rows for schema inference",
         config do
      csv =
        tmp_csv(config.tmp_dir, """
        a
        #{1..@default_infer_schema_length |> Enum.join("\n")}
        1.0
        """)

      df = DF.from_csv!(csv, infer_schema_length: nil, max_rows: @default_infer_schema_length + 1)
      assert %{"a" => :float} = Explorer.DataFrame.dtypes(df)

      csv =
        tmp_csv(config.tmp_dir, """
        a
        #{1..10 |> Enum.join("\n")}
        1.0
        """)

      assert_raise RuntimeError, ~r/from_csv failed:/, fn ->
        DF.from_csv!(csv, infer_schema_length: nil, max_rows: 10)
      end
    end

    @tag :tmp_dir
    test "header", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.from_csv!(csv, header: false)

      assert DF.to_columns(df, atom_keys: true) == %{
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

      df = DF.from_csv!(csv, max_rows: 1)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: ["c"],
               b: ["d"]
             }
    end

    @tag :tmp_dir
    test "nil_values", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        n/a,NA
        nil,
        c,d
        """)

      df = DF.from_csv!(csv, nil_values: ["n/a"])

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [nil, "nil", "c"],
               b: ["NA", nil, "d"]
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

      df = DF.from_csv!(csv, skip_rows: 1)

      assert DF.to_columns(df, atom_keys: true) == %{
               c: ["e"],
               d: ["f"]
             }
    end

    @tag :tmp_dir
    test "columns - str", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.from_csv!(csv, columns: ["b"])

      assert DF.to_columns(df, atom_keys: true) == %{
               b: ["d", "f"]
             }
    end

    @tag :tmp_dir
    test "columns - atom", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.from_csv!(csv, columns: [:b])

      assert DF.to_columns(df, atom_keys: true) == %{
               b: ["d", "f"]
             }
    end

    @tag :tmp_dir
    test "columns - integer", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.from_csv!(csv, columns: [1])

      assert DF.to_columns(df, atom_keys: true) == %{
               b: ["d", "f"]
             }
    end

    @tag :tmp_dir
    test "automatically detects gz and uncompresses", config do
      csv = Path.join(config.tmp_dir, "tmp.csv.gz")

      :ok =
        File.write!(
          csv,
          :zlib.gzip("""
          a,b
          1,2
          3,4
          """)
        )

      df = DF.from_csv!(csv)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 3],
               b: [2, 4]
             }
    end

    @tag :tmp_dir
    test "parse floats with nans and infinity", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a
        0.1
        NaN
        4.2
        Inf
        -Inf
        8.1
        """)

      df = DF.from_csv!(csv, dtypes: %{a: :float})

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [0.1, :nan, 4.2, :infinity, :neg_infinity, 8.1]
             }
    end

    @tag :tmp_dir
    test "custom newline delimiter", config do
      data =
        String.replace(
          """
          a
          0.1
          NaN
          4.2
          Inf
          -Inf
          8.1
          """,
          "\n",
          "\r"
        )

      csv = tmp_csv(config.tmp_dir, data)

      df = DF.from_csv!(csv, eol_delimiter: "\r", dtypes: %{a: :float})

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [0.1, :nan, 4.2, :infinity, :neg_infinity, 8.1]
             }
    end
  end

  describe "to_csv/3" do
    setup do
      [df: Explorer.Datasets.wine()]
    end

    @tag :tmp_dir
    test "can write a CSV to file", %{df: df, tmp_dir: tmp_dir} do
      csv_path = Path.join(tmp_dir, "test.csv")

      assert :ok = DF.to_csv(df, csv_path)
      assert {:ok, csv_df} = DF.from_csv(csv_path)

      assert DF.names(df) == DF.names(csv_df)
      assert DF.dtypes(df) == DF.dtypes(csv_df)
      assert DF.to_columns(df) == DF.to_columns(csv_df)
    end
  end

  describe "to_csv/3 - cloud" do
    setup do
      [df: Explorer.Datasets.wine()]
    end

    @tag :cloud_integration
    test "writes a CSV file to S3", %{df: df} do
      config = %FSS.S3.Config{
        access_key_id: "test",
        secret_access_key: "test",
        endpoint: "http://localhost:4566",
        region: "us-east-1"
      }

      path = "s3://test-bucket/test-writes/wine-#{System.monotonic_time()}.csv"

      assert :ok = DF.to_csv(df, path, config: config)

      saved_df = DF.from_csv!(path, config: config)
      assert DF.to_columns(saved_df) == DF.to_columns(Explorer.Datasets.wine())
    end
  end
end
