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
             "lat" => {:f, 64},
             "lng" => {:f, 64}
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
             "lat" => {:f, 64},
             "lng" => {:f, 64}
           }

    assert_in_delta(57.653484, frame["lat"][0], f64_epsilon())

    city = frame["city"]

    assert city[0] == "Elgin, Scotland, the UK"
    assert city[13] == "Aberdeen, Aberdeen City, UK"
  end

  test "load_csv/2 dtypes - mismatched names" do
    text = """
    first_name , last_name , dob
    Alice , Ant , 01/02/1970
    Billy , Bat , 03/04/1990
    """

    types = [
      {"first_name", :string},
      {"last_name", :string},
      {"dob", :string}
    ]

    assert text
           |> DF.load_csv!(dtypes: types)
           |> DF.to_columns(atom_keys: true) == %{
             dob: [" 01/02/1970", " 03/04/1990"],
             first_name: ["Alice ", "Billy "],
             last_name: [" Ant ", " Bat "]
           }
  end

  test "load_csv/2 dtypes - all as strings" do
    csv =
      """
      id,first_name,last_name,email,gender,ip_address,salary,latitude,longitude
      1,Torey,Geraghty,email@shutterfly.com,Male,119.110.38.172,14036.68,38.9187037,-76.9611991
      2,Nevin,Mandrake,email@ovh.net,Male,161.2.124.233,32530.27,41.4176872,-8.7653155
      3,Melisenda,Guiso,email@wp.com,Female,192.152.64.134,9177.8,21.3772424,110.2485736
      4,Noble,Doggett,email@springer.com,Male,252.234.29.244,20328.76,37.268428,55.1487513
      5,Janaya,Claypoole,email@infoseek.co.jp,Female,150.191.214.252,21442.93,15.3553417,120.5293228
      6,Sarah,Hugk,email@bbb.org,Female,211.158.246.13,79709.16,28.168408,120.482198
      7,Ulberto,Simenon,email@unblog.fr,Male,206.56.108.90,16248.98,48.4046776,-0.9746208
      8,Kevon,Lingner,email@dyndns.org,Male,181.71.212.116,7497.64,-23.351784,-47.6931718
      9,Sada,Garbert,email@flavors.me,Female,170.42.190.231,15969.95,30.3414125,114.1543243
      10,Salmon,Shoulders,email@prweb.com,Male,68.138.106.143,19996.71,49.2152833,17.7687416
      """

    headers = ~w(id first_name last_name email gender ip_address salary latitude longitude)

    # Out of order on purpose.
    df = DF.load_csv!(csv, dtypes: for(l <- Enum.shuffle(headers), do: {l, :string}))

    assert DF.names(df) == headers

    assert DF.to_columns(df, atom_keys: true) == %{
             email: [
               "email@shutterfly.com",
               "email@ovh.net",
               "email@wp.com",
               "email@springer.com",
               "email@infoseek.co.jp",
               "email@bbb.org",
               "email@unblog.fr",
               "email@dyndns.org",
               "email@flavors.me",
               "email@prweb.com"
             ],
             first_name: [
               "Torey",
               "Nevin",
               "Melisenda",
               "Noble",
               "Janaya",
               "Sarah",
               "Ulberto",
               "Kevon",
               "Sada",
               "Salmon"
             ],
             gender: [
               "Male",
               "Male",
               "Female",
               "Male",
               "Female",
               "Female",
               "Male",
               "Male",
               "Female",
               "Male"
             ],
             id: ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
             ip_address: [
               "119.110.38.172",
               "161.2.124.233",
               "192.152.64.134",
               "252.234.29.244",
               "150.191.214.252",
               "211.158.246.13",
               "206.56.108.90",
               "181.71.212.116",
               "170.42.190.231",
               "68.138.106.143"
             ],
             last_name: [
               "Geraghty",
               "Mandrake",
               "Guiso",
               "Doggett",
               "Claypoole",
               "Hugk",
               "Simenon",
               "Lingner",
               "Garbert",
               "Shoulders"
             ],
             latitude: [
               "38.9187037",
               "41.4176872",
               "21.3772424",
               "37.268428",
               "15.3553417",
               "28.168408",
               "48.4046776",
               "-23.351784",
               "30.3414125",
               "49.2152833"
             ],
             longitude: [
               "-76.9611991",
               "-8.7653155",
               "110.2485736",
               "55.1487513",
               "120.5293228",
               "120.482198",
               "-0.9746208",
               "-47.6931718",
               "114.1543243",
               "17.7687416"
             ],
             salary: [
               "14036.68",
               "32530.27",
               "9177.8",
               "20328.76",
               "21442.93",
               "79709.16",
               "16248.98",
               "7497.64",
               "15969.95",
               "19996.71"
             ]
           }
  end

  test "load_csv/2 quote_delimiter - different quote char" do
    data = """
    city,lat,lng
    'Elgin, Scotland, the UK',57.653484,-3.335724
    'Stoke-on-Trent, Staffordshire, the UK',53.002666,-2.179404
    'Solihull, Birmingham, UK',52.412811,-1.778197
    """

    frame = DF.load_csv!(data, quote_delimiter: "'")

    assert DF.n_rows(frame) == 3
    assert DF.n_columns(frame) == 3

    assert frame["city"][0] == "Elgin, Scotland, the UK"
    assert frame["city"][2] == "Solihull, Birmingham, UK"
  end

  test "load_csv/2 quote_delimiter - no quote char" do
    data = """
    city;nickname;lat;lng
    Elgin, Scotland, the UK;"Little Ireland";57.653484;-3.335724
    Stoke-on-Trent, Staffordshire, the UK;nil;53.002666;-2.179404
    Solihull, Birmingham, UK;nil;52.412811;-1.778197
    """

    frame = DF.load_csv!(data, quote_delimiter: nil, delimiter: ";", nil_values: ["nil"])

    assert DF.n_rows(frame) == 3
    assert DF.n_columns(frame) == 4

    assert frame["city"][0] == "Elgin, Scotland, the UK"
    assert frame["city"][2] == "Solihull, Birmingham, UK"

    assert frame["nickname"][0] == "\"Little Ireland\""
    assert frame["nickname"][1] == nil
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
      assert_csv({:s, 64}, "100", 100)
      assert_csv({:s, 64}, "-101", -101)
    end

    test "float" do
      assert_csv({:f, 64}, "2.3", 2.3)
      assert_csv({:f, 64}, "57.653484", 57.653484)
      assert_csv({:f, 64}, "-1.772232", -1.772232)
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

    test "naive datetime" do
      assert_csv(
        {:naive_datetime, :microsecond},
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
    test "dtypes - all as strings", config do
      csv =
        tmp_csv(config.tmp_dir, """
        id,first_name,last_name,email,gender,ip_address,salary,latitude,longitude
        1,Torey,Geraghty,email@shutterfly.com,Male,119.110.38.172,14036.68,38.9187037,-76.9611991
        2,Nevin,Mandrake,email@ovh.net,Male,161.2.124.233,32530.27,41.4176872,-8.7653155
        3,Melisenda,Guiso,email@wp.com,Female,192.152.64.134,9177.8,21.3772424,110.2485736
        4,Noble,Doggett,email@springer.com,Male,252.234.29.244,20328.76,37.268428,55.1487513
        5,Janaya,Claypoole,email@infoseek.co.jp,Female,150.191.214.252,21442.93,15.3553417,120.5293228
        6,Sarah,Hugk,email@bbb.org,Female,211.158.246.13,79709.16,28.168408,120.482198
        7,Ulberto,Simenon,email@unblog.fr,Male,206.56.108.90,16248.98,48.4046776,-0.9746208
        8,Kevon,Lingner,email@dyndns.org,Male,181.71.212.116,7497.64,-23.351784,-47.6931718
        9,Sada,Garbert,email@flavors.me,Female,170.42.190.231,15969.95,30.3414125,114.1543243
        10,Salmon,Shoulders,email@prweb.com,Male,68.138.106.143,19996.71,49.2152833,17.7687416
        """)

      headers = ~w(id first_name last_name email gender ip_address salary latitude longitude)

      # Out of order on purpose.
      df = DF.from_csv!(csv, dtypes: for(l <- Enum.shuffle(headers), do: {l, :string}))

      assert DF.names(df) == headers

      assert DF.to_columns(df, atom_keys: true) == %{
               email: [
                 "email@shutterfly.com",
                 "email@ovh.net",
                 "email@wp.com",
                 "email@springer.com",
                 "email@infoseek.co.jp",
                 "email@bbb.org",
                 "email@unblog.fr",
                 "email@dyndns.org",
                 "email@flavors.me",
                 "email@prweb.com"
               ],
               first_name: [
                 "Torey",
                 "Nevin",
                 "Melisenda",
                 "Noble",
                 "Janaya",
                 "Sarah",
                 "Ulberto",
                 "Kevon",
                 "Sada",
                 "Salmon"
               ],
               gender: [
                 "Male",
                 "Male",
                 "Female",
                 "Male",
                 "Female",
                 "Female",
                 "Male",
                 "Male",
                 "Female",
                 "Male"
               ],
               id: ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
               ip_address: [
                 "119.110.38.172",
                 "161.2.124.233",
                 "192.152.64.134",
                 "252.234.29.244",
                 "150.191.214.252",
                 "211.158.246.13",
                 "206.56.108.90",
                 "181.71.212.116",
                 "170.42.190.231",
                 "68.138.106.143"
               ],
               last_name: [
                 "Geraghty",
                 "Mandrake",
                 "Guiso",
                 "Doggett",
                 "Claypoole",
                 "Hugk",
                 "Simenon",
                 "Lingner",
                 "Garbert",
                 "Shoulders"
               ],
               latitude: [
                 "38.9187037",
                 "41.4176872",
                 "21.3772424",
                 "37.268428",
                 "15.3553417",
                 "28.168408",
                 "48.4046776",
                 "-23.351784",
                 "30.3414125",
                 "49.2152833"
               ],
               longitude: [
                 "-76.9611991",
                 "-8.7653155",
                 "110.2485736",
                 "55.1487513",
                 "120.5293228",
                 "120.482198",
                 "-0.9746208",
                 "-47.6931718",
                 "114.1543243",
                 "17.7687416"
               ],
               salary: [
                 "14036.68",
                 "32530.27",
                 "9177.8",
                 "20328.76",
                 "21442.93",
                 "79709.16",
                 "16248.98",
                 "7497.64",
                 "15969.95",
                 "19996.71"
               ]
             }
    end

    @tag :tmp_dir
    test "dtypes - parse datetime", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b,c
        1,2,2020-10-15 00:00:01
        3,4,2020-10-15 00:00:18
        """)

      df = DF.from_csv!(csv, parse_dates: true)
      assert %{"c" => {:naive_datetime, :microsecond}} = Explorer.DataFrame.dtypes(df)

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
        1,2,"2020-10-15 00:00:01"
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
      assert %{"a" => {:f, 64}} = Explorer.DataFrame.dtypes(df)

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
      assert %{"a" => {:f, 64}} = Explorer.DataFrame.dtypes(df)
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
      assert %{"a" => {:f, 64}} = Explorer.DataFrame.dtypes(df)
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
      assert %{"a" => {:f, 64}} = Explorer.DataFrame.dtypes(df)

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
    test "skip_rows_after_header", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        """)

      df = DF.from_csv!(csv, skip_rows_after_header: 1)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: ["e"],
               b: ["f"]
             }
    end

    @tag :tmp_dir
    test "skip_rows with skip_rows_after_header", config do
      csv =
        tmp_csv(config.tmp_dir, """
        a,b
        c,d
        e,f
        g,h
        """)

      df = DF.from_csv!(csv, skip_rows: 1, skip_rows_after_header: 1)

      assert DF.to_columns(df, atom_keys: true) == %{
               c: ["g"],
               d: ["h"]
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

      df = DF.from_csv!(csv, dtypes: %{a: {:f, 64}})

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

      df = DF.from_csv!(csv, eol_delimiter: "\r", dtypes: %{a: {:f, 64}})

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

  describe "cloud reads and writes" do
    setup do
      config = %FSS.S3.Config{
        access_key_id: "test",
        secret_access_key: "test",
        endpoint: "http://localhost:4566",
        region: "us-east-1"
      }

      [df: Explorer.Datasets.wine(), s3_config: config]
    end

    @tag :cloud_integration
    test "writes a CSV file to S3", %{df: df, s3_config: config} do
      path = "s3://test-bucket/test-writes/wine-#{System.monotonic_time()}.csv"

      assert :ok = DF.to_csv(df, path, config: config)

      saved_df = DF.from_csv!(path, config: config)
      assert DF.to_columns(saved_df) == DF.to_columns(Explorer.Datasets.wine())
    end

    @tag :cloud_integration
    test "returns an error in case file is not found in S3 bucket", %{s3_config: s3_config} do
      path = "s3://test-bucket/test-writes/file-does-not-exist.csv"

      assert {:error, %ArgumentError{message: "resource not found (404)"}} =
               DF.from_csv(path, config: s3_config)
    end

    @tag :cloud_integration
    test "writes a CSV file to endpoint ignoring bucket name", %{df: df} do
      config = %FSS.S3.Config{
        access_key_id: "test",
        secret_access_key: "test",
        endpoint: "http://localhost:4566/test-bucket",
        bucket: nil,
        region: "us-east-1"
      }

      entry = %FSS.S3.Entry{
        key: "wine-yolo-#{System.monotonic_time()}.csv",
        config: config
      }

      assert :ok = DF.to_csv(df, entry)

      saved_df = DF.from_csv!(entry)
      assert DF.to_columns(saved_df) == DF.to_columns(Explorer.Datasets.wine())
    end
  end

  describe "from_csv/2 - HTTP" do
    setup do
      [bypass: Bypass.open()]
    end

    test "reads a CSV file from an HTTP server", %{bypass: bypass} do
      Bypass.expect(bypass, "GET", "/path/to/file.csv", fn conn ->
        Plug.Conn.resp(conn, 200, @data)
      end)

      url = http_endpoint(bypass) <> "/path/to/file.csv"

      assert {:ok, df} = DF.from_csv(url)

      assert DF.names(df) == ["city", "lat", "lng"]
    end

    test "reads a CSV file from an HTTP server using headers", %{bypass: bypass} do
      Bypass.expect(bypass, "GET", "/path/to/file.csv", fn conn ->
        assert ["Bearer my-token"] = Plug.Conn.get_req_header(conn, "authorization")
        Plug.Conn.resp(conn, 200, @data)
      end)

      url = http_endpoint(bypass) <> "/path/to/file.csv"

      assert {:ok, df} =
               DF.from_csv(url,
                 config: [headers: [{"authorization", "Bearer my-token"}]]
               )

      assert DF.names(df) == ["city", "lat", "lng"]
    end

    test "cannot find a CSV file", %{bypass: bypass} do
      Bypass.expect(bypass, "GET", "/path/to/file.csv", fn conn ->
        Plug.Conn.resp(conn, 404, "not found")
      end)

      url = http_endpoint(bypass) <> "/path/to/file.csv"

      assert {:error, %ArgumentError{message: "resource not found (404)"}} = DF.from_csv(url)
    end

    test "returns an error with invalid config" do
      url = "http://localhost:9899/path/to/file.csv"

      assert {:error, error} = DF.from_csv(url, config: [auth: {:bearer, "token"}])

      assert error ==
               ArgumentError.exception(
                 "the keys [:auth] are not valid keys for the HTTP configuration"
               )
    end
  end

  defp http_endpoint(bypass), do: "http://localhost:#{bypass.port}"

  describe "quote_style option" do
    @tag :tmp_dir
    test "necessary quote_style", %{tmp_dir: tmp_dir} do
      df = DF.new(a: ["a,b", "c", "d,e"], b: [1, 2, 3])
      path = tmp_csv(tmp_dir, "")

      :ok = DF.to_csv!(df, path, quote_style: :necessary)
      contents = File.read!(path)

      assert contents == """
             a,b
             "a,b",1
             c,2
             "d,e",3
             """
    end

    @tag :tmp_dir
    test "always quote_style", %{tmp_dir: tmp_dir} do
      df = DF.new(a: ["a", "b"], b: [1, 2])
      path = tmp_csv(tmp_dir, "")

      :ok = DF.to_csv!(df, path, quote_style: :always)
      contents = File.read!(path)

      assert contents == """
             "a","b"
             "a","1"
             "b","2"
             """
    end

    @tag :tmp_dir
    test "non_numeric quote_style", %{tmp_dir: tmp_dir} do
      df = DF.new(a: ["abc", "def"], b: [1, 2])
      path = tmp_csv(tmp_dir, "")

      :ok = DF.to_csv!(df, path, quote_style: :non_numeric)
      contents = File.read!(path)

      assert contents == """
             "a","b"
             "abc",1
             "def",2
             """
    end

    @tag :tmp_dir
    test "never quote_style", %{tmp_dir: tmp_dir} do
      df = DF.new(a: ["a,b", "c"], b: [1, 2])
      path = tmp_csv(tmp_dir, "")

      :ok = DF.to_csv!(df, path, quote_style: :never)
      contents = File.read!(path)

      assert contents == """
             a,b
             a,b,1
             c,2
             """
    end

    @tag :tmp_dir
    test "invalid quote_style", %{tmp_dir: tmp_dir} do
      df = DF.new(a: ["a"], b: [1])
      path = tmp_csv(tmp_dir, "")

      assert_raise ErlangError,
                   "Erlang error: :invalid_variant",
                   fn ->
                     DF.to_csv!(df, path, quote_style: :invalid)
                   end
    end
  end
end
