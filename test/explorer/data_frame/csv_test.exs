defmodule Explorer.DataFrame.CSVTest do
  # Integration tests, based on:
  # https://github.com/jorgecarleitao/arrow2/tree/main/src/io/csv/read

  use ExUnit.Case, async: true
  alias Explorer.DataFrame, as: DF
  import Explorer.IOHelpers

  test "read" do
    data = """
    city,lat,lng
    "Elgin, Scotland, the UK",57.653484,-3.335724
    "Stoke-on-Trent, Staffordshire, the UK",53.002666,-2.179404
    "Solihull, Birmingham, UK",52.412811,-1.778197
    "Cardiff, Cardiff county, UK",51.481583,-3.179090
    "Eastbourne, East Sussex, UK",50.768036,0.290472
    "Oxford, Oxfordshire, UK",51.752022,-1.257677
    "London, UK",51.509865,-0.118092
    "Swindon, Swindon, UK",51.568535,-1.772232
    "Gravesend, Kent, UK",51.441883,0.370759
    "Northampton, Northamptonshire, UK",52.240479,-0.902656
    "Rugby, Warwickshire, UK",52.370876,-1.265032
    "Sutton Coldfield, West Midlands, UK",52.570385,-1.824042
    "Harlow, Essex, UK",51.772938,0.102310
    "Aberdeen, Aberdeen City, UK",57.149651,-2.099075
    """

    frame = DF.from_csv!(tmp_file!(data))

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
    assert DF.dump_csv(frame) == data
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
      assert_csv(:string, "some string", "some string")
      assert_csv(:string, "éphémère", "éphémère")
    end

    test "date" do
      assert_csv(:date, "2022-12-01", ~D[2022-12-01])
      assert_csv(:date, "1960-01-31", ~D[1960-01-31])
    end

    test "datetime" do
      assert_csv(:datetime, "2022-10-01T11:34:10.123456", ~N[2022-10-01 11:34:10.123456])
    end
  end
end
