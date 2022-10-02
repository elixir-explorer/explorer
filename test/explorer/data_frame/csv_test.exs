defmodule Explorer.DataFrame.CSVTest do
  @moduledoc """
  Integration tests, based on:
  https://github.com/jorgecarleitao/arrow2/tree/main/src/io/csv/read
  """
  use ExUnit.Case, async: true
  alias Explorer.DataFrame, as: DF

  # https://doc.rust-lang.org/std/primitive.f64.html#associatedconstant.EPSILON
  @f64_epsilon 2.2204460492503131e-16

  def tmp_file!(data) do
    filename = System.tmp_dir!() |> Path.join("data.csv")
    File.write!(filename, data)
    filename
  end

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

    assert_in_delta(57.653484, frame["lat"][0], @f64_epsilon)

    city = frame["city"]

    assert city[0] == "Elgin, Scotland, the UK"
    assert city[13] == "Aberdeen, Aberdeen City, UK"
  end

  describe "dtypes" do
    [
      {:integer, "100", 100},
      {:float, "2.3", 2.3},
      {:boolean, "true", true},
      {:string, "some string", "some string"},
      {:date, "2022-12-01", ~D[2022-12-01]},
      {:datetime, "2022-10-01T11:34:10.123456", ~N[2022-10-01 11:34:10.123456]}
      # :list (to be implemented)
    ]
    |> Enum.each(fn {type, csv_value, parsed_value} ->
      test "supports explicit #{type} parsing" do
        data = "column\n#{unquote(csv_value)}"
        frame = DF.from_csv!(tmp_file!(data), dtypes: [{"column", unquote(type)}])
        assert frame[0][0] == unquote(Macro.escape(parsed_value))
        assert frame[0].dtype == unquote(type)
      end

      test "supports inferred #{type} parsing" do
        data = "column\n#{unquote(csv_value)}"
        frame = DF.from_csv!(tmp_file!(data), parse_dates: true)
        assert frame[0][0] == unquote(Macro.escape(parsed_value))
        assert frame[0].dtype == unquote(type)
      end
    end)
  end
end
