defmodule Explorer.DataFrame.CSVTest do
  use ExUnit.Case, async: true

  @tag :focus
  test "hello" do
    # test data taken from arrow2
    # https://github.com/jorgecarleitao/arrow2/tree/main/src/io/csv/read
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

    filename = System.tmp_dir!() |> Path.join("input.csv")
    File.write!(filename, data)
    frame = Explorer.DataFrame.from_csv!(filename)
  end
end
