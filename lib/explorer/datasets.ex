defmodule Explorer.Datasets do
  alias Explorer.DataFrame

  @datasets_dir Path.join(File.cwd!(), "datasets")

  @doc """
  CO2 emissions from fossil fuels since 2010, by country

  ## Citation

    Boden, T.A., G. Marland, and R.J. Andres. 2013. Global, Regional, and National Fossil-Fuel CO2
    Emissions. Carbon Dioxide Information Analysis Center, Oak Ridge National Laboratory, U.S.
    Department of Energy, Oak Ridge, Tenn., U.S.A. doi 10.3334/CDIAC/00001_V2013
  """
  def fossil_fuels,
    do: @datasets_dir |> Path.join("fossil_fuels.csv") |> DataFrame.read_csv!()
end
