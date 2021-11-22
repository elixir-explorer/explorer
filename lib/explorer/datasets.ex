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
  def fossil_fuels, do: read_dataset!("fossil_fuels")

  @doc """
  Wine Dataset - The data is the result of a chemical analysis of wines grown in the same
  region in Italy but derived from three different cultivars. The analysis determined the quantities
  of 13 constituents found in each of the three types of wines.

  Downloaded and modified from: https://archive.ics.uci.edu/ml/machine-learning-databases/wine/wine.data

  ## Citation

    Original Owners:
    Forina, M. et al, PARVUS -
    An Extendible Package for Data Exploration, Classification and Correlation.
    Institute of Pharmaceutical and Food Analysis and Technologies, Via Brigata Salerno,
    16147 Genoa, Italy.

    Wine. (1991). UCI Machine Learning Repository.
  """
  def wine, do: read_dataset!("wine")

  defp read_dataset!(name) do
    @datasets_dir
    |> Path.join("#{name}.csv")
    |> DataFrame.read_csv!()
  end
end
