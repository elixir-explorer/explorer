defmodule Explorer.Datasets do
  @moduledoc """
  Datasets used in examples and exploration.

  Note those datasets are not available inside Elixir releases
  (see `mix release`), which is the usual way to deploy Elixir
  in production. Therefore, if you need one of those datasets
  in production, you must download the source files to your
  own application `priv` directory and load them yourself.
  For example:

      Explorer.DataFrame.from_csv!(Application.app_dir(:my_app, "priv/iris.csv"))
  """
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
  Wine Dataset.

  The data is the result of a chemical analysis of wines grown in the same
  region in Italy but derived from three different cultivars. The analysis
  determined the quantities of 13 constituents found in each of the three
  types of wines.

  Downloaded and modified from: https://archive.ics.uci.edu/ml/machine-learning-databases/wine/wine.data

  ## Citation

      Aeberhard,Stefan and Forina,M.. (1991). Wine. UCI Machine Learning Repository. https://doi.org/10.24432/C5PC7J.
  """
  def wine, do: read_dataset!("wine")

  @doc """
  Iris Dataset.

  This classic dataset was collected by Edgar Anderson in 1936
  and made famous by R. A. Fisher's 1936 paper. It consists of
  several measurements of three species of Iris (Iris setosa,
  Iris virginica and Iris versicolor).

  Downloaded and modified from: https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data

  ## Citation

      Fisher,R. A.. (1988). Iris. UCI Machine Learning Repository. https://doi.org/10.24432/C56C76.
  """
  def iris, do: read_dataset!("iris")

  defp read_dataset!(name) do
    key = {:explorer_datasets, name}

    # Persistent term is used as a cache, in order to avoid
    # several calls to the filesystem. This is mostly useful
    # to speed up reads in tests.
    case :persistent_term.get(key, nil) do
      nil ->
        @datasets_dir
        |> Path.join("#{name}.csv")
        |> DataFrame.from_csv!()
        |> tap(&:persistent_term.put(key, &1))

      %DataFrame{} = df ->
        df
    end
  end
end
