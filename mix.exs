defmodule Explorer.MixProject do
  use Mix.Project

  @source_url "https://github.com/elixir-nx/explorer"
  @version "0.1.0-dev"

  def project do
    [
      app: :explorer,
      name: "Explorer",
      version: @version,
      elixir: "~> 1.12",
      deps: deps(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      env: [default_backend: Explorer.PolarsBackend]
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.24", only: :dev, runtime: false},
      {:nx, "~> 0.1.0"},
      {:rustler, "~> 0.23.0"}
    ]
  end

  defp docs do
    [
      main: "Explorer",
      source_ref: "v#{@version}",
      source_url: @source_url,
      groups_for_modules: [
        # Explorer,
        # Explorer.DataFrame,
        # Explorer.Datasets,
        # Explorer.Series,
        Backends: [
          Explorer.Backend,
          Explorer.Backend.DataFrame,
          Explorer.Backend.Series,
          Explorer.PolarsBackend
        ]
      ]
    ]
  end
end
