defmodule Explorer.MixProject do
  use Mix.Project

  @source_url "https://github.com/elixir-nx/explorer"
  @version "0.11.2-dev"
  @dev? String.ends_with?(@version, "-dev")
  @force_build? System.get_env("EXPLORER_BUILD") in ["1", "true"]

  def project do
    [
      app: :explorer,
      name: "Explorer",
      description:
        "Series (one-dimensional) and dataframes (two-dimensional) for fast data exploration in Elixir",
      version: @version,
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      deps: deps(),
      docs: docs(),
      preferred_cli_env: [ci: :test, "localstack.setup": :test],
      aliases: [
        "rust.lint": ["cmd cargo clippy --manifest-path=native/explorer/Cargo.toml -- -Dwarnings"],
        "rust.fmt": ["cmd cargo fmt --manifest-path=native/explorer/Cargo.toml --all"],
        "localstack.setup": ["cmd ./test/support/setup-localstack.sh"],
        ci: ["format", "rust.fmt", "rust.lint", "test"]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :inets, :ssl],
      mod: {Explorer.Application, []},
      env: [default_backend: Explorer.PolarsBackend]
    ]
  end

  defp elixirc_paths(:test), do: ~w(lib test/support)
  defp elixirc_paths(_), do: ~w(lib)

  defp deps do
    [
      {:aws_signature, "~> 0.3"},
      {:fss, "~> 0.1"},
      {:rustler_precompiled, "~> 0.7"},
      {:table, "~> 0.1.2"},
      {:table_rex, "~> 4.1"},
      {:decimal, "~> 2.1"},

      ## Optional
      {:flame, "~> 0.3", optional: true},
      {:adbc, "~> 0.1", optional: true},
      {:nx, "~> 0.4", optional: true},
      {:rustler, "~> 0.37", optional: not (@dev? or @force_build?)},

      ## Test
      {:bypass, "~> 2.1", only: :test},
      {:stream_data, "~> 1.1", only: [:dev, :test]},
      {:tz, "~> 0.26", only: :test},

      ## Dev
      {:ex_doc, "~> 0.24", only: :dev},
      {:benchee, "~> 1.1", only: :dev}
    ]
  end

  defp docs do
    [
      main: "Explorer",
      logo: "explorer-exdoc.png",
      source_ref: "v#{@version}",
      source_url: @source_url,
      groups_for_modules: [
        # Explorer,
        # Explorer.DataFrame,
        # Explorer.Datasets,
        # Explorer.Query,
        # Explorer.Series,
        "Custom data types": [
          Explorer.Duration,
          Explorer.TensorFrame
        ],
        Backends: [
          Explorer.Backend,
          Explorer.Backend.DataFrame,
          Explorer.Backend.Series,
          Explorer.Backend.QueryFrame,
          Explorer.Backend.LazySeries,
          Explorer.PolarsBackend
        ]
      ],
      groups_for_docs: [
        "Functions: Conversion": &(&1[:type] == :conversion),
        "Functions: Single-table": &(&1[:type] == :single),
        "Functions: Multi-table": &(&1[:type] == :multi),
        "Functions: Row-based": &(&1[:type] == :rows),
        "Functions: Aggregation": &(&1[:type] == :aggregation),
        "Functions: Element-wise": &(&1[:type] == :element_wise),
        "Functions: Datetime ops": &(&1[:type] == :datetime_wise),
        "Functions: Float ops": &(&1[:type] == :float_wise),
        "Functions: String ops": &(&1[:type] == :string_wise),
        "Functions: List ops": &(&1[:type] == :list_wise),
        "Functions: Struct ops": &(&1[:type] == :struct_wise),
        "Functions: Introspection": &(&1[:type] == :introspection),
        "Functions: IO": &(&1[:type] == :io),
        "Functions: Shape": &(&1[:type] == :shape),
        "Functions: Window": &(&1[:type] == :window)
      ],
      extras: ["notebooks/exploring_explorer.livemd", "CHANGELOG.md"],
      skip_undefined_reference_warnings_on: ["CHANGELOG.md"]
    ]
  end

  defp package do
    [
      files: [
        "lib",
        "native",
        "datasets",
        "checksum-*.exs",
        "mix.exs",
        "CHANGELOG.md",
        "README.md",
        "LICENSE"
      ],
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/v#{@version}/CHANGELOG.md"
      },
      maintainers: ["Christopher Grainger", "Philip Sampaio", "José Valim"]
    ]
  end
end
