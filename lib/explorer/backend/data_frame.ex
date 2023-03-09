defmodule Explorer.Backend.DataFrame do
  @moduledoc """
  The behaviour for DataFrame backends.
  """

  @type t :: struct()

  @type df :: Explorer.DataFrame.t()

  @type option(type) :: type | nil
  @type ok_result :: :ok | {:error, term()}
  @type result(t) :: {:ok, t} | {:error, term()}

  @type series :: Explorer.Series.t()
  @type column_name :: String.t()
  @type dtype :: Explorer.Series.dtype()
  @type dtypes :: %{column_name() => dtype()}

  @type basic_types :: float() | integer() | String.t() | Date.t() | DateTime.t()
  @type mutate_value ::
          series()
          | basic_types()
          | [basic_types()]
          | (df() -> series() | basic_types() | [basic_types()])

  @type lazy_frame :: Explorer.Backend.LazyFrame.t()
  @type lazy_series :: Explorer.Backend.LazySeries.t()

  @type compression :: {algorithm :: option(atom()), level :: option(integer())}
  @type columns_for_io :: list(column_name()) | list(pos_integer()) | nil

  # IO: CSV
  @callback from_csv(
              filename :: String.t(),
              dtypes,
              delimiter :: String.t(),
              null_character :: String.t(),
              skip_rows :: integer(),
              header? :: boolean(),
              encoding :: String.t(),
              max_rows :: option(integer()),
              columns :: columns_for_io(),
              infer_schema_length :: option(integer()),
              parse_dates :: boolean()
            ) :: result(df)
  @callback to_csv(df, filename :: String.t(), header? :: boolean(), delimiter :: String.t()) ::
              ok_result()
  @callback dump_csv(df, header? :: boolean(), delimiter :: String.t()) :: result(binary())

  @callback load_csv(
              contents :: String.t(),
              dtypes,
              delimiter :: String.t(),
              null_character :: String.t(),
              skip_rows :: integer(),
              header? :: boolean(),
              encoding :: String.t(),
              max_rows :: option(integer()),
              columns :: columns_for_io(),
              infer_schema_length :: option(integer()),
              parse_dates :: boolean()
            ) :: result(df)

  # IO: Parquet
  @callback from_parquet(
              filename :: String.t(),
              max_rows :: option(integer()),
              columns :: columns_for_io()
            ) :: result(df)
  @callback to_parquet(
              df,
              filename :: String.t(),
              compression()
            ) ::
              ok_result()
  @callback dump_parquet(df, compression()) :: result(binary())
  @callback load_parquet(contents :: binary()) :: result(df)

  # IO: IPC
  @callback from_ipc(
              filename :: String.t(),
              columns :: columns_for_io()
            ) :: result(df)
  @callback to_ipc(df, filename :: String.t(), compression()) ::
              ok_result()
  @callback dump_ipc(df, compression()) :: result(binary())
  @callback load_ipc(
              contents :: binary(),
              columns :: columns_for_io()
            ) :: result(df)

  # IO: IPC Stream
  @callback from_ipc_stream(
              filename :: String.t(),
              columns :: columns_for_io()
            ) :: result(df)
  @callback to_ipc_stream(
              df,
              filename :: String.t(),
              compression()
            ) ::
              ok_result()
  @callback dump_ipc_stream(df, compression()) :: result(binary())
  @callback load_ipc_stream(
              contents :: binary(),
              columns :: columns_for_io()
            ) :: result(df)

  # IO: IPC NDJSON
  @callback from_ndjson(
              filename :: String.t(),
              infer_schema_length :: integer(),
              batch_size :: integer()
            ) :: result(df)
  @callback to_ndjson(df, filename :: String.t()) :: ok_result()

  @callback dump_ndjson(df) :: result(binary())

  @callback load_ndjson(
              contents :: String.t(),
              infer_schema_length :: integer(),
              batch_size :: integer()
            ) :: result(df)

  # Conversion

  @callback lazy() :: module()
  @callback to_lazy(df) :: df
  @callback collect(df) :: df
  @callback from_tabular(Table.Reader.t(), dtypes) :: df
  @callback from_series([{binary(), Series.t()}]) :: df
  @callback to_rows(df, atom_keys? :: boolean()) :: [map()]

  # Introspection

  @callback n_rows(df) :: integer()
  @callback inspect(df, opts :: Inspect.Opts.t()) :: Inspect.Algebra.t()

  # Single table verbs

  @callback head(df, rows :: integer()) :: df
  @callback tail(df, rows :: integer()) :: df
  @callback select(df, out_df :: df()) :: df
  @callback mask(df, mask :: series) :: df
  @callback filter_with(df, out_df :: df(), lazy_series()) :: df
  @callback mutate_with(df, out_df :: df(), mutations :: [{column_name(), lazy_series()}]) :: df
  @callback arrange_with(df, out_df :: df(), directions :: [{:asc | :desc, lazy_series()}]) :: df
  @callback distinct(df, out_df :: df(), columns :: [column_name()]) :: df
  @callback rename(df, out_df :: df(), [{old :: column_name(), new :: column_name()}]) :: df
  @callback dummies(df, out_df :: df(), columns :: [column_name()]) :: df
  @callback sample(
              df,
              n_or_frac :: number(),
              replace :: boolean(),
              shuffle :: boolean(),
              seed :: option(integer())
            ) :: df
  @callback pull(df, column :: column_name()) :: series
  @callback slice(df, indices :: list(integer()) | series() | %Range{}) :: df
  @callback slice(df, offset :: integer(), length :: integer()) :: df
  @callback drop_nil(df, columns :: [column_name()]) :: df
  @callback pivot_wider(
              df,
              id_columns :: [column_name()],
              names_from :: column_name(),
              values_from :: [column_name()],
              names_prefix :: String.t()
            ) :: df
  @callback pivot_longer(
              df,
              out_df :: df(),
              columns_to_pivot :: [column_name()],
              columns_to_keep :: [column_name()],
              names_to :: column_name(),
              values_to :: column_name()
            ) :: df
  @callback put(df, out_df :: df(), column_name(), series()) :: df
  @callback describe(df, out_df :: df(), percentiles :: option(list(float()))) :: df()

  # Two or more table verbs

  @callback join(
              left :: df(),
              right :: df(),
              out_df :: df(),
              on :: list({column_name(), column_name()}),
              how :: :left | :inner | :outer | :right | :cross
            ) :: df

  @callback concat_columns([df], out_df :: df()) :: df
  @callback concat_rows([df], out_df :: df()) :: df

  # Groups

  @callback summarise_with(df, out_df :: df(), aggregations :: [{column_name(), lazy_series()}]) ::
              df

  # Functions
  alias Explorer.{DataFrame, Series}

  @doc """
  Creates a new DataFrame for a given backend.
  """
  def new(data, names, dtypes) when is_list(dtypes) do
    dtypes = Map.new(Enum.zip(names, dtypes))

    new(data, names, dtypes)
  end

  def new(data, names, dtypes) when is_list(names) and is_map(dtypes) do
    %DataFrame{data: data, names: names, dtypes: dtypes, groups: []}
  end

  @default_limit 5
  import Inspect.Algebra

  @doc """
  Default inspect implementation for backends.
  """
  def inspect(df, backend, n_rows, inspect_opts, opts \\ [])
      when is_binary(backend) and (is_integer(n_rows) or is_nil(n_rows)) and is_list(opts) do
    inspect_opts = %{inspect_opts | limit: @default_limit}
    open = color("[", :list, inspect_opts)
    close = color("]", :list, inspect_opts)

    cols_algebra =
      for name <- DataFrame.names(df) do
        series = df[name]

        values =
          series
          |> Series.slice(0, inspect_opts.limit + 1)
          |> Series.to_list()

        data = container_doc(open, values, close, inspect_opts, &Explorer.Shared.to_string/2)

        concat([
          line(),
          color("#{name} ", :map, inspect_opts),
          color("#{Series.dtype(series)} ", :atom, inspect_opts),
          data
        ])
      end

    concat([
      color(backend, :atom, inspect_opts),
      open,
      "#{n_rows || "???"} x #{length(cols_algebra)}",
      close,
      groups_algebra(df.groups, inspect_opts) | cols_algebra
    ])
  end

  defp groups_algebra([_ | _] = groups, opts),
    do:
      Inspect.Algebra.concat([
        Inspect.Algebra.line(),
        Inspect.Algebra.color("Groups: ", :atom, opts),
        Inspect.Algebra.to_doc(groups, opts)
      ])

  defp groups_algebra([], _), do: ""
end
