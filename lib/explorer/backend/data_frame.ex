defmodule Explorer.Backend.DataFrame do
  @moduledoc """
  The behaviour for DataFrame backends.
  """

  @type t :: struct()

  @type df :: Explorer.DataFrame.t()

  @type option(type) :: type | nil

  # Types for IO operations
  @type ok_result() :: :ok | {:error, Exception.t()}
  @type io_result(t) :: {:ok, t} | {:error, Exception.t()}

  @type quote_style :: :necessary | :always | :non_numeric | :never
  # Generic result
  @type result(t) :: {:ok, t} | {:error, term()}

  @type series :: Explorer.Series.t()
  @type column_name :: String.t()
  @type dtype :: Explorer.Series.dtype()
  @type dtypes :: %{column_name() => dtype()}
  @type io_dtypes :: [{column_name(), dtype()}]

  @type basic_types :: float() | integer() | String.t() | Date.t() | DateTime.t()
  @type mutate_value ::
          series()
          | basic_types()
          | [basic_types()]
          | (df() -> series() | basic_types() | [basic_types()])

  @type query_frame :: Explorer.Backend.QueryFrame.t()
  @type lazy_series :: Explorer.Backend.LazySeries.t()

  @type compression :: {algorithm :: option(atom()), level :: option(integer())}
  @type columns_for_io :: list(column_name()) | list(pos_integer()) | nil

  @type fs_entry :: Explorer.DataFrame.fs_entry()

  # IO: query
  @callback from_query(
              Adbc.Connection.t(),
              query :: String.t(),
              params :: list(term)
            ) :: result(df)

  # IO: CSV
  @callback from_csv(
              entry :: fs_entry(),
              io_dtypes,
              delimiter :: String.t(),
              nil_values :: list(String.t()),
              skip_rows :: integer(),
              skip_rows_after_header :: integer(),
              header? :: boolean(),
              encoding :: String.t(),
              max_rows :: option(integer()),
              columns :: columns_for_io(),
              infer_schema_length :: option(integer()),
              parse_dates :: boolean(),
              eol_delimiter :: option(String.t())
            ) :: io_result(df)
  @callback to_csv(
              df,
              entry :: fs_entry(),
              header? :: boolean(),
              delimiter :: String.t(),
              quote_style :: quote_style,
              streaming :: boolean()
            ) ::
              ok_result()
  @callback dump_csv(
              df,
              header? :: boolean(),
              delimiter :: String.t(),
              quote_style :: quote_style
            ) :: io_result(binary())

  @callback load_csv(
              contents :: String.t(),
              io_dtypes,
              delimiter :: String.t(),
              nil_values :: list(String.t()),
              skip_rows :: integer(),
              skip_rows_after_header :: integer(),
              header? :: boolean(),
              encoding :: String.t(),
              max_rows :: option(integer()),
              columns :: columns_for_io(),
              infer_schema_length :: option(integer()),
              parse_dates :: boolean(),
              eol_delimiter :: option(String.t())
            ) :: io_result(df)

  # IO: Parquet
  @callback from_parquet(
              entry :: fs_entry(),
              max_rows :: option(integer()),
              columns :: columns_for_io(),
              rechunk :: boolean()
            ) :: io_result(df)
  @callback to_parquet(
              df,
              entry :: fs_entry(),
              compression(),
              streaming :: boolean()
            ) ::
              ok_result()
  @callback dump_parquet(df, compression()) :: io_result(binary())
  @callback load_parquet(contents :: binary()) :: io_result(df)

  # IO: IPC
  @callback from_ipc(
              entry :: fs_entry(),
              columns :: columns_for_io()
            ) :: io_result(df)
  @callback to_ipc(df, entry :: fs_entry(), compression(), streaming :: boolean()) ::
              ok_result()
  @callback dump_ipc(df, compression()) :: io_result(binary())
  @callback load_ipc(
              contents :: binary(),
              columns :: columns_for_io()
            ) :: io_result(df)

  # IO: IPC Stream
  @callback from_ipc_stream(
              filename :: fs_entry(),
              columns :: columns_for_io()
            ) :: io_result(df)
  @callback to_ipc_stream(
              df,
              entry :: fs_entry(),
              compression()
            ) ::
              ok_result()
  @callback dump_ipc_stream(df, compression()) :: io_result(binary())
  @callback load_ipc_stream(
              contents :: binary(),
              columns :: columns_for_io()
            ) :: io_result(df)

  # IO: IPC NDJSON
  @callback from_ndjson(
              filename :: fs_entry(),
              infer_schema_length :: integer(),
              batch_size :: integer()
            ) :: io_result(df)
  @callback to_ndjson(df, entry :: fs_entry()) :: ok_result()

  @callback dump_ndjson(df) :: io_result(binary())

  @callback load_ndjson(
              contents :: String.t(),
              infer_schema_length :: integer(),
              batch_size :: integer()
            ) :: io_result(df)

  # Conversion

  @callback lazy() :: module()
  @callback lazy(df) :: df
  @callback compute(df) :: df
  @callback from_tabular(Table.Reader.t(), io_dtypes) :: df
  @callback from_series([{binary(), Series.t()}]) :: df
  @callback to_rows(df, atom_keys? :: boolean()) :: [map()]
  @callback to_rows_stream(df, atom_keys? :: boolean(), chunk_size :: integer()) :: Enumerable.t()

  # Ownership

  @callback owner_reference(df) :: reference() | nil
  @callback owner_import(term()) :: io_result(df)
  @callback owner_export(df) :: io_result(term())

  # Introspection

  @callback n_rows(df) :: integer()
  @callback estimated_size(df) :: integer()
  @callback inspect(df, opts :: Inspect.Opts.t()) :: Inspect.Algebra.t()
  @callback re_dtype(String.t()) :: dtype()

  # Single table verbs

  @callback head(df, rows :: integer()) :: df
  @callback tail(df, rows :: integer()) :: df
  @callback select(df, out_df :: df()) :: df
  @callback mask(df, mask :: series) :: df
  @callback filter_with(df, out_df :: df(), lazy_series()) :: df
  @callback mutate_with(df, out_df :: df(), mutations :: [{column_name(), lazy_series()}]) :: df
  @callback sort_with(
              df,
              out_df :: df(),
              directions :: [{:asc | :desc, lazy_series()}],
              maintain_order? :: boolean(),
              multithreaded? :: boolean(),
              nulls_last? :: boolean()
            ) :: df
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
  @callback transpose(
              df,
              out_df :: df(),
              keep_names_as :: column_name(),
              new_column_names :: [column_name()]
            ) :: df
  @callback put(df, out_df :: df(), column_name(), series()) :: df
  @callback nil_count(df) :: df()
  @callback explode(df, out_df :: df(), columns :: [column_name()]) :: df()
  @callback unnest(df, out_df :: df(), columns :: [column_name()]) :: df()
  @callback correlation(df, out_df :: df(), method :: atom()) :: df()
  @callback covariance(df, out_df :: df(), ddof :: integer()) :: df()

  # Two or more table verbs

  @callback join(
              [df()],
              out_df :: df(),
              on :: list({column_name(), column_name()}),
              how :: :left | :inner | :outer | :right | :cross
            ) :: df

  @callback concat_columns([df], out_df :: df()) :: df
  @callback concat_rows([df], out_df :: df()) :: df

  # Groups

  @callback summarise_with(df, out_df :: df(), aggregations :: [{column_name(), lazy_series()}]) ::
              df

  # SQL

  @callback sql(df, sql_string :: binary(), table_name :: binary()) :: df()

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
    %DataFrame{data: data, names: names, dtypes: dtypes, groups: [], stable_groups?: true}
  end

  @default_limit 5
  alias Inspect.Algebra, as: A

  @doc """
  Default inspect implementation for backends.
  """
  def inspect(df, backend, n_rows, inspect_opts, opts \\ [])
      when is_binary(backend) and (is_integer(n_rows) or is_nil(n_rows)) and
             is_list(opts) do
    inspect_opts = %{inspect_opts | limit: @default_limit}
    elide_columns? = Keyword.get(opts, :elide_columns, false)
    open = A.color("[", :list, inspect_opts)
    close = A.color("]", :list, inspect_opts)

    cols_algebra = build_cols_algebra(df, inspect_opts, elide_columns?)
    df_info = "#{n_rows || "???"} x #{length(cols_algebra)}"

    A.concat([
      A.color(backend, :atom, inspect_opts),
      open,
      df_info,
      close,
      groups_algebra(df.groups, inspect_opts) | cols_algebra
    ])
  end

  defp build_cols_algebra(df, inspect_opts, true) do
    for name <- DataFrame.names(df) do
      type =
        df
        |> DataFrame.dtypes()
        |> Map.get(name)
        |> Explorer.Shared.dtype_to_string()

      A.concat([
        A.line(),
        A.color("#{name} ", :map, inspect_opts),
        A.color("#{type} ", :atom, inspect_opts),
        "???"
      ])
    end
  end

  defp build_cols_algebra(df, inspect_opts, false) do
    for name <- DataFrame.names(df) do
      series = df[name]

      series =
        case inspect_opts.limit do
          :infinity -> series
          limit when is_integer(limit) -> Series.slice(series, 0, limit + 1)
        end

      data =
        series
        |> Series.to_list()
        |> Explorer.Shared.to_doc(inspect_opts)

      type =
        series
        |> Series.dtype()
        |> Explorer.Shared.dtype_to_string()

      A.concat([
        A.line(),
        A.color("#{name} ", :map, inspect_opts),
        A.color("#{type} ", :atom, inspect_opts),
        data
      ])
    end
  end

  defp groups_algebra([_ | _] = groups, opts),
    do:
      A.concat([
        A.line(),
        A.color("Groups: ", :atom, opts),
        A.to_doc(groups, opts)
      ])

  defp groups_algebra([], _), do: ""
end
