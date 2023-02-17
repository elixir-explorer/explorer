defmodule Explorer.PolarsBackend.DataFrame do
  @moduledoc false

  alias Explorer.DataFrame, as: DataFrame
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries
  alias Explorer.PolarsBackend.Shared
  alias Explorer.Series, as: Series

  import Explorer.PolarsBackend.Expression, only: [to_expr: 1, alias_expr: 2]

  defstruct resource: nil

  @type t :: %__MODULE__{resource: reference()}

  @behaviour Explorer.Backend.DataFrame
  @default_infer_schema_length 1000

  # IO

  @impl true
  def from_csv(
        filename,
        dtypes,
        <<delimiter::utf8>>,
        null_character,
        skip_rows,
        header?,
        encoding,
        max_rows,
        columns,
        infer_schema_length,
        parse_dates
      ) do
    infer_schema_length =
      if infer_schema_length == nil,
        do: max_rows || @default_infer_schema_length,
        else: infer_schema_length

    dtypes =
      Enum.map(dtypes, fn {column_name, dtype} ->
        {column_name, Shared.internal_from_dtype(dtype)}
      end)

    {columns, with_projection} = column_names_or_projection(columns)

    df =
      Native.df_from_csv(
        filename,
        infer_schema_length,
        header?,
        max_rows,
        skip_rows,
        with_projection,
        delimiter,
        true,
        columns,
        dtypes,
        encoding,
        null_character,
        parse_dates
      )

    case df do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  # In case column names are given, it returns the columns.
  # Otherwise returns the list of integers, selecting the "projection" of columns.
  defp column_names_or_projection(nil), do: {nil, nil}

  defp column_names_or_projection(list) do
    cond do
      Enum.all?(list, &is_binary/1) ->
        {list, nil}

      Enum.all?(list, &is_integer/1) ->
        {nil, list}

      true ->
        {nil, nil}
    end
  end

  @impl true
  def to_csv(%DataFrame{data: df}, filename, header?, delimiter) do
    <<delimiter::utf8>> = delimiter

    case Native.df_to_csv(df, filename, header?, delimiter) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def dump_csv(%DataFrame{} = df, header?, <<delimiter::utf8>>) do
    Native.df_dump_csv(df.data, header?, delimiter)
  end

  @impl true
  def load_csv(
        contents,
        dtypes,
        <<delimiter::utf8>>,
        null_character,
        skip_rows,
        header?,
        encoding,
        max_rows,
        columns,
        infer_schema_length,
        parse_dates
      ) do
    infer_schema_length =
      if infer_schema_length == nil,
        do: max_rows || @default_infer_schema_length,
        else: infer_schema_length

    dtypes =
      Enum.map(dtypes, fn {column_name, dtype} ->
        {column_name, Shared.internal_from_dtype(dtype)}
      end)

    {columns, with_projection} = column_names_or_projection(columns)

    df =
      Native.df_load_csv(
        contents,
        infer_schema_length,
        header?,
        max_rows,
        skip_rows,
        with_projection,
        delimiter,
        true,
        columns,
        dtypes,
        encoding,
        null_character,
        parse_dates
      )

    case df do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def from_ndjson(filename, infer_schema_length, batch_size) do
    with {:ok, df} <- Native.df_from_ndjson(filename, infer_schema_length, batch_size) do
      {:ok, Shared.create_dataframe(df)}
    end
  end

  @impl true
  def to_ndjson(%DataFrame{data: df}, filename) do
    with {:ok, _} <- Native.df_to_ndjson(df, filename) do
      :ok
    end
  end

  @impl true
  def dump_ndjson(%DataFrame{} = df) do
    Native.df_dump_ndjson(df.data)
  end

  @impl true
  def load_ndjson(contents, infer_schema_length, batch_size) when is_binary(contents) do
    case Native.df_load_ndjson(contents, infer_schema_length, batch_size) do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def from_parquet(filename, max_rows, columns) do
    case Native.df_from_parquet(filename, max_rows, columns) do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def to_parquet(%DataFrame{data: df}, filename, {compression, compression_level}) do
    case Native.df_to_parquet(df, filename, parquet_compression(compression, compression_level)) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def dump_parquet(%DataFrame{data: df}, {compression, compression_level}) do
    Native.df_dump_parquet(df, parquet_compression(compression, compression_level))
  end

  defp parquet_compression(nil, _), do: :uncompressed

  defp parquet_compression(algorithm, level) when algorithm in ~w(gzip brotli zstd)a do
    {algorithm, level}
  end

  defp parquet_compression(algorithm, _) when algorithm in ~w(snappy lz4raw)a, do: algorithm

  @impl true
  def load_parquet(contents) when is_binary(contents) do
    case Native.df_load_parquet(contents) do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def from_ipc(filename, columns) do
    {columns, projection} = column_names_or_projection(columns)

    case Native.df_from_ipc(filename, columns, projection) do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def to_ipc(%DataFrame{data: df}, filename, {compression, _level}) do
    case Native.df_to_ipc(df, filename, Atom.to_string(compression)) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def dump_ipc(%DataFrame{data: df}, {compression, _level}) do
    Native.df_dump_ipc(df, Atom.to_string(compression))
  end

  @impl true
  def load_ipc(contents, columns) when is_binary(contents) do
    {columns, projection} = column_names_or_projection(columns)

    case Native.df_load_ipc(contents, columns, projection) do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def from_ipc_stream(filename, columns) do
    {columns, projection} = column_names_or_projection(columns)

    case Native.df_from_ipc_stream(filename, columns, projection) do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def to_ipc_stream(%DataFrame{data: df}, filename, {compression, _level}) do
    case Native.df_to_ipc_stream(df, filename, Atom.to_string(compression)) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def dump_ipc_stream(%DataFrame{data: df}, {compression, _level}) do
    Native.df_dump_ipc_stream(df, Atom.to_string(compression))
  end

  @impl true
  def load_ipc_stream(contents, columns) when is_binary(contents) do
    {columns, projection} = column_names_or_projection(columns)

    case Native.df_load_ipc_stream(contents, columns, projection) do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  # Conversion

  @impl true
  def lazy, do: Explorer.PolarsBackend.LazyFrame

  @impl true
  def to_lazy(df), do: Shared.apply_dataframe(df, df, :df_to_lazy, [])

  @impl true
  def collect(df), do: df

  @impl true
  def from_tabular(tabular, dtypes) do
    {_, %{columns: keys}, _} = reader = init_reader!(tabular)
    columns = Table.to_columns(reader)

    keys
    |> Enum.map(fn key ->
      column_name = to_column_name!(key)
      values = Enum.to_list(columns[key])
      dtype = Map.get(dtypes, column_name)
      series_from_list!(column_name, values, dtype)
    end)
    |> from_series_list()
  end

  defp init_reader!(tabular) do
    with :none <- Table.Reader.init(tabular) do
      raise ArgumentError, "expected valid tabular data, but got: #{inspect(tabular)}"
    end
  end

  @impl true
  def from_series(pairs) when is_list(pairs) do
    pairs
    |> Enum.map(fn {name, series} ->
      PolarsSeries.rename(series, name)
    end)
    |> from_series_list()
  end

  defp from_series_list(list) do
    list = Enum.map(list, & &1.data)

    Shared.apply(:df_from_series, [list])
    |> Shared.create_dataframe()
  end

  defp to_column_name!(column_name) when is_binary(column_name), do: column_name
  defp to_column_name!(column_name) when is_atom(column_name), do: Atom.to_string(column_name)

  defp to_column_name!(column_name) do
    raise ArgumentError,
          "expected column name to be either string or atom, got: #{inspect(column_name)}"
  end

  # Like `Explorer.Series.from_list/2`, but gives a better error message with the series name.
  defp series_from_list!(name, list, dtype) do
    type = Explorer.Shared.check_types!(list, dtype)
    {list, type} = Explorer.Shared.cast_numerics(list, type)
    series = Shared.from_list(list, type, name)
    Explorer.Backend.Series.new(series, type)
  rescue
    e ->
      raise ArgumentError, "cannot create series #{inspect(name)}: " <> Exception.message(e)
  end

  @impl true
  def to_rows(%DataFrame{data: polars_df, names: names} = df, atom_keys?) do
    keys = if atom_keys?, do: Enum.map(names, &String.to_atom/1), else: df.names

    names
    |> Enum.map(fn name ->
      {:ok, series} = Native.df_pull(polars_df, name)
      {:ok, list} = Native.s_to_list(series)
      list
    end)
    |> Enum.zip_with(fn row -> keys |> Enum.zip(row) |> Map.new() end)
  end

  # Introspection

  @impl true
  def n_rows(df), do: Shared.apply_dataframe(df, :df_n_rows)

  # Single table verbs

  @impl true
  def head(%DataFrame{} = df, rows),
    do: Shared.apply_dataframe(df, df, :df_head, [rows, df.groups])

  @impl true
  def tail(%DataFrame{} = df, rows),
    do: Shared.apply_dataframe(df, df, :df_tail, [rows, df.groups])

  @impl true
  def select(df, out_df),
    do: Shared.apply_dataframe(df, out_df, :df_select, [out_df.names])

  @impl true
  def mask(df, %Series{} = mask),
    do: Shared.apply_dataframe(df, df, :df_mask, [mask.data])

  @impl true
  def filter_with(df, out_df, %Explorer.Backend.LazySeries{} = lseries) do
    expressions = to_expr(lseries)
    Shared.apply_dataframe(df, out_df, :df_filter_with, [expressions, df.groups])
  end

  @impl true
  def mutate_with(%DataFrame{} = df, %DataFrame{} = out_df, column_pairs) do
    exprs =
      for {name, lazy_series} <- column_pairs do
        lazy_series
        |> to_expr()
        |> alias_expr(name)
      end

    Shared.apply_dataframe(df, out_df, :df_mutate_with_exprs, [exprs, df.groups])
  end

  @impl true
  def put(%DataFrame{} = df, %DataFrame{} = out_df, new_column_name, series) do
    series = PolarsSeries.rename(series, new_column_name)

    Shared.apply_dataframe(df, out_df, :df_put_column, [series.data])
  end

  @impl true
  def describe(%DataFrame{} = df, %DataFrame{} = out_df, percentiles) do
    Shared.apply_dataframe(df, out_df, :df_describe, [percentiles])
  end

  @impl true
  def arrange_with(%DataFrame{} = df, out_df, column_pairs) do
    {directions, expressions} =
      column_pairs
      |> Enum.map(fn {direction, lazy_series} ->
        expr = to_expr(lazy_series)
        {direction == :desc, expr}
      end)
      |> Enum.unzip()

    Shared.apply_dataframe(df, out_df, :df_arrange_with, [expressions, directions, df.groups])
  end

  @impl true
  def distinct(%DataFrame{} = df, %DataFrame{} = out_df, columns) do
    maybe_columns_to_keep = if df.names != out_df.names, do: out_df.names

    Shared.apply_dataframe(df, out_df, :df_distinct, [columns, maybe_columns_to_keep])
  end

  @impl true
  def rename(%DataFrame{} = df, %DataFrame{} = out_df, pairs),
    do: Shared.apply_dataframe(df, out_df, :df_rename_columns, [pairs])

  @impl true
  def dummies(df, out_df, names),
    do: Shared.apply_dataframe(df, out_df, :df_to_dummies, [names])

  @impl true
  def sample(df, n, replacement, seed) when is_integer(n) do
    Shared.apply_dataframe(df, df, :df_sample_n, [n, replacement, seed, df.groups])
  end

  @impl true
  def sample(df, frac, replacement, seed) when is_float(frac) do
    Shared.apply_dataframe(df, df, :df_sample_frac, [frac, replacement, seed, df.groups])
  end

  @impl true
  def pull(df, column), do: Shared.apply_dataframe(df, :df_pull, [column])

  @impl true
  def slice(%DataFrame{} = df, row_indices) when is_list(row_indices),
    do: Shared.apply_dataframe(df, df, :df_slice_by_indices, [row_indices, df.groups])

  # TODO: If we expose group_indices at the Explorer.DataFrame level,
  # then we can implement this in pure Elixir.
  @impl true
  def slice(%DataFrame{groups: [_ | _]} = df, %Range{} = range) do
    {:ok, group_indices} = Native.df_group_indices(df.data, df.groups)

    idx =
      Enum.reduce(group_indices, Native.s_from_list_u32("idx", []), fn series, acc ->
        {:ok, size} = Native.s_size(series)
        indices = Enum.slice(0..(size - 1)//1, range)
        {:ok, sliced} = Native.s_slice_by_indices(series, indices)
        {:ok, acc} = Native.s_concat(acc, sliced)
        acc
      end)

    Shared.apply_dataframe(df, df, :df_slice_by_series, [idx])
  end

  @impl true
  def slice(%DataFrame{} = df, offset, length)
      when is_integer(offset) and is_integer(length),
      do: Shared.apply_dataframe(df, df, :df_slice, [offset, length, df.groups])

  @impl true
  def drop_nil(df, columns), do: Shared.apply_dataframe(df, df, :df_drop_nils, [columns])

  @impl true
  def pivot_longer(df, out_df, columns_to_pivot, columns_to_keep, names_to, values_to) do
    Shared.apply_dataframe(df, out_df, :df_pivot_longer, [
      columns_to_keep,
      columns_to_pivot,
      names_to,
      values_to
    ])
  end

  @impl true
  def pivot_wider(df, out_df, id_columns, names_from, values_from, names_prefix) do
    result =
      Shared.apply_dataframe(df, out_df, :df_pivot_wider, [id_columns, names_from, values_from])

    if names_prefix == "" do
      result
    else
      new_names = Shared.apply_dataframe(result, :df_names, []) -- id_columns
      rename_pairs = for new_name <- new_names, do: {new_name, names_prefix <> new_name}
      rename(result, out_df, rename_pairs)
    end
  end

  # Two or more table verbs

  @impl true
  def join(left, right, out_df, on, :right) do
    # Join right is just the "join left" with inverted DFs and swapped "on" instructions.
    # If columns on left have the same names from right, and they are not in "on" instructions,
    # then we add a suffix "_left".
    {left_on, right_on} =
      on
      |> Enum.reverse()
      |> Enum.map(fn {left, right} -> {right, left} end)
      |> Enum.unzip()

    args = [left.data, left_on, right_on, "left", "_left"]
    Shared.apply_dataframe(right, out_df, :df_join, args)
  end

  @impl true
  def join(left, right, out_df, on, how) do
    how = Atom.to_string(how)
    {left_on, right_on} = Enum.unzip(on)

    args = [right.data, left_on, right_on, how, "_right"]
    Shared.apply_dataframe(left, out_df, :df_join, args)
  end

  @impl true
  def concat_rows([head | tail], out_df) do
    Shared.apply_dataframe(head, out_df, :df_concat_rows, [Enum.map(tail, & &1.data)])
  end

  @impl true
  def concat_columns([head | tail], out_df) do
    n_rows = n_rows(head)

    if Enum.all?(tail, &(n_rows(&1) == n_rows)) do
      Shared.apply_dataframe(head, out_df, :df_concat_columns, [Enum.map(tail, & &1.data)])
    else
      raise ArgumentError, "all dataframes must have the same number of rows"
    end
  end

  # Groups

  @impl true
  def summarise_with(%DataFrame{groups: groups} = df, %DataFrame{} = out_df, column_pairs) do
    exprs =
      for {name, lazy_series} <- column_pairs do
        original_expr = to_expr(lazy_series)
        alias_expr(original_expr, name)
      end

    groups_exprs = for group <- groups, do: Native.expr_column(group)

    Shared.apply_dataframe(df, out_df, :df_summarise_with_exprs, [groups_exprs, exprs])
  end

  # Inspect

  @impl true
  def inspect(df, opts) do
    Explorer.Backend.DataFrame.inspect(df, "Polars", n_rows(df), opts)
  end
end
