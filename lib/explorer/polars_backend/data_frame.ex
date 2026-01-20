defmodule Explorer.PolarsBackend.DataFrame do
  @moduledoc false

  alias Explorer.DataFrame, as: DataFrame
  alias Explorer.PolarsBackend.LazyFrame
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries
  alias Explorer.PolarsBackend.Shared
  alias Explorer.Series, as: Series

  import Explorer.PolarsBackend.Expression, only: [to_expr: 1]

  defstruct resource: nil

  @type t :: %__MODULE__{resource: reference()}

  @behaviour Explorer.Backend.DataFrame

  # IO

  @compile {:no_warn_undefined, Adbc.Connection}
  @impl true
  def from_query(conn, query, params) do
    adbc_result =
      Adbc.Connection.query_pointer(conn, query, params, fn pointer, _num_rows ->
        Explorer.PolarsBackend.Native.df_from_arrow_stream_pointer(pointer)
      end)

    with {:ok, df_result} <- adbc_result,
         {:ok, df} <- df_result,
         do: Shared.create_dataframe(df)
  end

  @impl true
  def from_csv(
        {backend, _path, _config} = entry,
        dtypes,
        delimiter,
        nil_values,
        skip_rows,
        skip_rows_after_header,
        header?,
        encoding,
        max_rows,
        columns,
        infer_schema_length,
        parse_dates,
        eol_delimiter,
        quote_delimiter
      )
      when backend in [:s3, :http] do
    path = Shared.build_path_for_entry(entry)

    with :ok <- Explorer.FSS.download(entry, path) do
      local_entry = {:local, path, %{}}

      result =
        from_csv(
          local_entry,
          dtypes,
          delimiter,
          nil_values,
          skip_rows,
          skip_rows_after_header,
          header?,
          encoding,
          max_rows,
          columns,
          infer_schema_length,
          parse_dates,
          eol_delimiter,
          quote_delimiter
        )

      File.rm(path)
      result
    end
  end

  @impl true
  def from_csv(
        {:local, path, _config},
        dtypes,
        <<delimiter::utf8>>,
        nil_values,
        skip_rows,
        skip_rows_after_header,
        header?,
        encoding,
        max_rows,
        columns,
        infer_schema_length,
        parse_dates,
        eol_delimiter,
        quote_delimiter
      ) do
    infer_schema_length =
      if infer_schema_length == nil,
        do: max_rows,
        else: infer_schema_length

    {columns, with_projection} = column_names_or_projection(columns)

    df =
      Native.df_from_csv(
        path,
        infer_schema_length,
        header?,
        max_rows,
        skip_rows,
        skip_rows_after_header,
        with_projection,
        delimiter,
        true,
        columns,
        dtypes,
        encoding,
        nil_values,
        parse_dates,
        char_byte(eol_delimiter),
        char_byte(quote_delimiter)
      )

    case df do
      {:ok, df} -> Shared.create_dataframe(df)
      {:error, error} -> {:error, RuntimeError.exception(error)}
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
  def to_csv(
        %DataFrame{data: df},
        {:local, path, _config},
        header?,
        delimiter,
        quote_style,
        _streaming
      ) do
    <<delimiter::utf8>> = delimiter

    case Native.df_to_csv(df, path, header?, delimiter, quote_style) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_csv(
        %DataFrame{data: df},
        {:s3, _key, _config} = entry,
        header?,
        delimiter,
        quote_style,
        _streaming
      ) do
    <<delimiter::utf8>> = delimiter

    case Native.df_to_csv_cloud(df, entry, header?, delimiter, quote_style) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def dump_csv(%DataFrame{} = df, header?, <<delimiter::utf8>>, quote_style) do
    case Native.df_dump_csv(df.data, header?, delimiter, quote_style) do
      {:ok, string} -> {:ok, string}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def load_csv(
        contents,
        dtypes,
        <<delimiter::utf8>>,
        nil_values,
        skip_rows,
        skip_rows_after_header,
        header?,
        encoding,
        max_rows,
        columns,
        infer_schema_length,
        parse_dates,
        eol_delimiter,
        quote_delimiter
      ) do
    infer_schema_length =
      if infer_schema_length == nil,
        do: max_rows,
        else: infer_schema_length

    {columns, with_projection} = column_names_or_projection(columns)

    df =
      Native.df_load_csv(
        contents,
        infer_schema_length,
        header?,
        max_rows,
        skip_rows,
        skip_rows_after_header,
        with_projection,
        delimiter,
        true,
        columns,
        dtypes,
        encoding,
        nil_values,
        parse_dates,
        char_byte(eol_delimiter),
        char_byte(quote_delimiter)
      )

    case df do
      {:ok, df} -> Shared.create_dataframe(df)
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  defp char_byte(nil), do: nil
  defp char_byte(<<char::utf8>>), do: char

  @impl true
  def from_ndjson({backend, _path, _config} = entry, infer_schema_length, batch_size)
      when backend in [:s3, :http] do
    path = Shared.build_path_for_entry(entry)

    with :ok <- Explorer.FSS.download(entry, path) do
      local_entry = {:local, path, %{}}

      result = from_ndjson(local_entry, infer_schema_length, batch_size)

      File.rm(path)
      result
    end
  end

  @impl true
  def from_ndjson({:local, path, _config}, infer_schema_length, batch_size) do
    case Native.df_from_ndjson(path, infer_schema_length, batch_size) do
      {:ok, df} -> Shared.create_dataframe(df)
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_ndjson(%DataFrame{data: df}, {:local, path, _config}) do
    case Native.df_to_ndjson(df, path) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_ndjson(%DataFrame{data: df}, {:s3, _key, _config} = entry) do
    case Native.df_to_ndjson_cloud(df, entry) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def dump_ndjson(%DataFrame{} = df) do
    case Native.df_dump_ndjson(df.data) do
      {:ok, string} -> {:ok, string}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def load_ndjson(contents, infer_schema_length, batch_size) when is_binary(contents) do
    case Native.df_load_ndjson(contents, infer_schema_length, batch_size) do
      {:ok, df} -> Shared.create_dataframe(df)
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def from_parquet({:s3, _key, _config} = entry, max_rows, columns, _rechunk) do
    # We first read using a lazy dataframe, then we collect.
    with {:ok, ldf} <- Native.lf_from_parquet_cloud(entry, max_rows, columns),
         {:ok, df} <- Native.lf_compute(ldf) do
      Shared.create_dataframe(df)
    end
  end

  @impl true
  def from_parquet({:http, _url, _config} = entry, max_rows, columns, rechunk) do
    path = Shared.build_path_for_entry(entry)

    with :ok <- Explorer.FSS.download(entry, path) do
      local_entry = {:local, path, %{}}

      result = from_parquet(local_entry, max_rows, columns, rechunk)

      File.rm(path)
      result
    end
  end

  @impl true
  def from_parquet({:local, path, _config}, max_rows, columns, rechunk) do
    {columns, with_projection} = column_names_or_projection(columns)

    df =
      Native.df_from_parquet(
        path,
        max_rows,
        columns,
        with_projection,
        rechunk
      )

    case df do
      {:ok, df} -> Shared.create_dataframe(df)
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_parquet(
        %DataFrame{data: df},
        {:local, path, _config},
        {compression, compression_level},
        _streaming
      ) do
    case Native.df_to_parquet(df, path, parquet_compression(compression, compression_level)) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_parquet(
        %DataFrame{data: df},
        {:s3, _key, _config} = entry,
        {compression, compression_level},
        _streaming
      ) do
    case Native.df_to_parquet_cloud(
           df,
           entry,
           parquet_compression(compression, compression_level)
         ) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def dump_parquet(%DataFrame{data: df}, {compression, compression_level}) do
    case Native.df_dump_parquet(df, parquet_compression(compression, compression_level)) do
      {:ok, string} -> {:ok, string}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  defp parquet_compression(nil, _), do: :uncompressed

  defp parquet_compression(algorithm, level) when algorithm in ~w(gzip brotli zstd)a do
    {algorithm, level}
  end

  defp parquet_compression(algorithm, _) when algorithm in ~w(snappy lz4raw)a, do: algorithm

  @impl true
  def load_parquet(contents) when is_binary(contents) do
    case Native.df_load_parquet(contents) do
      {:ok, df} -> Shared.create_dataframe(df)
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def from_ipc({backend, _path, _config} = entry, columns) when backend in [:s3, :http] do
    path = Shared.build_path_for_entry(entry)

    with :ok <- Explorer.FSS.download(entry, path) do
      local_entry = {:local, path, %{}}

      result = from_ipc(local_entry, columns)

      File.rm(path)
      result
    end
  end

  @impl true
  def from_ipc({:local, path, _config}, columns) do
    {columns, projection} = column_names_or_projection(columns)

    case Native.df_from_ipc(path, columns, projection) do
      {:ok, df} -> Shared.create_dataframe(df)
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_ipc(%DataFrame{data: df}, {:local, path, _config}, {compression, _level}, _streaming) do
    case Native.df_to_ipc(df, path, maybe_atom_to_string(compression)) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_ipc(
        %DataFrame{data: df},
        {:s3, _key, _config} = entry,
        {compression, _level},
        _streaming
      ) do
    case Native.df_to_ipc_cloud(df, entry, maybe_atom_to_string(compression)) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def dump_ipc(%DataFrame{data: df}, {compression, _level}) do
    case Native.df_dump_ipc(df, maybe_atom_to_string(compression)) do
      {:ok, string} -> {:ok, string}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def load_ipc(contents, columns) when is_binary(contents) do
    {columns, projection} = column_names_or_projection(columns)

    case Native.df_load_ipc(contents, columns, projection) do
      {:ok, df} -> Shared.create_dataframe(df)
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def dump_ipc_schema(%DataFrame{data: df}, compact_level) do
    case Native.df_dump_ipc_schema(df, maybe_atom_to_string(compact_level)) do
      {:ok, string} -> {:ok, string}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def dump_ipc_record_batch(
        %DataFrame{data: df},
        max_chunk_size,
        {compression, _level},
        compact_level
      ) do
    case Native.df_dump_ipc_record_batch(
           df,
           max_chunk_size,
           maybe_atom_to_string(compression),
           maybe_atom_to_string(compact_level)
         ) do
      {:ok, list} -> {:ok, list}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def from_ipc_stream({backend, _path, _config} = entry, columns) when backend in [:s3, :http] do
    path = Shared.build_path_for_entry(entry)

    with :ok <- Explorer.FSS.download(entry, path) do
      local_entry = {:local, path, %{}}

      result = from_ipc_stream(local_entry, columns)

      File.rm(path)
      result
    end
  end

  @impl true
  def from_ipc_stream({:local, path, _config}, columns) do
    {columns, projection} = column_names_or_projection(columns)

    case Native.df_from_ipc_stream(path, columns, projection) do
      {:ok, df} -> Shared.create_dataframe(df)
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_ipc_stream(%DataFrame{data: df}, {:local, path, _config}, {compression, _level}) do
    case Native.df_to_ipc_stream(df, path, maybe_atom_to_string(compression)) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_ipc_stream(%DataFrame{data: df}, {:s3, _key, _config} = entry, {compression, _level}) do
    case Native.df_to_ipc_stream_cloud(df, entry, maybe_atom_to_string(compression)) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def dump_ipc_stream(%DataFrame{data: df}, {compression, _level}) do
    case Native.df_dump_ipc_stream(df, maybe_atom_to_string(compression)) do
      {:ok, string} -> {:ok, string}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def load_ipc_stream(contents, columns) when is_binary(contents) do
    {columns, projection} = column_names_or_projection(columns)

    case Native.df_load_ipc_stream(contents, columns, projection) do
      {:ok, df} -> Shared.create_dataframe(df)
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  defp maybe_atom_to_string(nil), do: nil
  defp maybe_atom_to_string(atom) when is_atom(atom), do: Atom.to_string(atom)

  # Conversion

  @impl true
  def lazy, do: Explorer.PolarsBackend.LazyFrame

  @impl true
  def lazy(df) do
    case Native.df_lazy(df.data) do
      {:ok, polars_df} ->
        %{df | data: polars_df}

      {:error, error} ->
        raise "error when assigning lazy frame: #{inspect(error)}"
    end
  end

  @impl true
  def collect(df), do: df

  @impl true
  def from_tabular(tabular, io_dtypes) do
    dtypes = Map.new(io_dtypes)
    {_, %{columns: keys}, _} = reader = init_reader!(tabular)
    columns = Table.to_columns(reader)

    keys
    |> Enum.map(fn key ->
      column_name = to_column_name!(key)
      values = Enum.to_list(columns[key])
      dtype = Map.get(dtypes, column_name)

      column_name
      |> series_from_list!(values, dtype)
      |> then(fn series ->
        if not is_nil(dtype) and series.dtype != dtype do
          PolarsSeries.cast(series, dtype)
        else
          series
        end
      end)
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
    |> Shared.create_dataframe!()
  end

  defp to_column_name!(column_name) when is_binary(column_name), do: column_name
  defp to_column_name!(column_name) when is_atom(column_name), do: Atom.to_string(column_name)

  defp to_column_name!(column_name) do
    raise ArgumentError,
          "expected column name to be either string or atom, got: #{inspect(column_name)}"
  end

  # Like `Explorer.Series.from_list/2`, but gives a better error message with the series name.
  defp series_from_list!(name, list, dtype) do
    type = Explorer.Shared.dtype_from_list!(list, dtype)
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

  @impl true
  def to_rows_stream(%DataFrame{} = df, atom_keys?, chunk_size) do
    Range.new(0, n_rows(df) - 1, chunk_size)
    |> Stream.map(&slice(df, &1, chunk_size))
    |> Stream.flat_map(&to_rows(&1, atom_keys?))
  end

  # Introspection

  @impl true
  def n_rows(df), do: Shared.apply_dataframe(df, :df_n_rows)

  @impl true
  def estimated_size(df), do: Shared.apply_dataframe(df, :df_estimated_size)

  # Single table verbs

  @impl true
  def head(%DataFrame{} = df, rows) do
    df
    |> lazy()
    |> LazyFrame.head(rows)
    |> LazyFrame.collect()
  end

  @impl true
  def tail(%DataFrame{} = df, rows) do
    df
    |> lazy()
    |> LazyFrame.tail(rows)
    |> LazyFrame.collect()
  end

  @impl true
  def select(df, out_df) do
    df
    |> lazy()
    |> LazyFrame.select(out_df)
    |> LazyFrame.collect()
  end

  @impl true
  def mask(df, %Series{} = mask),
    do: Shared.apply_dataframe(df, df, :df_mask, [mask.data])

  @impl true
  def filter_with(df, out_df, %Explorer.Backend.LazySeries{} = lseries) do
    df
    |> lazy()
    |> LazyFrame.filter_with(out_df, lseries)
    |> LazyFrame.collect()
  end

  @impl true
  def mutate_with(%DataFrame{} = df, %DataFrame{} = out_df, column_pairs) do
    df
    |> lazy()
    |> LazyFrame.mutate_with(out_df, column_pairs)
    |> LazyFrame.collect()
  end

  @impl true
  def put(%DataFrame{} = df, %DataFrame{} = out_df, new_column_name, series) do
    series = PolarsSeries.rename(Explorer.Series.collect(series), new_column_name)

    Shared.apply_dataframe(df, out_df, :df_put_column, [series.data])
  end

  @impl true
  def nil_count(%DataFrame{} = df) do
    Shared.apply(:df_nil_count, [df.data])
    |> Shared.create_dataframe!()
  end

  @impl true
  def sort_with(
        %DataFrame{groups: groups} = df,
        out_df,
        column_pairs,
        maintain_order?,
        multithreaded?,
        nulls_last?
      )
      when is_boolean(maintain_order?) and is_boolean(multithreaded?) and
             is_boolean(nulls_last?) do
    if Enum.all?(column_pairs, fn {_, %{op: op}} -> op == :column end) do
      {directions, column_names} =
        column_pairs
        |> Enum.map(fn {dir, %{args: [col]}} -> {dir == :desc, col} end)
        |> Enum.unzip()

      Shared.apply_dataframe(df, out_df, :df_sort_by, [
        column_names,
        directions,
        maintain_order?,
        multithreaded?,
        nulls_last?,
        groups.columns,
        groups.stable?
      ])
    else
      {directions, expressions} =
        column_pairs
        |> Enum.map(fn {dir, lazy_series} -> {dir == :desc, to_expr(lazy_series)} end)
        |> Enum.unzip()

      Shared.apply_dataframe(df, out_df, :df_sort_with, [
        expressions,
        directions,
        maintain_order?,
        multithreaded?,
        nulls_last?,
        groups.columns,
        groups.stable?
      ])
    end
  end

  @impl true
  def distinct(%DataFrame{} = df, %DataFrame{} = out_df, columns) do
    df
    |> lazy()
    |> LazyFrame.distinct(out_df, columns)
    |> LazyFrame.collect()
  end

  @impl true
  def rename(%DataFrame{} = df, %DataFrame{} = out_df, pairs) do
    df
    |> lazy()
    |> LazyFrame.rename(out_df, pairs)
    |> LazyFrame.collect()
  end

  @impl true
  def dummies(df, out_df, names),
    do: Shared.apply_dataframe(df, out_df, :df_to_dummies, [names])

  @impl true
  def sample(%DataFrame{groups: groups} = df, n, replacement, shuffle, seed) when is_integer(n) do
    Shared.apply_dataframe(df, df, :df_sample_n, [
      n,
      replacement,
      shuffle,
      seed,
      groups.columns,
      groups.stable?
    ])
  end

  @impl true
  def sample(%DataFrame{groups: groups} = df, frac, replacement, shuffle, seed)
      when is_float(frac) do
    # Avoid grouping if the sample is of the entire DF.
    groups_columns = if frac == 1.0, do: [], else: groups.columns

    Shared.apply_dataframe(df, df, :df_sample_frac, [
      frac,
      replacement,
      shuffle,
      seed,
      groups_columns,
      groups.stable?
    ])
  end

  @impl true
  def pull(df, column), do: Shared.apply_dataframe(df, :df_pull, [column])

  @impl true
  def slice(%DataFrame{groups: groups} = df, row_indices) when is_list(row_indices) do
    Shared.apply_dataframe(df, df, :df_slice_by_indices, [
      row_indices,
      groups.columns,
      groups.stable?
    ])
  end

  @impl true
  def slice(%DataFrame{groups: groups} = df, %Series{} = row_indices) do
    Shared.apply_dataframe(df, df, :df_slice_by_series, [
      row_indices.data,
      groups.columns,
      groups.stable?
    ])
  end

  # TODO: If we expose group_indices at the Explorer.DataFrame level,
  # then we can implement this in pure Elixir.
  @impl true
  def slice(%DataFrame{groups: %{columns: group_columns = [_ | _]}} = df, %Range{} = range) do
    {:ok, group_indices} = Native.df_group_indices(df.data, group_columns)

    idx =
      Enum.map(group_indices, fn series ->
        {:ok, size} = Native.s_size(series)
        indices = Enum.slice(0..(size - 1)//1, range)

        {:ok, sliced} = Native.s_slice_by_indices(series, indices)
        sliced
      end)

    {:ok, idx} = Native.s_concat(idx)

    Shared.apply_dataframe(df, df, :df_slice_by_series, [idx, [], df.groups.stable?])
  end

  @impl true
  def slice(%DataFrame{groups: groups} = df, offset, length)
      when is_integer(offset) and is_integer(length),
      do:
        Shared.apply_dataframe(df, df, :df_slice, [offset, length, groups.columns, groups.stable?])

  @impl true
  def drop_nil(df, columns) do
    df
    |> lazy()
    |> LazyFrame.drop_nil(columns)
    |> LazyFrame.collect()
  end

  @impl true
  def pivot_longer(df, out_df, cols_to_pivot, cols_to_keep, names_to, values_to) do
    df
    |> lazy()
    |> LazyFrame.pivot_longer(out_df, cols_to_pivot, cols_to_keep, names_to, values_to)
    |> LazyFrame.collect()
  end

  @impl true
  def pivot_wider(df, id_columns, names_from, values_from, names_prefix) do
    names_prefix_optional = unless names_prefix == "", do: names_prefix

    Shared.apply(:df_pivot_wider, [
      df.data,
      id_columns,
      names_from,
      values_from,
      names_prefix_optional
    ])
    |> Shared.create_dataframe!()
  end

  @impl true
  def transpose(df, out_df, keep_names_as, new_col_names) do
    Shared.apply_dataframe(df, out_df, :df_transpose, [keep_names_as, new_col_names])
  end

  @impl true
  def explode(df, out_df, columns) do
    df
    |> lazy()
    |> LazyFrame.explode(out_df, columns)
    |> LazyFrame.collect()
  end

  @impl true
  def unnest(df, out_df, columns) do
    df
    |> lazy()
    |> LazyFrame.unnest(out_df, columns)
    |> LazyFrame.collect()
  end

  @impl true
  def correlation(df, out_df, method) do
    pairwise(df, out_df, fn left, right ->
      PolarsSeries.correlation(left, right, method)
    end)
  end

  @impl true
  def covariance(df, out_df, ddof) do
    pairwise(df, out_df, fn left, right -> PolarsSeries.covariance(left, right, ddof) end)
  end

  # Two or more table verbs

  @impl true
  def join([left, right], out_df, on, how, nulls_equal) do
    left = lazy(left)
    right = lazy(right)

    ldf = LazyFrame.join([left, right], out_df, on, how, nulls_equal)
    LazyFrame.collect(ldf)
  end

  @impl true
  def join_asof([left, right], out_df, on, by, strategy) do
    left = lazy(left)
    right = lazy(right)

    ldf = LazyFrame.join_asof([left, right], out_df, on, by, strategy)
    LazyFrame.collect(ldf)
  end

  @impl true
  def concat_rows([_head | _tail] = dfs, out_df) do
    lazy_dfs = Enum.map(dfs, &lazy/1)

    lazy_dfs
    |> LazyFrame.concat_rows(out_df)
    |> LazyFrame.collect()
  end

  @impl true
  def concat_columns([head | tail], out_df) do
    n_rows = n_rows(head)

    tail =
      Enum.map(tail, fn df ->
        if n_rows(df) != n_rows do
          raise ArgumentError, "all dataframes must have the same number of rows"
        end

        df.data
      end)

    out_data = Shared.apply(:df_concat_columns, [[head.data | tail]])
    %{out_df | data: out_data}
  end

  # Groups

  @impl true
  def summarise_with(%DataFrame{} = df, %DataFrame{} = out_df, column_pairs) do
    df
    |> lazy()
    |> LazyFrame.summarise_with(out_df, column_pairs)
    |> LazyFrame.collect()
  end

  # Inspect

  @impl true
  def inspect(df, opts) do
    Explorer.Backend.DataFrame.inspect(df, "Polars", n_rows(df), opts)
  end

  # SQL

  @impl true
  def sql(%DataFrame{} = df, sql_string, table_name) do
    df
    |> lazy()
    |> LazyFrame.sql(sql_string, table_name)
    |> LazyFrame.collect()
  end

  @impl true
  def re_dtype(regex_as_string) when is_binary(regex_as_string) do
    case Explorer.PolarsBackend.Native.df_re_dtype(regex_as_string) do
      {:ok, dtype} -> dtype
      {:error, error} -> raise error
    end
  end

  # helpers

  defp pairwise(df, out_df, operation) do
    [column_name | cols] = out_df.names

    pairwise_results =
      Enum.map(cols, fn left ->
        corr_series =
          cols
          |> Enum.map(fn right -> operation.(df[left], df[right]) end)
          |> Shared.from_list({:f, 64})
          |> Shared.create_series()

        {left, corr_series}
      end)

    names_series = cols |> Shared.from_list(:string) |> Shared.create_series()

    from_series([{column_name, names_series} | pairwise_results])
  end
end
