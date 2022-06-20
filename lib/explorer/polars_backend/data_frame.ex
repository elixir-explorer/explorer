defmodule Explorer.PolarsBackend.DataFrame do
  @moduledoc false

  alias Explorer.DataFrame, as: DataFrame
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries
  alias Explorer.PolarsBackend.Shared
  alias Explorer.Series, as: Series

  @type t :: %__MODULE__{resource: binary(), reference: reference()}

  defstruct resource: nil, reference: nil

  @behaviour Explorer.Backend.DataFrame
  @default_infer_schema_length 1000

  # IO

  @impl true
  def from_csv(
        filename,
        dtypes,
        delimiter,
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

    {columns, with_projection} = column_list_check(columns)

    df =
      Native.df_read_csv(
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

  defp column_list_check(list) do
    cond do
      is_nil(list) ->
        {nil, nil}

      Enum.all?(list, &is_atom/1) ->
        {Enum.map(list, &Atom.to_string/1), nil}

      Enum.all?(list, &is_binary/1) ->
        {list, nil}

      Enum.all?(list, &is_integer/1) ->
        {nil, list}

      true ->
        raise ArgumentError,
              "expected :columns to be a list of only integers, only atoms, or only binaries, " <>
                "got: #{inspect(list)}"
    end
  end

  @impl true
  def to_csv(%DataFrame{data: df}, filename, header?, delimiter) do
    <<delimiter::utf8>> = delimiter

    case Native.df_to_csv_file(df, filename, header?, delimiter) do
      {:ok, _} -> {:ok, filename}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def from_ndjson(filename, infer_schema_length, batch_size) do
    with {:ok, df} <- Native.df_read_ndjson(filename, infer_schema_length, batch_size) do
      {:ok, Shared.create_dataframe(df)}
    end
  end

  @impl true
  def to_ndjson(%DataFrame{data: df}, filename) do
    with {:ok, _} <- Native.df_write_ndjson(df, filename) do
      {:ok, filename}
    end
  end

  @impl true
  def dump_csv(%DataFrame{} = df, header?, delimiter) do
    <<delimiter::utf8>> = delimiter
    Shared.apply_dataframe(df, :df_to_csv, [header?, delimiter])
  end

  @impl true
  def from_parquet(filename) do
    case Native.df_read_parquet(filename) do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def to_parquet(%DataFrame{data: df}, filename) do
    case Native.df_write_parquet(df, filename) do
      {:ok, _} -> {:ok, filename}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def from_ipc(filename, columns) do
    {columns, projection} = column_list_check(columns)

    case Native.df_read_ipc(filename, columns, projection) do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def to_ipc(%DataFrame{data: df}, filename, compression) do
    case Native.df_write_ipc(df, filename, compression) do
      {:ok, _} -> {:ok, filename}
      {:error, error} -> {:error, error}
    end
  end

  # Conversion

  @impl true
  def lazy, do: Explorer.PolarsBackend.LazyDataFrame

  @impl true
  def to_lazy(df), do: Shared.apply_dataframe(df, :df_to_lazy)

  @impl true
  def collect(df), do: df

  @impl true
  def from_tabular(tabular) do
    {columns, %{columns: keys}} = Table.to_columns_with_info(tabular)

    keys
    |> Enum.map(fn key ->
      column_name = to_column_name!(key)
      values = Enum.to_list(columns[key])
      series_from_list!(column_name, values)
    end)
    |> from_series_list()
  end

  @impl true
  def from_series(pairs) do
    pairs
    |> Enum.map(fn {key, series} ->
      column_name = to_column_name!(key)
      PolarsSeries.rename(series, column_name)
    end)
    |> from_series_list()
  end

  defp from_series_list(list) do
    list = Enum.map(list, & &1.data)

    case Native.df_new(list) do
      {:ok, df} -> Shared.create_dataframe(df)
      {:error, error} -> raise ArgumentError, error
    end
  end

  defp to_column_name!(column_name) when is_binary(column_name), do: column_name
  defp to_column_name!(column_name) when is_atom(column_name), do: Atom.to_string(column_name)

  defp to_column_name!(column_name) do
    raise ArgumentError,
          "expected column name to be either string or atom, got: #{inspect(column_name)}"
  end

  # Like `Explorer.Series.from_list/2`, but gives a better error message with the series name.
  defp series_from_list!(name, list) do
    type = Explorer.Shared.check_types!(list)
    {list, type} = Explorer.Shared.cast_numerics(list, type)
    PolarsSeries.from_list(list, type, name)
  rescue
    e ->
      raise ArgumentError, "cannot create series #{inspect(name)}: " <> Exception.message(e)
  end

  @impl true
  def to_rows(%DataFrame{data: polars_df} = df, atom_keys?) do
    names = if atom_keys?, do: df |> names() |> Enum.map(&String.to_atom/1), else: names(df)

    polars_df
    |> Enum.map(fn s -> s |> Shared.create_series() |> PolarsSeries.to_list() end)
    |> Enum.zip_with(fn row -> names |> Enum.zip(row) |> Map.new() end)
  end

  # Introspection

  @impl true
  def names(df), do: Shared.apply_dataframe(df, :df_columns)

  @impl true
  def dtypes(df),
    do: df |> Shared.apply_dataframe(:df_dtypes) |> Enum.map(&Shared.normalise_dtype/1)

  @impl true
  def shape(df), do: Shared.apply_dataframe(df, :df_shape)

  @impl true
  def n_rows(%DataFrame{groups: []} = df), do: Shared.apply_dataframe(df, :df_height)

  def n_rows(%DataFrame{groups: groups} = df) do
    groupby = Shared.apply_dataframe(df, :df_groups, [groups])

    n =
      groupby
      |> pull("groups")
      |> Series.to_list()
      |> Enum.map(fn indices -> df |> DataFrame.ungroup() |> take(indices) |> n_rows() end)

    groupby
    |> DataFrame.select(["groups"], :drop)
    |> mutate(df, n: n)
    |> DataFrame.group_by(groups)
  end

  @impl true
  def n_columns(df), do: Shared.apply_dataframe(df, :df_width)

  # Single table verbs

  @impl true
  def head(df, rows), do: Shared.apply_dataframe(df, :df_head, [rows])

  @impl true
  def tail(df, rows), do: Shared.apply_dataframe(df, :df_tail, [rows])

  @impl true
  def select(df, out_df),
    do: Shared.apply_dataframe(df, out_df, :df_select, [out_df.names])

  @impl true
  def filter(df, %Series{} = mask),
    do: Shared.apply_dataframe(df, :df_filter, [mask.data])

  @impl true
  def mutate(%DataFrame{groups: []} = df, out_df, columns) do
    ungrouped_mutate(df, out_df, columns)
  end

  def mutate(%DataFrame{groups: [_ | _]} = df, out_df, columns) do
    apply_on_groups(df, out_df, fn group -> ungrouped_mutate(group, out_df, columns) end)
  end

  defp ungrouped_mutate(df, out_df, columns) do
    Enum.reduce(columns, df, fn {column_name, value}, acc_df when is_binary(column_name) ->
      series = to_series(acc_df, column_name, value)
      check_series_size!(acc_df, series, column_name)
      Shared.apply_dataframe(acc_df, out_df, :df_with_column, [series.data])
    end)
  end

  defp to_series(df, name, value) do
    case value do
      %Series{} = series ->
        PolarsSeries.rename(series, name)

      values when is_list(values) ->
        series_from_list!(name, values)

      callback when is_function(callback) ->
        to_series(df, name, callback.(df))

      any ->
        to_series(df, name, List.duplicate(any, n_rows(df)))
    end
  end

  defp check_series_size!(df, series, column_name) do
    df_len = n_rows(df)
    s_len = Series.size(series)

    if s_len != df_len,
      do:
        raise(
          ArgumentError,
          "size of new column #{column_name} (#{s_len}) must match number of rows in the " <>
            "dataframe (#{df_len})"
        )
  end

  @impl true
  def arrange(%DataFrame{groups: []} = df, columns) do
    ungrouped_arrange(df, columns)
  end

  def arrange(%DataFrame{groups: [_ | _]} = df, columns) do
    apply_on_groups(df, df, fn group -> ungrouped_arrange(group, columns) end)
  end

  defp ungrouped_arrange(df, columns) do
    Enum.reduce(columns, df, fn {direction, column}, df ->
      Shared.apply_dataframe(df, :df_sort, [column, direction == :desc])
    end)
  end

  @impl true
  def distinct(%DataFrame{groups: []} = df, %DataFrame{} = out_df, columns, keep_all?),
    do: ungrouped_distinct(df, out_df, columns, keep_all?)

  def distinct(%DataFrame{groups: [_ | _]} = df, %DataFrame{} = out_df, columns, keep_all?) do
    apply_on_groups(df, out_df, fn group ->
      ungrouped_distinct(group, out_df, columns, keep_all?)
    end)
  end

  defp ungrouped_distinct(df, out_df, columns, true) do
    Shared.apply_dataframe(df, out_df, :df_drop_duplicates, [true, columns])
  end

  defp ungrouped_distinct(df, out_df, columns, false) do
    df
    |> Shared.apply_dataframe(out_df, :df_drop_duplicates, [true, columns])
    |> select(out_df)
  end

  # Applies a callback function to each group of indexes in a dataframe. Then regroups it.
  defp apply_on_groups(%DataFrame{} = df, out_df, callback) when is_function(callback, 1) do
    ungrouped_df = DataFrame.ungroup(df)
    idx_column = "__original_row_idx__"

    df
    |> indexes_by_groups()
    |> Enum.map(fn indices ->
      ungrouped_df
      |> take(indices)
      |> then(callback)
      |> then(fn group_df ->
        idx_series = series_from_list!(idx_column, indices)

        Shared.apply_dataframe(group_df, :df_with_column, [idx_series.data])
      end)
    end)
    |> Enum.reduce(fn df, acc -> Shared.apply_dataframe(acc, :df_vstack, [df.data]) end)
    |> ungrouped_arrange([{:asc, idx_column}])
    |> select(out_df)
  end

  # Returns a list of lists, where each list is a group of row indexes.
  defp indexes_by_groups(%DataFrame{groups: [_ | _]} = df) do
    df
    |> Shared.apply_dataframe(:df_group_indices, [df.groups])
    |> Shared.apply_series(:s_to_list)
  end

  @impl true
  def rename(%DataFrame{} = df, %DataFrame{} = out_df),
    do: Shared.apply_dataframe(df, out_df, :df_set_column_names, [out_df.names])

  @impl true
  def dummies(df, names),
    do: Shared.apply_dataframe(df, :df_to_dummies, [names])

  @impl true
  def sample(df, n, replacement, seed) when is_integer(n) do
    indices =
      df
      |> n_rows()
      |> Native.s_seedable_random_indices(n, replacement, seed)

    take(df, indices)
  end

  @impl true
  def pull(df, column), do: Shared.apply_dataframe(df, :df_column, [column])

  @impl true
  def slice(df, offset, length), do: Shared.apply_dataframe(df, :df_slice, [offset, length])

  @impl true
  def take(df, row_indices), do: Shared.apply_dataframe(df, :df_take, [row_indices])

  @impl true
  def drop_nil(df, columns), do: Shared.apply_dataframe(df, :df_drop_nulls, [columns])

  @impl true
  def pivot_longer(df, out_df, value_columns) do
    [names_to, values_to] = not_ids = Enum.slice(out_df.names, -2, 2)
    id_columns = out_df.names -- not_ids

    Shared.apply_dataframe(df, out_df, :df_melt, [id_columns, value_columns, names_to, values_to])
  end

  @impl true
  def pivot_wider(df, id_columns, names_from, values_from, names_prefix) do
    df = Shared.apply_dataframe(df, :df_pivot_wider, [id_columns, names_from, values_from])
    names = names(df)

    new_names =
      Enum.map(names, fn name ->
        if name in id_columns, do: name, else: names_prefix <> name
      end)

    if names != new_names do
      Shared.apply_dataframe(df, :df_set_column_names, [new_names])
    else
      df
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

    Shared.apply_dataframe(right, out_df, :df_join, [
      left.data,
      left_on,
      right_on,
      "left",
      "_left"
    ])
  end

  def join(left, right, out_df, on, how) do
    how = Atom.to_string(how)
    {left_on, right_on} = Enum.unzip(on)

    Shared.apply_dataframe(left, out_df, :df_join, [right.data, left_on, right_on, how, "_right"])
  end

  @impl true
  def concat_rows(dfs) do
    Enum.reduce(dfs, fn x, acc ->
      # Polars requires the _order_ of columns to be the same
      x = DataFrame.select(x, DataFrame.names(acc))
      Shared.apply_dataframe(acc, :df_vstack, [x.data])
    end)
  end

  # Groups

  @impl true
  def group_by(%DataFrame{}, %DataFrame{} = out_df), do: out_df

  @impl true
  def ungroup(%DataFrame{}, %DataFrame{} = out_df), do: out_df

  @impl true
  def summarise(%DataFrame{groups: groups} = df, %DataFrame{} = out_df, columns) do
    columns =
      Enum.map(columns, fn {key, values} -> {key, Enum.map(values, &Atom.to_string/1)} end)

    order = Enum.map(groups, &{:asc, &1})

    df
    |> Shared.apply_dataframe(out_df, :df_groupby_agg, [groups, columns])
    |> ungrouped_arrange(order)
  end

  # Inspect

  @impl true
  def inspect(df, opts) do
    {n_rows, _} = shape(df)
    Explorer.Backend.DataFrame.inspect(df, "Polars", n_rows, opts)
  end
end

defimpl Enumerable, for: Explorer.PolarsBackend.DataFrame do
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries

  def count(df), do: Native.df_width(df)

  def slice(df) do
    {:ok, size} = count(df)
    {:ok, size, &slicing_fun(df, &1, &2)}
  end

  defp slicing_fun(df, start, length) do
    for idx <- start..(start + length - 1) do
      {:ok, df} = Native.df_select_at_idx(df, idx)
      df
    end
  end

  def reduce(_df, {:halt, acc}, _fun), do: {:halted, acc}
  def reduce(df, {:suspend, acc}, fun), do: {:suspended, acc, &reduce(df, &1, fun)}

  def reduce(df, {:cont, acc}, fun) do
    case Native.df_columns(df) do
      {:ok, []} ->
        {:done, acc}

      {:ok, [head | _tail]} ->
        {:ok, next_column} = Native.df_column(df, head)
        {:ok, df} = Native.df_drop(df, head)
        reduce(df, fun.(next_column, acc), fun)
    end
  end

  def member?(df, %PolarsSeries{} = series) do
    {:ok, columns} = Native.df_get_columns(df)

    {:ok, Enum.any?(columns, &Native.s_series_equal(&1, series, false))}
  end

  def member?(_, _), do: {:error, __MODULE__}
end
