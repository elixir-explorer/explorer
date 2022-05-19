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
    Shared.apply_native(df, :df_to_csv, [header?, delimiter])
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
  def to_lazy(df), do: Shared.apply_native(df, :df_to_lazy)

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
  def names(df), do: Shared.apply_native(df, :df_columns)

  @impl true
  def dtypes(df), do: df |> Shared.apply_native(:df_dtypes) |> Enum.map(&Shared.normalise_dtype/1)

  @impl true
  def shape(df), do: Shared.apply_native(df, :df_shape)

  @impl true
  def n_rows(%DataFrame{groups: []} = df), do: Shared.apply_native(df, :df_height)

  def n_rows(%DataFrame{groups: groups} = df) do
    groupby = Shared.apply_native(df, :df_groups, [groups])

    n =
      groupby
      |> pull("groups")
      |> Series.to_list()
      |> Enum.map(fn indices -> df |> ungroup([]) |> take(indices) |> n_rows() end)

    groupby |> select(["groups"], :drop) |> mutate(n: n) |> group_by(groups)
  end

  @impl true
  def n_columns(df), do: Shared.apply_native(df, :df_width)

  # Single table verbs

  @impl true
  def head(df, rows), do: Shared.apply_native(df, :df_head, [rows])

  @impl true
  def tail(df, rows), do: Shared.apply_native(df, :df_tail, [rows])

  @impl true
  def select(df, columns, :keep) when is_list(columns),
    do: Shared.apply_native(df, :df_select, [columns])

  def select(df, columns, :drop) when is_list(columns),
    do: df.data |> drop(columns) |> Shared.update_dataframe(df)

  defp drop(polars_df, column_names),
    do:
      Enum.reduce(column_names, polars_df, fn name, df ->
        {:ok, df} = Native.df_drop(df, name)
        df
      end)

  @impl true
  def filter(df, %Series{} = mask),
    do: Shared.apply_native(df, :df_filter, [mask.data])

  @impl true
  def mutate(%DataFrame{groups: []} = df, columns) do
    Enum.reduce(columns, df, &mutate_reducer/2)
  end

  def mutate(%DataFrame{groups: groups} = df, columns) do
    df
    |> Shared.apply_native(:df_groups, [groups])
    |> pull("groups")
    |> Series.to_list()
    |> Enum.map(fn indices -> df |> ungroup([]) |> take(indices) |> mutate(columns) end)
    |> Enum.reduce(fn df, acc -> Shared.apply_native(acc, :df_vstack, [df.data]) end)
    |> group_by(groups)
  end

  defp mutate_reducer({column_name, %Series{} = series}, %DataFrame{} = df)
       when is_binary(column_name) do
    check_series_size(df, series, column_name)
    series = PolarsSeries.rename(series, column_name)
    Shared.apply_native(df, :df_with_column, [series.data])
  end

  defp mutate_reducer({column_name, callback}, %DataFrame{} = df)
       when is_function(callback),
       do: mutate_reducer({column_name, callback.(df)}, df)

  defp mutate_reducer({column_name, values}, df) when is_list(values),
    do: mutate_reducer({column_name, series_from_list!(column_name, values)}, df)

  defp mutate_reducer({column_name, value}, %DataFrame{} = df)
       when is_binary(column_name),
       do: mutate_reducer({column_name, value |> List.duplicate(n_rows(df))}, df)

  defp check_series_size(df, series, column_name) do
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
  def arrange(%DataFrame{groups: []} = df, columns),
    do:
      Enum.reduce(columns, df, fn {direction, column}, df ->
        Shared.apply_native(df, :df_sort, [column, direction == :desc])
      end)

  def arrange(%DataFrame{groups: groups} = df, columns) do
    df
    |> Shared.apply_native(:df_groups, [groups])
    |> pull("groups")
    |> Series.to_list()
    |> Enum.map(fn indices -> df |> ungroup([]) |> take(indices) |> arrange(columns) end)
    |> Enum.reduce(fn df, acc -> Shared.apply_native(acc, :df_vstack, [df.data]) end)
    |> group_by(groups)
  end

  @impl true
  def distinct(%DataFrame{groups: []} = df, columns, true),
    do: Shared.apply_native(df, :df_drop_duplicates, [true, columns])

  def distinct(%DataFrame{groups: []} = df, columns, false),
    do:
      df
      |> Shared.apply_native(:df_drop_duplicates, [true, columns])
      |> select(columns, :keep)

  def distinct(%DataFrame{groups: groups} = df, columns, keep_all?) do
    df
    |> Shared.apply_native(:df_groups, [groups])
    |> pull("groups")
    |> Series.to_list()
    |> Enum.map(fn indices ->
      df |> ungroup([]) |> take(indices) |> distinct(columns, keep_all?)
    end)
    |> Enum.reduce(fn df, acc -> Shared.apply_native(acc, :df_vstack, [df.data]) end)
    |> group_by(groups)
  end

  @impl true
  def rename(df, names) when is_list(names),
    do: Shared.apply_native(df, :df_set_column_names, [names])

  @impl true
  def dummies(df, names),
    do:
      df
      |> select(names, :keep)
      |> Shared.apply_native(:df_to_dummies)

  @impl true
  def sample(df, n, replacement, seed) when is_integer(n) do
    indices =
      df
      |> n_rows()
      |> Native.s_seedable_random_indices(n, replacement, seed)

    take(df, indices)
  end

  @impl true
  def pull(df, column), do: Shared.apply_native(df, :df_column, [column])

  @impl true
  def slice(df, offset, length), do: Shared.apply_native(df, :df_slice, [offset, length])

  @impl true
  def take(df, row_indices), do: Shared.apply_native(df, :df_take, [row_indices])

  @impl true
  def drop_nil(df, columns), do: Shared.apply_native(df, :df_drop_nulls, [columns])

  @impl true
  def pivot_longer(df, id_columns, value_columns, names_to, values_to) do
    df = Shared.apply_native(df, :df_melt, [id_columns, value_columns])

    df
    |> names()
    |> Enum.map(fn
      "variable" -> names_to
      "value" -> values_to
      name -> name
    end)
    |> then(&rename(df, &1))
  end

  @impl true
  def pivot_wider(df, id_columns, names_from, values_from, names_prefix) do
    df = Shared.apply_native(df, :df_pivot_wider, [id_columns, names_from, values_from])

    df =
      df
      |> names()
      |> Enum.map(fn name ->
        if name in id_columns, do: name, else: names_prefix <> name
      end)
      |> then(&rename(df, &1))

    df
  end

  # Two or more table verbs

  @impl true
  def join(left, right, on, :right), do: join(right, left, on, :left)

  def join(left, right, on, how) do
    how = Atom.to_string(how)
    {left_on, right_on} = Enum.reduce(on, {[], []}, &join_on_reducer/2)

    Shared.apply_native(left, :df_join, [right.data, left_on, right_on, how])
  end

  defp join_on_reducer(column_name, {left, right}) when is_binary(column_name),
    do: {[column_name | left], [column_name | right]}

  defp join_on_reducer({new_left, new_right}, {left, right}),
    do: {[new_left | left], [new_right | right]}

  @impl true
  def concat_rows(dfs) do
    Enum.reduce(dfs, fn x, acc ->
      # Polars requires the _order_ of columns to be the same
      x = DataFrame.select(x, DataFrame.names(acc))
      Shared.apply_native(acc, :df_vstack, [x.data])
    end)
  end

  # Groups

  @impl true
  def group_by(%DataFrame{groups: groups} = df, new_groups),
    do: %DataFrame{df | groups: groups ++ new_groups}

  @impl true
  def ungroup(df, []), do: %DataFrame{df | groups: []}

  def ungroup(df, groups),
    do: %DataFrame{df | groups: Enum.filter(df.groups, &(&1 not in groups))}

  @impl true
  def summarise(%DataFrame{groups: groups} = df, columns) do
    columns =
      Enum.map(columns, fn {key, values} -> {key, Enum.map(values, &Atom.to_string/1)} end)

    df
    |> Shared.apply_native(:df_groupby_agg, [groups, columns])
    |> ungroup([])
    |> DataFrame.arrange(groups)
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
