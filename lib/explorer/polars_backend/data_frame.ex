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
        names,
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
    max_rows = if max_rows == Inf, do: nil, else: max_rows

    infer_schema_length =
      if infer_schema_length == nil,
        do: max_rows || @default_infer_schema_length,
        else: infer_schema_length

    dtypes =
      if dtypes do
        Enum.map(dtypes, fn {colname, dtype} ->
          {colname, Shared.internal_from_dtype(dtype)}
        end)
      end

    {with_columns, with_projection} = column_list_check(columns)

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
        with_columns,
        dtypes,
        encoding,
        null_character,
        parse_dates
      )

    case {df, names} do
      {{:ok, df}, nil} -> {:ok, Shared.to_dataframe(df)}
      {{:ok, df}, names} -> checked_rename(Shared.to_dataframe(df), names)
      {{:error, error}, _} -> {:error, error}
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

  defp checked_rename(df, names) do
    if n_cols(df) != length(names) do
      raise(
        ArgumentError,
        "Expected length of provided names (#{length(names)}) to match number of columns in dataframe (#{n_cols(df)})."
      )
    end

    {:ok, rename(df, names)}
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
  def from_ndjson(filename, infer_schema_length, with_batch_size) do
    with {:ok, df} <- Native.df_read_ndjson(filename, infer_schema_length, with_batch_size) do
      {:ok, Shared.to_dataframe(df)}
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
      {:ok, df} -> {:ok, Shared.to_dataframe(df)}
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
  def from_rows([h | _] = rows) when is_map(h) do
    case Native.df_from_map_rows(rows) do
      {:ok, df} -> Shared.to_dataframe(df)
      {:error, reason} -> raise ArgumentError, reason
    end
  end

  def from_rows([h | _] = rows) when is_list(h) do
    case Native.df_from_keyword_rows(rows) do
      {:ok, df} -> Shared.to_dataframe(df)
      {:error, reason} -> raise ArgumentError, reason
    end
  end

  @impl true
  def from_ipc(filename, columns) do
    {columns, projection} = column_list_check(columns)

    case Native.df_read_ipc(filename, columns, projection) do
      {:ok, df} -> {:ok, Shared.to_dataframe(df)}
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
  def from_columns(map) do
    series_list = Enum.map(map, &from_columns_handler/1)

    case Native.df_new(series_list) do
      {:ok, df} -> Shared.to_dataframe(df)
      {:error, error} -> raise ArgumentError, error
    end
  end

  defp from_columns_handler({key, value}) when is_atom(key) do
    colname = Atom.to_string(key)
    from_columns_handler({colname, value})
  end

  defp from_columns_handler({colname, value}) when is_list(value) do
    series = series_from_list!(colname, value)
    from_columns_handler({colname, series})
  end

  defp from_columns_handler({colname, %Series{} = series}) when is_binary(colname) do
    series |> PolarsSeries.rename(colname) |> Shared.to_polars_s()
  end

  # Like `Explorer.Series.from_list/2`, but gives a better error message with the series name.
  defp series_from_list!(name, list) do
    case Explorer.Shared.check_types(list) do
      {:ok, type} ->
        {list, type} = Explorer.Shared.cast_numerics(list, type)
        PolarsSeries.from_list(list, type, name)

      {:error, error} ->
        message = "cannot create series #{inspect(name)}: " <> error
        raise ArgumentError, message
    end
  end

  @impl true
  def to_columns(%DataFrame{data: df}, convert_series?, atom_keys?) do
    Enum.reduce(df, %{}, &to_columns_reducer(&1, &2, convert_series?, atom_keys?))
  end

  defp to_columns_reducer(series, acc, convert_series?, atom_keys?) do
    series_name =
      series
      |> Native.s_name()
      |> then(fn {:ok, name} ->
        if atom_keys? do
          String.to_atom(name)
        else
          name
        end
      end)

    series = Shared.to_series(series)
    series = if convert_series?, do: PolarsSeries.to_list(series), else: series
    Map.put(acc, series_name, series)
  end

  @impl true
  def to_rows(%DataFrame{data: polars_df} = df, atom_keys?) do
    names = if atom_keys?, do: df |> names() |> Enum.map(&String.to_atom/1), else: names(df)

    polars_df
    |> Enum.map(fn s -> s |> Shared.to_series() |> PolarsSeries.to_list() end)
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
  def n_cols(df), do: Shared.apply_native(df, :df_width)

  # Single table verbs

  @impl true
  def head(df, rows), do: Shared.apply_native(df, :df_head, [rows])

  @impl true
  def tail(df, rows), do: Shared.apply_native(df, :df_tail, [rows])

  @impl true
  def select(df, columns, :keep) when is_list(columns),
    do: Shared.apply_native(df, :df_select, [columns])

  def select(%{groups: groups} = df, columns, :drop) when is_list(columns),
    do: df |> Shared.to_polars_df() |> drop(columns) |> Shared.to_dataframe(groups)

  defp drop(polars_df, colnames),
    do:
      Enum.reduce(colnames, polars_df, fn name, df ->
        {:ok, df} = Native.df_drop(df, name)
        df
      end)

  @impl true
  def filter(df, %Series{} = mask),
    do: Shared.apply_native(df, :df_filter, [Shared.to_polars_s(mask)])

  @impl true
  def mutate(%DataFrame{groups: []} = df, columns) do
    columns |> Enum.reduce(df, &mutate_reducer/2) |> Shared.to_dataframe()
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

  defp mutate_reducer({colname, %Series{} = series}, %DataFrame{} = df) when is_binary(colname) do
    check_series_size(df, series, colname)
    series = series |> PolarsSeries.rename(colname) |> Shared.to_polars_s()
    Shared.apply_native(df, :df_with_column, [series])
  end

  defp mutate_reducer({colname, callback}, %DataFrame{} = df)
       when is_function(callback),
       do: mutate_reducer({colname, callback.(df)}, df)

  defp mutate_reducer({colname, values}, df) when is_list(values),
    do: mutate_reducer({colname, series_from_list!(colname, values)}, df)

  defp mutate_reducer({colname, value}, %DataFrame{} = df)
       when is_binary(colname),
       do: mutate_reducer({colname, value |> List.duplicate(n_rows(df))}, df)

  defp check_series_size(df, series, colname) do
    df_len = n_rows(df)
    s_len = Series.size(series)

    if s_len != df_len,
      do:
        raise(
          ArgumentError,
          "size of new column #{colname} (#{s_len}) must match number of rows in the " <>
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
  def sample(df, n, with_replacement?, seed) when is_integer(n) do
    indices =
      df
      |> n_rows()
      |> Native.s_seedable_random_indices(n, with_replacement?, seed)

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
  def pivot_longer(df, id_cols, value_cols, names_to, values_to) do
    df = Shared.apply_native(df, :df_melt, [id_cols, value_cols])

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
  def pivot_wider(df, id_cols, names_from, values_from, names_prefix) do
    df = Shared.apply_native(df, :df_pivot_wider, [id_cols, names_from, values_from])

    df =
      df
      |> names()
      |> Enum.map(fn name ->
        if name in id_cols, do: name, else: names_prefix <> name
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

    Shared.apply_native(left, :df_join, [Shared.to_polars_df(right), left_on, right_on, how])
  end

  defp join_on_reducer(colname, {left, right}) when is_binary(colname),
    do: {[colname | left], [colname | right]}

  defp join_on_reducer({new_left, new_right}, {left, right}),
    do: {[new_left | left], [new_right | right]}

  @impl true
  def concat_rows(dfs) do
    Enum.reduce(dfs, fn x, acc ->
      # Polars requires the _order_ of columns to be the same
      x = DataFrame.select(x, DataFrame.names(acc))
      Shared.apply_native(acc, :df_vstack, [Shared.to_polars_df(x)])
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
  def summarise(%DataFrame{groups: groups} = df, with_columns) do
    with_columns =
      Enum.map(with_columns, fn {key, values} -> {key, Enum.map(values, &Atom.to_string/1)} end)

    df
    |> Shared.apply_native(:df_groupby_agg, [groups, with_columns])
    |> ungroup([])
    |> DataFrame.arrange(groups)
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
        {:ok, next_col} = Native.df_column(df, head)
        {:ok, df} = Native.df_drop(df, head)
        reduce(df, fun.(next_col, acc), fun)
    end
  end

  def member?(df, %PolarsSeries{} = series) do
    {:ok, columns} = Native.df_get_columns(df)

    {:ok, Enum.any?(columns, &Native.s_series_equal(&1, series, false))}
  end

  def member?(_, _), do: {:error, __MODULE__}
end

defimpl Inspect, for: Explorer.PolarsBackend.DataFrame do
  alias Explorer.PolarsBackend.Native

  def inspect(df, _opts) do
    case Native.df_as_str(df) do
      {:ok, str} -> str
      {:error, error} -> raise "#{error}"
    end
  end
end
