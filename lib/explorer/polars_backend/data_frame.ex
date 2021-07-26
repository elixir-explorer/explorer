defmodule Explorer.PolarsBackend.DataFrame do
  @moduledoc """
  Polars backend for `Explorer.DataFrame`.
  """

  alias Explorer.DataFrame, as: DataFrame
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries
  alias Explorer.PolarsBackend.Shared
  alias Explorer.Series, as: Series

  @type t :: %__MODULE__{resource: binary()}

  defstruct resource: nil, reference: nil

  @behaviour Explorer.Backend.DataFrame

  # IO

  @impl true
  def read_csv(
        filename,
        _names,
        dtypes,
        delimiter,
        null_character,
        skip_rows,
        header?,
        encoding,
        max_rows,
        with_columns
      ) do
    max_rows = if max_rows == Inf, do: nil, else: max_rows

    df =
      Native.df_read_csv(
        filename,
        1000,
        header?,
        max_rows,
        skip_rows,
        nil,
        delimiter,
        true,
        with_columns,
        dtypes,
        null_character,
        encoding
      )

    case df do
      {:ok, df} -> {:ok, Shared.to_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def write_csv(%DataFrame{data: df}, filename, header?, delimiter) do
    <<delimiter::utf8>> = delimiter

    case Native.df_to_csv_file(df, filename, header?, delimiter) do
      {:ok, _} -> {:ok, filename}
      {:error, error} -> {:error, error}
    end
  end

  # Conversion

  @impl true
  def from_map(map) do
    series_list = Enum.map(map, &from_map_handler/1)

    {:ok, df} = Native.df_new(series_list)
    Shared.to_dataframe(df)
  end

  defp from_map_handler({key, value}) when is_atom(key) do
    colname = Atom.to_string(key)
    from_map_handler({colname, value})
  end

  defp from_map_handler({colname, value}) when is_list(value) do
    series = Series.from_list(value)
    from_map_handler({colname, series})
  end

  defp from_map_handler({colname, %Series{} = series}) when is_binary(colname) do
    series |> PolarsSeries.rename(colname) |> Shared.to_polars_s()
  end

  @impl true
  def to_map(%DataFrame{data: df}, convert_series?) do
    Enum.reduce(df, %{}, &to_map_reducer(&1, &2, convert_series?))
  end

  defp to_map_reducer(series, acc, convert_series?) do
    series_name =
      series
      |> Native.s_name()
      |> then(fn {:ok, name} ->
        String.to_atom(name)
      end)

    series = Shared.to_series(series)
    series = if convert_series?, do: PolarsSeries.to_list(series), else: series
    Map.put(acc, series_name, series)
  end

  # Introspection

  @impl true
  def names(df), do: Shared.apply_native(df, :df_columns)

  @impl true
  def dtypes(df), do: df |> Shared.apply_native(:df_dtypes) |> Enum.map(&Shared.normalise_dtype/1)

  @impl true
  def shape(df), do: Shared.apply_native(df, :df_shape)

  @impl true
  def n_rows(df) do
    {rows, _cols} = shape(df)
    rows
  end

  @impl true
  def n_cols(df), do: Shared.apply_native(df, :df_width)

  # Single table verbs

  @impl true
  def head(df, rows), do: Shared.apply_native(df, :df_head, [rows])

  @impl true
  def tail(df, rows), do: Shared.apply_native(df, :df_tail, [rows])

  @impl true
  def select(df, columns, keep_or_drop) when is_list(columns) do
    func =
      case keep_or_drop do
        :keep -> &Native.df_select/2
        :drop -> &drop/2
      end

    df |> Shared.to_polars_df() |> func.(columns) |> Shared.unwrap()
  end

  defp drop(polars_df, colnames),
    do: Enum.reduce(colnames, polars_df, fn name, df -> Native.df_drop(df, name) end)

  @impl true
  def filter(df, %Series{} = mask),
    do: Shared.apply_native(df, :df_filter, [Shared.to_polars_s(mask)])

  @impl true
  def mutate(df, columns \\ []) do
    columns |> Enum.reduce(df, &mutate_reducer/2) |> Shared.to_dataframe()
  end

  defp mutate_reducer({colname, %Series{} = series}, %DataFrame{} = df) when is_binary(colname) do
    series = series |> PolarsSeries.rename(colname) |> Shared.to_polars_s()
    Shared.apply_native(df, :df_with_column, [series])
  end

  @impl true
  def arrange(df, columns),
    do:
      Enum.reduce(columns, df, fn {column, direction}, df ->
        Shared.apply_native(df, :df_sort, [column, direction == :desc])
      end)

  @impl true
  def distinct(df, columns, true),
    do: Shared.apply_native(df, :df_drop_duplicates, [true, columns])

  def distinct(df, columns, false),
    do:
      df
      |> Shared.apply_native(:df_drop_duplicates, [true, columns])
      |> select(columns, :keep)

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
  def sample(df, n, with_replacement?) when is_integer(n),
    do: Shared.apply_native(df, :df_sample_n, [n, with_replacement?])

  def sample(df, frac, with_replacement?) when is_float(frac),
    do: Shared.apply_native(df, :df_sample_n, [frac, with_replacement?])

  @impl true
  def pull(df, column), do: Shared.apply_native(df, :df_column, [column])

  @impl true
  def slice(df, offset, length), do: Shared.apply_native(df, :df_slice, [offset, length])

  @impl true
  def take(df, row_indices), do: Shared.apply_native(df, :df_take, [row_indices])

  # Two table verbs

  @impl true
  def join(left, right, how, :right), do: join(right, left, how, :left)

  def join(left, right, how, on) do
    how = Atom.to_string(how)
    {left_on, right_on} = Enum.reduce(on, {[], []}, &join_on_reducer/2)

    Native.df_join(left, right, left_on, right_on, how)
  end

  defp join_on_reducer(colname, {left, right}) when is_binary(colname),
    do: {[colname | left], [colname | right]}

  defp join_on_reducer({new_left, new_right}, {left, right}),
    do: {[new_left | left], [new_right | right]}
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
    Enum.any?(columns, &Native.s_series_equal(&1, series, false))
  end
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
