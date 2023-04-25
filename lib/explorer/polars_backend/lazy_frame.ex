defmodule Explorer.PolarsBackend.LazyFrame do
  @moduledoc false

  alias Explorer.Backend.LazySeries
  alias Explorer.DataFrame, as: DF

  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Shared
  alias Explorer.PolarsBackend.DataFrame, as: Eager
  alias Explorer.PolarsBackend.LazyFrame, as: PolarsLazyFrame

  import Explorer.PolarsBackend.Expression, only: [to_expr: 1, alias_expr: 2]

  defstruct resource: nil

  @type t :: %__MODULE__{resource: reference()}

  @behaviour Explorer.Backend.DataFrame

  # Conversion

  @impl true
  def lazy, do: __MODULE__

  @impl true
  def to_lazy(ldf), do: ldf

  @impl true
  def collect(ldf), do: Shared.apply_dataframe(ldf, ldf, :lf_collect, [])

  @impl true
  def from_tabular(tabular, dtypes),
    do: Eager.from_tabular(tabular, dtypes) |> Eager.to_lazy()

  @impl true
  def from_series(pairs), do: Eager.from_series(pairs) |> Eager.to_lazy()

  # Introspection

  @impl true
  def inspect(ldf, opts) do
    df = Shared.apply_dataframe(ldf, ldf, :lf_fetch, [opts.limit])
    Explorer.Backend.DataFrame.inspect(df, "LazyPolars", nil, opts)
  end

  # Single table verbs

  @impl true
  def head(ldf, rows), do: Shared.apply_dataframe(ldf, ldf, :lf_head, [rows])

  @impl true
  def tail(ldf, rows), do: Shared.apply_dataframe(ldf, ldf, :lf_tail, [rows])

  @impl true
  def select(ldf, out_ldf), do: Shared.apply_dataframe(ldf, out_ldf, :lf_select, [out_ldf.names])

  @impl true
  def slice(ldf, offset, length),
    do: Shared.apply_dataframe(ldf, ldf, :lf_slice, [offset, length])

  # IO

  @default_infer_schema_length 1000

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
        parse_dates,
        eol_delimiter
      ) do
    if columns do
      raise ArgumentError,
            "`columns` is not supported by Polars' lazy backend. " <>
              "Consider using `select/2` after reading the CSV"
    end

    infer_schema_length =
      if infer_schema_length == nil,
        do: max_rows || @default_infer_schema_length,
        else: infer_schema_length

    dtypes =
      Enum.map(dtypes, fn {column_name, dtype} ->
        {column_name, Shared.internal_from_dtype(dtype)}
      end)

    df =
      Native.lf_from_csv(
        filename,
        infer_schema_length,
        header?,
        max_rows,
        skip_rows,
        delimiter,
        true,
        dtypes,
        encoding,
        null_character,
        parse_dates,
        char_byte(eol_delimiter)
      )

    case df do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  defp char_byte(nil), do: nil
  defp char_byte(<<char::utf8>>), do: char

  @impl true
  def from_parquet(filename, max_rows, columns) do
    if columns do
      raise ArgumentError,
            "`columns` is not supported by Polars' lazy backend. " <>
              "Consider using `select/2` after reading the parquet file"
    end

    case Native.lf_from_parquet(filename, max_rows) do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def from_ndjson(filename, infer_schema_length, batch_size) do
    case Native.lf_from_ndjson(filename, infer_schema_length, batch_size) do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def from_ipc(filename, columns) do
    if columns do
      raise ArgumentError,
            "`columns` is not supported by Polars' lazy backend. " <>
              "Consider using `select/2` after reading the IPC file"
    end

    case Native.lf_from_ipc(filename) do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def load_csv(
        contents,
        dtypes,
        delimiter,
        null_character,
        skip_rows,
        header?,
        encoding,
        max_rows,
        columns,
        infer_schema_length,
        parse_dates,
        eol_delimiter
      ) do
    case Eager.load_csv(
           contents,
           dtypes,
           delimiter,
           null_character,
           skip_rows,
           header?,
           encoding,
           max_rows,
           columns,
           infer_schema_length,
           parse_dates,
           eol_delimiter
         ) do
      {:ok, df} -> {:ok, Eager.to_lazy(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def load_parquet(contents) do
    case Eager.load_parquet(contents) do
      {:ok, df} -> {:ok, Eager.to_lazy(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def load_ndjson(contents, infer_schema_length, batch_size) do
    case Eager.load_ndjson(contents, infer_schema_length, batch_size) do
      {:ok, df} -> {:ok, Eager.to_lazy(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def load_ipc(contents, columns) do
    case Eager.load_ipc(contents, columns) do
      {:ok, df} -> {:ok, Eager.to_lazy(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def load_ipc_stream(contents, columns) do
    case Eager.load_ipc_stream(contents, columns) do
      {:ok, df} -> {:ok, Eager.to_lazy(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def to_parquet(%DF{} = df, filename, {compression, level}) do
    case Native.lf_to_parquet(df.data, filename, Shared.parquet_compression(compression, level)) do
      {:ok, _} -> :ok
      {:error, _} = err -> err
    end
  end

  @impl true
  def filter_with(
        %DF{},
        %DF{groups: [_ | _]},
        %LazySeries{aggregation: true}
      ) do
    raise "filter_with/2 with groups and aggregations is not supported yet for lazy frames"
  end

  @impl true
  def filter_with(df, out_df, %LazySeries{} = lseries) do
    Shared.apply_dataframe(df, out_df, :lf_filter_with, [to_expr(lseries)])
  end

  @impl true
  def arrange_with(%DF{groups: []} = df, out_df, column_pairs) do
    {directions, expressions} =
      column_pairs
      |> Enum.map(fn {direction, lazy_series} -> {direction == :desc, to_expr(lazy_series)} end)
      |> Enum.unzip()

    Shared.apply_dataframe(df, out_df, :lf_arrange_with, [expressions, directions])
  end

  @impl true
  def arrange_with(_df, _out_df, _directions) do
    raise "arrange_with/2 with groups is not supported yet for lazy frames"
  end

  @impl true
  def distinct(%DF{} = df, %DF{} = out_df, columns) do
    maybe_columns_to_keep =
      if df.names != out_df.names, do: Enum.map(out_df.names, &Native.expr_column/1)

    Shared.apply_dataframe(df, out_df, :lf_distinct, [columns, maybe_columns_to_keep])
  end

  @impl true
  def mutate_with(%DF{} = df, %DF{groups: []} = out_df, column_pairs) do
    exprs =
      for {name, lazy_series} <- column_pairs do
        lazy_series
        |> to_expr()
        |> alias_expr(name)
      end

    Shared.apply_dataframe(df, out_df, :lf_mutate_with, [exprs])
  end

  @impl true
  def mutate_with(_df, _out_df, _mutations) do
    raise "mutate_with/2 with groups is not supported yet for lazy frames"
  end

  @impl true
  def rename(%DF{} = df, %DF{} = out_df, pairs),
    do: Shared.apply_dataframe(df, out_df, :lf_rename_columns, [pairs])

  @impl true
  def drop_nil(%DF{} = df, columns) do
    exprs = for col <- columns, do: Native.expr_column(col)
    Shared.apply_dataframe(df, df, :lf_drop_nils, [exprs])
  end

  @impl true
  def pivot_longer(%DF{} = df, %DF{} = out_df, cols_to_pivot, cols_to_keep, names_to, values_to),
    do:
      Shared.apply_dataframe(df, out_df, :lf_pivot_longer, [
        cols_to_keep,
        cols_to_pivot,
        names_to,
        values_to
      ])

  # Groups

  @impl true
  def summarise_with(%DF{groups: groups} = df, %DF{} = out_df, column_pairs) do
    exprs =
      for {name, lazy_series} <- column_pairs do
        original_expr = to_expr(lazy_series)
        alias_expr(original_expr, name)
      end

    groups_exprs = for group <- groups, do: Native.expr_column(group)

    Shared.apply_dataframe(df, out_df, :lf_summarise_with, [groups_exprs, exprs])
  end

  # Two or more tables

  @impl true
  def join(%DF{} = left, %DF{} = right, %DF{} = out_df, on, how)
      when is_list(on) and how in [:left, :inner, :cross, :outer] do
    how = Atom.to_string(how)

    {left_on, right_on} =
      on
      |> Enum.map(fn {left, right} -> {Native.expr_column(left), Native.expr_column(right)} end)
      |> Enum.unzip()

    Shared.apply_dataframe(left, out_df, :lf_join, [right.data, left_on, right_on, how, "_right"])
  end

  @impl true
  def join(%DF{} = left, %DF{} = right, %DF{} = out_df, on, :right)
      when is_list(on) do
    # Right join is the opposite of left join. So we swap the "on" keys, and swap the DFs
    # in the join.
    {left_on, right_on} =
      on
      |> Enum.map(fn {left, right} -> {Native.expr_column(right), Native.expr_column(left)} end)
      |> Enum.unzip()

    Shared.apply_dataframe(right, out_df, :lf_join, [
      left.data,
      left_on,
      right_on,
      "left",
      "_left"
    ])
  end

  @impl true
  def concat_rows([%DF{} | _t] = dfs, %DF{} = out_df) do
    polars_dfs = Enum.map(dfs, & &1.data)

    case Native.lf_concat_rows(polars_dfs) do
      {:ok, %PolarsLazyFrame{} = polars_df} -> %{out_df | data: polars_df}
      {:error, error} -> raise "could not concat dataframe rows. Reason: #{inspect(error)}"
    end
  end

  @impl true
  def concat_columns([%DF{} = head | tail], %DF{} = out_df) do
    Shared.apply_dataframe(head, out_df, :lf_concat_columns, [Enum.map(tail, & &1.data)])
  end

  not_available_funs = [
    describe: 2,
    dummies: 3,
    dump_csv: 3,
    dump_ipc: 2,
    dump_ipc_stream: 2,
    dump_ndjson: 1,
    dump_parquet: 2,
    from_ipc_stream: 2,
    mask: 2,
    n_rows: 1,
    pivot_wider: 5,
    pull: 2,
    put: 4,
    sample: 5,
    slice: 2,
    to_csv: 4,
    to_ipc: 3,
    to_ipc_stream: 3,
    to_ndjson: 2,
    to_rows: 2,
    to_rows_stream: 3
  ]

  for {fun, arity} <- not_available_funs do
    args = Macro.generate_arguments(arity, __MODULE__)

    @impl true
    def unquote(fun)(unquote_splicing(args)) do
      raise "the function `#{unquote(fun)}/#{unquote(arity)}` is not available for the Explorer.PolarsBackend.LazyFrame backend. " <>
              "Please use Explorer.DataFrame.collect/1 and then call this function upon the resultant dataframe"
    end
  end
end
