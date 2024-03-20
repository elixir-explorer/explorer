defmodule Explorer.PolarsBackend.LazyFrame do
  @moduledoc false

  alias Explorer.Backend.LazySeries
  alias Explorer.DataFrame, as: DF

  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Shared
  alias Explorer.PolarsBackend.DataFrame, as: Eager

  alias FSS.Local
  alias FSS.S3

  import Explorer.PolarsBackend.Expression, only: [to_expr: 1, alias_expr: 2]

  defstruct frame: nil, stack: []

  defmodule Frame do
    @moduledoc false

    # This struct represents the lazy frame from the initialization
    # of a lazy "pipeline". The idea is to be able to inspect it, but
    # not to modify it until we collect.
    # This struct is named ExLazyFrame in the Rust side.
    defstruct resource: nil
  end

  @type frame :: %Frame{resource: reference()}
  @type operation :: {name :: atom(), args :: list(any())}
  @type t :: %__MODULE__{frame: frame(), stack: list(operation())}

  @behaviour Explorer.Backend.DataFrame

  # Conversion

  @impl true
  def lazy, do: __MODULE__

  @impl true
  def lazy(ldf), do: ldf

  @impl true
  def collect(ldf) do
    {:ok, polars_eager_df} =
      ldf
      |> apply_operations()
      |> Native.lf_collect()

    %{ldf | data: polars_eager_df}
  end

  defp stack(ldf), do: Enum.reverse(ldf.data.stack)

  defp reset_stack(ldf) do
    %{ldf | data: %{ldf.data | stack: []}}
  end

  defp apply_operations(ldf) do
    ldf
    |> stack()
    |> Enum.reduce_while(ldf.data.frame, fn {operation, args}, polars_ldf ->
      args = normalise_operation_args(operation, args, polars_ldf)

      case apply(Native, operation, args) do
        {:ok, polars_ldf} ->
          {:cont, polars_ldf}

        {:error, error} when is_binary(error) ->
          {:halt, RuntimeError.exception(error)}
      end
    end)
  end

  defp normalise_operation_args(:lf_concat_rows, args, _polars_df), do: args
  defp normalise_operation_args(_operation, args, polars_ldf), do: [polars_ldf | args]

  defp push_operation(ldf, {operation, args}) do
    stack = [{operation, args} | ldf.data.stack]

    %{ldf | data: %{ldf.data | stack: stack}}
  end

  @impl true
  def from_tabular(tabular, dtypes),
    do: Eager.from_tabular(tabular, dtypes) |> Eager.lazy()

  @impl true
  def from_series(pairs), do: Eager.from_series(pairs) |> Eager.lazy()

  # Introspection

  @impl true
  def inspect(%{data: %{stack: []}} = ldf, opts) do
    case Native.lf_fetch(ldf.data.frame, opts.limit) do
      {:ok, df} ->
        df = Explorer.Backend.DataFrame.new(df, ldf.names, ldf.dtypes)
        df = %{df | groups: ldf.groups}

        Explorer.Backend.DataFrame.inspect(df, "LazyPolars", nil, opts)

      {:error, error} ->
        raise "inspection error: #{inspect(error)}"
    end
  end

  def inspect(ldf, opts) do
    Explorer.Backend.DataFrame.inspect(ldf, "LazyPolars (stale)", nil, opts, elide_columns: true)
  end

  # Single table verbs

  @impl true
  def head(ldf, rows), do: push_operation(ldf, {:lf_head, [rows]})

  @impl true
  def tail(ldf, rows), do: push_operation(ldf, {:lf_tail, [rows]})

  @impl true
  def select(_ldf, out_ldf), do: push_operation(out_ldf, {:lf_select, [out_ldf.names]})

  @impl true
  def slice(ldf, offset, length), do: push_operation(ldf, {:lf_slice, [offset, length]})

  # IO

  @impl true
  def from_query(conn, query, params) do
    with {:ok, df} <- Eager.from_query(conn, query, params) do
      {:ok, Eager.lazy(df)}
    end
  end

  @impl true
  def from_csv(
        %S3.Entry{},
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _
      ) do
    {:error,
     ArgumentError.exception("reading CSV from AWS S3 is not supported for Lazy dataframes")}
  end

  @impl true
  def from_csv(
        %Local.Entry{} = entry,
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
        eol_delimiter
      )
      when is_nil(columns) do
    infer_schema_length =
      if infer_schema_length == nil,
        do: max_rows,
        else: infer_schema_length

    result =
      Native.lf_from_csv(
        entry.path,
        infer_schema_length,
        header?,
        max_rows,
        skip_rows,
        skip_rows_after_header,
        delimiter,
        true,
        Map.to_list(dtypes),
        encoding,
        nil_values,
        parse_dates,
        char_byte(eol_delimiter)
      )

    case result do
      {:ok, polars_ldf} -> {:ok, Shared.create_dataframe(%__MODULE__{frame: polars_ldf})}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def from_csv(
        %Local.Entry{},
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _
      ) do
    {:error,
     ArgumentError.exception(
       "`columns` is not supported by Polars' lazy backend. " <>
         "Consider using `select/2` after reading the CSV"
     )}
  end

  defp char_byte(nil), do: nil
  defp char_byte(<<char::utf8>>), do: char

  @impl true
  def from_parquet(%S3.Entry{} = entry, max_rows, columns) do
    case Native.lf_from_parquet_cloud(entry, max_rows, columns) do
      {:ok, polars_ldf} -> {:ok, Shared.create_dataframe(%__MODULE__{frame: polars_ldf})}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def from_parquet(%Local.Entry{} = entry, max_rows, columns) do
    case Native.lf_from_parquet(entry.path, max_rows, columns) do
      {:ok, polars_ldf} -> {:ok, Shared.create_dataframe(%__MODULE__{frame: polars_ldf})}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def from_ndjson(%S3.Entry{}, _, _) do
    {:error,
     ArgumentError.exception("reading NDJSON from AWS S3 is not supported for Lazy dataframes")}
  end

  @impl true
  def from_ndjson(%Local.Entry{} = entry, infer_schema_length, batch_size) do
    case Native.lf_from_ndjson(entry.path, infer_schema_length, batch_size) do
      {:ok, polars_ldf} -> {:ok, Shared.create_dataframe(%__MODULE__{frame: polars_ldf})}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def from_ipc(%S3.Entry{}, _) do
    {:error,
     ArgumentError.exception("reading IPC from AWS S3 is not supported for Lazy dataframes")}
  end

  @impl true
  def from_ipc(%Local.Entry{} = entry, columns) when is_nil(columns) do
    case Native.lf_from_ipc(entry.path) do
      {:ok, polars_ldf} -> {:ok, Shared.create_dataframe(%__MODULE__{frame: polars_ldf})}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def from_ipc(%Local.Entry{}, _columns) do
    {:error,
     ArgumentError.exception(
       "`columns` is not supported by Polars' lazy backend. " <>
         "Consider using `select/2` after reading the IPC file"
     )}
  end

  @impl true
  def from_ipc_stream(%S3.Entry{}, _) do
    {:error,
     ArgumentError.exception(
       "reading IPC Stream from AWS S3 is not supported for Lazy dataframes"
     )}
  end

  @impl true
  def from_ipc_stream(%Local.Entry{} = fs_entry, columns) do
    with {:ok, df} <- Eager.from_ipc_stream(fs_entry, columns) do
      {:ok, Eager.lazy(df)}
    end
  end

  @impl true
  def load_csv(
        contents,
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
        eol_delimiter
      ) do
    with {:ok, df} <-
           Eager.load_csv(
             contents,
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
             eol_delimiter
           ) do
      {:ok, Eager.lazy(df)}
    end
  end

  @impl true
  def load_parquet(contents) do
    with {:ok, df} <- Eager.load_parquet(contents) do
      {:ok, Eager.lazy(df)}
    end
  end

  @impl true
  def load_ndjson(contents, infer_schema_length, batch_size) do
    with {:ok, df} <- Eager.load_ndjson(contents, infer_schema_length, batch_size) do
      {:ok, Eager.lazy(df)}
    end
  end

  @impl true
  def load_ipc(contents, columns) do
    with {:ok, df} <- Eager.load_ipc(contents, columns) do
      {:ok, Eager.lazy(df)}
    end
  end

  @impl true
  def load_ipc_stream(contents, columns) do
    with {:ok, df} <- Eager.load_ipc_stream(contents, columns) do
      {:ok, Eager.lazy(df)}
    end
  end

  @impl true
  def to_parquet(%DF{} = ldf, %Local.Entry{} = entry, {compression, level}, streaming) do
    polars_df = apply_operations(ldf)

    case Native.lf_to_parquet(
           polars_df,
           entry.path,
           Shared.parquet_compression(compression, level),
           streaming
         ) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_parquet(%DF{} = ldf, %S3.Entry{} = entry, {compression, level}, _streaming = true) do
    polars_df = apply_operations(ldf)

    case Native.lf_to_parquet_cloud(
           polars_df,
           entry,
           Shared.parquet_compression(compression, level)
         ) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_parquet(%DF{} = ldf, %S3.Entry{} = entry, compression, _streaming = false) do
    eager_df = collect(ldf)

    Eager.to_parquet(eager_df, entry, compression, false)
  end

  @impl true
  def to_ipc(%DF{} = ldf, %Local.Entry{} = entry, {compression, _level}, streaming) do
    polars_df = apply_operations(ldf)

    case Native.lf_to_ipc(polars_df, entry.path, Atom.to_string(compression), streaming) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_ipc(_df, %S3.Entry{}, _compression, _streaming = true) do
    {:error, ArgumentError.exception("streaming is not supported for writes to AWS S3")}
  end

  @impl true
  def to_ipc(%DF{} = ldf, %S3.Entry{} = entry, compression, _streaming = false) do
    eager_df = collect(ldf)

    Eager.to_ipc(eager_df, entry, compression, false)
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
  def filter_with(_df, out_df, %LazySeries{} = lseries) do
    push_operation(out_df, {:lf_filter_with, [to_expr(lseries)]})
  end

  @impl true
  def sort_with(
        %DF{groups: []},
        out_df,
        column_pairs,
        maintain_order?,
        multithreaded?,
        nulls_last?
      )
      when is_boolean(maintain_order?) and is_boolean(multithreaded?) and
             is_boolean(nulls_last?) do
    {directions, expressions} =
      column_pairs
      |> Enum.map(fn {direction, lazy_series} -> {direction == :desc, to_expr(lazy_series)} end)
      |> Enum.unzip()

    push_operation(
      out_df,
      {:lf_sort_with,
       [
         expressions,
         directions,
         maintain_order?,
         nulls_last?
       ]}
    )
  end

  @impl true
  def sort_with(_df, _out_df, _directions, _maintain_order?, _multithreaded?, _nulls_last?) do
    raise "sort_with/2 with groups is not supported yet for lazy frames"
  end

  @impl true
  def distinct(%DF{} = df, %DF{} = out_df, columns) do
    maybe_columns_to_keep =
      if df.names != out_df.names, do: Enum.map(out_df.names, &Native.expr_column/1)

    push_operation(out_df, {:lf_distinct, [columns, maybe_columns_to_keep]})
  end

  @impl true
  def mutate_with(%DF{}, %DF{groups: []} = out_df, column_pairs) do
    exprs =
      for {name, lazy_series} <- column_pairs do
        lazy_series
        |> to_expr()
        |> alias_expr(name)
      end

    push_operation(out_df, {:lf_mutate_with, [exprs]})
  end

  @impl true
  def mutate_with(_df, _out_df, _mutations) do
    raise "mutate_with/2 with groups is not supported yet for lazy frames"
  end

  @impl true
  def rename(%DF{}, %DF{} = out_df, pairs),
    do: push_operation(out_df, {:lf_rename_columns, [pairs]})

  @impl true
  def drop_nil(%DF{} = df, columns) do
    exprs = for col <- columns, do: Native.expr_column(col)
    push_operation(df, {:lf_drop_nils, [exprs]})
  end

  @impl true
  def pivot_longer(%DF{}, %DF{} = out_df, cols_to_pivot, cols_to_keep, names_to, values_to),
    do:
      push_operation(
        out_df,
        {:lf_pivot_longer,
         [
           cols_to_keep,
           cols_to_pivot,
           names_to,
           values_to
         ]}
      )

  @impl true
  def explode(%DF{}, %DF{} = out_df, columns),
    do: push_operation(out_df, {:lf_explode, [columns]})

  @impl true
  def unnest(%DF{}, %DF{} = out_df, columns),
    do: push_operation(out_df, {:lf_unnest, [columns]})

  # Groups

  @impl true
  def summarise_with(%DF{groups: groups}, %DF{} = out_df, column_pairs) do
    exprs =
      for {name, lazy_series} <- column_pairs do
        original_expr = to_expr(lazy_series)
        alias_expr(original_expr, name)
      end

    groups_exprs = for group <- groups, do: Native.expr_column(group)

    push_operation(out_df, {:lf_summarise_with, [groups_exprs, exprs]})
  end

  # Two or more tables

  @impl true
  def join(%DF{} = _left, %DF{} = right, %DF{} = out_df, on, how)
      when is_list(on) and how in [:left, :inner, :cross, :outer] do
    how = Atom.to_string(how)

    {left_on, right_on} =
      on
      |> Enum.map(fn {left, right} -> {Native.expr_column(left), Native.expr_column(right)} end)
      |> Enum.unzip()

    push_operation(
      out_df,
      {:lf_join, [apply_operations(right), left_on, right_on, how, "_right"]}
    )
  end

  @impl true
  def join(%DF{} = left, %DF{} = _right, %DF{} = out_df, on, :right)
      when is_list(on) do
    # Right join is the opposite of left join. So we swap the "on" keys, and swap the DFs
    # in the join.
    {left_on, right_on} =
      on
      |> Enum.map(fn {left, right} -> {Native.expr_column(right), Native.expr_column(left)} end)
      |> Enum.unzip()

    push_operation(
      out_df,
      {:lf_join,
       [
         apply_operations(left),
         left_on,
         right_on,
         "left",
         "_left"
       ]}
    )
  end

  @impl true
  def concat_rows([%DF{} | _tail] = dfs, %DF{} = out_df) do
    polars_dfs =
      Enum.map(dfs, &apply_operations/1)

    # Since we apply operations in all DFs, and out_df is pointing to the `head`,
    # we need to reset the operations for `out_df` (they were applied already).
    out_df
    |> reset_stack()
    |> push_operation({:lf_concat_rows, [polars_dfs]})
  end

  @impl true
  def concat_columns([%DF{} | tail], %DF{} = out_df) do
    # Out df is the "same" as `head`, so we ignore the first.
    polars_dfs = Enum.map(tail, &apply_operations/1)

    push_operation(out_df, {:lf_concat_columns, [polars_dfs]})
  end

  not_available_funs = [
    correlation: 4,
    covariance: 3,
    nil_count: 1,
    dummies: 3,
    dump_csv: 3,
    dump_ipc: 2,
    dump_ipc_stream: 2,
    dump_ndjson: 1,
    dump_parquet: 2,
    mask: 2,
    n_rows: 1,
    pivot_wider: 5,
    pull: 2,
    put: 4,
    sample: 5,
    slice: 2,
    to_csv: 4,
    to_ipc_stream: 3,
    to_ndjson: 2,
    to_rows: 2,
    to_rows_stream: 3,
    transpose: 4
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
