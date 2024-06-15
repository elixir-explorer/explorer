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

  # This resource is going to be a "ResourceArc" on Rust side.
  defstruct resource: nil

  @type t :: %__MODULE__{resource: reference()}

  @behaviour Explorer.Backend.DataFrame

  # Conversion

  @impl true
  def lazy, do: __MODULE__

  @impl true
  def lazy(ldf), do: ldf

  @impl true
  def collect(ldf), do: Shared.apply_dataframe(ldf, ldf, :lf_collect, [])

  @impl true
  def from_tabular(tabular, dtypes),
    do: Eager.from_tabular(tabular, dtypes) |> Eager.lazy()

  @impl true
  def from_series(pairs), do: Eager.from_series(pairs) |> Eager.lazy()

  # Introspection

  @impl true
  def inspect(ldf, opts) when node(ldf.data.resource) != node() do
    Explorer.Backend.DataFrame.inspect(
      ldf,
      "LazyPolars (node: #{node(ldf.data.resource)})",
      nil,
      opts,
      elide_columns: true
    )
  end

  def inspect(ldf, opts) do
    case Native.lf_fetch(ldf.data, opts.limit) do
      {:ok, df} ->
        df = Explorer.Backend.DataFrame.new(df, ldf.names, ldf.dtypes)
        df = %{df | groups: ldf.groups}

        Explorer.Backend.DataFrame.inspect(df, "LazyPolars", nil, opts)

      {:error, error} ->
        raise "inspection error: #{inspect(error)}"
    end
  end

  # Single table verbs

  @impl true
  def head(ldf, rows),
    do: Shared.apply_dataframe(ldf, ldf, :lf_head, [rows, groups_exprs(ldf.groups)])

  @impl true
  def tail(ldf, rows),
    do: Shared.apply_dataframe(ldf, ldf, :lf_tail, [rows, groups_exprs(ldf.groups)])

  @impl true
  def select(ldf, out_ldf), do: Shared.apply_dataframe(ldf, out_ldf, :lf_select, [out_ldf.names])

  @impl true
  def slice(ldf, offset, length),
    do: Shared.apply_dataframe(ldf, ldf, :lf_slice, [offset, length, ldf.groups])

  defp groups_exprs(groups), do: Enum.map(groups, &Native.expr_column/1)

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
      {:ok, polars_ldf} -> {:ok, Shared.create_dataframe(polars_ldf)}
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
  def from_parquet(%S3.Entry{} = entry, max_rows, columns, _rechunk) do
    case Native.lf_from_parquet_cloud(entry, max_rows, columns) do
      {:ok, polars_ldf} -> {:ok, Shared.create_dataframe(polars_ldf)}
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def from_parquet(%Local.Entry{} = entry, max_rows, columns, _rechunk) do
    case Native.lf_from_parquet(entry.path, max_rows, columns) do
      {:ok, polars_ldf} -> {:ok, Shared.create_dataframe(polars_ldf)}
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
      {:ok, polars_ldf} -> {:ok, Shared.create_dataframe(polars_ldf)}
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
      {:ok, polars_ldf} -> {:ok, Shared.create_dataframe(polars_ldf)}
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
  def to_csv(%DF{} = ldf, %Local.Entry{} = entry, header?, delimiter, streaming) do
    <<delimiter::utf8>> = delimiter

    case Native.lf_to_csv(ldf.data, entry.path, header?, delimiter, streaming) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  @impl true
  def to_csv(%DF{} = ldf, %S3.Entry{} = entry, header?, delimiter, _streaming) do
    eager_df = collect(ldf)

    Eager.to_csv(eager_df, entry, header?, delimiter, false)
  end

  @impl true
  def to_parquet(%DF{} = ldf, %Local.Entry{} = entry, {compression, level}, streaming) do
    case Native.lf_to_parquet(
           ldf.data,
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
    case Native.lf_to_parquet_cloud(
           ldf.data,
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
    case Native.lf_to_ipc(ldf.data, entry.path, Atom.to_string(compression), streaming) do
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
  def filter_with(df, out_df, %LazySeries{} = lseries) do
    expression =
      if df.groups == [] do
        to_expr(lseries)
      else
        lseries
        |> to_expr()
        |> Native.expr_over(groups_exprs(df.groups))
      end

    Shared.apply_dataframe(df, out_df, :lf_filter_with, [expression])
  end

  @impl true
  def sort_with(
        %DF{} = df,
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

    if df.groups == [] do
      Shared.apply_dataframe(
        df,
        out_df,
        :lf_sort_with,
        [
          expressions,
          directions,
          maintain_order?,
          nulls_last?
        ]
      )
    else
      # TODO: log that we cannot set `maintain_order?` nor `nulls_last?`
      # This is a limitation of the grouped lazy frame.
      Shared.apply_dataframe(
        df,
        out_df,
        :lf_grouped_sort_with,
        [
          expressions,
          groups_exprs(df.groups),
          directions
        ]
      )
    end
  end

  @impl true
  def distinct(%DF{} = df, %DF{} = out_df, columns) do
    maybe_columns_to_keep =
      if df.names != out_df.names, do: Enum.map(out_df.names, &Native.expr_column/1)

    Shared.apply_dataframe(df, out_df, :lf_distinct, [columns, maybe_columns_to_keep])
  end

  @impl true
  def mutate_with(%DF{} = df, %DF{} = out_df, column_pairs) do
    maybe_over_groups_fun =
      if df.groups == [] do
        &Function.identity/1
      else
        fn expr -> Native.expr_over(expr, groups_exprs(df.groups)) end
      end

    exprs =
      for {name, lazy_series} <- column_pairs do
        lazy_series
        |> to_expr()
        |> then(maybe_over_groups_fun)
        |> alias_expr(name)
      end

    Shared.apply_dataframe(df, out_df, :lf_mutate_with, [exprs])
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
  def pivot_longer(%DF{} = df, %DF{} = out_df, cols_to_pivot, cols_to_keep, names_to, values_to) do
    Shared.apply_dataframe(
      df,
      out_df,
      :lf_pivot_longer,
      [
        cols_to_keep,
        cols_to_pivot,
        names_to,
        values_to
      ]
    )
  end

  @impl true
  def explode(%DF{} = df, %DF{} = out_df, columns),
    do: Shared.apply_dataframe(df, out_df, :lf_explode, [columns])

  @impl true
  def unnest(%DF{} = df, %DF{} = out_df, columns),
    do: Shared.apply_dataframe(df, out_df, :lf_unnest, [columns])

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

    Shared.apply_dataframe(
      left,
      out_df,
      :lf_join,
      [right.data, left_on, right_on, how, "_right"]
    )
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

    Shared.apply_dataframe(
      right,
      out_df,
      :lf_join,
      [
        left.data,
        left_on,
        right_on,
        "left",
        "_left"
      ]
    )
  end

  @impl true
  def concat_rows([%DF{} | _tail] = dfs, %DF{} = out_df) do
    polars_dfs = Enum.map(dfs, fn df -> select(df, out_df).data end)
    %__MODULE__{} = polars_df = Shared.apply(:lf_concat_rows, [polars_dfs])

    %{out_df | data: polars_df}
  end

  @impl true
  def concat_columns([%DF{} = head | tail], %DF{} = out_df) do
    Shared.apply_dataframe(head, out_df, :lf_concat_columns, [Enum.map(tail, & &1.data)])
  end

  @impl true
  def re_dtype(regex_as_string) when is_binary(regex_as_string) do
    Eager.re_dtype(regex_as_string)
  end

  @impl true
  def execute_sql(ldf, sql_string) do
    ldf.data
    |> Native.lf_execute_sql(sql_string)
    |> Shared.create_dataframe()
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
