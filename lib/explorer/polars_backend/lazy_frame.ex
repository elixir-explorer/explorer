defmodule Explorer.PolarsBackend.LazyFrame do
  @moduledoc false

  alias Explorer.Backend.LazySeries
  alias Explorer.DataFrame, as: DF

  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Shared
  alias Explorer.PolarsBackend.DataFrame, as: Eager

  @type t :: %__MODULE__{resource: binary(), reference: reference()}

  defstruct resource: nil, reference: nil

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
  def head(ldf, rows), do: Shared.apply_dataframe(ldf, :lf_head, [rows])

  @impl true
  def tail(ldf, rows), do: Shared.apply_dataframe(ldf, :lf_tail, [rows])

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
        parse_dates
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
        parse_dates
      )

    case df do
      {:ok, df} -> {:ok, Shared.create_dataframe(df)}
      {:error, error} -> {:error, error}
    end
  end

  @impl true
  def from_parquet(filename) do
    case Native.lf_from_parquet(filename) do
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
        parse_dates
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
           parse_dates
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
  def filter_with(
        %DF{} = df,
        %DF{groups: [_ | _]} = out_df,
        %LazySeries{aggregation: true} = lseries
      ) do
    aggregation = Explorer.PolarsBackend.Expression.to_expr(lseries)

    Shared.apply_dataframe(df, out_df, :lf_filter_with_aggregation, [aggregation, out_df.groups])
  end

  @impl true
  def filter_with(df, out_df, %LazySeries{} = lseries) do
    expression = Explorer.PolarsBackend.Expression.to_expr(lseries)

    Shared.apply_dataframe(df, out_df, :lf_filter_with, [expression])
  end

  # Groups

  # TODO: Make the functions of non-implemented functions
  # explicit once the lazy interface is ready.
  funs =
    Explorer.Backend.DataFrame.behaviour_info(:callbacks) --
      (Explorer.Backend.DataFrame.behaviour_info(:optional_callbacks) ++
         Module.definitions_in(__MODULE__, :def))

  for {fun, arity} <- funs do
    args = Macro.generate_arguments(arity, __MODULE__)

    @impl true
    def unquote(fun)(unquote_splicing(args)) do
      raise "cannot perform operation on an Explorer.PolarsBackend.LazyFrame"
    end
  end
end
