defmodule Explorer.PolarsBackend.LazyFrame do
  @moduledoc false

  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Shared

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
