defmodule Explorer.Backend.LazyFrame do
  @moduledoc """
  Represents a dataframe in an opaque lazy format.
  """

  alias Explorer.Backend
  alias Explorer.Backend.LazySeries

  defstruct dtypes: %{}, names: [], original: nil

  @type t :: %__MODULE__{
          dtypes: Backend.DataFrame.dtypes(),
          names: Backend.DataFrame.column_name()
        }
  @behaviour Backend.DataFrame

  @doc false
  def new(df) do
    %__MODULE__{names: df.names, dtypes: df.dtypes, original: df}
  end

  @impl true
  def lazy, do: __MODULE__

  @impl true
  def to_lazy(ldf), do: ldf

  @impl true
  def inspect(ldf, opts) do
    Backend.DataFrame.inspect(ldf.data.original, "OpaqueLazyFrame", nil, opts)
  end

  @impl true
  def pull(df, column) do
    dtype_for_column = df.dtypes[column]
    data = LazySeries.new(:column, [column])
    Backend.Series.new(data, dtype_for_column)
  end

  # TODO: Make the functions of non-implemented functions
  # explicit once the lazy interface is ready.
  funs =
    Backend.DataFrame.behaviour_info(:callbacks) --
      (Backend.DataFrame.behaviour_info(:optional_callbacks) ++
         Module.definitions_in(__MODULE__, :def) ++ [{:inspect, 2}])

  for {fun, arity} <- funs do
    args = Macro.generate_arguments(arity, __MODULE__)

    @impl true
    def unquote(fun)(unquote_splicing(args)) do
      raise "cannot perform operation on an Explorer.Backend.LazyFrame"
    end
  end
end
