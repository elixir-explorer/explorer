defmodule Explorer.Backend.LazyFrame do
  @moduledoc """
  Represents a dataframe in a lazy format.
  """

  defstruct dtypes: %{}, names: []
  alias Explorer.Backend.LazySeries

  @behaviour Explorer.Backend.DataFrame

  @doc false
  def new(df) do
    %__MODULE__{names: df.names, dtypes: df.dtypes}
  end

  @impl true
  def lazy, do: __MODULE__

  @impl true
  def to_lazy(ldf), do: ldf

  @impl true
  def inspect(_ldf, opts) do
    df = Explorer.DataFrame.new(foo: [1, 2, 3], bar: ["a", "b", "c"])
    Explorer.Backend.DataFrame.inspect(df, "LazyFrame", nil, opts)
  end

  @impl true
  def pull(df, column) do
    dtype_for_column = df.dtypes[column]
    LazySeries.new(dtype_for_column, :column, [column])
  end

  # TODO: Make the functions of non-implemented functions
  # explicit once the lazy interface is ready.
  funs =
    Explorer.Backend.DataFrame.behaviour_info(:callbacks) --
      (Explorer.Backend.DataFrame.behaviour_info(:optional_callbacks) ++
         Module.definitions_in(__MODULE__, :def) ++ [{:inspect, 2}])

  for {fun, arity} <- funs do
    args = Macro.generate_arguments(arity, __MODULE__)

    @impl true
    def unquote(fun)(unquote_splicing(args)) do
      raise "cannot perform operation on an Explorer.Backend.LazyFrame"
    end
  end
end
