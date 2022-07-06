defmodule Explorer.Backend.LazySeries do
  @moduledoc """
  This is an opaque implementation of a Series.

  It represents an operation with its arguments.
  """
  alias Explorer.Series
  alias Explorer.Backend

  @behaviour Explorer.Backend.Series

  defstruct op: nil, args: []

  @doc false
  def new(dtype, op, args) do
    Backend.Series.new(%__MODULE__{op: op, args: args}, dtype)
  end

  @impl true
  def eq(%Series{} = left, %Series{} = right), do: eq(left, right.data)

  def eq(%Series{dtype: left_dtype, data: left_lazy}, value) do
    new(left_dtype, :equal, [left_lazy, value])
  end

  @impl true
  def inspect(_lseries, opts) do
    series = Explorer.Series.from_list([1, 2, 3])

    Backend.Series.inspect(series, "LazySeries", nil, opts)
  end

  # TODO: Make the functions of non-implemented functions
  # explicit once the lazy interface is ready.
  funs =
    Backend.Series.behaviour_info(:callbacks) --
      (Backend.Series.behaviour_info(:optional_callbacks) ++
         Module.definitions_in(__MODULE__, :def) ++ [{:inspect, 2}])

  for {fun, arity} <- funs do
    args = Macro.generate_arguments(arity, __MODULE__)

    @impl true
    def unquote(fun)(unquote_splicing(args)) do
      raise "cannot perform operation on an Explorer.Backend.LazySeries"
    end
  end
end
