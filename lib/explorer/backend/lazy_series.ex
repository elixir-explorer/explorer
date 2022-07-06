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
  def new(op, args) do
    %__MODULE__{op: op, args: args}
  end

  @impl true
  def eq(%Series{} = left, %Series{} = right), do: eq(left, right.data)

  def eq(%Series{dtype: left_dtype, data: left_lazy}, value) do
    data = new(:equal, [left_lazy, value])

    Backend.Series.new(data, left_dtype)
  end

  # TODO: handle nested series
  @impl true
  def inspect(series, opts) do
    import Inspect.Algebra

    open = color("[", :list, opts)
    close = color("]", :list, opts)
    dtype = color("#{Series.dtype(series)}", :atom, opts)

    data =
      container_doc(
        open,
        series.data.args,
        close,
        opts,
        &Explorer.Shared.to_string/2
      )

    concat([
      color("LazySeries ", :atom, opts),
      dtype,
      line(),
      open,
      "???",
      close,
      line(),
      color("Operation: ", :atom, opts),
      color("#{series.data.op}", :atom, opts),
      line(),
      color("Args: ", :atom, opts),
      data
    ])
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
