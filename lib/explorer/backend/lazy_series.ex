defmodule Explorer.Backend.LazySeries do
  @moduledoc """
  This is an opaque implementation of a Series.

  It represents an operation with its arguments.
  """
  alias Explorer.Series
  alias Explorer.Backend

  @behaviour Explorer.Backend.Series

  defstruct op: nil, args: []

  @type t :: %__MODULE__{op: atom(), args: list()}

  @operations [
    column: 1,
    eq: 2,
    neq: 2,
    gt: 2,
    gt_eq: 2,
    lt: 2,
    lt_eq: 2,
    binary_and: 2,
    binary_or: 2
  ]
  @comparison_ops_with_numbers [:eq, :neq, :gt, :gt_eq, :lt, :lt_eq]

  @doc false
  def new(op, args) do
    %__MODULE__{op: op, args: args}
  end

  @doc false
  def operations, do: @operations

  # Implements all the comparison operations that
  # accepts Series or number on the right-hand side.
  for op <- @comparison_ops_with_numbers do
    @impl true
    def unquote(op)(%Series{} = left, %Series{} = right), do: unquote(op)(left, right.data)

    def unquote(op)(%Series{} = left, value) do
      data = new(unquote(op), [left.data, value])

      Backend.Series.new(data, :boolean)
    end
  end

  for op <- [:binary_and, :binary_or] do
    @impl true
    def unquote(op)(%Series{} = left, %Series{} = right) do
      data = new(unquote(op), [left.data, right.data])

      Backend.Series.new(data, :boolean)
    end
  end

  @impl true
  def inspect(series, opts) do
    import Inspect.Algebra

    open = color("[", :list, opts)
    close = color("]", :list, opts)
    dtype = color("#{Series.dtype(series)}", :atom, opts)

    concat([
      color("LazySeries ", :atom, opts),
      dtype,
      line(),
      open,
      "???",
      close,
      line(),
      Code.quoted_to_algebra(to_elixir_ast(series.data))
    ])
  end

  @to_elixir_op %{
    add: :+,
    subtract: :-,
    eq: :==,
    neq: :!=,
    gt: :>,
    gt_eq: :>=,
    lt: :<,
    lt_eq: :<=,
    binary_and: :and,
    binary_or: :or
  }

  defp to_elixir_ast(%{op: op, args: args}) do
    {Map.get(@to_elixir_op, op, op), [], Enum.map(args, &to_elixir_ast/1)}
  end

  defp to_elixir_ast(other) do
    other
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
