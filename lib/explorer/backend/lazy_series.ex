defmodule Explorer.Backend.LazySeries do
  @moduledoc """
  This is an opaque implementation of a Series.

  It represents an operation with its arguments.
  """
  alias Explorer.Series
  alias Explorer.Backend

  @behaviour Explorer.Backend.Series

  defstruct op: nil, args: [], aggregation: false, window: false

  @type t :: %__MODULE__{op: atom(), args: list(), aggregation: boolean(), window: boolean()}

  @operations [
    column: 1,
    # Comparisons
    eq: 2,
    neq: 2,
    gt: 2,
    gt_eq: 2,
    lt: 2,
    lt_eq: 2,
    is_nil: 1,
    is_not_nil: 1,
    binary_and: 2,
    binary_or: 2,
    # Arithmetics
    add: 2,
    subtract: 2,
    multiply: 2,
    divide: 2,
    pow: 2,
    # Slice and dice
    coalesce: 2,
    # Window functions
    cumulative_max: 2,
    cumulative_min: 2,
    cumulative_sum: 2,
    window_max: 5,
    window_mean: 5,
    window_min: 5,
    window_sum: 5,
    # Aggregations
    sum: 1,
    min: 1,
    max: 1,
    mean: 1,
    median: 1,
    var: 1,
    std: 1,
    quantile: 2,
    first: 1,
    last: 1,
    count: 1
  ]

  @comparison_operations [:eq, :neq, :gt, :gt_eq, :lt, :lt_eq]

  @arithmetic_operations [:add, :subtract, :multiply, :divide, :pow]

  @aggregation_operations [:sum, :min, :max, :mean, :median, :var, :std, :count, :first, :last]

  @window_fun_operations [:window_max, :window_mean, :window_min, :window_sum]
  @cumulative_operations [:cumulative_max, :cumulative_min, :cumulative_sum]

  @doc false
  def new(op, args, aggregation \\ false, window \\ false) do
    %__MODULE__{op: op, args: args, aggregation: aggregation, window: window}
  end

  @doc false
  def operations, do: @operations

  @doc false
  def window_operations, do: @cumulative_operations ++ @window_fun_operations

  # Implements all the comparison operations that
  # accepts Series or number on the right-hand side.
  for op <- @comparison_operations do
    @impl true
    def unquote(op)(%Series{} = left, %Series{} = right),
      do: unquote(op)(left, lazy_series!(right))

    def unquote(op)(%Series{} = left, value) do
      args = [lazy_series!(left), value]
      data = new(unquote(op), args, aggregations?(args), window_functions?(args))

      Backend.Series.new(data, :boolean)
    end
  end

  # These are also comparison operations, but they only accept `Series`.
  for op <- [:binary_and, :binary_or] do
    @impl true
    def unquote(op)(%Series{} = left, %Series{} = right) do
      args = [lazy_series!(left), lazy_series!(right)]
      data = new(unquote(op), args, aggregations?(args), window_functions?(args))

      Backend.Series.new(data, :boolean)
    end
  end

  for op <- @arithmetic_operations do
    @impl true
    def unquote(op)(%Series{} = left, value_or_series) do
      dtype = resolve_numeric_dtype([left, value_or_series])

      value = with %Series{} <- value_or_series, do: lazy_series!(value_or_series)

      args = [lazy_series!(left), value]
      data = new(unquote(op), args, aggregations?(args), window_functions?(args))

      Backend.Series.new(data, dtype)
    end
  end

  for op <- @aggregation_operations do
    @impl true
    def unquote(op)(%Series{} = series) do
      args = [lazy_series!(series)]
      data = new(unquote(op), args, true, window_functions?(args))

      dtype = dtype_for_agg_operation(unquote(op), series)

      Backend.Series.new(data, dtype)
    end
  end

  for op <- @window_fun_operations do
    @impl true
    def unquote(op)(%Series{} = series, window_size, opts) do
      weights = Keyword.fetch!(opts, :weights)
      min_periods = Keyword.fetch!(opts, :min_periods)
      center = Keyword.fetch!(opts, :center)

      args = [lazy_series!(series), window_size, weights, min_periods, center]

      if aggregations?(args) do
        raise "it's not possible to have an aggregation operation inside a window function"
      end

      dtype = resolve_numeric_dtype([series | List.wrap(weights)])

      data = new(unquote(op), args, false, true)

      Backend.Series.new(data, dtype)
    end
  end

  for op <- @cumulative_operations do
    @impl true
    def unquote(op)(%Series{} = series, reverse) do
      args = [lazy_series!(series), reverse]

      if aggregations?(args) do
        raise "it's not possible to have an aggregation operation inside a window function"
      end

      data = new(unquote(op), args, false, true)

      Backend.Series.new(data, series.dtype)
    end
  end

  @impl true
  def quantile(%Series{} = series, float) when is_float(float) do
    args = [lazy_series!(series), float]
    data = new(:quantile, args, true, window_functions?(args))

    Backend.Series.new(data, series.dtype)
  end

  @impl true
  def coalesce(%Series{} = left, %Series{} = right) do
    args = [lazy_series!(left), lazy_series!(right)]
    data = new(:coalesce, args, aggregations?(args), window_functions?(args))

    dtype =
      if left.dtype in [:float, :integer] do
        resolve_numeric_dtype([left, right])
      else
        left.dtype
      end

    Backend.Series.new(data, dtype)
  end

  defp dtype_for_agg_operation(:count, _), do: :integer

  defp dtype_for_agg_operation(op, series) when op in [:first, :last, :sum, :min, :max],
    do: series.dtype

  defp dtype_for_agg_operation(_, _), do: :float

  defp resolve_numeric_dtype(items) do
    dtypes =
      for item <- items, uniq: true do
        case item do
          %Series{dtype: dtype} -> dtype
          other -> Explorer.Shared.check_types!([other])
        end
      end

    case dtypes do
      [dtype] when dtype in [:integer, :float] -> dtype
      [_, _] -> :float
    end
  end

  # Returns the inner `data` if it's a lazy series. Otherwise raises an error.
  defp lazy_series!(series) do
    case series do
      %Series{data: %__MODULE__{} = lazy} ->
        lazy

      %Series{} ->
        raise ArgumentError, "expecting a LazySeries, but instead got #{inspect(series)}"
    end
  end

  defp aggregations?(args) do
    Enum.any?(args, fn
      %__MODULE__{aggregation: is_agg} -> is_agg
      _other -> false
    end)
  end

  defp window_functions?(args) do
    Enum.any?(args, fn
      %__MODULE__{window: is_window} -> is_window
      _other -> false
    end)
  end

  @impl true
  def is_nil(%Series{} = series) do
    data = new(:is_nil, [lazy_series!(series)])

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def is_not_nil(%Series{} = series) do
    data = new(:is_not_nil, [lazy_series!(series)])

    Backend.Series.new(data, :boolean)
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
    multiply: :*,
    divide: :/,
    pow: :**,
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

  defp to_elixir_ast(other), do: other

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
