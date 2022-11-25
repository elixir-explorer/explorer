defmodule Explorer.Backend.LazySeries do
  @moduledoc """
  This is an opaque implementation of a Series.

  It represents an operation with its arguments.
  """
  alias Explorer.Series
  alias Explorer.Backend

  @behaviour Explorer.Backend.Series

  # TODO: Validate if the window field is really required once we have distinct_with/arrange_with
  defstruct op: nil, args: [], aggregation: false, window: false

  @type t :: %__MODULE__{op: atom(), args: list(), aggregation: boolean(), window: boolean()}

  @operations [
    # Element-wise
    all_equal: 2,
    equal: 2,
    not_equal: 2,
    greater: 2,
    greater_equal: 2,
    less: 2,
    less_equal: 2,
    is_nil: 1,
    is_not_nil: 1,
    binary_and: 2,
    binary_or: 2,
    add: 2,
    subtract: 2,
    multiply: 2,
    divide: 2,
    quotient: 2,
    remainder: 2,
    pow: 2,
    fill_missing: 2,
    fill_missing_with_value: 2,
    concat: 2,
    coalesce: 2,
    cast: 2,
    select: 3,
    # Window functions
    cumulative_max: 2,
    cumulative_min: 2,
    cumulative_sum: 2,
    window_max: 5,
    window_mean: 5,
    window_min: 5,
    window_sum: 5,
    # Transformation
    column: 1,
    reverse: 1,
    argsort: 3,
    sort: 3,
    distinct: 1,
    unordered_distinct: 1,
    slice: 3,
    sample_n: 4,
    sample_frac: 4,
    head: 2,
    tail: 2,
    peaks: 2,
    # Aggregations
    sum: 1,
    min: 1,
    max: 1,
    mean: 1,
    median: 1,
    n_distinct: 1,
    variance: 1,
    standard_deviation: 1,
    quantile: 2,
    first: 1,
    last: 1,
    size: 1,
    count: 1
  ]

  @comparison_operations [:equal, :not_equal, :greater, :greater_equal, :less, :less_equal]

  @arithmetic_operations [:add, :subtract, :multiply, :divide, :pow, :quotient, :remainder]

  @aggregation_operations [
    :sum,
    :min,
    :max,
    :mean,
    :median,
    :variance,
    :standard_deviation,
    :count,
    :size,
    :first,
    :last,
    :n_distinct
  ]

  @window_fun_operations [:window_max, :window_mean, :window_min, :window_sum]
  @cumulative_operations [:cumulative_max, :cumulative_min, :cumulative_sum]

  @doc false
  def new(op, args, aggregation \\ false, window \\ false) do
    %__MODULE__{op: op, args: args, aggregation: aggregation, window: window}
  end

  @doc false
  def operations, do: @operations

  @impl true
  def dtype(%Series{} = s), do: s.dtype

  @impl true
  def cast(%Series{} = s, dtype) when is_atom(dtype) do
    args = [lazy_series!(s), dtype]
    data = new(:cast, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, dtype)
  end

  @impl true
  def from_list(list, dtype) when is_list(list) and is_atom(dtype) do
    data = new(:from_list, [list, dtype], false, false)

    Backend.Series.new(data, dtype)
  end

  @impl true
  def from_binary(binary, bintype, alignment)
      when is_binary(binary) and is_atom(bintype) and is_integer(alignment) do
    data = new(:from_binary, [binary, bintype, alignment], false, false)

    case bintype do
      :s -> Backend.Series.new(data, :integer)
      :u -> Backend.Series.new(data, :integer)
      :f -> Backend.Series.new(data, :float)
    end
  end

  @impl true
  def reverse(%Series{} = s) do
    args = [lazy_series!(s)]
    data = new(:reverse, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def argsort(%Series{} = s, descending?, nils_last?) do
    args = [lazy_series!(s), descending?, nils_last?]
    data = new(:argsort, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, :integer)
  end

  @impl true
  def sort(%Series{} = s, descending?, nils_last?) do
    args = [lazy_series!(s), descending?, nils_last?]
    data = new(:sort, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def distinct(%Series{} = s) do
    args = [lazy_series!(s)]
    data = new(:distinct, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def unordered_distinct(%Series{} = s) do
    args = [lazy_series!(s)]
    data = new(:unordered_distinct, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def slice(%Series{} = s, offset, length) do
    args = [lazy_series!(s), offset, length]
    data = new(:slice, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def head(%Series{} = s, length) do
    args = [lazy_series!(s), length]
    data = new(:head, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def tail(%Series{} = s, length) do
    args = [lazy_series!(s), length]
    data = new(:tail, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def peaks(%Series{} = s, min_or_max) when is_atom(min_or_max) do
    args = [lazy_series!(s), Atom.to_string(min_or_max)]
    data = new(:peaks, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def fill_missing(%Series{} = s, strategy) when is_atom(strategy) do
    args = [lazy_series!(s), Atom.to_string(strategy)]
    data = new(:fill_missing, args, aggregations?(args), window_functions?(args))

    dtype = if strategy == :mean, do: :float, else: s.dtype
    Backend.Series.new(data, dtype)
  end

  @impl true
  def fill_missing(%Series{} = s, value) do
    args = [lazy_series!(s), value]
    data = new(:fill_missing_with_value, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def sample(%Series{} = s, n, replacement, seed) when is_integer(n) do
    args = [lazy_series!(s), n, replacement, seed]
    data = new(:sample_n, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def sample(%Series{} = s, frac, replacement, seed) when is_float(frac) do
    args = [lazy_series!(s), frac, replacement, seed]
    data = new(:sample_frac, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, s.dtype)
  end

  # Implements all the comparison operations that
  # accepts Series or number on both sides.
  for op <- @comparison_operations do
    @impl true
    def unquote(op)(left, right) do
      args = [data!(left), data!(right)]
      data = new(unquote(op), args, aggregations?(args), window_functions?(args))

      Backend.Series.new(data, :boolean)
    end
  end

  # These are also comparison operations, but they only accept `Series`.
  for op <- [:binary_and, :binary_or] do
    @impl true
    def unquote(op)(%Series{} = left, %Series{} = right) do
      args = [series_or_lazy_series!(left), series_or_lazy_series!(right)]
      data = new(unquote(op), args, aggregations?(args), window_functions?(args))

      Backend.Series.new(data, :boolean)
    end
  end

  for op <- @arithmetic_operations do
    @impl true
    def unquote(op)(left, right) do
      dtype = resolve_numeric_dtype([left, right])

      args = [data!(left), data!(right)]
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

      if aggregations?(args), do: raise_agg_inside_window(unquote(op))

      dtype = resolve_numeric_dtype(unquote(op), [series | List.wrap(weights)])

      data = new(unquote(op), args, false, true)

      Backend.Series.new(data, dtype)
    end
  end

  for op <- @cumulative_operations do
    @impl true
    def unquote(op)(%Series{} = series, reverse) do
      args = [lazy_series!(series), reverse]

      if aggregations?(args), do: raise_agg_inside_window(unquote(op))

      data = new(unquote(op), args, false, true)

      Backend.Series.new(data, series.dtype)
    end
  end

  defp raise_agg_inside_window(op) do
    raise "it's not possible to have an aggregation operation inside #{inspect(op)}, " <>
            "which is a window function"
  end

  @impl true
  def quantile(%Series{} = series, float) when is_float(float) do
    args = [lazy_series!(series), float]
    data = new(:quantile, args, true, window_functions?(args))

    Backend.Series.new(data, series.dtype)
  end

  @impl true
  def coalesce(%Series{} = left, %Series{} = right) do
    args = [series_or_lazy_series!(left), series_or_lazy_series!(right)]
    data = new(:coalesce, args, aggregations?(args), window_functions?(args))

    dtype =
      if left.dtype in [:float, :integer] do
        resolve_numeric_dtype([left, right])
      else
        left.dtype
      end

    Backend.Series.new(data, dtype)
  end

  @impl true
  def select(%Series{} = predicate, %Series{} = on_true, %Series{} = on_false) do
    args = [
      series_or_lazy_series!(predicate),
      series_or_lazy_series!(on_true),
      series_or_lazy_series!(on_false)
    ]

    data = new(:select, args, aggregations?(args), window_functions?(args))

    dtype =
      if on_true.dtype in [:float, :integer] do
        resolve_numeric_dtype([on_true, on_false])
      else
        on_true.dtype
      end

    Backend.Series.new(data, dtype)
  end

  @impl true
  def concat(%Series{} = left, %Series{} = right) do
    args = [series_or_lazy_series!(left), series_or_lazy_series!(right)]
    data = new(:concat, args, aggregations?(args), window_functions?(args))

    dtype =
      if left.dtype in [:float, :integer] do
        resolve_numeric_dtype([left, right])
      else
        left.dtype
      end

    Backend.Series.new(data, dtype)
  end

  defp dtype_for_agg_operation(op, _) when op in [:count, :n_distinct], do: :integer

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

  defp resolve_numeric_dtype(:window_mean, _items), do: :float
  defp resolve_numeric_dtype(_op, items), do: resolve_numeric_dtype(items)

  # Returns the inner `data` if it's a lazy series. Otherwise raises an error.
  defp lazy_series!(series) do
    case series do
      %Series{data: %__MODULE__{}} ->
        data!(series)

      %Series{} ->
        raise ArgumentError, "expecting a LazySeries, but instead got #{inspect(series)}"
    end
  end

  defp series_or_lazy_series!(%Series{} = series), do: data!(series)

  defp data!(%Series{data: data}), do: data
  defp data!(value), do: value

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
  def all_equal(%Series{} = left, %Series{} = right) do
    args = [lazy_series!(left), lazy_series!(right)]
    data = new(:all_equal, args, aggregations?(args), window_functions?(args))

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def inspect(series, opts) do
    import Inspect.Algebra

    open = color("(", :list, opts)
    close = color(")", :list, opts)
    dtype = color("#{Series.dtype(series)}", :atom, opts)

    concat([
      color("LazySeries[???]", :atom, opts),
      line(),
      dtype,
      " ",
      open,
      Code.quoted_to_algebra(to_elixir_ast(series.data)),
      close
    ])
  end

  @to_elixir_op %{
    add: :+,
    subtract: :-,
    multiply: :*,
    divide: :/,
    pow: :**,
    equal: :==,
    not_equal: :!=,
    greater: :>,
    greater_equal: :>=,
    less: :<,
    less_equal: :<=,
    binary_and: :and,
    binary_or: :or
  }

  defp to_elixir_ast(%{op: op, args: args}) do
    {Map.get(@to_elixir_op, op, op), [], Enum.map(args, &to_elixir_ast/1)}
  end

  defp to_elixir_ast(other), do: other

  @impl true
  def transform(_series, _fun) do
    raise """
    #{unsupported(:transform, 2)}

    If you want to transform a column, you must do so outside of a query.
    For example, instead of:

        Explorer.DataFrame.mutate(df, new_column: transform(column, &String.upcase/1))

    You must write:

        Explorer.DataFrame.put(df, :new_column, Explorer.Series.transform(column, &String.upcase/1))

    However, keep in mind that in such cases you are loading the data into Elixir and
    serializing it back, which may be expensive for large datasets
    """
  end

  @remaining_non_lazy_operations [
    bintype: 1,
    fetch!: 2,
    mask: 2,
    slice: 2,
    take_every: 2,
    to_list: 1,
    to_iovec: 1
  ]

  for {fun, arity} <- @remaining_non_lazy_operations do
    args = Macro.generate_arguments(arity, __MODULE__)
    @impl true
    def unquote(fun)(unquote_splicing(args)), do: raise(unsupported(unquote(fun), unquote(arity)))
  end

  defp unsupported(fun, arity) do
    "cannot perform #{fun}/#{arity} operation on Explorer.Backend.LazySeries. " <>
      "Query operations work on lazy series and those support only a subset of series operations"
  end
end
