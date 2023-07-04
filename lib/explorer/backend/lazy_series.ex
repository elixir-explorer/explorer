defmodule Explorer.Backend.LazySeries do
  @moduledoc """
  This is an opaque implementation of a Series.

  It represents an operation with its arguments.
  """
  alias Explorer.Series
  alias Explorer.Backend

  @behaviour Explorer.Backend.Series

  defstruct op: nil, args: [], aggregation: false

  @type t :: %__MODULE__{op: atom(), args: list(), aggregation: boolean()}

  @operations [
    # Element-wise
    lit: 1,
    all_equal: 2,
    equal: 2,
    not_equal: 2,
    greater: 2,
    greater_equal: 2,
    less: 2,
    less_equal: 2,
    is_nil: 1,
    is_not_nil: 1,
    is_finite: 1,
    is_infinite: 1,
    is_nan: 1,
    binary_and: 2,
    binary_or: 2,
    binary_in: 2,
    add: 2,
    subtract: 2,
    multiply: 2,
    divide: 2,
    quotient: 2,
    remainder: 2,
    pow: 2,
    log: 1,
    log: 2,
    exp: 1,
    fill_missing_with_value: 2,
    fill_missing_with_strategy: 2,
    format: 1,
    concat: 1,
    coalesce: 2,
    cast: 2,
    select: 3,
    abs: 1,
    strptime: 2,
    strftime: 2,

    # Trigonometric functions
    acos: 1,
    asin: 1,
    atan: 1,
    cos: 1,
    sin: 1,
    tan: 1,

    # Window functions
    cumulative_max: 2,
    cumulative_min: 2,
    cumulative_sum: 2,
    cumulative_product: 2,
    window_max: 5,
    window_mean: 5,
    window_min: 5,
    window_sum: 5,
    window_standard_deviation: 5,
    ewm_mean: 5,
    # Transformation
    column: 1,
    reverse: 1,
    argsort: 3,
    sort: 3,
    distinct: 1,
    unordered_distinct: 1,
    slice: 2,
    slice: 3,
    sample_n: 5,
    sample_frac: 5,
    head: 2,
    tail: 2,
    shift: 3,
    peaks: 2,
    unary_not: 1,
    # Aggregations
    sum: 1,
    min: 1,
    max: 1,
    argmin: 1,
    argmax: 1,
    mean: 1,
    median: 1,
    n_distinct: 1,
    variance: 1,
    standard_deviation: 1,
    quantile: 2,
    rank: 4,
    product: 1,
    first: 1,
    last: 1,
    count: 1,
    nil_count: 1,
    skew: 2,
    correlation: 3,
    covariance: 2,
    # Strings
    contains: 2,
    trim_leading: 1,
    trim_trailing: 1,
    trim: 1,
    upcase: 1,
    downcase: 1,
    # Float round
    round: 2,
    floor: 1,
    ceil: 1,
    # Date functions
    day_of_week: 1,
    month: 1,
    year: 1,
    hour: 1,
    minute: 1,
    second: 1
  ]

  @comparison_operations [:equal, :not_equal, :greater, :greater_equal, :less, :less_equal]

  @arithmetic_operations [:add, :subtract, :multiply, :pow, :quotient, :remainder]

  @aggregation_operations [
    :sum,
    :min,
    :max,
    :argmin,
    :argmax,
    :mean,
    :median,
    :variance,
    :standard_deviation,
    :count,
    :product,
    :nil_count,
    :size,
    :first,
    :last,
    :n_distinct
  ]

  @window_fun_operations [
    :window_max,
    :window_mean,
    :window_min,
    :window_sum
  ]
  @cumulative_operations [
    :cumulative_max,
    :cumulative_min,
    :cumulative_sum,
    :cumulative_product
  ]

  @float_predicates [:is_finite, :is_infinite, :is_nan]

  @doc false
  def new(op, args, aggregation \\ false) do
    %__MODULE__{op: op, args: args, aggregation: aggregation}
  end

  @doc false
  def operations, do: @operations

  @impl true
  def dtype(%Series{} = s), do: s.dtype

  @impl true
  def cast(%Series{} = s, dtype) when is_atom(dtype) do
    args = [lazy_series!(s), dtype]
    data = new(:cast, args, aggregations?(args))

    Backend.Series.new(data, dtype)
  end

  @impl true
  def divide(left, right) do
    args = [data!(left), data!(right)]
    data = new(:divide, args, aggregations?(args))
    Backend.Series.new(data, :float)
  end

  @impl true
  def lit(%Series{} = s), do: s

  def lit(value) do
    dtype = Explorer.Shared.check_types!([value])

    Backend.Series.new(value, dtype)
  end

  @impl true
  def from_list(list, dtype) when is_list(list) and is_atom(dtype) do
    data = new(:from_list, [list, dtype], false)

    Backend.Series.new(data, dtype)
  end

  @impl true
  def from_binary(binary, dtype) when is_binary(binary) and is_atom(dtype) do
    data = new(:from_binary, [binary, dtype], false)
    Backend.Series.new(data, dtype)
  end

  @impl true
  def reverse(%Series{} = s) do
    args = [lazy_series!(s)]
    data = new(:reverse, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def argsort(%Series{} = s, descending?, nils_last?) do
    args = [lazy_series!(s), descending?, nils_last?]
    data = new(:argsort, args, aggregations?(args))

    Backend.Series.new(data, :integer)
  end

  @impl true
  def sort(%Series{} = s, descending?, nils_last?) do
    args = [lazy_series!(s), descending?, nils_last?]
    data = new(:sort, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def distinct(%Series{} = s) do
    args = [lazy_series!(s)]
    data = new(:distinct, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def unordered_distinct(%Series{} = s) do
    args = [lazy_series!(s)]
    data = new(:unordered_distinct, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def slice(%Series{} = s, offset, length) do
    args = [lazy_series!(s), offset, length]
    data = new(:slice, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def slice(%Series{} = s, indices) when is_list(indices) do
    args = [lazy_series!(s), indices]
    data = new(:slice, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def slice(%Series{} = s, %Series{} = indices) do
    args = [lazy_series!(s), series_or_lazy_series!(indices)]
    data = new(:slice, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def head(%Series{} = s, length) do
    args = [lazy_series!(s), length]
    data = new(:head, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def tail(%Series{} = s, length) do
    args = [lazy_series!(s), length]
    data = new(:tail, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def shift(%Series{} = s, offset, default) do
    args = [lazy_series!(s), offset, default]
    data = new(:shift, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def peaks(%Series{} = s, min_or_max) when is_atom(min_or_max) do
    args = [lazy_series!(s), Atom.to_string(min_or_max)]
    data = new(:peaks, args, aggregations?(args))

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def fill_missing_with_strategy(%Series{} = s, strategy) do
    args = [lazy_series!(s), strategy]
    data = new(:fill_missing_with_strategy, args, aggregations?(args))
    dtype = if strategy == :mean, do: :float, else: s.dtype
    Backend.Series.new(data, dtype)
  end

  @impl true
  def fill_missing_with_value(%Series{} = s, value) do
    args = [lazy_series!(s), value]
    data = new(:fill_missing_with_value, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def sample(%Series{} = s, n, replacement, shuffle, seed) when is_integer(n) do
    args = [lazy_series!(s), n, replacement, shuffle, seed]
    data = new(:sample_n, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def sample(%Series{} = s, frac, replacement, shuffle, seed) when is_float(frac) do
    args = [lazy_series!(s), frac, replacement, shuffle, seed]
    data = new(:sample_frac, args, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  # Implements all the comparison operations that
  # accepts Series or number on both sides.
  #
  # It also handles the case for binaries and strings, creating
  # a one element series when binaries are present on either sides.
  for op <- @comparison_operations do
    @impl true
    def unquote(op)(left, right) do
      args = binary_args(left, right)
      data = new(unquote(op), args, aggregations?(args))

      Backend.Series.new(data, :boolean)
    end
  end

  defp binary_args(left, right) do
    case {left, right} do
      {%Series{}, %Series{}} ->
        [left.data, right.data]

      {%Series{dtype: dtype}, value}
      when dtype in [:binary, :string, :category] and is_binary(value) ->
        [left.data, from_list([value], dtype).data]

      {value, %Series{dtype: dtype}}
      when dtype in [:binary, :string, :category] and is_binary(value) ->
        [from_list([value], dtype).data, right.data]

      {%Series{}, other} ->
        [left.data, other]

      {other, %Series{}} ->
        [other, right.data]
    end
  end

  # These are also comparison operations, but they only accept `Series`.
  for op <- [:binary_and, :binary_or, :binary_in] do
    @impl true
    def unquote(op)(%Series{} = left, %Series{} = right) do
      args = [series_or_lazy_series!(left), series_or_lazy_series!(right)]
      data = new(unquote(op), args, aggregations?(args))

      Backend.Series.new(data, :boolean)
    end
  end

  for op <- @arithmetic_operations do
    @impl true
    def unquote(op)(left, right) do
      dtype = resolve_numeric_dtype([left, right])

      args = [data!(left), data!(right)]
      data = new(unquote(op), args, aggregations?(args))

      Backend.Series.new(data, dtype)
    end
  end

  for op <- @aggregation_operations do
    @impl true
    def unquote(op)(%Series{} = series) do
      args = [lazy_series!(series)]
      data = new(unquote(op), args, true)

      dtype = dtype_for_agg_operation(unquote(op), series)

      Backend.Series.new(data, dtype)
    end
  end

  for op <- @window_fun_operations do
    @impl true
    def unquote(op)(%Series{} = series, window_size, weights, min_periods, center) do
      args = [lazy_series!(series), window_size, weights, min_periods, center]

      if aggregations?(args), do: raise_agg_inside_window(unquote(op))

      dtype = resolve_numeric_dtype(unquote(op), [series | List.wrap(weights)])

      data = new(unquote(op), args, false)

      Backend.Series.new(data, dtype)
    end
  end

  @impl true
  def window_standard_deviation(%Series{} = series, window_size, weights, min_periods, center) do
    args = [lazy_series!(series), window_size, weights, min_periods, center]

    if aggregations?(args), do: raise_agg_inside_window(:window_standard_deviation)

    data = new(:window_standard_deviation, args, false)

    Backend.Series.new(data, :float)
  end

  for op <- @cumulative_operations do
    @impl true
    def unquote(op)(%Series{} = series, reverse) do
      args = [lazy_series!(series), reverse]

      if aggregations?(args), do: raise_agg_inside_window(unquote(op))

      data = new(unquote(op), args, false)

      Backend.Series.new(data, series.dtype)
    end
  end

  for predicate <- @float_predicates do
    @impl true
    def unquote(predicate)(%Series{} = series) do
      data = new(unquote(predicate), [lazy_series!(series)])

      Backend.Series.new(data, :boolean)
    end
  end

  defp raise_agg_inside_window(op) do
    raise "it's not possible to have an aggregation operation inside #{inspect(op)}, " <>
            "which is a window function"
  end

  @impl true
  def quantile(%Series{} = series, float) when is_float(float) do
    args = [lazy_series!(series), float]
    data = new(:quantile, args, true)

    Backend.Series.new(data, series.dtype)
  end

  @impl true
  def rank(%Series{} = series, method, descending, seed) do
    args = [series_or_lazy_series!(series), method, descending, seed]
    data = new(:rank, args, true)

    Backend.Series.new(data, series.dtype)
  end

  @impl true
  def skew(%Series{} = series, bias) do
    args = [series_or_lazy_series!(series), bias]
    data = new(:skew, args, true)

    Backend.Series.new(data, :float)
  end

  @impl true
  def correlation(%Series{} = left, %Series{} = right, ddof) do
    args = [series_or_lazy_series!(left), series_or_lazy_series!(right), ddof]
    data = new(:correlation, args, true)

    Backend.Series.new(data, :float)
  end

  @impl true
  def covariance(%Series{} = left, %Series{} = right) do
    args = [series_or_lazy_series!(left), series_or_lazy_series!(right)]
    data = new(:covariance, args, true)

    Backend.Series.new(data, :float)
  end

  @impl true
  def coalesce(%Series{} = left, %Series{} = right) do
    args = [series_or_lazy_series!(left), series_or_lazy_series!(right)]
    data = new(:coalesce, args, aggregations?(args))

    dtype =
      if left.dtype in [:float, :integer] do
        resolve_numeric_dtype([left, right])
      else
        left.dtype
      end

    Backend.Series.new(data, dtype)
  end

  @impl true
  def select(%Series{} = predicate, on_true, on_false) do
    args = [series_or_lazy_series!(predicate) | binary_args(on_true, on_false)]
    data = new(:select, args, aggregations?(args))

    dtype =
      case {on_true, on_false} do
        {%Series{dtype: dtype}, _} -> dtype
        {_, %Series{dtype: dtype}} -> dtype
        _ -> Explorer.Shared.check_types!([on_true])
      end

    dtype =
      if dtype in [:float, :integer] do
        resolve_numeric_dtype([on_true, on_false])
      else
        dtype
      end

    Backend.Series.new(data, dtype)
  end

  @impl true
  def format(list) do
    series_list = Enum.map(list, &series_or_lazy_series!/1)
    data = new(:format, [series_list], aggregations?(series_list))

    Backend.Series.new(data, :string)
  end

  @impl true
  def concat([%Series{} = head | _tail] = series) do
    series_list = Enum.map(series, &series_or_lazy_series!/1)
    data = new(:concat, [series_list], aggregations?(series_list))

    Backend.Series.new(data, head.dtype)
  end

  @impl true
  def day_of_week(%Series{} = s) do
    data = new(:day_of_week, [lazy_series!(s)])

    Backend.Series.new(data, :integer)
  end

  @impl true
  def month(%Series{} = s) do
    data = new(:month, [lazy_series!(s)])

    Backend.Series.new(data, :integer)
  end

  @impl true
  def year(%Series{} = s) do
    data = new(:year, [lazy_series!(s)])

    Backend.Series.new(data, :integer)
  end

  @impl true
  def hour(%Series{} = s) do
    data = new(:hour, [lazy_series!(s)])

    Backend.Series.new(data, :integer)
  end

  @impl true
  def minute(%Series{} = s) do
    data = new(:minute, [lazy_series!(s)])

    Backend.Series.new(data, :integer)
  end

  @impl true
  def second(%Series{} = s) do
    data = new(:second, [lazy_series!(s)])

    Backend.Series.new(data, :integer)
  end

  @impl true
  def ewm_mean(%Series{} = series, alpha, adjust, min_periods, ignore_nils) do
    args = [lazy_series!(series), alpha, adjust, min_periods, ignore_nils]

    if aggregations?(args), do: raise_agg_inside_window(:ewm_mean)

    data = new(:ewm_mean, args, false)

    Backend.Series.new(data, :float)
  end

  defp dtype_for_agg_operation(op, _) when op in [:count, :nil_count, :n_distinct], do: :integer

  defp dtype_for_agg_operation(op, series)
       when op in [:first, :last, :sum, :min, :max, :argmin, :argmax],
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
    data = new(:all_equal, args, aggregations?(args))

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def unary_not(%Series{} = series) do
    data = new(:unary_not, [lazy_series!(series)])

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def log(%Series{} = series) do
    data = new(:log, [lazy_series!(series)])

    Backend.Series.new(data, :float)
  end

  @impl true
  def log(%Series{} = series, base) do
    data = new(:log, [lazy_series!(series), base])

    Backend.Series.new(data, :float)
  end

  @impl true
  def exp(%Series{} = series) do
    data = new(:exp, [lazy_series!(series)])

    Backend.Series.new(data, :float)
  end

  @impl true
  def abs(%Series{} = series) do
    data = new(:abs, [lazy_series!(series)])

    Backend.Series.new(data, :float)
  end

  @impl true
  def strptime(%Series{} = series, format_string) do
    data = new(:strptime, [lazy_series!(series), format_string])

    Backend.Series.new(data, :datetime)
  end

  @impl true
  def strftime(%Series{} = series, format_string) do
    data = new(:strftime, [lazy_series!(series), format_string])

    Backend.Series.new(data, :datetime)
  end

  @impl true
  def sin(%Series{} = series) do
    data = new(:sin, [lazy_series!(series)])

    Backend.Series.new(data, :float)
  end

  @impl true
  def cos(%Series{} = series) do
    data = new(:cos, [lazy_series!(series)])

    Backend.Series.new(data, :float)
  end

  @impl true
  def tan(%Series{} = series) do
    data = new(:tan, [lazy_series!(series)])

    Backend.Series.new(data, :float)
  end

  @impl true
  def asin(%Series{} = series) do
    data = new(:asin, [lazy_series!(series)])

    Backend.Series.new(data, :float)
  end

  @impl true
  def acos(%Series{} = series) do
    data = new(:acos, [lazy_series!(series)])

    Backend.Series.new(data, :float)
  end

  @impl true
  def atan(%Series{} = series) do
    data = new(:atan, [lazy_series!(series)])

    Backend.Series.new(data, :float)
  end

  @impl true
  def inspect(series, opts) do
    alias Inspect.Algebra, as: A

    open = A.color("(", :list, opts)
    close = A.color(")", :list, opts)
    dtype = A.color("#{Series.dtype(series)}", :atom, opts)

    A.concat([
      A.color("LazySeries[???]", :atom, opts),
      A.line(),
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
    binary_or: :or,
    binary_in: :in,
    unary_not: :not
  }

  defp to_elixir_ast(%{op: op, args: args}) do
    {Map.get(@to_elixir_op, op, op), [], Enum.map(args, &to_elixir_ast/1)}
  end

  defp to_elixir_ast(other), do: other

  @impl true
  def size(_series) do
    raise """
    cannot retrieve the size of a lazy series, use count/1 instead
    """
  end

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

  @impl true
  def contains(series, pattern) do
    data = new(:contains, [lazy_series!(series), pattern])

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def upcase(series) do
    data = new(:upcase, [lazy_series!(series)])

    Backend.Series.new(data, :string)
  end

  @impl true
  def downcase(series) do
    data = new(:downcase, [lazy_series!(series)])

    Backend.Series.new(data, :string)
  end

  @impl true
  def trim(series) do
    data = new(:trim, [lazy_series!(series)])

    Backend.Series.new(data, :string)
  end

  @impl true
  def trim_leading(series) do
    data = new(:trim_leading, [lazy_series!(series)])

    Backend.Series.new(data, :string)
  end

  @impl true
  def trim_trailing(series) do
    data = new(:trim_trailing, [lazy_series!(series)])

    Backend.Series.new(data, :string)
  end

  @impl true
  def round(series, decimals) when is_integer(decimals) and decimals >= 0 do
    data = new(:round, [lazy_series!(series), decimals])

    Backend.Series.new(data, :float)
  end

  @impl true
  def floor(series) do
    data = new(:floor, [lazy_series!(series)])

    Backend.Series.new(data, :float)
  end

  @impl true
  def ceil(series) do
    data = new(:ceil, [lazy_series!(series)])

    Backend.Series.new(data, :float)
  end

  @remaining_non_lazy_operations [
    at: 2,
    at_every: 2,
    iotype: 1,
    categories: 1,
    categorise: 2,
    frequencies: 1,
    cut: 6,
    qcut: 6,
    mask: 2,
    to_iovec: 1,
    to_list: 1
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
