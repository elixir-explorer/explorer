defmodule Explorer.Backend.LazySeries do
  @moduledoc """
  This is an opaque implementation of a Series.

  It represents an operation with its arguments.
  """
  alias Explorer.Series
  alias Explorer.Backend

  @behaviour Explorer.Backend.Series

  defstruct op: nil, args: [], dtype: nil, aggregation: false

  @type t :: %__MODULE__{op: atom(), args: list(), dtype: any(), aggregation: boolean()}

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
    clip_float: 3,
    clip_integer: 3,

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
    window_median: 5,
    window_min: 5,
    window_sum: 5,
    window_standard_deviation: 5,
    ewm_mean: 5,
    ewm_standard_deviation: 6,
    ewm_variance: 6,
    # Transformation
    column: 1,
    reverse: 1,
    argsort: 5,
    sort: 5,
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
    mode: 1,
    n_distinct: 1,
    variance: 2,
    standard_deviation: 2,
    quantile: 2,
    rank: 4,
    product: 1,
    first: 1,
    last: 1,
    count: 1,
    nil_count: 1,
    size: 1,
    skew: 2,
    correlation: 4,
    covariance: 3,
    all: 1,
    any: 1,
    row_index: 1,
    # Strings
    contains: 2,
    replace: 3,
    lstrip: 2,
    rstrip: 2,
    strip: 2,
    upcase: 1,
    downcase: 1,
    substring: 3,
    split: 2,
    split_into: 3,
    json_decode: 2,
    json_path_match: 2,
    # Float round
    round: 2,
    floor: 1,
    ceil: 1,
    # Date functions
    day_of_week: 1,
    day_of_year: 1,
    week_of_year: 1,
    month: 1,
    year: 1,
    hour: 1,
    minute: 1,
    second: 1,
    # List functions
    join: 2,
    lengths: 1,
    member: 3,
    # Struct functions
    field: 2
  ]

  @comparison_operations [:equal, :not_equal, :greater, :greater_equal, :less, :less_equal]

  @basic_arithmetic_operations [:add, :subtract, :multiply, :divide, :pow]
  @other_arithmetic_operations [:quotient, :remainder]

  @aggregation_operations [
    :sum,
    :min,
    :max,
    :argmin,
    :argmax,
    :mean,
    :median,
    :mode,
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
    :window_median,
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
  def new(op, args, dtype, aggregation \\ false) do
    %__MODULE__{op: op, args: args, dtype: dtype, aggregation: aggregation}
  end

  @doc false
  def operations, do: @operations

  @impl true
  def dtype(%Series{} = s), do: s.dtype

  @impl true
  def cast(%Series{} = s, dtype) do
    args = [lazy_series!(s), dtype]
    data = new(:cast, args, dtype, aggregations?(args))
    Backend.Series.new(data, dtype)
  end

  @impl true
  def from_list(list, dtype) when is_list(list) do
    data = new(:from_list, [list, dtype], dtype, false)

    Backend.Series.new(data, dtype)
  end

  @impl true
  def from_binary(binary, dtype) when is_binary(binary) do
    data = new(:from_binary, [binary, dtype], dtype, false)
    Backend.Series.new(data, dtype)
  end

  @impl true
  def reverse(%Series{} = s) do
    args = [lazy_series!(s)]
    data = new(:reverse, args, s.dtype, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def argsort(%Series{} = s, descending?, maintain_order?, multithreaded?, nulls_last?) do
    args = [lazy_series!(s), descending?, maintain_order?, multithreaded?, nulls_last?]
    data = new(:argsort, args, {:u, 32}, aggregations?(args))

    Backend.Series.new(data, {:u, 32})
  end

  @impl true
  def sort(%Series{} = s, descending?, maintain_order?, multithreaded?, nulls_last?) do
    args = [lazy_series!(s), descending?, maintain_order?, multithreaded?, nulls_last?]
    data = new(:sort, args, s.dtype, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def distinct(%Series{} = s) do
    args = [lazy_series!(s)]
    data = new(:distinct, args, s.dtype, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def unordered_distinct(%Series{} = s) do
    args = [lazy_series!(s)]
    data = new(:unordered_distinct, args, s.dtype, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def slice(%Series{} = s, offset, length) do
    args = [lazy_series!(s), offset, length]
    data = new(:slice, args, s.dtype, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def slice(%Series{} = s, indices) when is_list(indices) do
    args = [lazy_series!(s), indices]
    data = new(:slice, args, s.dtype, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def slice(%Series{} = s, %Series{} = indices) do
    args = [lazy_series!(s), series_or_lazy_series!(indices)]
    data = new(:slice, args, s.dtype, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def head(%Series{} = s, length) do
    args = [lazy_series!(s), length]
    data = new(:head, args, s.dtype, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def tail(%Series{} = s, length) do
    args = [lazy_series!(s), length]
    data = new(:tail, args, s.dtype, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def shift(%Series{} = s, offset, default) do
    args = [lazy_series!(s), offset, default]
    data = new(:shift, args, s.dtype, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def peaks(%Series{} = s, min_or_max) when is_atom(min_or_max) do
    args = [lazy_series!(s), Atom.to_string(min_or_max)]
    data = new(:peaks, args, :boolean, aggregations?(args))

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def fill_missing_with_strategy(%Series{} = s, strategy) do
    args = [lazy_series!(s), strategy]
    dtype = if strategy == :mean, do: {:f, 64}, else: s.dtype
    data = new(:fill_missing_with_strategy, args, dtype, aggregations?(args))
    Backend.Series.new(data, dtype)
  end

  @impl true
  def fill_missing_with_value(%Series{} = s, value) do
    args = [lazy_series!(s), value]
    data = new(:fill_missing_with_value, args, s.dtype, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def sample(%Series{} = s, n, replacement, shuffle, seed) when is_integer(n) do
    args = [lazy_series!(s), n, replacement, shuffle, seed]
    data = new(:sample_n, args, s.dtype, aggregations?(args))

    Backend.Series.new(data, s.dtype)
  end

  @impl true
  def sample(%Series{} = s, frac, replacement, shuffle, seed) when is_float(frac) do
    args = [lazy_series!(s), frac, replacement, shuffle, seed]
    data = new(:sample_frac, args, s.dtype, aggregations?(args))

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
      data = new(unquote(op), args, :boolean, aggregations?(args))

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
      data = new(unquote(op), args, :boolean, aggregations?(args))

      Backend.Series.new(data, :boolean)
    end
  end

  for op <- @basic_arithmetic_operations do
    @impl true
    def unquote(op)(dtype, %Series{} = left, %Series{} = right) do
      args = [data!(left), data!(right)]
      data = new(unquote(op), args, dtype, aggregations?(args))

      Backend.Series.new(data, dtype)
    end
  end

  for op <- @other_arithmetic_operations do
    @impl true
    def unquote(op)(left, right) do
      dtype = resolve_numeric_dtype([left, right])

      args = [data!(left), data!(right)]
      data = new(unquote(op), args, dtype, aggregations?(args))

      Backend.Series.new(data, dtype)
    end
  end

  for op <- @aggregation_operations do
    @impl true
    def unquote(op)(%Series{} = series) do
      args = [lazy_series!(series)]
      dtype = dtype_for_agg_operation(unquote(op), series)
      data = new(unquote(op), args, dtype, true)

      Backend.Series.new(data, dtype)
    end
  end

  for op <- @window_fun_operations do
    @impl true
    def unquote(op)(%Series{} = series, window_size, weights, min_periods, center) do
      args = [lazy_series!(series), window_size, weights, min_periods, center]

      if aggregations?(args), do: raise_agg_inside_window(unquote(op))

      dtype = resolve_numeric_dtype(unquote(op), [series | List.wrap(weights)])

      data = new(unquote(op), args, dtype, false)

      Backend.Series.new(data, dtype)
    end
  end

  @impl true
  def window_standard_deviation(%Series{} = series, window_size, weights, min_periods, center) do
    args = [lazy_series!(series), window_size, weights, min_periods, center]

    if aggregations?(args), do: raise_agg_inside_window(:window_standard_deviation)

    data = new(:window_standard_deviation, args, {:f, 64}, false)

    Backend.Series.new(data, {:f, 64})
  end

  for op <- @cumulative_operations do
    @impl true
    def unquote(op)(%Series{} = series, reverse) do
      args = [lazy_series!(series), reverse]

      if aggregations?(args), do: raise_agg_inside_window(unquote(op))

      data = new(unquote(op), args, series.dtype, false)

      Backend.Series.new(data, series.dtype)
    end
  end

  for predicate <- @float_predicates do
    @impl true
    def unquote(predicate)(%Series{} = series) do
      data = new(unquote(predicate), [lazy_series!(series)], :boolean)

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
    # According to https://github.com/pola-rs/polars/issues/4796,
    # it's expected that quantile returns float for integer columns.
    dtype =
      if series.dtype in Explorer.Shared.integer_types() do
        {:f, 64}
      else
        series.dtype
      end

    data = new(:quantile, args, dtype, true)

    Backend.Series.new(data, dtype)
  end

  @impl true
  def rank(%Series{} = series, method, descending, seed) do
    args = [series_or_lazy_series!(series), method, descending, seed]

    target_dtype =
      if method == :ordinal do
        {:u, 32}
      else
        series.dtype
      end

    data = new(:rank, args, target_dtype, true)

    Backend.Series.new(data, target_dtype)
  end

  @impl true
  def skew(%Series{} = series, bias) do
    args = [series_or_lazy_series!(series), bias]
    data = new(:skew, args, {:f, 64}, true)

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def correlation(%Series{} = left, %Series{} = right, ddof, method) do
    args = [series_or_lazy_series!(left), series_or_lazy_series!(right), ddof, method]
    data = new(:correlation, args, {:f, 64}, true)

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def covariance(%Series{} = left, %Series{} = right, ddof \\ 1) do
    args = [series_or_lazy_series!(left), series_or_lazy_series!(right), ddof]
    data = new(:covariance, args, {:f, 64}, true)

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def variance(%Series{} = s, ddof \\ 1) do
    args = [series_or_lazy_series!(s), ddof]
    data = new(:variance, args, {:f, 64}, true)

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def standard_deviation(%Series{} = s, ddof \\ 1) do
    args = [series_or_lazy_series!(s), ddof]
    data = new(:standard_deviation, args, {:f, 64}, true)

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def all?(%Series{} = s) do
    args = [series_or_lazy_series!(s)]

    data = new(:all, args, :boolean, true)

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def any?(%Series{} = s) do
    args = [series_or_lazy_series!(s)]

    data = new(:any, args, :boolean, true)

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def coalesce(%Series{} = left, %Series{} = right) do
    args = [series_or_lazy_series!(left), series_or_lazy_series!(right)]

    dtype =
      if left.dtype in Explorer.Shared.numeric_types() do
        resolve_numeric_dtype([left, right])
      else
        left.dtype
      end

    data = new(:coalesce, args, dtype, aggregations?(args))

    Backend.Series.new(data, dtype)
  end

  @impl true
  def select(%Series{} = predicate, %{dtype: dtype} = on_true, on_false) do
    args = [series_or_lazy_series!(predicate) | binary_args(on_true, on_false)]

    dtype =
      if dtype in Explorer.Shared.numeric_types() do
        resolve_numeric_dtype([on_true, on_false])
      else
        dtype
      end

    data = new(:select, args, dtype, aggregations?(args))

    Backend.Series.new(data, dtype)
  end

  @impl true
  def format(list) do
    series_list =
      Enum.map(list, fn
        s when is_binary(s) -> s
        s -> series_or_lazy_series!(s)
      end)

    data = new(:format, [series_list], :string, aggregations?(series_list))

    Backend.Series.new(data, :string)
  end

  @impl true
  def concat([%Series{} = head | _tail] = series) do
    series_list = Enum.map(series, &series_or_lazy_series!/1)
    data = new(:concat, [series_list], head.dtype, aggregations?(series_list))

    Backend.Series.new(data, head.dtype)
  end

  @impl true
  def day_of_week(%Series{} = s) do
    data = new(:day_of_week, [lazy_series!(s)], {:s, 8})

    Backend.Series.new(data, {:s, 8})
  end

  @impl true
  def day_of_year(%Series{} = s) do
    data = new(:day_of_year, [lazy_series!(s)], {:s, 16})

    Backend.Series.new(data, {:s, 16})
  end

  @impl true
  def week_of_year(%Series{} = s) do
    data = new(:week_of_year, [lazy_series!(s)], {:s, 8})

    Backend.Series.new(data, {:s, 8})
  end

  @impl true
  def month(%Series{} = s) do
    data = new(:month, [lazy_series!(s)], {:s, 8})

    Backend.Series.new(data, {:s, 8})
  end

  @impl true
  def year(%Series{} = s) do
    data = new(:year, [lazy_series!(s)], {:s, 32})

    Backend.Series.new(data, {:s, 32})
  end

  @impl true
  def hour(%Series{} = s) do
    data = new(:hour, [lazy_series!(s)], {:s, 8})

    Backend.Series.new(data, {:s, 8})
  end

  @impl true
  def minute(%Series{} = s) do
    data = new(:minute, [lazy_series!(s)], {:s, 8})

    Backend.Series.new(data, {:s, 8})
  end

  @impl true
  def second(%Series{} = s) do
    data = new(:second, [lazy_series!(s)], {:s, 8})

    Backend.Series.new(data, {:s, 8})
  end

  @impl true
  def ewm_mean(%Series{} = series, alpha, adjust, min_periods, ignore_nils) do
    args = [lazy_series!(series), alpha, adjust, min_periods, ignore_nils]

    if aggregations?(args), do: raise_agg_inside_window(:ewm_mean)

    data = new(:ewm_mean, args, {:f, 64}, false)

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def ewm_standard_deviation(%Series{} = series, alpha, adjust, bias, min_periods, ignore_nils) do
    args = [lazy_series!(series), alpha, adjust, bias, min_periods, ignore_nils]

    if aggregations?(args), do: raise_agg_inside_window(:ewm_standard_deviation)

    data = new(:ewm_standard_deviation, args, {:f, 64}, false)

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def ewm_variance(%Series{} = series, alpha, adjust, bias, min_periods, ignore_nils) do
    args = [lazy_series!(series), alpha, adjust, bias, min_periods, ignore_nils]

    if aggregations?(args), do: raise_agg_inside_window(:ewm_variance)

    data = new(:ewm_variance, args, {:f, 64}, false)

    Backend.Series.new(data, {:f, 64})
  end

  defp dtype_for_agg_operation(op, _)
       when op in [:count, :nil_count, :size, :n_distinct, :argmin, :argmax],
       do: {:u, 32}

  defp dtype_for_agg_operation(op, series)
       when op in [:first, :last, :sum, :min, :max],
       do: series.dtype

  defp dtype_for_agg_operation(op, _) when op in [:all?, :any?], do: :boolean
  defp dtype_for_agg_operation(:mode, series), do: {:list, series.dtype}

  defp dtype_for_agg_operation(_, _), do: {:f, 64}

  defp resolve_numeric_dtype(items) do
    dtypes =
      for item <- items, uniq: true do
        case item do
          %Series{dtype: dtype} -> dtype
          other -> Explorer.Shared.dtype_from_list!([other])
        end
      end

    case dtypes do
      [dtype] ->
        if dtype in Explorer.Shared.numeric_types() do
          dtype
        else
          raise "invalid dtype for numeric items: #{inspect(dtype)}"
        end

      [_, _] ->
        {:f, 64}
    end
  end

  defp resolve_numeric_dtype(:window_mean, _items), do: {:f, 64}
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
    data = new(:is_nil, [lazy_series!(series)], :boolean)

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def is_not_nil(%Series{} = series) do
    data = new(:is_not_nil, [lazy_series!(series)], :boolean)

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def all_equal(%Series{} = left, %Series{} = right) do
    args = [lazy_series!(left), lazy_series!(right)]
    data = new(:all_equal, args, :boolean, aggregations?(args))

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def unary_not(%Series{} = series) do
    data = new(:unary_not, [lazy_series!(series)], :boolean)

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def log(%Series{} = series) do
    data = new(:log, [lazy_series!(series)], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def log(%Series{} = series, base) do
    data = new(:log, [lazy_series!(series), base], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def exp(%Series{} = series) do
    data = new(:exp, [lazy_series!(series)], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def abs(%Series{} = series) do
    data = new(:abs, [lazy_series!(series)], series.dtype)

    Backend.Series.new(data, series.dtype)
  end

  @impl true
  def strptime(%Series{} = series, format_string) do
    dtype = {:datetime, :microsecond}
    data = new(:strptime, [lazy_series!(series), format_string], dtype)

    Backend.Series.new(data, dtype)
  end

  @impl true
  def strftime(%Series{} = series, format_string) do
    dtype = :string
    data = new(:strftime, [lazy_series!(series), format_string], dtype)

    Backend.Series.new(data, dtype)
  end

  @impl true
  def clip(%Series{dtype: {s_or_u, _} = dtype} = series, min, max)
      when s_or_u in [:s, :u] and is_integer(min) and is_integer(max) do
    data = new(:clip_integer, [lazy_series!(series), min, max], dtype)

    Backend.Series.new(data, dtype)
  end

  def clip(%Series{} = series, min, max) do
    data = new(:clip_float, [lazy_series!(series), min * 1.0, max * 1.0], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def sin(%Series{} = series) do
    data = new(:sin, [lazy_series!(series)], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def cos(%Series{} = series) do
    data = new(:cos, [lazy_series!(series)], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def tan(%Series{} = series) do
    data = new(:tan, [lazy_series!(series)], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def asin(%Series{} = series) do
    data = new(:asin, [lazy_series!(series)], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def acos(%Series{} = series) do
    data = new(:acos, [lazy_series!(series)], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def atan(%Series{} = series) do
    data = new(:atan, [lazy_series!(series)], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def inspect(series, opts) do
    alias Inspect.Algebra, as: A

    open = A.color("(", :list, opts)
    close = A.color(")", :list, opts)

    dtype =
      series
      |> Series.dtype()
      |> Explorer.Shared.dtype_to_string()
      |> A.color(:atom, opts)

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

  defp to_elixir_ast(%__MODULE__{op: :from_list, args: [[single], _]}) do
    single
  end

  defp to_elixir_ast(%__MODULE__{op: op, args: args}) do
    {Map.get(@to_elixir_op, op, op), [], Enum.map(args, &to_elixir_ast/1)}
  end

  defp to_elixir_ast(%Explorer.PolarsBackend.Series{} = series) do
    series = Explorer.PolarsBackend.Shared.create_series(series)

    case Explorer.Series.size(series) do
      1 -> series[0]
      _ -> series
    end
  end

  defp to_elixir_ast(other), do: other

  @impl true
  def size(series) do
    data = new(:size, [lazy_series!(series)], {:u, 32})

    Backend.Series.new(data, {:u, 32})
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
    data = new(:contains, [lazy_series!(series), pattern], :boolean)

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def upcase(series) do
    data = new(:upcase, [lazy_series!(series)], :string)

    Backend.Series.new(data, :string)
  end

  @impl true
  def downcase(series) do
    data = new(:downcase, [lazy_series!(series)], :string)

    Backend.Series.new(data, :string)
  end

  @impl true
  def replace(series, pattern, replacement) do
    data = new(:replace, [lazy_series!(series), pattern, replacement], :string)

    Backend.Series.new(data, :string)
  end

  @impl true
  def strip(series, string) do
    data = new(:strip, [lazy_series!(series), string], :string)
    Backend.Series.new(data, :string)
  end

  @impl true
  def lstrip(series, string) do
    data = new(:lstrip, [lazy_series!(series), string], :string)

    Backend.Series.new(data, :string)
  end

  @impl true
  def rstrip(series, string) do
    data = new(:rstrip, [lazy_series!(series), string], :string)

    Backend.Series.new(data, :string)
  end

  @impl true
  def substring(series, offset, length) do
    data = new(:substring, [lazy_series!(series), offset, length], :string)

    Backend.Series.new(data, :string)
  end

  @impl true
  def split(series, by) do
    data = new(:split, [lazy_series!(series), by], :string)

    Backend.Series.new(data, {:list, :string})
  end

  @impl true
  def split_into(series, by, fields) do
    data = new(:split_into, [lazy_series!(series), by, fields], :string)

    Backend.Series.new(data, {:list, :struct})
  end

  @impl true
  def round(series, decimals) when is_integer(decimals) and decimals >= 0 do
    data = new(:round, [lazy_series!(series), decimals], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def floor(series) do
    data = new(:floor, [lazy_series!(series)], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def ceil(series) do
    data = new(:ceil, [lazy_series!(series)], {:f, 64})

    Backend.Series.new(data, {:f, 64})
  end

  @impl true
  def join(series, separator) do
    data = new(:join, [lazy_series!(series), separator], {:list, :string})

    Backend.Series.new(data, :string)
  end

  @impl true
  def lengths(series) do
    data = new(:lengths, [lazy_series!(series)], {:u, 32})

    Backend.Series.new(data, {:u, 32})
  end

  @impl true
  def member?(%Series{dtype: {:list, inner_dtype}} = series, value) do
    data = new(:member, [lazy_series!(series), value, inner_dtype], :boolean)

    Backend.Series.new(data, :boolean)
  end

  @impl true
  def field(%Series{dtype: {:struct, inner_dtype}} = series, name) do
    {^name, dtype} = List.keyfind!(inner_dtype, name, 0)
    data = new(:field, [lazy_series!(series), name], dtype)

    Backend.Series.new(data, dtype)
  end

  @impl true
  def json_decode(series, dtype) do
    data = new(:json_decode, [lazy_series!(series), dtype], dtype)

    Backend.Series.new(data, dtype)
  end

  @impl true
  def json_path_match(series, json_path) do
    data = new(:json_path_match, [lazy_series!(series), json_path], :string)

    Backend.Series.new(data, :string)
  end

  @impl true
  def row_index(series) do
    data = new(:row_index, [lazy_series!(series)], {:u, 32})

    Backend.Series.new(data, {:u, 32})
  end

  @remaining_non_lazy_operations [
    at: 2,
    at_every: 2,
    categories: 1,
    categorise: 2,
    frequencies: 1,
    cut: 5,
    qcut: 5,
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
