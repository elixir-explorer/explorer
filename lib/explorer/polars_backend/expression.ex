defmodule Explorer.PolarsBackend.Expression do
  @moduledoc false
  # This module is responsible for translating the opaque LazySeries
  # to polars expressions in the Rust side.

  alias Explorer.DataFrame
  alias Explorer.Backend.LazySeries
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries

  defstruct resource: nil, reference: nil

  @type t :: %__MODULE__{resource: binary(), reference: reference()}

  @all_expressions [
    add: 2,
    all_equal: 2,
    binary_and: 2,
    binary_or: 2,
    binary_in: 2,
    coalesce: 2,
    concat: 2,
    count: 1,
    distinct: 1,
    divide: 2,
    equal: 2,
    fill_missing_with_value: 2,
    first: 1,
    greater: 2,
    greater_equal: 2,
    is_nil: 1,
    is_not_nil: 1,
    is_finite: 1,
    is_infinite: 1,
    is_nan: 1,
    last: 1,
    less: 2,
    less_equal: 2,
    max: 1,
    mean: 1,
    median: 1,
    min: 1,
    multiply: 2,
    n_distinct: 1,
    nil_count: 1,
    not_equal: 2,
    unary_not: 1,
    pow: 2,
    quotient: 2,
    remainder: 2,
    reverse: 1,
    select: 3,
    standard_deviation: 1,
    subtract: 2,
    sum: 1,
    unordered_distinct: 1,
    variance: 1
  ]

  @first_only_expressions [
    quantile: 2,
    argsort: 3,
    sort: 3,
    slice: 3,
    head: 2,
    tail: 2,
    peaks: 2,
    sample_n: 4,
    sample_frac: 4,
    fill_missing: 2,

    # Window operations
    cumulative_max: 2,
    cumulative_min: 2,
    cumulative_sum: 2,
    window_max: 5,
    window_mean: 5,
    window_min: 5,
    window_sum: 5,

    # Strings
    contains: 2,
    trim: 1,
    trim_leading: 1,
    trim_trailing: 1,
    downcase: 1,
    upcase: 1
  ]

  @custom_expressions [
    cast: 2,
    from_list: 2,
    from_binary: 2,
    to_lazy: 1,
    shift: 3,
    column: 1
  ]

  missing =
    ((Explorer.Backend.LazySeries.operations() -- @all_expressions) -- @first_only_expressions) --
      @custom_expressions

  if missing != [] do
    raise ArgumentError, "missing #{inspect(__MODULE__)} nodes: #{inspect(missing)}"
  end

  def to_expr(%LazySeries{op: :cast, args: [lazy_series, dtype]}) do
    expr = to_expr(lazy_series)
    Native.expr_cast(expr, Atom.to_string(dtype))
  end

  def to_expr(%LazySeries{op: :from_list, args: [list, dtype]}) do
    series = Explorer.PolarsBackend.Shared.from_list(list, dtype)
    Native.expr_series(series)
  end

  def to_expr(%LazySeries{op: :from_binary, args: [binary, dtype]}) do
    series = Explorer.PolarsBackend.Shared.from_binary(binary, dtype)
    Native.expr_series(series)
  end

  def to_expr(%LazySeries{op: :to_lazy, args: [data]}) do
    to_expr(data)
  end

  def to_expr(%LazySeries{op: :shift, args: [lazy_series, offset, nil]}) do
    Native.expr_shift(to_expr(lazy_series), offset, nil)
  end

  def to_expr(%LazySeries{op: :column, args: [name]}) do
    Native.expr_column(name)
  end

  for {op, _arity} <- @first_only_expressions do
    expr_op = :"expr_#{op}"

    def to_expr(%LazySeries{op: unquote(op), args: [lazy_series | args]}) do
      expr = to_expr(lazy_series)

      apply(Native, unquote(expr_op), [expr | args])
    end
  end

  for {op, arity} <- @all_expressions do
    args = Macro.generate_arguments(arity, __MODULE__)

    updates =
      for arg <- args do
        quote do
          to_expr(unquote(arg))
        end
      end

    expr_op = :"expr_#{op}"

    def to_expr(%LazySeries{op: unquote(op), args: unquote(args)}) do
      Native.unquote(expr_op)(unquote_splicing(updates))
    end
  end

  def to_expr(bool) when is_boolean(bool), do: Native.expr_boolean(bool)
  def to_expr(binary) when is_binary(binary), do: Native.expr_string(binary)
  def to_expr(number) when is_integer(number), do: Native.expr_integer(number)
  def to_expr(number) when is_float(number), do: Native.expr_float(number)
  def to_expr(%Date{} = date), do: Native.expr_date(date)
  def to_expr(%NaiveDateTime{} = datetime), do: Native.expr_datetime(datetime)
  def to_expr(%PolarsSeries{} = polars_series), do: Native.expr_series(polars_series)

  # Used by Explorer.PolarsBackend.DataFrame
  def alias_expr(%__MODULE__{} = expr, alias_name) when is_binary(alias_name) do
    Native.expr_alias(expr, alias_name)
  end

  # Only for inspecting the expression in tests
  def describe_filter_plan(%DataFrame{data: polars_df}, %__MODULE__{} = expression) do
    Native.expr_describe_filter_plan(polars_df, expression)
  end
end
