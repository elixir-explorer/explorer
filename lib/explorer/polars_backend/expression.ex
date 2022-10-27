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

  @window_operations [
    cumulative_max: 2,
    cumulative_min: 2,
    cumulative_sum: 2,
    window_max: 5,
    window_mean: 5,
    window_min: 5,
    window_sum: 5
  ]

  @lazy_series_and_literal_args_funs [
                                       quantile: 2,
                                       argsort: 2,
                                       sort: 2,
                                       slice: 3,
                                       head: 2,
                                       tail: 2,
                                       peaks: 2,
                                       sample_n: 4,
                                       sample_frac: 4,
                                       fill_missing: 2
                                     ] ++
                                       @window_operations
  @special_operations [cast: 2, column: 1] ++ @lazy_series_and_literal_args_funs

  # Some operations are special because they don't receive all args as lazy series.
  # We define them first.

  def to_expr(%LazySeries{op: :cast, args: [lazy_series, dtype]}) do
    expr = to_expr(lazy_series)
    Native.expr_cast(expr, Atom.to_string(dtype))
  end

  def to_expr(%LazySeries{op: :column, args: [name]}) do
    Native.expr_column(name)
  end

  for {op, _arity} <- @lazy_series_and_literal_args_funs do
    expr_op = :"expr_#{op}"

    def to_expr(%LazySeries{op: unquote(op), args: [lazy_series | args]}) do
      expr = to_expr(lazy_series)

      apply(Native, unquote(expr_op), [expr | args])
    end
  end

  # We are going to generate all functions for each valid operation.
  for {op, arity} <-
        Explorer.Backend.LazySeries.operations() -- @special_operations do
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

  def alias_expr(%__MODULE__{} = expr, alias_name) when is_binary(alias_name) do
    Native.expr_alias(expr, alias_name)
  end

  # Only for inspecting the expression in tests
  def describe_filter_plan(%DataFrame{data: polars_df}, %__MODULE__{} = expression) do
    Native.expr_describe_filter_plan(polars_df, expression)
  end
end
