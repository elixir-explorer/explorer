defmodule Explorer.PolarsBackend.Expression do
  @moduledoc false
  # This module is responsible for translating the opaque LazySeries
  # to polars expressions in the Rust side.

  alias Explorer.DataFrame
  alias Explorer.Backend.LazySeries
  alias Explorer.PolarsBackend.Native

  defstruct resource: nil, reference: nil

  @type t :: %__MODULE__{resource: binary(), reference: reference()}

  def to_expr(%LazySeries{op: :column, args: [name]}) do
    Native.expr_column(name)
  end

  def to_expr(%LazySeries{op: :equal, args: [left, right]}) do
    left = to_expr(left)
    right = to_expr(right)

    Native.expr_equal(left, right)
  end

  def to_expr(number) when is_integer(number) do
    Native.expr_integer(number)
  end

  def to_expr(number) when is_float(number) do
    Native.expr_float(number)
  end

  # Only for inspecting the expression in tests
  def describe_filter_plan(%DataFrame{data: polars_df}, %__MODULE__{} = expression) do
    Native.expr_describe_filter_plan(polars_df, expression)
  end
end
