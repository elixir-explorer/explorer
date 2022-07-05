defmodule Explorer.PolarsBackend.Expression do
  @moduledoc false
  # This module is responsible for translating the opaque LazySeries
  # into expressions that can be encoded to types in the Rustler side.
  # Type annotations are only for information propouses.

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

  # TODO: filter what can be anything
  def to_expr(anything), do: anything
end
