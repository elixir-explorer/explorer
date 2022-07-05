defmodule Explorer.PolarsBackend.Expression do
  @moduledoc false
  # This module is responsible for translating the opaque LazySeries
  # into expressions that can be encoded to types in the Rustler side.
  # Type annotations are only for information propouses.

  alias Explorer.Backend.LazySeries

  def to_expr(%LazySeries{op: :column, args: [name]}) do
    {:column, name}
  end

  def to_expr(%LazySeries{op: :equal, args: [left, right]}) do
    left = to_expr(left)
    right = to_expr(right)

    {:equal, [left, right]}
  end

  # TODO: filter what can be anything
  def to_expr(anything), do: anything
end
