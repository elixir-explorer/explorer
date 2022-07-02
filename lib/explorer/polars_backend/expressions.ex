defmodule Explorer.PolarsBackend.Expressions do
  @moduledoc false
  # This module is responsible for translating the opaque LazySeries
  # into expressions that can be encoded to types in the Rustler side.
  # Type annotations are only for information propouses.

  alias Explorer.Backend.LazySeries

  def to_expressions(%LazySeries{op: :column, args: [name]}) do
    {:column, name}
  end

  def to_expressions(%LazySeries{op: :equal, args: [left, right]}) do
    left = to_expressions(left)
    right = to_expressions(right)

    op_name =
      cond do
        is_integer(right) -> :equal_int
        is_float(right) -> :equal_float
        true -> :equal
      end

    {op_name, left, right}
  end

  # TODO: filter what can be anything
  def to_expressions(anything), do: anything
end
