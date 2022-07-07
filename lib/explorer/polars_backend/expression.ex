defmodule Explorer.PolarsBackend.Expression do
  @moduledoc false
  # This module is responsible for translating the opaque LazySeries
  # to polars expressions in the Rust side.

  alias Explorer.DataFrame
  alias Explorer.Backend.LazySeries
  alias Explorer.PolarsBackend.Native

  defstruct resource: nil, reference: nil

  @type t :: %__MODULE__{resource: binary(), reference: reference()}

  # We are going to generate all functions for each valid operation.
  for {op, arity} <- Explorer.Backend.LazySeries.operations() do
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

  def to_expr(number) when is_integer(number), do: Native.expr_integer(number)
  def to_expr(number) when is_float(number), do: Native.expr_float(number)
  def to_expr(binary) when is_binary(binary), do: binary

  # Only for inspecting the expression in tests
  def describe_filter_plan(%DataFrame{data: polars_df}, %__MODULE__{} = expression) do
    Native.expr_describe_filter_plan(polars_df, expression)
  end
end
