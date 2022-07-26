defmodule Explorer.PolarsBackend.Expression do
  @moduledoc false
  # This module is responsible for translating the opaque LazySeries
  # to polars expressions in the Rust side.

  alias Explorer.DataFrame
  alias Explorer.Backend.LazySeries
  alias Explorer.PolarsBackend.Native

  defstruct resource: nil, reference: nil

  @type t :: %__MODULE__{resource: binary(), reference: reference()}

  # Column is special
  def to_expr(%LazySeries{op: :column, args: [name]}) do
    Native.expr_column(name)
  end

  # We are going to generate all functions for each valid operation.
  for {op, arity} <-
        Explorer.Backend.LazySeries.operations() -- [column: 1] do
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

  def alias_expr(%__MODULE__{} = expr, alias_name) when is_binary(alias_name) do
    Native.expr_alias(expr, alias_name)
  end

  # Only for inspecting the expression in tests
  def describe_filter_plan(%DataFrame{data: polars_df}, %__MODULE__{} = expression) do
    Native.expr_describe_filter_plan(polars_df, expression)
  end
end
