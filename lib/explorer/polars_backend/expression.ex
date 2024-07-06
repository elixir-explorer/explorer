defmodule Explorer.PolarsBackend.Expression do
  @moduledoc false
  # This module is responsible for translating the opaque LazySeries
  # to polars expressions in the Rust side.

  alias Explorer.DataFrame
  alias Explorer.Backend.LazySeries
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries

  defstruct resource: nil

  @type t :: %__MODULE__{resource: reference()}

  def to_expr(nil), do: Native.expr_nil()
  def to_expr(bool) when is_boolean(bool), do: Native.expr_boolean(bool)
  def to_expr(atom) when is_atom(atom), do: Native.expr_atom(Atom.to_string(atom))
  def to_expr(binary) when is_binary(binary), do: Native.expr_string(binary)
  def to_expr(number) when is_integer(number), do: Native.expr_integer(number)
  def to_expr(number) when is_float(number), do: Native.expr_float(number)
  def to_expr(%Date{} = date), do: Native.expr_date(date)
  def to_expr(%NaiveDateTime{} = naive_datetime), do: Native.expr_naive_datetime(naive_datetime)
  # def to_expr(%DateTime{} = datetime), do: Native.expr_datetime(datetime)
  def to_expr(%Explorer.Duration{} = duration), do: Native.expr_duration(duration)
  def to_expr(%PolarsSeries{} = polars_series), do: Native.expr_series(polars_series)
  # TODO: move the unwrapping upstream so this module can be unaware of the need.
  # See: test/explorer/data_frame_test.exs:"filter_with/2"."filter columns with equal comparison"
  def to_expr(%Explorer.Series{data: %PolarsSeries{} = polars_series}), do: to_expr(polars_series)

  def to_expr(map) when is_map(map) and not is_struct(map) do
    expr_list =
      Enum.map(map, fn {name, series} ->
        series |> to_expr() |> Native.expr_alias(name)
      end)

    Native.expr_struct(expr_list)
  end

  def to_expr(%LazySeries{op: :col, args: [col]}), do: Native.expr_col(col)

  # TODO: remove the `:column` op in favor of `:col`.
  def to_expr(%LazySeries{op: :column, args: [col]}), do: Native.expr_col(col)

  def to_expr(%LazySeries{op: :lit, args: [lit]}), do: to_expr(lit)

  # TODO: generically handle ops whose only arg is a list of args?
  def to_expr(%LazySeries{op: :format, args: [args]}) when is_list(args) do
    apply(Native, :expr_format, [Enum.map(args, &to_expr/1)])
  end

  # The trailing arguments to these functions should not be converted to exprs.

  @cumulative_ops [:cumulative_max, :cumulative_min, :cumulative_product, :cumulative_sum]
  @window_ops [:window_max, :window_mean, :window_median, :window_min, :window_sum]
  @ops_arity_1 @cumulative_ops ++ @window_ops
  for op <- @ops_arity_1 do
    def to_expr(%LazySeries{op: unquote(op), args: [arg | opts]}) do
      apply(Native, :"expr_#{unquote(op)}", [to_expr(arg) | opts])
    end
  end

  @ops_arity_2 [:correlation, :covariance]
  for op <- @ops_arity_2 do
    def to_expr(%LazySeries{op: unquote(op), args: [left, right | opts]}) do
      apply(Native, :"expr_#{unquote(op)}", [to_expr(left), to_expr(right) | opts])
    end
  end

  # Default

  def to_expr(%LazySeries{op: op, args: args}) when is_list(args) do
    apply(Native, :"expr_#{op}", Enum.map(args, &to_expr/1))
  end

  # Only for inspecting the expression in tests
  def describe_filter_plan(%DataFrame{data: polars_df}, %__MODULE__{} = expression) do
    Native.expr_describe_filter_plan(polars_df, expression)
  end
end
