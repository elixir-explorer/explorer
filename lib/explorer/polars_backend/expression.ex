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
  # TODO: (probably) move the unwrapping upstream.
  def to_expr(%Explorer.Series{data: %LazySeries{} = lazy_series}), do: to_expr(lazy_series)

  def to_expr(map) when is_map(map) and not is_struct(map) do
    expr_list =
      Enum.map(map, fn {name, series} ->
        series |> to_expr() |> Native.expr_alias(to_string(name))
      end)

    Native.expr_struct(expr_list)
  end

  def to_expr(%LazySeries{op: :col, args: [col]}), do: Native.expr_col(col)

  # TODO: remove the `:column` op in favor of `:col`.
  def to_expr(%LazySeries{op: :column, args: [col]}), do: Native.expr_col(col)

  def to_expr(%LazySeries{op: :lit, args: [map]}) when is_map(map) and not is_struct(map) do
    expr_list =
      map
      |> Enum.sort()
      |> Enum.map(fn {name, series} ->
        series |> to_expr() |> Native.expr_alias(to_string(name))
      end)

    Native.expr_struct(expr_list)
  end

  def to_expr(%LazySeries{op: :lit, args: [lit]}), do: to_expr(lit)

  def to_expr(%LazySeries{op: :clip, args: [series, min_num, max_num]}) do
    lazy_series =
      if is_integer(min_num) and is_integer(max_num) do
        %LazySeries{op: :clip_integer, args: [series, min_num, max_num]}
      else
        %LazySeries{op: :clip_float, args: [series, 1.0 * min_num, 1.0 * max_num]}
      end

    to_expr(lazy_series)
  end

  def to_expr(%LazySeries{op: :fill_missing_with_strategy, args: [series, strategy]})
      when is_atom(strategy) do
    args = [to_expr(series), Atom.to_string(strategy)]
    apply(Native, :expr_fill_missing_with_strategy, args)
  end

  def to_expr(%LazySeries{op: :log, args: [series | args]}) do
    case args do
      [base] -> apply(Native, :expr_log, [to_expr(series), base])
      [] -> apply(Native, :expr_log_natural, [to_expr(series)])
    end
  end

  def to_expr(%LazySeries{op: :peaks, args: [series, method]}) do
    method =
      case method do
        method when is_binary(method) -> method
        method when is_atom(method) -> Atom.to_string(method)
      end

    apply(Native, :expr_peaks, [to_expr(series), method])
  end

  def to_expr(%LazySeries{op: :sample, args: [series, value | opts]}) do
    case value do
      n when is_integer(n) -> apply(Native, :expr_sample_n, [to_expr(series), n | opts])
      f when is_float(f) -> apply(Native, :expr_sample_frac, [to_expr(series), f | opts])
    end
  end

  def to_expr(%LazySeries{op: :slice, args: [series | args]}) do
    case args do
      [%mod{} = indices] when mod == LazySeries or mod == Explorer.Series ->
        apply(Native, :expr_slice_by_indices, [to_expr(series), to_expr(indices)])

      [offset, size] ->
        apply(Native, :expr_slice, [to_expr(series), offset, size])
    end
  end

  # The only argument to these functions is a list of exprs.

  @ops_only_arg_is_list [:concat, :format, :struct]
  for op <- @ops_only_arg_is_list do
    def to_expr(%LazySeries{op: unquote(op), args: [args]}) when is_list(args) do
      apply(Native, :"expr_#{unquote(op)}", [Enum.map(args, &to_expr/1)])
    end
  end

  # The trailing arguments to these functions should not be converted to exprs.

  @ops_first_1_only [
    :argsort,
    :cast,
    :clip_float,
    :clip_integer,
    :count_matches,
    :cumulative_max,
    :cumulative_min,
    :cumulative_product,
    :cumulative_sum,
    :ewm_mean,
    :ewm_standard_deviation,
    :ewm_variance,
    :field,
    :fill_missing,
    :head,
    :join,
    :json_decode,
    :json_path_match,
    :lstrip,
    :named_captures,
    :peaks,
    :quantile,
    :rank,
    :re_count_matches,
    :re_named_captures,
    :re_replace,
    :re_scan,
    :replace,
    :round,
    :rstrip,
    :sample_frac,
    :sample_n,
    :scan,
    :shift,
    :skew,
    :sort,
    :split_into,
    :split,
    :standard_deviation,
    :strip,
    :strftime,
    :strptime,
    :substring,
    :tail,
    :variance,
    :window_max,
    :window_mean,
    :window_median,
    :window_min,
    :window_standard_deviation,
    :window_sum
  ]
  for op <- @ops_first_1_only do
    def to_expr(%LazySeries{op: unquote(op), args: [arg | opts]}) do
      apply(Native, :"expr_#{unquote(op)}", [to_expr(arg) | opts])
    end
  end

  @ops_first_2_only [:correlation, :covariance]
  for op <- @ops_first_2_only do
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
