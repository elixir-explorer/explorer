defmodule Explorer.PolarsBackend.Series do
  @moduledoc false

  alias Explorer.DataFrame
  alias Explorer.PolarsBackend.Shared
  alias Explorer.Series

  @type t :: %__MODULE__{resource: reference()}

  defstruct resource: nil

  @behaviour Explorer.Backend.Series

  defguardp is_non_finite(n) when n in [:nan, :infinity, :neg_infinity]
  defguardp is_numeric(n) when is_number(n) or is_non_finite(n)

  @integer_types Explorer.Shared.integer_types()
  @numeric_types Explorer.Shared.numeric_types()

  # Conversion

  @impl true
  def from_list(data, type) when is_list(data) do
    series = Shared.from_list(data, type)
    Explorer.Backend.Series.new(series, type)
  end

  @impl true
  def from_binary(data, dtype) when is_binary(data) do
    series = Shared.from_binary(data, dtype)
    Explorer.Backend.Series.new(series, dtype)
  end

  @impl true
  def to_list(series), do: Shared.apply_series(series, :s_to_list)

  @impl true
  def to_iovec(series), do: Shared.apply_series(series, :s_to_iovec)

  @impl true
  def cast(%Series{dtype: :string} = series, {:datetime, precision}),
    do: Shared.apply_series(series, :s_strptime, [nil, precision])

  def cast(series, dtype),
    do: Shared.apply_series(series, :s_cast, [dtype])

  @impl true
  def strptime(%Series{} = series, format_string) do
    Shared.apply_series(series, :s_strptime, [format_string, nil])
  end

  @impl true
  def strftime(%Series{} = series, format_string) do
    Shared.apply_series(series, :s_strftime, [format_string])
  end

  # Introspection

  @impl true
  def dtype(series), do: series |> Shared.apply_series(:s_dtype)

  @impl true
  def size(series), do: Shared.apply_series(series, :s_size)

  @impl true
  def categories(%Series{dtype: :category} = series),
    do: Shared.apply_series(series, :s_categories)

  @impl true
  def categorise(%Series{dtype: {integer_type, _}} = series, %Series{dtype: dtype} = categories)
      when dtype in [:string, :category] and integer_type in [:s, :u],
      do: Shared.apply_series(series, :s_categorise, [categories.data])

  @impl true
  def categorise(%Series{dtype: :string} = series, %Series{dtype: dtype} = categories)
      when dtype in [:string, :category],
      do: Shared.apply_series(series, :s_categorise, [categories.data])

  # Slice and dice

  @impl true
  def head(series, n_elements), do: Shared.apply_series(series, :s_head, [n_elements])

  @impl true
  def tail(series, n_elements), do: Shared.apply_series(series, :s_tail, [n_elements])

  @impl true
  def first(series), do: series[0]

  @impl true
  def last(series), do: series[-1]

  @impl true
  def shift(series, offset, nil), do: Shared.apply_series(series, :s_shift, [offset])

  @impl true
  def sample(series, n, replacement, shuffle, seed) when is_integer(n) do
    Shared.apply_series(series, :s_sample_n, [n, replacement, shuffle, seed])
  end

  @impl true
  def sample(series, frac, replacement, shuffle, seed) when is_float(frac) do
    Shared.apply_series(series, :s_sample_frac, [frac, replacement, shuffle, seed])
  end

  @impl true
  def rank(series, method, descending, seed) do
    Shared.apply_series(series, :s_rank, [method, descending, seed])
  end

  @impl true
  def at_every(series, every_n),
    do: Shared.apply_series(series, :s_at_every, [every_n])

  @impl true
  def mask(series, %Series{} = mask),
    do: Shared.apply_series(series, :s_mask, [mask.data])

  @impl true
  def slice(series, indices) when is_list(indices),
    do: Shared.apply_series(series, :s_slice_by_indices, [indices])

  @impl true
  def slice(series, %Series{} = indices),
    do: Shared.apply_series(series, :s_slice_by_series, [indices.data])

  @impl true
  def slice(series, offset, length), do: Shared.apply_series(series, :s_slice, [offset, length])

  @impl true
  def at(series, idx), do: Shared.apply_series(series, :s_at, [idx])

  @impl true
  def format(list) do
    {_, df_args, params} =
      Enum.reduce(list, {0, [], []}, fn s, {counter, df_args, params} ->
        if is_binary(s) or Kernel.is_nil(s) do
          {counter, df_args, [s | params]}
        else
          counter = counter + 1
          name = "#{counter}"
          column = Explorer.Backend.LazySeries.new(:column, [name], :string)
          {counter, [{name, s} | df_args], [column | params]}
        end
      end)

    df = Explorer.PolarsBackend.DataFrame.from_series(df_args)
    format_expr = Explorer.Backend.LazySeries.new(:format, [Enum.reverse(params)], :string)
    out_dtypes = Map.put(df.dtypes, "result", :string)
    out_names = ["result" | df.names]
    out_df = %{df | dtypes: out_dtypes, names: out_names}

    Explorer.PolarsBackend.DataFrame.mutate_with(df, out_df, [{"result", format_expr}])
    |> Explorer.PolarsBackend.DataFrame.pull("result")
  end

  @impl true
  def concat([%Series{} | _] = series) do
    polars_series = for s <- series, do: s.data

    Shared.apply(:s_concat, [polars_series])
    |> Shared.create_series()
  end

  @impl true
  def coalesce(s1, s2), do: Shared.apply_series(s1, :s_coalesce, [s2.data])

  @impl true
  def select(%Series{} = predicate, %Series{} = on_true, %Series{} = on_false) do
    predicate_size = size(predicate)
    on_true_size = size(on_true)
    on_false_size = size(on_false)
    singleton_condition = on_true_size == 1 or on_false_size == 1

    if on_true_size != on_false_size and not singleton_condition do
      raise ArgumentError,
            "series in select/3 must have the same size or size of 1, got: #{on_true_size} and #{on_false_size}"
    end

    if predicate_size != 1 and predicate_size != on_true_size and predicate_size != on_false_size and
         not singleton_condition do
      raise ArgumentError,
            "predicate in select/3 must have size of 1 or have the same size as operands, got: #{predicate_size} and #{Enum.max([on_true_size, on_false_size])}"
    end

    Shared.apply_series(predicate, :s_select, [on_true.data, on_false.data])
  end

  # Aggregation

  @impl true
  # There is no `count` equivalent in Polars, so we need to make our own.
  def count(series), do: size(series) - nil_count(series)

  @impl true
  def nil_count(series), do: Shared.apply_series(series, :s_nil_count)

  @impl true
  def sum(series), do: Shared.apply_series(series, :s_sum)

  @impl true
  def min(series), do: Shared.apply_series(series, :s_min)

  @impl true
  def max(series), do: Shared.apply_series(series, :s_max)

  @impl true
  def argmin(series), do: Shared.apply_series(series, :s_argmin)

  @impl true
  def argmax(series), do: Shared.apply_series(series, :s_argmax)

  @impl true
  def mean(series), do: Shared.apply_series(series, :s_mean)

  @impl true
  def median(series), do: Shared.apply_series(series, :s_median)

  @impl true
  def mode(series), do: Shared.apply_series(series, :s_mode)

  @impl true
  def variance(series, ddof), do: series |> Shared.apply_series(:s_variance, [ddof]) |> at(0)

  @impl true
  def standard_deviation(series, ddof),
    do: series |> Shared.apply_series(:s_standard_deviation, [ddof]) |> at(0)

  @impl true
  def quantile(series, quantile),
    do: Shared.apply_series(series, :s_quantile, [quantile, "nearest"])

  @impl true
  def product(series), do: Shared.apply_series(series, :s_product)

  @impl true
  def skew(series, bias?),
    do: Shared.apply_series(series, :s_skew, [bias?])

  @impl true
  def correlation(left, right, ddof, method),
    do:
      Shared.apply_series(matching_size!(left, right), :s_correlation, [right.data, ddof, method])

  @impl true
  def covariance(left, right, ddof),
    do: Shared.apply_series(matching_size!(left, right), :s_covariance, [right.data, ddof])

  @impl true
  def all?(series), do: Shared.apply_series(series, :s_all)

  @impl true
  def any?(series), do: Shared.apply_series(series, :s_any)

  @impl true
  def row_index(series), do: Shared.apply_series(series, :s_row_index)

  # Cumulative

  @impl true
  def cumulative_max(series, reverse?),
    do: Shared.apply_series(series, :s_cumulative_max, [reverse?])

  @impl true
  def cumulative_min(series, reverse?),
    do: Shared.apply_series(series, :s_cumulative_min, [reverse?])

  @impl true
  def cumulative_sum(series, reverse?),
    do: Shared.apply_series(series, :s_cumulative_sum, [reverse?])

  @impl true
  def cumulative_product(series, reverse?),
    do: Shared.apply_series(series, :s_cumulative_product, [reverse?])

  # Local minima/maxima

  @impl true
  def peaks(series, :max), do: Shared.apply_series(series, :s_peak_max)
  def peaks(series, :min), do: Shared.apply_series(series, :s_peak_min)

  # Arithmetic

  @impl true
  def add(_out_dtype, left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_add, [right.data])

  @impl true
  def subtract(_out_dtype, left, right) do
    left = matching_size!(left, right)

    Shared.apply_series(left, :s_subtract, [right.data])
  end

  @impl true
  def multiply(out_dtype, left, right) do
    result = Shared.apply_series(matching_size!(left, right), :s_multiply, [right.data])

    # Polars currently returns inconsistent dtypes, e.g.:
    #   * `integer * duration -> duration` when `integer` is a scalar
    #   * `integer * duration ->  integer` when `integer` is a series
    # We need to return duration in these cases, so we need an additional cast.
    if match?({:duration, _}, out_dtype) and out_dtype != dtype(result) do
      cast(result, out_dtype)
    else
      result
    end
  end

  @impl true
  def divide(out_dtype, left, right) do
    result = Shared.apply_series(matching_size!(left, right), :s_divide, [right.data])

    # Polars currently returns inconsistent dtypes, e.g.:
    #   * `duration / integer -> duration` when `integer` is a scalar
    #   * `duration / integer ->  integer` when `integer` is a series
    # We need to return duration in these cases, so we need an additional cast.
    if match?({:duration, _}, out_dtype) and out_dtype != dtype(result) do
      cast(result, out_dtype)
    else
      result
    end
  end

  @impl true
  def quotient(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_quotient, [right.data])

  @impl true
  def remainder(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_remainder, [right.data])

  @impl true
  def pow(out_dtype, left, right) do
    _ = matching_size!(left, right)

    # We need to pre-cast or we may lose precision.
    left = Explorer.Series.cast(left, out_dtype)

    left_lazy = Explorer.Backend.LazySeries.new(:column, ["base"], left.dtype)
    right_lazy = Explorer.Backend.LazySeries.new(:column, ["exponent"], right.dtype)

    {df_args, pow_args} =
      case {size(left), size(right)} do
        {n, n} -> {[{"base", left}, {"exponent", right}], [left_lazy, right_lazy]}
        {1, _} -> {[{"exponent", right}], [Explorer.Series.at(left, 0), right_lazy]}
        {_, 1} -> {[{"base", left}], [left_lazy, Explorer.Series.at(right, 0)]}
      end

    df = Explorer.PolarsBackend.DataFrame.from_series(df_args)
    pow = Explorer.Backend.LazySeries.new(:pow, pow_args, out_dtype)

    out_dtypes = Map.put(df.dtypes, "pow", out_dtype)
    out_names = df.names ++ ["pow"]
    out_df = %{df | dtypes: out_dtypes, names: out_names}

    Explorer.PolarsBackend.DataFrame.mutate_with(df, out_df, [{"pow", pow}])
    |> Explorer.PolarsBackend.DataFrame.pull("pow")
  end

  @impl true
  def log(%Series{} = argument), do: Shared.apply_series(argument, :s_log_natural, [])

  @impl true
  def log(%Series{} = argument, base) when is_numeric(base),
    do: Shared.apply_series(argument, :s_log, [base])

  @impl true
  def exp(%Series{} = s), do: Shared.apply_series(s, :s_exp, [])

  @impl true
  def abs(%Series{} = s), do: Shared.apply_series(s, :s_abs, [])

  @impl true
  def clip(%Series{dtype: dtype} = s, min, max)
      when dtype in @integer_types and is_integer(min) and is_integer(max),
      do: Shared.apply_series(s, :s_clip_integer, [min, max])

  def clip(%Series{} = s, min, max),
    do: s |> cast({:f, 64}) |> Shared.apply_series(:s_clip_float, [min * 1.0, max * 1.0])

  # Trigonometry

  @impl true
  def sin(%Series{} = s), do: Shared.apply_series(s, :s_sin, [])

  @impl true
  def cos(%Series{} = s), do: Shared.apply_series(s, :s_cos, [])

  @impl true
  def tan(%Series{} = s), do: Shared.apply_series(s, :s_tan, [])

  @impl true
  def asin(%Series{} = s), do: Shared.apply_series(s, :s_asin, [])

  @impl true
  def acos(%Series{} = s), do: Shared.apply_series(s, :s_acos, [])

  @impl true
  def atan(%Series{} = s), do: Shared.apply_series(s, :s_atan, [])

  # Comparisons

  @impl true
  def equal(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_equal, [right.data])

  @impl true
  def not_equal(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_not_equal, [right.data])

  @impl true
  def greater(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_greater, [right.data])

  @impl true
  def less(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_less, [right.data])

  @impl true
  def greater_equal(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_greater_equal, [right.data])

  @impl true
  def less_equal(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_less_equal, [right.data])

  @impl true
  def all_equal(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(matching_size!(left, right), :s_series_equal, [right.data, true])

  @impl true
  def binary_in(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_in, [right.data])

  @impl true
  def binary_and(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(matching_size!(left, right), :s_and, [right.data])

  @impl true
  def binary_or(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(matching_size!(left, right), :s_or, [right.data])

  # Float predicates

  @impl true
  def is_finite(series), do: Shared.apply_series(series, :s_is_finite)

  @impl true
  def is_infinite(series), do: Shared.apply_series(series, :s_is_infinite)

  @impl true
  def is_nan(series), do: Shared.apply_series(series, :s_is_nan)

  # Sort

  @impl true
  def sort(series, descending?, maintain_order?, multithreaded?, nulls_last?)
      when is_boolean(descending?) and is_boolean(maintain_order?) and is_boolean(multithreaded?) and
             is_boolean(nulls_last?) do
    Shared.apply_series(series, :s_sort, [
      descending?,
      maintain_order?,
      multithreaded?,
      nulls_last?
    ])
  end

  @impl true
  def argsort(series, descending?, maintain_order?, multithreaded?, nulls_last?)
      when is_boolean(descending?) and is_boolean(maintain_order?) and is_boolean(multithreaded?) and
             is_boolean(nulls_last?) do
    Shared.apply_series(series, :s_argsort, [
      descending?,
      maintain_order?,
      multithreaded?,
      nulls_last?
    ])
  end

  @impl true
  def reverse(series), do: Shared.apply_series(series, :s_reverse)

  # Distinct

  @impl true
  def distinct(series), do: Shared.apply_series(series, :s_distinct)

  @impl true
  def unordered_distinct(series), do: Shared.apply_series(series, :s_unordered_distinct)

  @impl true
  def n_distinct(series), do: Shared.apply_series(series, :s_n_distinct)

  @impl true
  def frequencies(%Series{dtype: {:list, inner_dtype} = dtype})
      when inner_dtype not in @numeric_types do
    raise ArgumentError,
          "frequencies/1 only works with series of lists of numeric types, but #{Explorer.Shared.dtype_to_string(dtype)} was given"
  end

  def frequencies(%Series{} = series) do
    Shared.apply(:s_frequencies, [series.data])
    |> Shared.create_dataframe()
    |> DataFrame.rename(["values", "counts"])
  end

  # Categorisation

  @impl true
  def cut(series, bins, labels, break_point_label, category_label) do
    case Explorer.PolarsBackend.Native.s_cut(
           series.data,
           bins,
           labels,
           break_point_label,
           category_label
         ) do
      {:ok, polars_df} ->
        Shared.create_dataframe(polars_df)

      {:error, "Polars Error: lengths don't match: " <> _rest} ->
        raise ArgumentError, "lengths don't match: labels count must equal bins count"

      {:error, msg} ->
        raise msg
    end
  end

  @impl true
  def qcut(series, quantiles, labels, break_point_label, category_label) do
    Shared.apply(:s_qcut, [
      series.data,
      quantiles,
      labels,
      break_point_label,
      category_label
    ])
    |> Shared.create_dataframe()
  end

  # Window

  @impl true
  def window_max(series, window_size, weights, min_periods, center) do
    window_function(:s_window_max, series, window_size, weights, min_periods, center)
  end

  @impl true
  def window_mean(series, window_size, weights, min_periods, center) do
    window_function(:s_window_mean, series, window_size, weights, min_periods, center)
  end

  @impl true
  def window_median(series, window_size, weights, min_periods, center) do
    window_function(:s_window_median, series, window_size, weights, min_periods, center)
  end

  @impl true
  def window_min(series, window_size, weights, min_periods, center) do
    window_function(:s_window_min, series, window_size, weights, min_periods, center)
  end

  @impl true
  def window_sum(series, window_size, weights, min_periods, center) do
    window_function(:s_window_sum, series, window_size, weights, min_periods, center)
  end

  @impl true
  def window_standard_deviation(series, window_size, weights, min_periods, center) do
    window_function(
      :s_window_standard_deviation,
      series,
      window_size,
      weights,
      min_periods,
      center
    )
  end

  defp window_function(operation, series, window_size, weights, min_periods, center) do
    Shared.apply_series(series, operation, [window_size, weights, min_periods, center])
  end

  @impl true
  def ewm_mean(series, alpha, adjust, min_periods, ignore_nils) do
    Shared.apply_series(series, :s_ewm_mean, [alpha, adjust, min_periods, ignore_nils])
  end

  @impl true
  def ewm_standard_deviation(series, alpha, adjust, bias, min_periods, ignore_nils) do
    Shared.apply_series(
      series,
      :s_ewm_standard_deviation,
      [alpha, adjust, bias, min_periods, ignore_nils]
    )
  end

  @impl true
  def ewm_variance(series, alpha, adjust, bias, min_periods, ignore_nils) do
    Shared.apply_series(series, :s_ewm_variance, [alpha, adjust, bias, min_periods, ignore_nils])
  end

  # Missing values

  @impl true
  def fill_missing_with_strategy(series, strategy),
    do: Shared.apply_series(series, :s_fill_missing_with_strategy, [Atom.to_string(strategy)])

  @impl true
  def fill_missing_with_value(series, value) when is_atom(value) and not is_boolean(value) do
    Shared.apply_series(series, :s_fill_missing_with_atom, [Atom.to_string(value)])
  end

  def fill_missing_with_value(series, value) do
    operation =
      cond do
        is_float(value) -> :s_fill_missing_with_float
        is_integer(value) -> :s_fill_missing_with_int
        is_binary(value) -> :s_fill_missing_with_bin
        is_boolean(value) -> :s_fill_missing_with_boolean
        is_struct(value, Date) -> :s_fill_missing_with_date
        is_struct(value, NaiveDateTime) -> :s_fill_missing_with_datetime
      end

    Shared.apply_series(series, operation, [value])
  end

  @impl true
  def is_nil(series), do: Shared.apply_series(series, :s_is_null)

  @impl true
  def is_not_nil(series), do: Shared.apply_series(series, :s_is_not_null)

  @impl true
  def transform(series, fun) do
    series
    |> Series.to_list()
    |> Enum.map(fun)
    |> Series.from_list(backend: Explorer.PolarsBackend)
  end

  @impl true
  def inspect(series, opts) when node(series.data.resource) != node() do
    Explorer.Backend.Series.inspect(series, "Polars", "node: #{node(series.data.resource)}", opts,
      elide_columns: true
    )
  end

  def inspect(series, opts) do
    Explorer.Backend.Series.inspect(series, "Polars", Series.size(series), opts)
  end

  # Inversions

  @impl true
  def unary_not(%Series{} = series), do: Shared.apply_series(series, :s_not, [])

  # Strings

  @impl true
  def contains(series, pattern),
    do: Shared.apply_series(series, :s_contains, [pattern])

  @impl true
  def upcase(series),
    do: Shared.apply_series(series, :s_upcase)

  @impl true
  def downcase(series),
    do: Shared.apply_series(series, :s_downcase)

  @impl true
  def replace(series, pattern, replacement),
    do: Shared.apply_series(series, :s_replace, [pattern, replacement])

  @impl true
  def strip(series, str),
    do: Shared.apply_series(series, :s_strip, [str])

  @impl true
  def lstrip(series, str),
    do: Shared.apply_series(series, :s_lstrip, [str])

  @impl true
  def rstrip(series, str),
    do: Shared.apply_series(series, :s_rstrip, [str])

  @impl true
  def substring(series, offset, length),
    do: Shared.apply_series(series, :s_substring, [offset, length])

  @impl true
  def split(series, by),
    do: Shared.apply_series(series, :s_split, [by])

  # Float round
  @impl true
  def round(series, decimals),
    do: Shared.apply_series(series, :s_round, [decimals])

  @impl true
  def floor(series),
    do: Shared.apply_series(series, :s_floor)

  @impl true
  def ceil(series),
    do: Shared.apply_series(series, :s_ceil)

  # Date / DateTime
  @impl true
  def day_of_week(series),
    do: Shared.apply_series(series, :s_day_of_week)

  @impl true
  def day_of_year(series),
    do: Shared.apply_series(series, :s_day_of_year)

  @impl true
  def week_of_year(series),
    do: Shared.apply_series(series, :s_week_of_year)

  @impl true
  def month(series),
    do: Shared.apply_series(series, :s_month)

  @impl true
  def year(series),
    do: Shared.apply_series(series, :s_year)

  @impl true
  def hour(series),
    do: Shared.apply_series(series, :s_hour)

  @impl true
  def minute(series),
    do: Shared.apply_series(series, :s_minute)

  @impl true
  def second(series),
    do: Shared.apply_series(series, :s_second)

  # Lists

  @impl true
  def join(series, separator),
    do: Shared.apply_series(series, :s_join, [separator])

  @impl true
  def lengths(series),
    do: Shared.apply_series(series, :s_lengths)

  @impl true
  def member?(%Series{dtype: {:list, inner_dtype}} = series, value),
    do: Shared.apply_series(series, :s_member, [value, inner_dtype])

  @impl true
  def field(%Series{dtype: {:struct, _inner_dtype}} = series, name),
    do: Shared.apply_series(series, :s_field, [name])

  @impl true
  def json_decode(series, dtype),
    do: Shared.apply_series(series, :s_json_decode, [dtype])

  @impl true
  def json_path_match(series, json_path),
    do: Shared.apply_series(series, :s_json_path_match, [json_path])

  # Polars specific functions

  def name(series), do: Shared.apply_series(series, :s_name)
  def rename(series, name), do: Shared.apply_series(series, :s_rename, [name])

  # Helpers

  defp matching_size!(series, other) do
    case size(series) do
      1 ->
        series

      i ->
        case size(other) do
          1 ->
            series

          ^i ->
            series

          j ->
            raise ArgumentError,
                  "series must either have the same size or one of them must have size of 1, got: #{i} and #{j}"
        end
    end
  end
end

defimpl Inspect, for: Explorer.PolarsBackend.Series do
  import Inspect.Algebra

  def inspect(s, _opts) do
    doc =
      case Explorer.PolarsBackend.Native.s_as_str(s) do
        {:ok, str} -> str
        {:error, _} -> inspect(s.resource)
      end
      |> String.split("\n")
      |> Enum.intersperse(line())
      |> then(&concat([line() | &1]))
      |> nest(2)

    concat(["#Explorer.PolarsBackend.Series<", doc, line(), ">"])
  end
end
