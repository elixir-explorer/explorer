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
  def cast(series, {:datetime, :millisecond}),
    do: Shared.apply_series(series, :s_cast, ["datetime[ms]"])

  def cast(series, {:datetime, :microsecond}),
    do: Shared.apply_series(series, :s_cast, ["datetime[μs]"])

  def cast(series, {:datetime, :nanosecond}),
    do: Shared.apply_series(series, :s_cast, ["datetime[ns]"])

  def cast(series, {:duration, :millisecond}),
    do: Shared.apply_series(series, :s_cast, ["duration[ms]"])

  def cast(series, {:duration, :microsecond}),
    do: Shared.apply_series(series, :s_cast, ["duration[μs]"])

  def cast(series, {:duration, :nanosecond}),
    do: Shared.apply_series(series, :s_cast, ["duration[ns]"])

  def cast(series, dtype), do: Shared.apply_series(series, :s_cast, [Atom.to_string(dtype)])

  @impl true
  def strptime(%Series{} = series, format_string) do
    Shared.apply_series(series, :s_strptime, [format_string])
  end

  @impl true
  def strftime(%Series{} = series, format_string) do
    Shared.apply_series(series, :s_strftime, [format_string])
  end

  # Introspection

  @impl true
  def dtype(series), do: series |> Shared.apply_series(:s_dtype) |> Shared.normalise_dtype()

  @impl true
  def size(series), do: Shared.apply_series(series, :s_size)

  @impl true
  def iotype(series) do
    case Shared.apply_series(series, :s_dtype) do
      "u8" -> {:u, 8}
      "u32" -> {:u, 32}
      "i32" -> {:s, 32}
      "i64" -> {:s, 64}
      "f64" -> {:f, 64}
      "bool" -> {:u, 8}
      "date" -> {:s, 32}
      "time" -> {:s, 64}
      "datetime[ms]" -> {:s, 64}
      "datetime[μs]" -> {:s, 64}
      "datetime[ns]" -> {:s, 64}
      "duration[ms]" -> {:s, 64}
      "duration[μs]" -> {:s, 64}
      "duration[ns]" -> {:s, 64}
      "cat" -> {:u, 32}
      dtype -> raise "cannot convert dtype #{inspect(dtype)} to iotype"
    end
  end

  @impl true
  def categories(%Series{dtype: :category} = series),
    do: Shared.apply_series(series, :s_categories)

  @impl true
  def categorise(%Series{dtype: :integer} = series, %Series{dtype: dtype} = categories)
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
    polars_series = for s <- list, do: s.data

    Shared.apply(:s_format, [polars_series])
    |> Shared.create_series()
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
  def count(series), do: Shared.apply_series(series, :s_size)

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
  def variance(series), do: Shared.apply_series(series, :s_variance)

  @impl true
  def standard_deviation(series), do: Shared.apply_series(series, :s_standard_deviation)

  @impl true
  def quantile(series, quantile),
    do: Shared.apply_series(series, :s_quantile, [quantile, "nearest"])

  @impl true
  def product(series), do: Shared.apply_series(series, :s_product)

  @impl true
  def skew(series, bias?),
    do: Shared.apply_series(series, :s_skew, [bias?])

  @impl true
  def correlation(left, right, ddof),
    do: Shared.apply_series(matching_size!(left, right), :s_correlation, [right.data, ddof])

  @impl true
  def covariance(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_covariance, [right.data])

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
  def add(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_add, [right.data])

  @impl true
  def subtract(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_subtract, [right.data])

  @impl true
  def multiply(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_multiply, [right.data])

  @impl true
  def divide(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_divide, [right.data])

  @impl true
  def quotient(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_quotient, [right.data])

  @impl true
  def remainder(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_remainder, [right.data])

  @impl true
  def pow(left, right),
    do: Shared.apply_series(matching_size!(left, right), :s_pow, [right.data])

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
  def clip(%Series{dtype: :integer} = s, min, max)
      when is_integer(min) and is_integer(max),
      do: Shared.apply_series(s, :s_clip_integer, [min, max])

  def clip(%Series{} = s, min, max),
    do: Shared.apply_series(s, :s_clip_float, [min * 1.0, max * 1.0])

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
  def sort(series, descending?, nils_last?)
      when is_boolean(descending?) and is_boolean(nils_last?) do
    Shared.apply_series(series, :s_sort, [descending?, nils_last?])
  end

  @impl true
  def argsort(series, descending?, nils_last?)
      when is_boolean(descending?) and is_boolean(nils_last?) do
    Shared.apply_series(series, :s_argsort, [descending?, nils_last?])
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
  def frequencies(series) do
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
