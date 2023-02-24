defmodule Explorer.PolarsBackend.Series do
  @moduledoc false

  alias Explorer.DataFrame
  alias Explorer.PolarsBackend.Shared
  alias Explorer.Series

  @type t :: %__MODULE__{resource: reference()}

  defstruct resource: nil

  @behaviour Explorer.Backend.Series

  defguardp is_numerical(n) when is_number(n) or n in [:nan, :infinity, :neg_infinity]

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
  def cast(series, dtype), do: Shared.apply_series(series, :s_cast, [Atom.to_string(dtype)])

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
  def at_every(series, every_n),
    do: Shared.apply_series(series, :s_at_every, [every_n])

  @impl true
  def mask(series, %Series{} = mask),
    do: Shared.apply_series(series, :s_mask, [mask.data])

  @impl true
  def slice(series, indices) when is_list(indices),
    do: Shared.apply_series(series, :s_slice_by_indices, [indices])

  @impl true
  def slice(series, offset, length), do: Shared.apply_series(series, :s_slice, [offset, length])

  @impl true
  def at(series, idx), do: Shared.apply_series(series, :s_at, [idx])

  @impl true
  def format(s1, s2), do: Shared.apply_series(matching_size!(s1, s2), :s_format, [s2.data])

  @impl true
  def concat(s1, s2), do: Shared.apply_series(s1, :s_concat, [s2.data])

  @impl true
  def coalesce(s1, s2), do: Shared.apply_series(s1, :s_coalesce, [s2.data])

  @impl true
  def select(%Series{} = predicate, %Series{} = on_true, %Series{} = on_false) do
    predicate_size = size(predicate)
    on_true_size = size(on_true)
    on_false_size = size(on_false)

    if on_true_size != on_false_size do
      raise ArgumentError,
            "series in select/3 must have the same size, got: #{on_true_size} and #{on_false_size}"
    end

    if predicate_size != 1 and predicate_size != on_true_size do
      raise ArgumentError,
            "predicate in select/3 must have size of 1 or have the same size as operands, got: #{predicate_size} and #{on_true_size}"
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

  # Local minima/maxima

  @impl true
  def peaks(series, :max), do: Shared.apply_series(series, :s_peak_max)
  def peaks(series, :min), do: Shared.apply_series(series, :s_peak_min)

  # Arithmetic

  @impl true
  def add(left, right),
    do: Shared.apply_series(to_series(left, right), :s_add, [to_polars_series(right, left)])

  @impl true
  def subtract(left, right),
    do: Shared.apply_series(to_series(left, right), :s_subtract, [to_polars_series(right, left)])

  @impl true
  def multiply(left, right),
    do: Shared.apply_series(to_series(left, right), :s_multiply, [to_polars_series(right, left)])

  @impl true
  def divide(left, right),
    do: Shared.apply_series(to_series(left, right), :s_divide, [to_polars_series(right, left)])

  @impl true
  def quotient(left, right),
    do: Shared.apply_series(to_series(left, right), :s_quotient, [to_polars_series(right, left)])

  @impl true
  def remainder(left, right),
    do: Shared.apply_series(to_series(left, right), :s_remainder, [to_polars_series(right, left)])

  @impl true
  def pow(%Series{dtype: dtype} = left, %Series{dtype: dtype} = right),
    do: Shared.apply_series(matching_size!(left, right), :s_pow, [right.data])

  def pow(%Series{dtype: :float} = left, %Series{dtype: :integer} = right) do
    left = matching_size!(left, right)
    right = Series.cast(right, :float)
    Shared.apply_series(left, :s_pow, [right.data])
  end

  def pow(%Series{dtype: :integer} = left, %Series{dtype: :float} = right) do
    left = Series.cast(matching_size!(left, right), :float)
    Shared.apply_series(left, :s_pow, [right.data])
  end

  def pow(%Series{dtype: :integer}, exponent) when is_integer(exponent) and exponent < 0,
    do:
      raise(
        "negative exponent with an integer base is not allowed (you may explicitly cast the exponent to float if desired)"
      )

  def pow(left, exponent) when is_integer(exponent) do
    cond do
      Series.dtype(left) == :integer -> Shared.apply_series(left, :s_pow_i_rhs, [exponent])
      Series.dtype(left) == :float -> Shared.apply_series(left, :s_pow_f_rhs, [exponent / 1])
    end
  end

  def pow(left, exponent) when is_numerical(exponent),
    do: Shared.apply_series(left, :s_pow_f_rhs, [exponent])

  def pow(exponent, right) when is_integer(exponent) do
    cond do
      Series.dtype(right) == :integer -> Shared.apply_series(right, :s_pow_i_lhs, [exponent])
      Series.dtype(right) == :float -> Shared.apply_series(right, :s_pow_f_lhs, [exponent / 1])
    end
  end

  def pow(exponent, right) when is_numerical(exponent),
    do: Shared.apply_series(right, :s_pow_f_lhs, [exponent])

  # Comparisons

  @impl true
  def equal(left, right),
    do: Shared.apply_series(to_series(left, right), :s_equal, [to_polars_series(right, left)])

  @impl true
  def not_equal(left, right),
    do: Shared.apply_series(to_series(left, right), :s_not_equal, [to_polars_series(right, left)])

  @impl true
  def greater(left, right),
    do: Shared.apply_series(to_series(left, right), :s_greater, [to_polars_series(right, left)])

  @impl true
  def less(left, right),
    do: Shared.apply_series(to_series(left, right), :s_less, [to_polars_series(right, left)])

  @impl true
  def greater_equal(left, right) do
    Shared.apply_series(to_series(left, right), :s_greater_equal, [to_polars_series(right, left)])
  end

  @impl true
  def less_equal(left, right) do
    Shared.apply_series(to_series(left, right), :s_less_equal, [to_polars_series(right, left)])
  end

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

  # Window

  @impl true
  def window_max(series, window_size, opts) do
    window_function(:s_window_max, series, window_size, opts)
  end

  @impl true
  def window_mean(series, window_size, opts) do
    window_function(:s_window_mean, series, window_size, opts)
  end

  @impl true
  def window_min(series, window_size, opts) do
    window_function(:s_window_min, series, window_size, opts)
  end

  @impl true
  def window_sum(series, window_size, opts) do
    window_function(:s_window_sum, series, window_size, opts)
  end

  defp window_function(operation, series, window_size, opts) do
    weights = Keyword.fetch!(opts, :weights)
    min_periods = Keyword.fetch!(opts, :min_periods)
    center = Keyword.fetch!(opts, :center)

    Shared.apply_series(series, operation, [window_size, weights, min_periods, center])
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
  def trim(series),
    do: Shared.apply_series(series, :s_trim)

  @impl true
  def trim_leading(series),
    do: Shared.apply_series(series, :s_trim_leading)

  @impl true
  def trim_trailing(series),
    do: Shared.apply_series(series, :s_trim_trailing)

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
  def to_date(series),
    do: Shared.apply_series(series, :s_to_date)

  @impl true
  def to_time(series),
    do: Shared.apply_series(series, :s_to_time)

  # Polars specific functions

  def name(series), do: Shared.apply_series(series, :s_name)
  def rename(series, name), do: Shared.apply_series(series, :s_rename, [name])

  # Helpers

  defp to_series(%Series{} = series, %Series{} = other), do: matching_size!(series, other)
  defp to_series(%Series{} = series, _other), do: series
  defp to_series(series, other), do: to_mod_series(series, other, __MODULE__)

  defp to_polars_series(%Series{data: data}, _other), do: data
  defp to_polars_series(series, other), do: to_mod_series(series, other, Shared)

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

  defp to_mod_series(value, %{dtype: :float}, mod) when is_integer(value),
    do: mod.from_list([1.0 * value], :float)

  defp to_mod_series(value, %{dtype: :integer}, mod) when is_float(value),
    do: mod.from_list([value], :float)

  defp to_mod_series(value, %{dtype: dtype}, mod),
    do: mod.from_list([value], dtype)
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
