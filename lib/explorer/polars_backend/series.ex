defmodule Explorer.PolarsBackend.Series do
  @moduledoc false

  import Kernel, except: [length: 1]

  alias Explorer.DataFrame
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Shared
  alias Explorer.Series

  @type t :: %__MODULE__{resource: binary(), reference: term()}

  defstruct resource: nil, reference: nil

  @behaviour Explorer.Backend.Series

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
  def bintype(series) do
    case Shared.apply_series(series, :s_dtype) do
      "u8" -> {:u, 8}
      "u32" -> {:u, 32}
      "i32" -> {:s, 32}
      "i64" -> {:s, 64}
      "f64" -> {:f, 64}
      "bool" -> {:u, 8}
      "str" -> :utf8
      "date" -> {:s, 32}
      "datetime[ms]" -> {:s, 64}
      "datetime[μs]" -> {:s, 64}
      "datetime[ns]" -> {:s, 64}
      dtype -> raise "cannot convert dtype #{inspect(dtype)} to bintype"
    end
  end

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
  def sample(series, n, replacement, seed) when is_integer(n) do
    indices =
      series
      |> size()
      |> Native.s_seedable_random_indices(n, replacement, seed)

    slice(series, indices)
  end

  @impl true
  def sample(series, frac, replacement, seed) when is_float(frac) do
    size = size(series)
    n = round(frac * size)

    sample(series, n, replacement, seed)
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
  def concat(s1, s2), do: Shared.apply_series(s1, :s_concat, [s2.data])

  @impl true
  def coalesce(s1, s2), do: Shared.apply_series(s1, :s_coalesce, [s2.data])

  @impl true
  def select(predicate, %Series{} = on_true, %Series{} = on_false),
    do: Shared.apply_series(predicate, :s_select, [on_true.data, on_false.data])

  # Aggregation

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
  def add(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_add, [right.data])

  def add(left, right) when is_number(right), do: apply_scalar_on_rhs(:add, left, right)
  def add(left, right) when is_number(left), do: apply_scalar_on_lhs(:add, left, right)

  @impl true
  def subtract(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_subtract, [right.data])

  def subtract(left, right) when is_number(right), do: apply_scalar_on_rhs(:subtract, left, right)
  def subtract(left, right) when is_number(left), do: apply_scalar_on_lhs(:subtract, left, right)

  @impl true
  def multiply(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_multiply, [right.data])

  def multiply(left, right) when is_number(right), do: apply_scalar_on_rhs(:multiply, left, right)
  def multiply(left, right) when is_number(left), do: apply_scalar_on_lhs(:multiply, left, right)

  @impl true
  def divide(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_divide, [right.data])

  def divide(left, right) when is_number(right), do: apply_scalar_on_rhs(:divide, left, right)
  def divide(left, right) when is_number(left), do: apply_scalar_on_lhs(:divide, left, right)

  @impl true
  def quotient(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_quotient, [right.data])

  def quotient(left, right) when is_integer(right),
    do: apply_scalar_on_rhs(:quotient, left, right)

  def quotient(left, right) when is_integer(left), do: apply_scalar_on_lhs(:quotient, left, right)

  @impl true
  def remainder(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_remainder, [right.data])

  def remainder(left, right) when is_integer(right),
    do: apply_scalar_on_rhs(:remainder, left, right)

  def remainder(left, right) when is_integer(left),
    do: apply_scalar_on_lhs(:remainder, left, right)

  @impl true
  def pow(left, exponent) when is_float(exponent),
    do: Shared.apply_series(left, :s_pow_f_rhs, [exponent])

  def pow(left, exponent) when is_integer(exponent) and exponent >= 0 do
    cond do
      Series.dtype(left) == :integer -> Shared.apply_series(left, :s_pow_i_rhs, [exponent])
      Series.dtype(left) == :float -> Shared.apply_series(left, :s_pow_f_rhs, [exponent / 1])
    end
  end

  def pow(exponent, right) when is_float(exponent),
    do: Shared.apply_series(right, :s_pow_f_lhs, [exponent])

  def pow(exponent, right) when is_integer(exponent) and exponent >= 0 do
    cond do
      Series.dtype(right) == :integer -> Shared.apply_series(right, :s_pow_i_lhs, [exponent])
      Series.dtype(right) == :float -> Shared.apply_series(right, :s_pow_f_lhs, [exponent / 1])
    end
  end

  # Comparisons

  @impl true
  def equal(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_equal, [right.data])

  def equal(%Series{} = left, right), do: apply_scalar_on_rhs(:equal, left, right)
  def equal(left, %Series{} = right), do: apply_scalar_on_lhs(:equal, left, right)

  @impl true
  def not_equal(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_not_equal, [right.data])

  def not_equal(%Series{} = left, right), do: apply_scalar_on_rhs(:not_equal, left, right)
  def not_equal(left, %Series{} = right), do: apply_scalar_on_lhs(:not_equal, left, right)

  @impl true
  def greater(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_greater, [right.data])

  def greater(%Series{} = left, right), do: apply_scalar_on_rhs(:greater, left, right)
  def greater(left, %Series{} = right), do: apply_scalar_on_lhs(:greater, left, right)

  @impl true
  def greater_equal(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_greater_equal, [right.data])

  def greater_equal(%Series{} = left, right), do: apply_scalar_on_rhs(:greater_equal, left, right)
  def greater_equal(left, %Series{} = right), do: apply_scalar_on_lhs(:greater_equal, left, right)

  @impl true
  def less(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_less, [right.data])

  def less(%Series{} = left, right), do: apply_scalar_on_rhs(:less, left, right)
  def less(left, %Series{} = right), do: apply_scalar_on_lhs(:less, left, right)

  @impl true
  def less_equal(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_less_equal, [right.data])

  def less_equal(%Series{} = left, right), do: apply_scalar_on_rhs(:less_equal, left, right)
  def less_equal(left, %Series{} = right), do: apply_scalar_on_lhs(:less_equal, left, right)

  @impl true
  def all_equal(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_series_equal, [right.data, true])

  @impl true
  def binary_and(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_and, [right.data])

  @impl true
  def binary_or(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_or, [right.data])

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
  def count(series) do
    Shared.apply(:s_value_counts, [series.data])
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
  def fill_missing(series, strategy) when is_atom(strategy) and not is_boolean(strategy),
    do: Shared.apply_series(series, :s_fill_missing, [Atom.to_string(strategy)])

  def fill_missing(series, value) do
    operation =
      cond do
        is_float(value) -> :s_fill_missing_with_float
        is_integer(value) -> :s_fill_missing_with_int
        is_binary(value) -> :s_fill_missing_with_bin
        is_boolean(value) -> :s_fill_missing_with_boolean
      end

    Shared.apply_series(series, operation, [value])
  end

  @impl true
  def is_nil(series), do: Shared.apply_series(series, :s_is_null)

  @impl true
  def is_not_nil(series), do: Shared.apply_series(series, :s_is_not_null)

  # Escape hatch
  @impl true
  def transform(series, fun), do: series |> Series.to_list() |> Enum.map(fun)

  @impl true
  def inspect(series, opts) do
    Explorer.Backend.Series.inspect(series, "Polars", Series.size(series), opts)
  end

  # Polars specific functions

  def name(series), do: Shared.apply_series(series, :s_name)
  def rename(series, name), do: Shared.apply_series(series, :s_rename, [name])

  # Helpers

  defp apply_scalar_on_rhs(fun_name, %Series{} = left, scalar) when is_atom(fun_name) do
    df =
      DataFrame.mutate_with(polars_df(left: left), fn ldf ->
        [result: apply(Explorer.Series, fun_name, [ldf["left"], scalar])]
      end)

    df["result"]
  end

  defp apply_scalar_on_lhs(fun_name, scalar, %Series{} = right) when is_atom(fun_name) do
    df =
      DataFrame.mutate_with(polars_df(right: right), fn ldf ->
        [result: apply(Explorer.Series, fun_name, [scalar, ldf["right"]])]
      end)

    df["result"]
  end

  defp polars_df(series) do
    Explorer.PolarsBackend.DataFrame.from_series(series)
  end
end

defimpl Inspect, for: Explorer.PolarsBackend.Series do
  import Inspect.Algebra

  def inspect(s, _opts) do
    concat([
      "#Explorer.PolarsBackend.Series<",
      nest(Explorer.PolarsBackend.Shared.apply_series(s, []), 2),
      ">"
    ])
  end
end
