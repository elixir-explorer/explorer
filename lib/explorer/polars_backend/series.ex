defmodule Explorer.PolarsBackend.Series do
  @moduledoc false

  import Kernel, except: [length: 1]

  alias Explorer.DataFrame
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Shared
  alias Explorer.Series
  alias __MODULE__, as: PolarsSeries

  @type t :: %__MODULE__{resource: binary(), reference: term()}

  defstruct resource: nil, reference: nil

  @behaviour Explorer.Backend.Series

  # Conversion

  @impl true
  def from_list(data, type, name \\ "") when is_list(data) do
    series =
      case type do
        :integer -> Native.s_new_i64(name, data)
        :float -> Native.s_new_f64(name, data)
        :boolean -> Native.s_new_bool(name, data)
        :string -> Native.s_new_str(name, data)
        :date -> Native.s_new_date32(name, data)
        :datetime -> Native.s_new_date64(name, data)
      end

    Explorer.Backend.Series.new(series, type)
  end

  @impl true
  def to_list(series), do: Shared.apply_series(series, :s_to_list)

  @impl true
  def to_enum(series) do
    Explorer.PolarsBackend.Series.Iterator.new(series)
  end

  @impl true
  def cast(series, dtype), do: Shared.apply_series(series, :s_cast, [Atom.to_string(dtype)])

  # Introspection

  @impl true
  def dtype(series), do: series |> Shared.apply_series(:s_dtype) |> Shared.normalise_dtype()

  @impl true
  def size(series), do: Shared.apply_series(series, :s_len)

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
  def sample(series, n, replacement, seed) when is_integer(n) do
    indices =
      series
      |> size()
      |> Native.s_seedable_random_indices(n, replacement, seed)

    slice(series, indices)
  end

  @impl true
  def take_every(series, every_n),
    do: Shared.apply_series(series, :s_take_every, [every_n])

  @impl true
  def filter(series, %Series{} = mask),
    do: Shared.apply_series(series, :s_filter, [mask.data])

  def filter(series, callback) when is_function(callback) do
    mask = callback.(series)
    filter(series, mask)
  end

  @impl true
  def slice(series, indices) when is_list(indices),
    do: Shared.apply_series(series, :s_slice_by_indices, [indices])

  @impl true
  def slice(series, offset, length), do: Shared.apply_series(series, :s_slice, [offset, length])

  @impl true
  def get(series, idx) do
    idx = if idx < 0, do: size(series) + idx, else: idx
    Shared.apply_series(series, :s_get, [idx])
  end

  @impl true
  def concat(s1, s2), do: Shared.apply_series(s1, :s_append, [s2.data])

  @impl true
  def coalesce(s1, s2), do: Shared.apply_series(s1, :s_coalesce, [s2.data])

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
  def var(series), do: Shared.apply_series(series, :s_var)

  @impl true
  def std(series), do: Shared.apply_series(series, :s_std)

  @impl true
  def quantile(series, quantile),
    do: Shared.apply_series(series, :s_quantile, [quantile, "nearest"])

  # Cumulative

  @impl true
  def cumulative_max(series, reverse?),
    do: Shared.apply_series(series, :s_cum_max, [reverse?])

  @impl true
  def cumulative_min(series, reverse?),
    do: Shared.apply_series(series, :s_cum_min, [reverse?])

  @impl true
  def cumulative_sum(series, reverse?),
    do: Shared.apply_series(series, :s_cum_sum, [reverse?])

  # Local minima/maxima

  @impl true
  def peaks(series, :max), do: Shared.apply_series(series, :s_peak_max)
  def peaks(series, :min), do: Shared.apply_series(series, :s_peak_min)

  # Arithmetic

  @impl true
  def add(left, %Series{} = right),
    do: Shared.apply_series(left, :s_add, [right.data])

  def add(left, right) when is_number(right), do: add(left, scalar_rhs(right, left))

  @impl true
  def subtract(left, %Series{} = right),
    do: Shared.apply_series(left, :s_sub, [right.data])

  def subtract(left, right) when is_number(right), do: subtract(left, scalar_rhs(right, left))

  @impl true
  def multiply(left, %Series{} = right),
    do: Shared.apply_series(left, :s_mul, [right.data])

  def multiply(left, right) when is_number(right), do: multiply(left, scalar_rhs(right, left))

  @impl true
  def divide(left, %Series{} = right),
    do: Shared.apply_series(left, :s_div, [right.data])

  def divide(left, right) when is_number(right), do: divide(left, scalar_rhs(right, left))

  @impl true
  def pow(left, exponent) when is_float(exponent),
    do: Shared.apply_series(left, :s_pow, [exponent])

  def pow(left, exponent) when is_integer(exponent) and exponent >= 0 do
    cond do
      Series.dtype(left) == :integer -> Shared.apply_series(left, :s_int_pow, [exponent])
      Series.dtype(left) == :float -> Shared.apply_series(left, :s_pow, [exponent / 1])
    end
  end

  # Comparisons

  @impl true
  def eq(left, %Series{} = right),
    do: Shared.apply_series(left, :s_eq, [right.data])

  def eq(left, right), do: eq(left, scalar_rhs(right, left))

  @impl true
  def neq(left, %Series{} = right),
    do: Shared.apply_series(left, :s_neq, [right.data])

  def neq(left, right), do: neq(left, scalar_rhs(right, left))

  @impl true
  def gt(left, %Series{} = right),
    do: Shared.apply_series(left, :s_gt, [right.data])

  def gt(left, right), do: gt(left, scalar_rhs(right, left))

  @impl true
  def gt_eq(left, %Series{} = right),
    do: Shared.apply_series(left, :s_gt_eq, [right.data])

  def gt_eq(left, right), do: gt_eq(left, scalar_rhs(right, left))

  @impl true
  def lt(left, %Series{} = right),
    do: Shared.apply_series(left, :s_lt, [right.data])

  def lt(left, right), do: lt(left, scalar_rhs(right, left))

  @impl true
  def lt_eq(left, %Series{} = right),
    do: Shared.apply_series(left, :s_lt_eq, [right.data])

  def lt_eq(left, right), do: lt_eq(left, scalar_rhs(right, left))

  @impl true
  def all_equal(left, right),
    do: Shared.apply_series(left, :s_series_equal, [right.data, true])

  @impl true
  def binary_and(left, right), do: Shared.apply_series(left, :s_and, [right.data])

  @impl true
  def binary_or(left, right), do: Shared.apply_series(left, :s_or, [right.data])
  # Sort

  @impl true
  def sort(series, reverse?), do: Shared.apply_series(series, :s_sort, [reverse?])

  @impl true
  def argsort(series, reverse?), do: Shared.apply_series(series, :s_argsort, [reverse?])

  @impl true
  def reverse(series), do: Shared.apply_series(series, :s_reverse)

  # Distinct

  @impl true
  def distinct(series), do: Shared.apply_series(series, :s_distinct)

  @impl true
  def unordered_distinct(series), do: Shared.apply_series(series, :s_unordered_distinct)

  @impl true
  def n_distinct(series), do: Shared.apply_series(series, :s_n_unique)

  @impl true
  def count(%Series{data: polars_series}) do
    df =
      case Native.s_value_counts(polars_series) do
        {:ok, polars_df} -> Shared.create_dataframe(polars_df)
        {:error, error} -> raise "#{error}"
      end

    DataFrame.rename(df, ["values", "counts"])
  end

  # Window

  @impl true
  def window_max(series, window_size, opts) do
    window_function(:s_rolling_max, series, window_size, opts)
  end

  @impl true
  def window_mean(series, window_size, opts) do
    window_function(:s_rolling_mean, series, window_size, opts)
  end

  @impl true
  def window_min(series, window_size, opts) do
    window_function(:s_rolling_min, series, window_size, opts)
  end

  @impl true
  def window_sum(series, window_size, opts) do
    window_function(:s_rolling_sum, series, window_size, opts)
  end

  defp window_function(operation, series, window_size, opts) do
    weights = Keyword.fetch!(opts, :weights)
    min_periods = Keyword.fetch!(opts, :min_periods)
    center = Keyword.fetch!(opts, :center)

    Shared.apply_series(series, operation, [window_size, weights, min_periods, center])
  end

  # Missing values

  @impl true
  def fill_missing(series, strategy) when is_atom(strategy),
    do: Shared.apply_series(series, :s_fill_none, [Atom.to_string(strategy)])

  def fill_missing(series, value) do
    operation =
      cond do
        is_float(value) -> :s_fill_none_with_float
        is_integer(value) -> :s_fill_none_with_int
        is_binary(value) -> :s_fill_none_with_bin
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

  defp scalar_rhs(scalar, lhs),
    do:
      scalar
      |> List.duplicate(PolarsSeries.size(lhs))
      |> Series.from_list()
end

defimpl Inspect, for: Explorer.PolarsBackend.Series do
  alias Explorer.PolarsBackend.Native

  def inspect(s, _opts) do
    case Native.s_as_str(s) do
      {:ok, str} -> str
      {:error, error} -> raise "#{error}"
    end
  end
end
