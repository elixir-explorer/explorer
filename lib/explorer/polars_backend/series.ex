defmodule Explorer.PolarsBackend.Series do
  @moduledoc false

  import Kernel, except: [length: 1]
  import Explorer.Shared, only: [check_types: 1, cast_numerics: 2]

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

    %Series{data: series, dtype: type}
  end

  @impl true
  def to_list(%Series{data: series}) do
    case Native.s_to_list(series) do
      {:ok, list} -> list
      {:error, e} -> raise "#{e}"
    end
  end

  @impl true
  def cast(series, dtype), do: Shared.apply_native(series, :s_cast, [Atom.to_string(dtype)])

  # Introspection

  @impl true
  def dtype(series), do: series |> Shared.apply_native(:s_dtype) |> Shared.normalise_dtype()

  @impl true
  def length(series), do: Shared.apply_native(series, :s_len)

  # Slice and dice

  @impl true
  def head(series, n_elements), do: Shared.apply_native(series, :s_head, [n_elements])

  @impl true
  def tail(series, n_elements), do: Shared.apply_native(series, :s_tail, [n_elements])

  @impl true
  def sample(series, n, with_replacement?, seed) when is_integer(n) do
    indices =
      series
      |> length()
      |> Native.s_seedable_random_indices(n, with_replacement?, seed)

    take(series, indices)
  end

  @impl true
  def take_every(series, every_n),
    do: Shared.apply_native(series, :s_take_every, [every_n])

  @impl true
  def filter(series, %Series{} = mask),
    do: Shared.apply_native(series, :s_filter, [Shared.to_polars_s(mask)])

  def filter(series, callback) when is_function(callback) do
    mask = callback.(series)
    filter(series, mask)
  end

  @impl true
  def slice(series, offset, length), do: Shared.apply_native(series, :s_slice, [offset, length])

  @impl true
  def take(series, indices) when is_list(indices),
    do: Shared.apply_native(series, :s_take, [indices])

  @impl true
  def get(series, idx) do
    idx = if idx < 0, do: length(series) + idx, else: idx
    Shared.apply_native(series, :s_get, [idx])
  end

  @impl true
  def concat(s1, s2), do: Shared.apply_native(s1, :s_append, [Shared.to_polars_s(s2)])

  # Aggregation

  @impl true
  def sum(series), do: Shared.apply_native(series, :s_sum)

  @impl true
  def min(series), do: Shared.apply_native(series, :s_min)

  @impl true
  def max(series), do: Shared.apply_native(series, :s_max)

  @impl true
  def mean(series), do: Shared.apply_native(series, :s_mean)

  @impl true
  def median(series), do: Shared.apply_native(series, :s_median)

  @impl true
  def var(series), do: Shared.apply_native(series, :s_var)

  @impl true
  def std(series), do: Shared.apply_native(series, :s_std)

  @impl true
  def quantile(series, quantile), do: Shared.apply_native(series, :s_quantile, [quantile])

  # Cumulative

  @impl true
  def cum_max(series, reverse?), do: Shared.apply_native(series, :s_cum_max, [reverse?])

  @impl true
  def cum_min(series, reverse?), do: Shared.apply_native(series, :s_cum_min, [reverse?])

  @impl true
  def cum_sum(series, reverse?), do: Shared.apply_native(series, :s_cum_sum, [reverse?])

  # Local minima/maxima

  @impl true
  def peaks(series, :max), do: Shared.apply_native(series, :s_peak_max)
  def peaks(series, :min), do: Shared.apply_native(series, :s_peak_min)

  # Arithmetic

  @impl true
  def add(left, %Series{} = right),
    do: Shared.apply_native(left, :s_add, [Shared.to_polars_s(right)])

  def add(left, right) when is_number(right), do: add(left, scalar_rhs(right, left))

  @impl true
  def subtract(left, %Series{} = right),
    do: Shared.apply_native(left, :s_sub, [Shared.to_polars_s(right)])

  def subtract(left, right) when is_number(right), do: subtract(left, scalar_rhs(right, left))

  @impl true
  def multiply(left, %Series{} = right),
    do: Shared.apply_native(left, :s_mul, [Shared.to_polars_s(right)])

  def multiply(left, right) when is_number(right), do: multiply(left, scalar_rhs(right, left))

  @impl true
  def divide(left, %Series{} = right),
    do: Shared.apply_native(left, :s_div, [Shared.to_polars_s(right)])

  def divide(left, right) when is_number(right), do: divide(left, scalar_rhs(right, left))

  @impl true
  def pow(left, exponent) when is_float(exponent),
    do: Shared.apply_native(left, :s_pow, [exponent])

  def pow(left, exponent) when is_integer(exponent) and exponent >= 0 do
    cond do
      Series.dtype(left) == :integer -> Shared.apply_native(left, :s_int_pow, [exponent])
      Series.dtype(left) == :float -> Shared.apply_native(left, :s_pow, [exponent / 1])
    end
  end

  # Comparisons

  @impl true
  def eq(left, %Series{} = right),
    do: Shared.apply_native(left, :s_eq, [Shared.to_polars_s(right)])

  def eq(left, right), do: eq(left, scalar_rhs(right, left))

  @impl true
  def neq(left, %Series{} = right),
    do: Shared.apply_native(left, :s_neq, [Shared.to_polars_s(right)])

  def neq(left, right), do: neq(left, scalar_rhs(right, left))

  @impl true
  def gt(left, %Series{} = right),
    do: Shared.apply_native(left, :s_gt, [Shared.to_polars_s(right)])

  def gt(left, right), do: gt(left, scalar_rhs(right, left))

  @impl true
  def gt_eq(left, %Series{} = right),
    do: Shared.apply_native(left, :s_gt_eq, [Shared.to_polars_s(right)])

  def gt_eq(left, right), do: gt_eq(left, scalar_rhs(right, left))

  @impl true
  def lt(left, %Series{} = right),
    do: Shared.apply_native(left, :s_lt, [Shared.to_polars_s(right)])

  def lt(left, right), do: lt(left, scalar_rhs(right, left))

  @impl true
  def lt_eq(left, %Series{} = right),
    do: Shared.apply_native(left, :s_lt_eq, [Shared.to_polars_s(right)])

  def lt_eq(left, right), do: lt_eq(left, scalar_rhs(right, left))

  @impl true
  def all_equal?(left, right),
    do: Shared.apply_native(left, :s_series_equal, [Shared.to_polars_s(right), true])

  @impl true
  def binary_and(left, right), do: Shared.apply_native(left, :s_and, [Shared.to_polars_s(right)])

  @impl true
  def binary_or(left, right), do: Shared.apply_native(left, :s_or, [Shared.to_polars_s(right)])
  # Sort

  @impl true
  def sort(series, reverse?), do: Shared.apply_native(series, :s_sort, [reverse?])

  @impl true
  def argsort(series, reverse?), do: Shared.apply_native(series, :s_argsort, [reverse?])

  @impl true
  def reverse(series), do: Shared.apply_native(series, :s_reverse)

  # Distinct

  @impl true
  def distinct(series), do: Shared.apply_native(series, :s_distinct)

  @impl true
  def unordered_distinct(series), do: Shared.apply_native(series, :s_unordered_distinct)

  @impl true
  def n_distinct(series), do: Shared.apply_native(series, :s_n_unique)

  @impl true
  def count(series),
    do:
      series
      |> Shared.to_polars_s()
      |> Native.s_value_counts()
      |> Shared.unwrap()
      |> Shared.to_dataframe()
      |> DataFrame.rename(["values", "counts"])
      |> DataFrame.mutate(counts: &Series.cast(&1["counts"], :integer))

  # Rolling

  @impl true
  def rolling_max(series, window_size, opts) do
    weights = Keyword.fetch!(opts, :weights)
    min_periods = Keyword.fetch!(opts, :min_periods)
    center = Keyword.fetch!(opts, :center)

    Shared.apply_native(series, :s_rolling_max, [window_size, weights, min_periods, center])
  end

  @impl true
  def rolling_mean(series, window_size, opts) do
    weights = Keyword.fetch!(opts, :weights)
    min_periods = Keyword.fetch!(opts, :min_periods)
    center = Keyword.fetch!(opts, :center)

    Shared.apply_native(series, :s_rolling_mean, [window_size, weights, min_periods, center])
  end

  @impl true
  def rolling_min(series, window_size, opts) do
    weights = Keyword.fetch!(opts, :weights)
    min_periods = Keyword.fetch!(opts, :min_periods)
    center = Keyword.fetch!(opts, :center)

    Shared.apply_native(series, :s_rolling_min, [window_size, weights, min_periods, center])
  end

  @impl true
  def rolling_sum(series, window_size, opts) do
    weights = Keyword.fetch!(opts, :weights)
    min_periods = Keyword.fetch!(opts, :min_periods)
    center = Keyword.fetch!(opts, :center)

    Shared.apply_native(series, :s_rolling_sum, [window_size, weights, min_periods, center])
  end

  # Missing values

  @impl true
  def fill_missing(series, strategy) when is_atom(strategy),
    do: Shared.apply_native(series, :s_fill_none, [Atom.to_string(strategy)])

  def fill_missing(series, strategy) do
    cond do
      is_float(strategy) -> Shared.apply_native(series, :s_fill_none_with_float, [strategy])
      is_integer(strategy) -> Shared.apply_native(series, :s_fill_none_with_int, [strategy])
      is_binary(strategy) -> Shared.apply_native(series, :s_fill_none_with_bin, [strategy])
    end
  end

  @impl true
  def nil?(series), do: Shared.apply_native(series, :s_is_null)

  @impl true
  def not_nil?(series), do: Shared.apply_native(series, :s_is_not_null)

  # Escape hatch
  @impl true
  def transform(series, fun) do
    list = series |> Series.to_list() |> Enum.map(fun)
    type = check_types(list)
    {list, type} = cast_numerics(list, type)

    from_list(list, type)
  end

  # Polars specific functions

  def name(series), do: Shared.apply_native(series, :s_name)
  def rename(series, name), do: Shared.apply_native(series, :s_rename, [name])

  # Helpers

  defp scalar_rhs(scalar, lhs),
    do:
      scalar
      |> List.duplicate(PolarsSeries.length(lhs))
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
