defmodule Explorer.PolarsBackend.Series do
  @moduledoc """
  Polars backend for `Explorer.Series`.
  """

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
        :integer ->
          Native.s_new_i64(name, data)

        :float ->
          Native.s_new_f64(name, data)

        :boolean ->
          Native.s_new_bool(name, data)

        :string ->
          Native.s_new_str(name, data)

        :date ->
          data
          |> Enum.map(&encode_date/1)
          |> then(&Native.s_new_date32(name, &1))

        :datetime ->
          data |> Enum.map(&encode_datetime/1) |> then(&Native.s_new_date64(name, &1))
      end

    %Series{data: series, dtype: type}
  end

  @impl true
  def to_list(%Series{data: series, dtype: dtype}) do
    list =
      case Native.s_to_list(series) do
        {:ok, list} -> list
        {:error, e} -> raise "#{e}"
      end

    case dtype do
      :date -> Enum.map(list, &decode_date/1)
      :datetime -> Enum.map(list, &decode_datetime/1)
      _ -> list
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
  def sample(series, n, with_replacement) when is_integer(n),
    do: Shared.apply_native(series, :s_sample_n, [n, with_replacement])

  def sample(series, frac, with_replacement) when is_float(frac),
    do: Shared.apply_native(series, :s_sample_frac, [frac, with_replacement])

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
  def get(%Series{dtype: :date} = series, idx),
    do:
      series
      |> Shared.apply_native(:s_get, [idx])
      |> decode_date()

  def get(%Series{dtype: :datetime} = series, idx),
    do:
      series
      |> Shared.apply_native(:s_get, [idx])
      |> decode_datetime()

  def get(series, idx), do: Shared.apply_native(series, :s_get, [idx])

  # Aggregation

  @impl true
  def sum(series), do: Shared.apply_native(series, :s_sum)

  @impl true
  def min(%Series{dtype: :date} = series),
    do:
      series
      |> Shared.apply_native(:s_min)
      |> decode_date()

  def min(%Series{dtype: :datetime} = series),
    do:
      series
      |> Shared.apply_native(:s_min)
      |> decode_datetime()

  def min(series), do: Shared.apply_native(series, :s_min)

  @impl true
  def max(%Series{dtype: :date} = series),
    do:
      series
      |> Shared.apply_native(:s_max)
      |> decode_date()

  def max(%Series{dtype: :datetime} = series),
    do:
      series
      |> Shared.apply_native(:s_max)
      |> decode_datetime()

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
  def quantile(series, quantile),
    do: series |> Shared.apply_native(:s_quantile, [quantile]) |> get(0)

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
  def pow(left, exponent) when is_number(exponent),
    do: Shared.apply_native(left, :s_pow, [exponent])

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

  # Sort

  @impl true
  def sort(series, reverse?), do: Shared.apply_native(series, :s_sort, [reverse?])

  @impl true
  def argsort(series, reverse?), do: Shared.apply_native(series, :s_argsort, [reverse?])

  @impl true
  def reverse(series), do: Shared.apply_native(series, :s_reverse)

  # Distinct

  @impl true
  def distinct(series), do: Shared.apply_native(series, :s_unique)

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
  def rolling_max(series, window_size, weight, ignore_nil?),
    do:
      Shared.apply_native(series, :s_rolling_max, [window_size, weight, ignore_nil?, window_size])

  @impl true
  def rolling_mean(series, window_size, weight, ignore_nil?),
    do:
      Shared.apply_native(series, :s_rolling_mean, [window_size, weight, ignore_nil?, window_size])

  @impl true
  def rolling_min(series, window_size, weight, ignore_nil?),
    do:
      Shared.apply_native(series, :s_rolling_min, [window_size, weight, ignore_nil?, window_size])

  @impl true
  def rolling_sum(series, window_size, weight, ignore_nil?),
    do:
      Shared.apply_native(series, :s_rolling_sum, [window_size, weight, ignore_nil?, window_size])

  # Missing values

  @impl true
  def fill_missing(series, strategy),
    do: Shared.apply_native(series, :s_fill_none, [Atom.to_string(strategy)])

  # Escape hatch
  @impl true
  def map(series, fun), do: series |> to_list() |> Enum.map(fun) |> from_list(dtype(series))

  # Polars specific functions

  def name(series), do: Shared.apply_native(series, :s_name)
  def rename(series, name), do: Shared.apply_native(series, :s_rename, [name])

  # Helpers

  defp scalar_rhs(scalar, lhs),
    do:
      scalar
      |> List.duplicate(PolarsSeries.length(lhs))
      |> Series.from_list()

  defp encode_date(%Date{} = date), do: Date.to_iso8601(date)
  defp encode_date(date) when is_nil(date), do: nil

  defp encode_datetime(%NaiveDateTime{} = datetime), do: NaiveDateTime.to_iso8601(datetime)
  defp encode_datetime(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp encode_datetime(datetime) when is_nil(datetime), do: nil

  defp decode_date(date) when is_integer(date), do: Date.add(~D[1970-01-01], date)
  defp decode_date(date) when is_nil(date), do: nil

  defp decode_datetime(date) when is_integer(date),
    do: NaiveDateTime.add(~N[1970-01-01 00:00:00], date, :millisecond)

  defp decode_datetime(date) when is_nil(date), do: nil
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
