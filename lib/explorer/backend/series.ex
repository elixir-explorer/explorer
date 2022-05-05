defmodule Explorer.Backend.Series do
  @moduledoc """
  The behaviour for series backends.
  """

  @type t :: %{__struct__: atom()}

  @type s :: Explorer.Series.t()
  @type df :: Explorer.DataFrame.t()
  @type dtype :: :integer | :float | :boolean | :string | :date | :datetime

  # Conversion

  @callback from_list(list(), dtype()) :: s
  @callback to_list(s) :: list()
  @callback to_enum(s) :: Enumerable.t()
  @callback cast(s, dtype) :: s

  # Introspection

  @callback dtype(s) :: dtype()
  @callback size(s) :: integer()

  # Slice and dice

  @callback head(s, n :: integer()) :: s
  @callback tail(s, n :: integer()) :: s
  @callback sample(s, n :: integer(), replacement :: boolean(), seed :: integer()) :: s
  @callback take_every(s, integer()) :: s
  @callback filter(s, mask :: s) :: s
  @callback filter(s, function()) :: s
  @callback slice(s, offset :: integer(), length :: integer()) :: s
  @callback take(s, indices :: list()) :: s
  @callback get(s, idx :: integer()) :: s
  @callback concat(s, s) :: s

  # Aggregation

  @callback sum(s) :: number()
  @callback min(s) :: number() | Date.t() | NaiveDateTime.t()
  @callback max(s) :: number() | Date.t() | NaiveDateTime.t()
  @callback mean(s) :: float()
  @callback median(s) :: float()
  @callback var(s) :: float()
  @callback std(s) :: float()
  @callback quantile(s, float()) :: number | Date.t() | NaiveDateTime.t()

  # Cumulative

  @callback cumulative_max(s, reverse? :: boolean()) :: s
  @callback cumulative_min(s, reverse? :: boolean()) :: s
  @callback cumulative_sum(s, reverse? :: boolean()) :: s

  # Local minima/maxima

  @callback peaks(s, :max | :min) :: s

  # Arithmetic

  @callback add(s, s | number()) :: s
  @callback subtract(s, s | number()) :: s
  @callback multiply(s, s | number()) :: s
  @callback divide(s, s | number()) :: s
  @callback pow(s, number()) :: s

  # Comparisons

  @callback eq(s, s | number()) :: s
  @callback neq(s, s | number()) :: s
  @callback gt(s, s | number()) :: s
  @callback gt_eq(s, s | number()) :: s
  @callback lt(s, s | number()) :: s
  @callback lt_eq(s, s | number()) :: s
  @callback all_equal?(s, s) :: boolean()

  @callback binary_and(s, s) :: s
  @callback binary_or(s, s) :: s

  # Coercion

  # Sort

  @callback sort(s, reverse? :: boolean()) :: s
  @callback argsort(s, reverse? :: boolean()) :: s
  @callback reverse(s) :: s

  # Distinct

  @callback distinct(s) :: s
  @callback unordered_distinct(s) :: s
  @callback n_distinct(s) :: integer()
  @callback count(s) :: df

  # Rolling

  @type window_option ::
          {:weights, [float()] | nil}
          | {:min_periods, integer() | nil}
          | {:center, boolean()}

  @callback window_sum(s, window_size :: integer(), [window_option()]) :: s
  @callback window_min(s, window_size :: integer(), [window_option()]) :: s
  @callback window_max(s, window_size :: integer(), [window_option()]) :: s
  @callback window_mean(s, window_size :: integer(), [window_option()]) :: s

  # Nulls

  @callback fill_missing(s, strategy :: :backward | :forward | :min | :max | :mean) :: s
  @callback nil?(s) :: s
  @callback not_nil?(s) :: s

  # Escape hatch

  @callback transform(s, fun) :: s | list()
end
