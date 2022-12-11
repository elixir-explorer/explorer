defmodule Explorer.Backend.Series do
  @moduledoc """
  The behaviour for series backends.
  """

  @valid_dtypes [:integer, :float, :boolean, :string, :date, :datetime, :list, :binary]

  @type t :: struct()

  @type s :: Explorer.Series.t()
  @type lazy_s :: Explorer.Series.lazy_t()
  @type df :: Explorer.DataFrame.t()
  @type dtype :: Explorer.Series.dtype()
  @type valid_types :: number() | boolean() | String.t() | Date.t() | NaiveDateTime.t()

  # Conversion

  @callback from_list(list(), dtype()) :: s
  @callback from_binary(binary(), dtype()) :: s
  @callback to_list(s) :: list()
  @callback to_iovec(s) :: [binary()]
  @callback cast(s, dtype) :: s

  # Introspection

  @callback dtype(s) :: dtype()
  @callback size(s) :: non_neg_integer() | lazy_s()
  @callback inspect(s, opts :: Inspect.Opts.t()) :: Inspect.Algebra.t()
  @callback bintype(s) :: :uft8 | :binary | {:s | :u | :f, non_neg_integer}

  # Slice and dice

  @callback head(s, n :: integer()) :: s
  @callback tail(s, n :: integer()) :: s
  @callback sample(s, n_or_frac :: number(), replacement :: boolean(), seed :: integer()) :: s
  @callback at(s, idx :: integer()) :: s
  @callback at_every(s, integer()) :: s
  @callback mask(s, mask :: s) :: s
  @callback slice(s, indices :: list()) :: s
  @callback slice(s, offset :: integer(), length :: integer()) :: s
  @callback concat(s, s) :: s
  @callback coalesce(s, s) :: s
  @callback first(s) :: valid_types() | lazy_s()
  @callback last(s) :: valid_types() | lazy_s()
  @callback select(predicate :: s, s, s) :: s
  @callback shift(s, offset :: integer, default :: nil) :: s

  # Aggregation

  @callback count(s) :: number() | lazy_s()
  @callback sum(s) :: number() | lazy_s() | nil
  @callback min(s) :: number() | Date.t() | NaiveDateTime.t() | lazy_s() | nil
  @callback max(s) :: number() | Date.t() | NaiveDateTime.t() | lazy_s() | nil
  @callback mean(s) :: float() | lazy_s() | nil
  @callback median(s) :: float() | lazy_s() | nil
  @callback variance(s) :: float() | lazy_s() | nil
  @callback standard_deviation(s) :: float() | lazy_s() | nil
  @callback quantile(s, float()) :: number | Date.t() | NaiveDateTime.t() | lazy_s() | nil
  @callback nil_count(s) :: number() | lazy_s()

  # Cumulative

  @callback cumulative_max(s, reverse? :: boolean()) :: s
  @callback cumulative_min(s, reverse? :: boolean()) :: s
  @callback cumulative_sum(s, reverse? :: boolean()) :: s

  # Local minima/maxima

  @callback peaks(s, :max | :min) :: s

  # Arithmetic

  @callback add(s | number(), s | number()) :: s
  @callback subtract(s | number(), s | number()) :: s
  @callback multiply(s | number(), s | number()) :: s
  @callback divide(s | number(), s | number()) :: s
  @callback quotient(s | neg_integer() | pos_integer(), s | neg_integer() | pos_integer()) :: s
  @callback remainder(s | neg_integer() | pos_integer(), s | neg_integer() | pos_integer()) :: s
  @callback pow(s | number(), s | number()) :: s

  # Comparisons

  @callback equal(s | valid_types(), s | valid_types()) :: s
  @callback not_equal(s | valid_types(), s | valid_types()) :: s
  @callback greater(s | valid_types(), s | valid_types()) :: s
  @callback greater_equal(s | valid_types(), s | valid_types()) :: s
  @callback less(s | valid_types(), s | valid_types()) :: s
  @callback less_equal(s | valid_types(), s | valid_types()) :: s
  @callback all_equal(s, s) :: boolean() | lazy_s()

  @callback binary_and(s, s) :: s
  @callback binary_or(s, s) :: s
  @callback binary_in(s, s) :: s

  # Coercion

  # Sort

  @callback sort(s, descending? :: boolean(), nils_last :: boolean()) :: s
  @callback argsort(s, descending? :: boolean(), nils_last :: boolean()) :: s
  @callback reverse(s) :: s

  # Distinct

  @callback distinct(s) :: s
  @callback unordered_distinct(s) :: s
  @callback n_distinct(s) :: integer() | lazy_s()
  @callback frequencies(s) :: df

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

  @callback fill_missing(
              s,
              strategy :: :backward | :forward | :min | :max | :mean | valid_types()
            ) :: s
  @callback is_nil(s) :: s
  @callback is_not_nil(s) :: s

  # Escape hatch

  @callback transform(s, fun) :: s | list()

  # Inversions

  @callback unary_not(s) :: s

  # Functions

  @doc """
  Create a new `Series`.
  """
  def new(data, dtype) when dtype in @valid_dtypes do
    %Explorer.Series{data: data, dtype: dtype}
  end

  import Inspect.Algebra
  alias Explorer.Series

  @doc """
  Default inspect implementation for backends.
  """
  def inspect(series, backend, n_rows, inspect_opts, opts \\ [])
      when is_binary(backend) and (is_integer(n_rows) or is_nil(n_rows)) and is_list(opts) do
    open = color("[", :list, inspect_opts)
    close = color("]", :list, inspect_opts)
    dtype = color("#{Series.dtype(series)} ", :atom, inspect_opts)

    data =
      container_doc(
        open,
        series |> Series.slice(0, inspect_opts.limit + 1) |> Series.to_list(),
        close,
        inspect_opts,
        &Explorer.Shared.to_string/2
      )

    concat([
      color(backend, :atom, inspect_opts),
      open,
      "#{n_rows || "???"}",
      close,
      line(),
      dtype,
      data
    ])
  end
end
