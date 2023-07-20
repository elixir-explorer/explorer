defmodule Explorer.Backend.Series do
  @moduledoc """
  The behaviour for series backends.
  """

  @valid_dtypes Explorer.Shared.dtypes()

  @type t :: struct()

  @type s :: Explorer.Series.t()
  @type lazy_s :: Explorer.Series.lazy_t()
  @type df :: Explorer.DataFrame.t()
  @type dtype :: Explorer.Series.dtype()
  @type valid_types :: number() | boolean() | String.t() | Date.t() | Time.t() | NaiveDateTime.t()
  @type non_finite :: Explorer.Series.non_finite()
  @type option(type) :: type | nil

  # Conversion

  @callback from_list(list(), dtype()) :: s
  @callback from_binary(binary(), dtype()) :: s
  @callback to_list(s) :: list()
  @callback to_iovec(s) :: [binary()]
  @callback cast(s, dtype) :: s
  @callback categorise(s, s) :: s
  @callback strptime(s, String.t()) :: s
  @callback strftime(s, String.t()) :: s

  # Introspection

  @callback dtype(s) :: dtype()
  @callback size(s) :: non_neg_integer() | lazy_s()
  @callback inspect(s, opts :: Inspect.Opts.t()) :: Inspect.Algebra.t()
  @callback iotype(s) :: :uft8 | :binary | {:s | :u | :f, non_neg_integer}
  @callback categories(s) :: s

  # Slice and dice

  @callback head(s, n :: integer()) :: s
  @callback tail(s, n :: integer()) :: s
  @callback sample(
              s,
              n_or_frac :: number(),
              replacement :: boolean(),
              shuffle :: boolean(),
              seed :: option(integer())
            ) :: s
  @callback at(s, idx :: integer()) :: s
  @callback at_every(s, integer()) :: s
  @callback mask(s, mask :: s) :: s
  @callback slice(s, indices :: list() | s()) :: s
  @callback slice(s, offset :: integer(), length :: integer()) :: s
  @callback format(list(s)) :: s
  @callback concat(list(s)) :: s
  @callback coalesce(s, s) :: s
  @callback first(s) :: valid_types() | lazy_s()
  @callback last(s) :: valid_types() | lazy_s()
  @callback select(
              predicate :: s,
              s | valid_types() | non_finite(),
              s | valid_types() | non_finite()
            ) :: s
  @callback shift(s, offset :: integer, default :: nil) :: s
  @callback rank(
              s,
              method :: String.t(),
              descending :: boolean(),
              seed :: option(integer())
            ) :: s | lazy_s()
  # Aggregation

  @callback count(s) :: number() | lazy_s()
  @callback sum(s) :: number() | non_finite() | lazy_s() | nil
  @callback min(s) :: number() | non_finite() | Date.t() | NaiveDateTime.t() | lazy_s() | nil
  @callback max(s) :: number() | non_finite() | Date.t() | NaiveDateTime.t() | lazy_s() | nil
  @callback argmin(s) :: number() | non_finite() | lazy_s() | nil
  @callback argmax(s) :: number() | non_finite() | lazy_s() | nil
  @callback mean(s) :: float() | non_finite() | lazy_s() | nil
  @callback median(s) :: float() | non_finite() | lazy_s() | nil
  @callback variance(s) :: float() | non_finite() | lazy_s() | nil
  @callback standard_deviation(s) :: float() | non_finite() | lazy_s() | nil
  @callback quantile(s, float()) ::
              number() | non_finite() | Date.t() | NaiveDateTime.t() | lazy_s() | nil
  @callback nil_count(s) :: number() | lazy_s()
  @callback product(s) :: float() | non_finite() | lazy_s() | nil
  @callback skew(s, bias? :: boolean()) :: float() | non_finite() | lazy_s() | nil
  @callback correlation(s, s, ddof :: non_neg_integer()) ::
              float() | non_finite() | lazy_s() | nil
  @callback covariance(s, s) :: float() | non_finite() | lazy_s() | nil

  # Cumulative

  @callback cumulative_max(s, reverse? :: boolean()) :: s
  @callback cumulative_min(s, reverse? :: boolean()) :: s
  @callback cumulative_sum(s, reverse? :: boolean()) :: s
  @callback cumulative_product(s, reverse? :: boolean()) :: s

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
  @callback log(argument :: s) :: s
  @callback log(argument :: s, base :: float()) :: s
  @callback exp(s) :: s
  @callback abs(s) :: s
  @callback clip(s, number(), number()) :: s

  # Trigonometry

  @callback acos(s) :: s
  @callback asin(s) :: s
  @callback atan(s) :: s
  @callback cos(s) :: s
  @callback sin(s) :: s
  @callback tan(s) :: s

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

  # Float predicates
  @callback is_finite(s) :: s
  @callback is_infinite(s) :: s
  @callback is_nan(s) :: s

  # Float round
  @callback round(s, decimals :: non_neg_integer()) :: s
  @callback floor(s) :: s
  @callback ceil(s) :: s

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

  # Categorisation

  @callback cut(s, [float()], [String.t()] | nil, String.t() | nil, String.t() | nil) ::
              df
  @callback qcut(s, [float()], [String.t()] | nil, String.t() | nil, String.t() | nil) ::
              df

  # Rolling

  @callback window_sum(
              s,
              window_size :: integer(),
              weights :: [float()] | nil,
              min_periods :: integer() | nil,
              center :: boolean()
            ) :: s
  @callback window_min(
              s,
              window_size :: integer(),
              weights :: [float()] | nil,
              min_periods :: integer() | nil,
              center :: boolean()
            ) :: s
  @callback window_max(
              s,
              window_size :: integer(),
              weights :: [float()] | nil,
              min_periods :: integer() | nil,
              center :: boolean()
            ) :: s
  @callback window_mean(
              s,
              window_size :: integer(),
              weights :: [float()] | nil,
              min_periods :: integer() | nil,
              center :: boolean()
            ) :: s
  @callback window_standard_deviation(
              s,
              window_size :: integer(),
              weights :: [float()] | nil,
              min_periods :: integer() | nil,
              center :: boolean()
            ) :: s

  # Exponentially weighted windows

  @callback ewm_mean(
              s,
              alpha :: float(),
              adjust :: boolean(),
              min_periods :: integer(),
              ignore_nils :: boolean()
            ) :: s

  # Nulls

  @callback fill_missing_with_strategy(s, :backward | :forward | :min | :max | :mean) :: s
  @callback fill_missing_with_value(s, :nan | valid_types()) :: s
  @callback is_nil(s) :: s
  @callback is_not_nil(s) :: s

  # Escape hatch

  @callback transform(s, fun) :: s

  # Inversions

  @callback unary_not(s) :: s

  # Strings

  @callback contains(s, String.t()) :: s
  @callback upcase(s) :: s
  @callback downcase(s) :: s
  @callback trim(s) :: s
  @callback trim_leading(s) :: s
  @callback trim_trailing(s) :: s

  # Date / DateTime

  @callback day_of_week(s) :: s
  @callback month(s) :: s
  @callback year(s) :: s
  @callback hour(s) :: s
  @callback minute(s) :: s
  @callback second(s) :: s

  # Functions

  @doc """
  Create a new `Series`.
  """
  def new(data, dtype) when dtype in @valid_dtypes do
    %Explorer.Series{data: data, dtype: dtype}
  end

  alias Inspect.Algebra, as: A
  alias Explorer.Series

  @doc """
  Default inspect implementation for backends.
  """
  def inspect(series, backend, n_rows, inspect_opts, opts \\ [])
      when is_binary(backend) and (is_integer(n_rows) or is_nil(n_rows)) and is_list(opts) do
    open = A.color("[", :list, inspect_opts)
    close = A.color("]", :list, inspect_opts)
    dtype = A.color("#{Series.dtype(series)} ", :atom, inspect_opts)

    data =
      A.container_doc(
        open,
        series |> Series.slice(0, inspect_opts.limit + 1) |> Series.to_list(),
        close,
        inspect_opts,
        &Explorer.Shared.to_string/2
      )

    A.concat([
      A.color(backend, :atom, inspect_opts),
      open,
      "#{n_rows || "???"}",
      close,
      A.line(),
      dtype,
      data
    ])
  end
end
