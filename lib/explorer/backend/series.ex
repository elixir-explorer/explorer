defmodule Explorer.Backend.Series do
  @moduledoc """
  The behaviour for series backends.
  """

  @type t :: struct()

  @type s :: Explorer.Series.t()
  @type lazy_s :: Explorer.Series.lazy_t()
  @type df :: Explorer.DataFrame.t()
  @type dtype :: Explorer.Series.dtype()

  @type valid_types ::
          number()
          | boolean()
          | String.t()
          | Date.t()
          | Time.t()
          | NaiveDateTime.t()
          | Explorer.Duration.t()

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
  @callback select(predicate :: s, s, s) :: s
  @callback shift(s, offset :: integer, default :: nil) :: s
  @callback rank(
              s,
              method :: atom(),
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
  @callback mode(s) :: s | lazy_s()
  @callback variance(s, ddof :: non_neg_integer()) :: float() | non_finite() | lazy_s() | nil
  @callback standard_deviation(s, ddof :: non_neg_integer()) ::
              float() | non_finite() | lazy_s() | nil
  @callback quantile(s, float()) ::
              number() | non_finite() | Date.t() | NaiveDateTime.t() | lazy_s() | nil
  @callback nil_count(s) :: number() | lazy_s()
  @callback product(s) :: float() | non_finite() | lazy_s() | nil
  @callback skew(s, bias? :: boolean()) :: float() | non_finite() | lazy_s() | nil
  @callback correlation(s, s, ddof :: non_neg_integer(), method :: atom()) ::
              float() | non_finite() | lazy_s() | nil
  @callback covariance(s, s, ddof :: non_neg_integer()) :: float() | non_finite() | lazy_s() | nil
  @callback all?(s) :: boolean() | lazy_s()
  @callback any?(s) :: boolean() | lazy_s()
  @callback row_index(s) :: s | lazy_s()

  # Cumulative

  @callback cumulative_max(s, reverse? :: boolean()) :: s
  @callback cumulative_min(s, reverse? :: boolean()) :: s
  @callback cumulative_sum(s, reverse? :: boolean()) :: s
  @callback cumulative_product(s, reverse? :: boolean()) :: s

  # Local minima/maxima

  @callback peaks(s, :max | :min) :: s

  # Arithmetic

  @callback add(out_dtype :: dtype(), s, s) :: s
  @callback subtract(out_dtype :: dtype(), s, s) :: s
  @callback multiply(out_dtype :: dtype(), s, s) :: s
  @callback divide(out_dtype :: dtype(), s, s) :: s
  @callback quotient(s, s) :: s
  @callback remainder(s, s) :: s
  @callback pow(out_dtype :: dtype(), s, s) :: s
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

  @callback equal(s, s) :: s
  @callback not_equal(s, s) :: s
  @callback greater(s, s) :: s
  @callback greater_equal(s, s) :: s
  @callback less(s, s) :: s
  @callback less_equal(s, s) :: s
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

  @callback sort(
              s,
              descending? :: boolean(),
              maintain_order? :: boolean(),
              multithreaded? :: boolean(),
              nulls_last? :: boolean()
            ) :: s
  @callback argsort(
              s,
              descending? :: boolean(),
              maintain_order? :: boolean(),
              multithreaded? :: boolean(),
              nulls_last? :: boolean()
            ) :: s
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
  @callback window_median(
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

  @callback ewm_standard_deviation(
              s,
              alpha :: float(),
              adjust :: boolean(),
              bias :: boolean(),
              min_periods :: integer(),
              ignore_nils :: boolean()
            ) :: s

  @callback ewm_variance(
              s,
              alpha :: float(),
              adjust :: boolean(),
              bias :: boolean(),
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
  @callback replace(s, String.t(), String.t()) :: s
  @callback strip(s, String.t() | nil) :: s
  @callback lstrip(s, String.t() | nil) :: s
  @callback rstrip(s, String.t() | nil) :: s
  @callback substring(s, integer(), non_neg_integer() | nil) :: s
  @callback split(s, String.t()) :: s
  @callback split_into(s, String.t(), list(String.t() | atom())) :: s
  @callback json_decode(s, dtype()) :: s
  @callback json_path_match(s, String.t()) :: s

  # Date / DateTime

  @callback day_of_week(s) :: s
  @callback day_of_year(s) :: s
  @callback week_of_year(s) :: s
  @callback month(s) :: s
  @callback year(s) :: s
  @callback hour(s) :: s
  @callback minute(s) :: s
  @callback second(s) :: s

  # List
  @callback join(s, String.t()) :: s
  @callback lengths(s) :: s
  @callback member?(s, valid_types()) :: s

  # Struct
  @callback field(s, String.t()) :: s

  # Functions

  @doc """
  Create a new `Series`.
  """
  def new(data, dtype) do
    %Explorer.Series{data: data, dtype: dtype}
  end

  alias Inspect.Algebra, as: A
  alias Explorer.Series

  @doc """
  Default inspect implementation for backends.
  """
  def inspect(series, backend, n_rows, inspect_opts, opts \\ [])
      when is_binary(backend) and (is_integer(n_rows) or is_nil(n_rows) or is_binary(n_rows)) and
             is_list(opts) do
    elide_columns? = Keyword.get(opts, :elide_columns, false)
    open = A.color("[", :list, inspect_opts)
    close = A.color("]", :list, inspect_opts)

    type =
      series
      |> Series.dtype()
      |> Explorer.Shared.dtype_to_string()

    dtype = A.color("#{type} ", :atom, inspect_opts)

    data = build_series_data(series, inspect_opts, elide_columns?)

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

  defp build_series_data(_series, _inspect_opts, true) do
    "???"
  end

  defp build_series_data(series, inspect_opts, _elide_columns?) do
    series =
      case inspect_opts.limit do
        :infinity -> series
        limit when is_integer(limit) -> Series.slice(series, 0, limit + 1)
      end

    series
    |> Series.to_list()
    |> Explorer.Shared.to_doc(inspect_opts)
  end
end
