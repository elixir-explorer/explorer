defmodule Explorer.PolarsBackend.Series do
  @moduledoc false

  alias Explorer.Backend
  alias Explorer.DataFrame
  alias Explorer.PolarsBackend.Shared
  alias Explorer.Series

  @type t :: %__MODULE__{resource: reference()}

  defstruct resource: nil

  @behaviour Backend.Series

  @integer_types Explorer.Shared.integer_types()
  @numeric_types Explorer.Shared.numeric_types()

  @ops_all Backend.Series.behaviour_info(:callbacks) |> Enum.sort()

  # The first two arguments are series which must match in size (or have size 1).
  @ops_matching_size [
    add: 2,
    all_equal: 2,
    correlation: 4,
    covariance: 3,
    equal: 2,
    greater_equal: 2,
    greater: 2,
    less_equal: 2,
    less: 2,
    not_equal: 2,
    quotient: 2,
    remainder: 2,
    subtract: 2
  ]

  for {op, arity} <- @ops_matching_size do
    [s1, s2 | rest] = args = Macro.generate_arguments(arity, __MODULE__)

    @impl Backend.Series
    def unquote(op)(unquote_splicing(args)) do
      s1 = matching_size!(unquote(s1), unquote(s2))
      Shared.apply_series(s1, :"s_#{unquote(op)}", unquote([s2 | rest]))
    end
  end

  # For certain string functions we have a regex-based counterpart.
  @ops_re_pairs [
    [contains: 2, re_contains: 2],
    [count_matches: 2, re_count_matches: 2],
    [replace: 3, re_replace: 3]
  ]

  for [{op, arity}, {re_op, re_arity}] <- @ops_re_pairs do
    [series | rest] = args = Macro.generate_arguments(arity, __MODULE__)
    @impl Backend.Series
    def unquote(op)(unquote_splicing(args)) do
      Shared.apply_series(unquote(series), :"s_#{unquote(op)}", unquote(rest) ++ [true])
    end

    [re_series | re_rest] = re_args = Macro.generate_arguments(re_arity, __MODULE__)
    @impl Backend.Series
    def unquote(re_op)(unquote_splicing(re_args)) do
      Shared.apply_series(unquote(re_series), :"s_#{unquote(re_op)}", unquote(re_rest) ++ [false])
    end
  end

  # Some functions require a custom definition.
  @ops_custom [
    binary_and: 2,
    binary_in: 2,
    binary_or: 2,
    cast: 2,
    categories: 1,
    categorise: 2,
    clip: 3,
    concat: 1,
    correlation: 4,
    count: 1,
    covariance: 3,
    cut: 5,
    divide: 3,
    field: 2,
    fill_missing_with_strategy: 2,
    fill_missing_with_value: 2,
    first: 1,
    format: 1,
    frequencies: 1,
    from_binary: 2,
    from_list: 2,
    inspect: 2,
    is_nil: 1,
    is_not_nil: 1,
    last: 1,
    log: 1,
    member?: 2,
    multiply: 3,
    peaks: 2,
    pow: 3,
    qcut: 5,
    quantile: 2,
    sample: 5,
    select: 3,
    shift: 3,
    slice: 2,
    standard_deviation: 2,
    strptime: 2,
    transform: 2,
    unary_not: 1,
    variance: 2
  ]

  # All other functions have the default definition.
  @default_ops @ops_all -- (@ops_matching_size ++ @ops_custom ++ List.flatten(@ops_re_pairs))
  for {op, arity} <- @default_ops do
    [series | rest] = args = Macro.generate_arguments(arity, __MODULE__)

    @impl Backend.Series
    def unquote(op)(unquote_splicing(args)) do
      Shared.apply_series(unquote(series), :"s_#{unquote(op)}", [unquote_splicing(rest)])
    end
  end

  @impl true
  def from_list(data, type) when is_list(data) do
    series = Shared.from_list(data, type)
    Backend.Series.new(series, type)
  end

  @impl true
  def from_binary(data, dtype) when is_binary(data) do
    series = Shared.from_binary(data, dtype)
    Backend.Series.new(series, dtype)
  end

  @impl true
  def cast(series, dtype) do
    case {series.dtype, dtype} do
      {:string, {:naive_datetime, precision}} ->
        Shared.apply_series(series, :s_strptime, [nil, precision])

      _ ->
        Shared.apply_series(series, :s_cast, [dtype])
    end
  end

  @impl true
  def strptime(%Series{} = series, format_string) do
    Shared.apply_series(series, :s_strptime, [format_string, nil])
  end

  @impl true
  def categories(%Series{dtype: :category} = series),
    do: Shared.apply_series(series, :s_categories)

  @impl true
  def categorise(%Series{dtype: {integer_type, _}} = series, %Series{dtype: dtype} = categories)
      when dtype in [:string, :category] and integer_type in [:s, :u],
      do: Shared.apply_series(series, :s_categorise, [categories.data])

  @impl true
  def categorise(%Series{dtype: :string} = series, %Series{dtype: dtype} = categories)
      when dtype in [:string, :category],
      do: Shared.apply_series(series, :s_categorise, [categories.data])

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
  def slice(series, indices) when is_list(indices),
    do: Shared.apply_series(series, :s_slice_by_indices, [indices])

  @impl true
  def slice(series, %Series{} = indices),
    do: Shared.apply_series(series, :s_slice_by_series, [indices.data])

  @impl true
  def format(list) do
    {_, df_args, params} =
      Enum.reduce(list, {0, [], []}, fn s, {counter, df_args, params} ->
        if is_binary(s) or Kernel.is_nil(s) do
          {counter, df_args, [s | params]}
        else
          counter = counter + 1
          name = "#{counter}"
          column = Explorer.Backend.LazySeries.new(:column, [name], :string)
          {counter, [{name, s} | df_args], [column | params]}
        end
      end)

    df = Explorer.PolarsBackend.DataFrame.from_series(df_args)
    format_expr = Explorer.Backend.LazySeries.new(:format, [Enum.reverse(params)], :string)
    out_dtypes = Map.put(df.dtypes, "result", :string)
    out_names = ["result" | df.names]
    out_df = %{df | dtypes: out_dtypes, names: out_names}

    Explorer.PolarsBackend.DataFrame.mutate_with(df, out_df, [{"result", format_expr}])
    |> Explorer.PolarsBackend.DataFrame.pull("result")
  end

  @impl true
  def concat(series_list) do
    Shared.apply(:s_concat, [series_list])
    |> Shared.create_series()
  end

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

  @impl true
  # There is no `count` equivalent in Polars, so we need to make our own.
  def count(series), do: size(series) - nil_count(series)

  @impl true
  def variance(series, ddof), do: series |> Shared.apply_series(:s_variance, [ddof]) |> at(0)

  @impl true
  def standard_deviation(series, ddof),
    do: series |> Shared.apply_series(:s_standard_deviation, [ddof]) |> at(0)

  @impl true
  def quantile(series, quantile),
    do: Shared.apply_series(series, :s_quantile, [quantile, "nearest"])

  @impl true
  def peaks(series, :max), do: Shared.apply_series(series, :s_peak_max)
  def peaks(series, :min), do: Shared.apply_series(series, :s_peak_min)

  @impl true
  def multiply(out_dtype, left, right) do
    result = Shared.apply_series(matching_size!(left, right), :s_multiply, [right.data])

    # Polars currently returns inconsistent dtypes, e.g.:
    #   * `integer * duration -> duration` when `integer` is a scalar
    #   * `integer * duration ->  integer` when `integer` is a series
    # We need to return duration in these cases, so we need an additional cast.
    if match?({:duration, _}, out_dtype) and out_dtype != dtype(result) do
      cast(result, out_dtype)
    else
      result
    end
  end

  @impl true
  def divide(out_dtype, left, right) do
    result = Shared.apply_series(matching_size!(left, right), :s_divide, [right.data])

    # Polars currently returns inconsistent dtypes, e.g.:
    #   * `duration / integer -> duration` when `integer` is a scalar
    #   * `duration / integer ->  integer` when `integer` is a series
    # We need to return duration in these cases, so we need an additional cast.
    if match?({:duration, _}, out_dtype) and out_dtype != dtype(result) do
      cast(result, out_dtype)
    else
      result
    end
  end

  @impl true
  def pow(out_dtype, left, right) do
    _ = matching_size!(left, right)

    # We need to pre-cast or we may lose precision.
    left = Explorer.Series.cast(left, out_dtype)

    left_lazy = Explorer.Backend.LazySeries.new(:column, ["base"], left.dtype)
    right_lazy = Explorer.Backend.LazySeries.new(:column, ["exponent"], right.dtype)

    {df_args, pow_args} =
      case {size(left), size(right)} do
        {n, n} -> {[{"base", left}, {"exponent", right}], [left_lazy, right_lazy]}
        {1, _} -> {[{"exponent", right}], [Explorer.Series.at(left, 0), right_lazy]}
        {_, 1} -> {[{"base", left}], [left_lazy, Explorer.Series.at(right, 0)]}
      end

    df = Explorer.PolarsBackend.DataFrame.from_series(df_args)
    pow = Explorer.Backend.LazySeries.new(:pow, pow_args, out_dtype)

    out_dtypes = Map.put(df.dtypes, "pow", out_dtype)
    out_names = df.names ++ ["pow"]
    out_df = %{df | dtypes: out_dtypes, names: out_names}

    Explorer.PolarsBackend.DataFrame.mutate_with(df, out_df, [{"pow", pow}])
    |> Explorer.PolarsBackend.DataFrame.pull("pow")
  end

  @impl true
  def log(%Series{} = argument), do: Shared.apply_series(argument, :s_log_natural, [])

  @impl true
  def clip(%Series{dtype: dtype} = s, min, max)
      when dtype in @integer_types and is_integer(min) and is_integer(max),
      do: Shared.apply_series(s, :s_clip_integer, [min, max])

  def clip(%Series{} = s, min, max),
    do: s |> cast({:f, 64}) |> Shared.apply_series(:s_clip_float, [min * 1.0, max * 1.0])

  @impl true
  def binary_in(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(left, :s_in, [right.data])

  @impl true
  def binary_and(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(matching_size!(left, right), :s_and, [right.data])

  @impl true
  def binary_or(%Series{} = left, %Series{} = right),
    do: Shared.apply_series(matching_size!(left, right), :s_or, [right.data])

  @impl true
  def frequencies(%Series{dtype: {:list, inner_dtype} = dtype})
      when inner_dtype not in @numeric_types do
    raise ArgumentError,
          "frequencies/1 only works with series of lists of numeric types, but #{Explorer.Shared.dtype_to_string(dtype)} was given"
  end

  def frequencies(%Series{} = series) do
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
  def inspect(series, opts) when node(series.data.resource) != node() do
    Backend.Series.inspect(series, "Polars", "node: #{node(series.data.resource)}", opts,
      elide_columns: true
    )
  end

  def inspect(series, opts) do
    Backend.Series.inspect(series, "Polars", Series.size(series), opts)
  end

  # Inversions

  @impl true
  def unary_not(%Series{} = series), do: Shared.apply_series(series, :s_not, [])

  @impl true
  def member?(%Series{dtype: {:list, inner_dtype}} = series, value),
    do: Shared.apply_series(series, :s_member, [value, inner_dtype])

  @impl true
  def field(%Series{dtype: {:struct, _inner_dtype}} = series, name),
    do: Shared.apply_series(series, :s_field, [name])

  # Polars specific functions

  def col(%Series{}) do
    raise "Only implemented for `%LazySeries{}`."
  end

  def name(series), do: Shared.apply_series(series, :s_name)
  def rename(series, name), do: Shared.apply_series(series, :s_rename, [name])

  # Helpers

  defp matching_size!(series, other) do
    case size(series) do
      1 ->
        series

      i ->
        case size(other) do
          1 -> series
          ^i -> series
          j -> raise_size_mismatch_error(i, j)
        end
    end
  end

  defp raise_size_mismatch_error(i, j) do
    raise ArgumentError,
          "series must either have the same size or one of them must have size of 1, got: #{i} and #{j}"
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
