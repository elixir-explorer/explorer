defmodule Explorer.Query do
  @moduledoc """
  High-level query for Explorer.

  Queries convert regular Elixir code which compile to efficient
  dataframes operations. Inside a query, only the limited set of
  Series operations are available and identifiers, such as `strs`
  and `nums`, represent dataframe column names:

      iex> df = Explorer.DataFrame.new(strs: ["a", "b", "c"], nums: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, nums > 2)
      #Explorer.DataFrame<
        Polars[1 x 2]
        strs string ["c"]
        nums integer [3]
      >

  If you want to access variables defined outside of the query
  or get access to all Elixir constructs, you must use `^`:

      iex> min = 2
      iex> df = Explorer.DataFrame.new(strs: ["a", "b", "c"], nums: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, nums > ^min)
      #Explorer.DataFrame<
        Polars[1 x 2]
        strs string ["c"]
        nums integer [3]
      >

      iex> min = 2
      iex> df = Explorer.DataFrame.new(strs: ["a", "b", "c"], nums: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, nums < ^if(min > 0, do: 10, else: -10))
      #Explorer.DataFrame<
        Polars[3 x 2]
        strs string ["a", "b", "c"]
        nums integer [1, 2, 3]
      >

  All operations from `Explorer.Series` are imported inside queries.
  This module also provides operators to use in queries, which are
  also imported into queries.

  ## Supported operations

  Queries are supported in the following operations:

    * `Explorer.DataFrame.arrange/2`
    * `Explorer.DataFrame.filter/2`
    * `Explorer.DataFrame.mutate/2`
    * `Explorer.DataFrame.summarise/2`

  ## Implementation details

  Queries simply become lazy dataframe operations at runtime.
  For example, the following query

      Explorer.DataFrame.filter(df, nums > 2)

  is equivalent to

      Explorer.DataFrame.filter_with(df, fn df -> df["nums"] > 2 end)

  This means that, whenever you want to generate queries programatically,
  you can fallback to the regular `_with` APIs.
  """

  kernel_all = Kernel.__info__(:functions) ++ Kernel.__info__(:macros)

  kernel_only = [
    @: 1,
    |>: 2,
    dbg: 0,
    dbg: 1,
    dbg: 2,
    sigil_c: 2,
    sigil_C: 2,
    sigil_D: 2,
    sigil_N: 2,
    sigil_s: 2,
    sigil_S: 2,
    sigil_w: 2,
    sigil_W: 2,
    tap: 2,
    then: 2
  ]

  @kernel_only kernel_only -- kernel_only -- kernel_all

  @doc """
  Builds an anonymous function from a query.

  This is the entry point used by `Explorer.DataFrame.filter/2`
  and friends to convert queries into anonymous functions.
  See the moduledoc for more information.
  """
  defmacro query(expression) do
    df = Macro.unique_var(:df, __MODULE__)
    {query, vars} = traverse(expression, [], %{df: df})

    quote do
      fn unquote(df) ->
        unquote_splicing(Enum.reverse(vars))
        import Kernel, only: unquote(@kernel_only)
        import Explorer.Query, except: [query: 1]
        import Explorer.Series
        unquote(query)
      end
    end
  end

  defp traverse({:^, meta, [expr]}, vars, _state) do
    var = Macro.unique_var(:pin, __MODULE__)
    {var, [{:=, meta, [var, expr]} | vars]}
  end

  defp traverse({var, meta, ctx}, vars, state) when is_atom(var) and is_atom(ctx) do
    {{{:., meta, [Explorer.DataFrame, :pull]}, meta, [state.df, var]}, vars}
  end

  defp traverse({left, meta, right}, vars, state) do
    if is_atom(left) and is_list(right) and special_form_defines_var?(left, right) do
      raise ArgumentError, "#{left}/#{length(right)} is not currently supported in Explorer.Query"
    end

    {left, vars} = traverse(left, vars, state)
    {right, vars} = traverse(right, vars, state)
    {{left, meta, right}, vars}
  end

  defp traverse({left, right}, vars, state) do
    {left, vars} = traverse(left, vars, state)
    {right, vars} = traverse(right, vars, state)
    {{left, right}, vars}
  end

  defp traverse(list, vars, state) when is_list(list) do
    Enum.map_reduce(list, vars, &traverse(&1, &2, state))
  end

  defp traverse(other, vars, _state), do: {other, vars}

  defp special_form_defines_var?(:=, [_, _]), do: true
  defp special_form_defines_var?(:case, [_, _]), do: true
  defp special_form_defines_var?(:cond, [_]), do: true
  defp special_form_defines_var?(:for, [_ | _]), do: true
  defp special_form_defines_var?(:receive, [_]), do: true
  defp special_form_defines_var?(:try, [_]), do: true
  defp special_form_defines_var?(:with, [_ | _]), do: true
  defp special_form_defines_var?(_, _), do: false

  # and and or are sent as is to queries
  binary_delegates = [
    ==: :equal,
    !=: :not_equal,
    >: :greater,
    >=: :greater_equal,
    <: :less,
    <=: :less_equal,
    +: :add,
    -: :subtract,
    *: :multiply,
    /: :divide,
    **: :pow
  ]

  for {operator, delegate} <- binary_delegates do
    @doc """
    Delegate to `Explorer.Series.#{delegate}/2`.
    """
    def unquote(operator)(left, right), do: Explorer.Series.unquote(delegate)(left, right)
  end

  @doc """
  Unary minus operator.

  Works with numbers and series.
  """
  def -number when is_number(number), do: Kernel.-(number)

  def -series when is_struct(series, Explorer.Series),
    do: Explorer.Series.multiply(series, Kernel.-(1))

  @doc """
  Unary plus operator.

  Works with numbers and series.
  """
  def +number when is_number(number), do: number
  def +series when is_struct(series, Explorer.Series), do: series
end
