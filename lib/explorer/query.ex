defmodule Explorer.Query do
  @moduledoc ~S"""
  High-level query for Explorer.

  > #### Explorer.DataFrame vs DF {: .tip}
  >
  > All examples below assume you have defined aliased
  > `Explorer.DataFrame` to `DF` as shown below:
  >
  >     alias Explorer.DataFrame, as: DF
  >

  Queries convert regular Elixir code which compile to efficient
  dataframes operations. Inside a query, only the limited set of
  Series operations are available and identifiers, such as `strs`
  and `nums`, represent dataframe column names:

      iex> df = DF.new(strs: ["a", "b", "c"], nums: [1, 2, 3])
      iex> DF.filter(df, nums > 2)
      #Explorer.DataFrame<
        Polars[1 x 2]
        strs string ["c"]
        nums integer [3]
      >

  If a column has unusual format, you can either rename it before-hand,
  or use `col/1` inside queries:

      iex> df = DF.new("unusual nums": [1, 2, 3])
      iex> DF.filter(df, col("unusual nums") > 2)
      #Explorer.DataFrame<
        Polars[1 x 1]
        unusual nums integer [3]
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

  ## Interpolation

  If you want to access variables defined outside of the query
  or get access to all Elixir constructs, you must use `^`:

      iex> min = 2
      iex> df = DF.new(strs: ["a", "b", "c"], nums: [1, 2, 3])
      iex> DF.filter(df, nums > ^min)
      #Explorer.DataFrame<
        Polars[1 x 2]
        strs string ["c"]
        nums integer [3]
      >

      iex> min = 2
      iex> df = DF.new(strs: ["a", "b", "c"], nums: [1, 2, 3])
      iex> DF.filter(df, nums < ^if(min > 0, do: 10, else: -10))
      #Explorer.DataFrame<
        Polars[3 x 2]
        strs string ["a", "b", "c"]
        nums integer [1, 2, 3]
      >

  `^` can be used with `col` to access columns dynamically:

      iex> df = DF.new("unusual nums": [1, 2, 3])
      iex> name = "unusual nums"
      iex> DF.filter(df, col(^name) > 2)
      #Explorer.DataFrame<
        Polars[1 x 1]
        unusual nums integer [3]
      >

  ## Conditionals

  `cond/1` can be used to write multi-clause conditions:

      iex> df = DF.new(a: [10, 4, 6])
      iex> DF.mutate(df,
      ...>   b:
      ...>     cond do
      ...>       a > 9 -> "Exceptional"
      ...>       a > 5 -> "Passed"
      ...>       true -> "Failed"
      ...>     end
      ...> )
      #Explorer.DataFrame<
        Polars[3 x 2]
        a integer [10, 4, 6]
        b string ["Exceptional", "Failed", "Passed"]
      >

  ## Across and comprehensions

  `Explorer.Query` leverages the power behind Elixir for-comprehensions
  to provide a powerful syntax for traversing several columns in a dataframe
  at once. For example, imagine you want to standardization the data on the
  iris dataset, you could write this:

      iex> iris = Explorer.Datasets.iris()
      iex> DF.mutate(iris,
      ...>   sepal_width: (sepal_width - mean(sepal_width)) / variance(sepal_width),
      ...>   sepal_length: (sepal_length - mean(sepal_length)) / variance(sepal_length),
      ...>   petal_length: (petal_length - mean(petal_length)) / variance(petal_length),
      ...>   petal_width: (petal_width - mean(petal_width)) / variance(petal_width)
      ...> )
      #Explorer.DataFrame<
        Polars[150 x 5]
        sepal_length float [-1.0840606189132314, -1.3757361217598396, -1.6674116246064494, -1.8132493760297548, -1.2298983703365356, ...]
        sepal_width float [2.372289612531505, -0.28722789030650403, 0.7765791108287006, 0.24467561026109824, 2.904193113099107, ...]
        petal_length float [-0.7576391687443842, -0.7576391687443842, -0.7897606710936372, -0.725517666395131, -0.7576391687443842, ...]
        petal_width float [-1.7147014356654704, -1.7147014356654704, -1.7147014356654704, -1.7147014356654704, -1.7147014356654704, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
      >

  While the code above does its job, it is quite repetitive. With across and for-comprehensions,
  we could instead write:

      iex> iris = Explorer.Datasets.iris()
      iex> DF.mutate(iris,
      ...>   for col <- across(["sepal_width", "sepal_length", "petal_length", "petal_width"]) do
      ...>     {col.name, (col - mean(col)) / variance(col)}
      ...>   end
      ...> )
      #Explorer.DataFrame<
        Polars[150 x 5]
        sepal_length float [-1.0840606189132314, -1.3757361217598396, -1.6674116246064494, -1.8132493760297548, -1.2298983703365356, ...]
        sepal_width float [2.372289612531505, -0.28722789030650403, 0.7765791108287006, 0.24467561026109824, 2.904193113099107, ...]
        petal_length float [-0.7576391687443842, -0.7576391687443842, -0.7897606710936372, -0.725517666395131, -0.7576391687443842, ...]
        petal_width float [-1.7147014356654704, -1.7147014356654704, -1.7147014356654704, -1.7147014356654704, -1.7147014356654704, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
      >

  Which achieves the same result in a more concise and maintainable way.
  `across/1` may receive any of the following input as arguments:

    * a list of columns indexes or names as atoms and strings

    * a range

    * a regex that keeps only the names matching the regex

  For example, since we know the width and length columns are the first four,
  we could also have written (remember ranges in Elixir are inclusive):

      DF.mutate(iris,
        for col <- across(0..3) do
          {col.name, (col - mean(col)) / variance(col)}
        end
      )

  Or using a regex:

      DF.mutate(iris,
        for col <- across(~r/(sepal|petal)_(length|width)/) do
          {col.name, (col - mean(col)) / variance(col)}
        end
      )

  For those new to Elixir, for-comprehensions have the following format:

      for PATTERN <- GENERATOR, FILTER do
        EXPR
      end

  A comprehension filter is a mechanism that allows us to keep only columns
  based on additional properties, such as its `dtype`. A for-comprehension can
  have multiple generators and filters. For instance, if you want to apply
  standardization to all float columns, we can use `across/0` to access all
  columns and then use a filter to keep only the float ones:

      iex> iris = Explorer.Datasets.iris()
      iex> DF.mutate(iris,
      ...>   for col <- across(), col.dtype == :float do
      ...>     {col.name, (col - mean(col)) / variance(col)}
      ...>   end
      ...> )
      #Explorer.DataFrame<
        Polars[150 x 5]
        sepal_length float [-1.0840606189132314, -1.3757361217598396, -1.6674116246064494, -1.8132493760297548, -1.2298983703365356, ...]
        sepal_width float [2.372289612531505, -0.28722789030650403, 0.7765791108287006, 0.24467561026109824, 2.904193113099107, ...]
        petal_length float [-0.7576391687443842, -0.7576391687443842, -0.7897606710936372, -0.725517666395131, -0.7576391687443842, ...]
        petal_width float [-1.7147014356654704, -1.7147014356654704, -1.7147014356654704, -1.7147014356654704, -1.7147014356654704, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
      >

  For-comprehensions works with all dataframe verbs. As we have seen
  above, for mutations we must return tuples as pair with the mutation
  name and its value. `summarise` works similarly. Note in both cases
  the name could also be generated dynamically. For example, to compute
  the mean per species, you could write:

      iex> Explorer.Datasets.iris()
      ...> |> DF.group_by("species")
      ...> |> DF.summarise(
      ...>   for col <- across(), col.dtype == :float do
      ...>     {"#{col.name}_mean", mean(col)}
      ...>   end
      ...> )
      #Explorer.DataFrame<
        Polars[3 x 5]
        species string ["Iris-setosa", "Iris-versicolor", "Iris-virginica"]
        sepal_length_mean float [5.005999999999999, 5.936, 6.587999999999998]
        sepal_width_mean float [3.4180000000000006, 2.7700000000000005, 2.9739999999999998]
        petal_length_mean float [1.464, 4.26, 5.552]
        petal_width_mean float [0.2439999999999999, 1.3259999999999998, 2.026]
      >

  `arrange` expects a list of columns to sort by, while for-comprehensions
  in `filter` generate a list of conditions, which are joined using `and`.
  For example, to filter all entries have both sepal and petal length above
  average, using a filter on the column name, one could write:

      iex> iris = Explorer.Datasets.iris()
      iex> DF.filter(iris,
      ...>   for col <- across(), String.ends_with?(col.name, "_length") do
      ...>     col > mean(col)
      ...>   end
      ...> )
      #Explorer.DataFrame<
        Polars[70 x 5]
        sepal_length float [7.0, 6.4, 6.9, 6.5, 6.3, ...]
        sepal_width float [3.2, 3.2, 3.1, 2.8, 3.3, ...]
        petal_length float [4.7, 4.5, 4.9, 4.6, 4.7, ...]
        petal_width float [1.4, 1.5, 1.5, 1.5, 1.6, ...]
        species string ["Iris-versicolor", "Iris-versicolor", "Iris-versicolor", "Iris-versicolor", "Iris-versicolor", ...]
      >

  > #### Do not mix comprehension and queries {: .warning}
  >
  > The filter inside a for-comprehension works at the meta level:
  > it can only filter columns based on their names and dtypes, but
  > not on their values. For example, this code does not make any
  > sense and it will fail to compile:
  >
  >     |> DF.filter(
  >       for col <- across(), col > mean(col) do
  >         col
  >       end
  >     end)
  >
  > Another way to think about it, the comprehensions traverse on the
  > columns themselves, the contents inside the comprehension do-block
  > traverse on the values inside the columns.

  ## Implementation details

  Queries simply become lazy dataframe operations at runtime.
  For example, the following query

      Explorer.DataFrame.filter(df, nums > 2)

  is equivalent to

      Explorer.DataFrame.filter_with(df, fn df -> Explorer.Series.greater(df["nums"], 2) end)

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
    df = df_var()

    quote do
      fn unquote(df) ->
        unquote(traverse(expression, df))
      end
    end
  end

  defp traverse({:for, meta, [_ | _] = args}, df) do
    {args, [opts]} = Enum.split(args, Kernel.-(1))

    block =
      Keyword.get(opts, :do) || raise ArgumentError, "expected do-block in for-comprehension"

    {args, known_vars} =
      Enum.map_reduce(args, %{}, fn
        {:<-, meta, [pattern, generator]}, acc ->
          generator = traverse_for(generator, df, acc)
          {{:<-, meta, [pattern, generator]}, collect_pattern_vars(pattern, acc)}

        other, acc ->
          {traverse_for(other, df, acc), acc}
      end)

    {query, vars} =
      traverse(block, [], %{df: df, known_vars: known_vars, collect_pins_and_vars: true})

    block =
      quote do
        unquote_splicing(Enum.reverse(vars))
        import Kernel, only: unquote(@kernel_only)
        import Explorer.Query, except: [query: 1]
        import Explorer.Series
        unquote(query)
      end

    for = {:for, meta, args ++ [Keyword.put(opts, :do, block)]}

    quote do
      import Explorer.Query, only: [across: 0, across: 1]
      unquote(for)
    end
  end

  defp traverse(expression, df) do
    {query, vars} =
      traverse(expression, [], %{df: df, known_vars: %{}, collect_pins_and_vars: true})

    quote do
      unquote_splicing(Enum.reverse(vars))
      import Kernel, only: unquote(@kernel_only)
      import Explorer.Query, except: [query: 1]
      import Explorer.Series
      unquote(query)
    end
  end

  defp traverse({:^, meta, [expr]}, vars, state) do
    if state.collect_pins_and_vars do
      var = Macro.unique_var(:pin, __MODULE__)
      {var, [{:=, meta, [var, expr]} | vars]}
    else
      {expr, vars}
    end
  end

  defp traverse({:for, _meta, [_ | _]}, _vars, _state) do
    raise ArgumentError, "for-comprehensions are only supported at the root of queries"
  end

  defp traverse({:"::", meta, [left, right]}, vars, state) do
    {left, vars} = traverse(left, vars, state)
    {{:"::", meta, [left, right]}, vars}
  end

  defp traverse({:cond, _meta, [[do: clauses]]}, vars, state) do
    conditions =
      clauses
      |> Enum.map(fn {:->, _, [[on_condition], on_true]} ->
        {condition, _} = traverse(on_condition, vars, state)
        {truthy, _} = traverse(on_true, vars, state)

        {condition, truthy}
      end)
      |> Enum.reverse()

    block =
      quote do
        import Explorer.Query

        unquote(conditions)
        |> Enum.reduce(nil, fn
          {true, truthy}, _acc ->
            Explorer.Shared.lazy_series!(truthy)

          {false, _truthy}, acc ->
            Explorer.Shared.lazy_series!(acc)

          {nil, _truthy}, acc ->
            Explorer.Shared.lazy_series!(acc)

          {condition, truthy}, acc ->
            predicate = Explorer.Shared.lazy_series!(condition)
            on_true = Explorer.Shared.lazy_series!(truthy)

            on_false =
              case acc do
                nil -> Explorer.Backend.LazySeries.from_list([nil], on_true.dtype)
                _ -> Explorer.Shared.lazy_series!(acc)
              end

            Explorer.Backend.LazySeries.select(predicate, on_true, on_false)
        end)
      end

    {block, vars}
  end

  defp traverse({var, meta, ctx} = expr, vars, state) when is_atom(var) and is_atom(ctx) do
    cond do
      Map.has_key?(state.known_vars, {var, ctx}) ->
        {expr, vars}

      state.collect_pins_and_vars ->
        {{{:., meta, [Explorer.DataFrame, :pull]}, meta, [state.df, var]}, vars}

      true ->
        raise ArgumentError, "undefined variable \"#{Macro.to_string(expr)}\""
    end
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
  defp special_form_defines_var?(:receive, [_]), do: true
  defp special_form_defines_var?(:try, [_]), do: true
  defp special_form_defines_var?(:with, [_ | _]), do: true
  defp special_form_defines_var?(_, _), do: false

  defp traverse_for(expr, df, known_vars) do
    {expr, []} =
      traverse(expr, [], %{df: df, known_vars: known_vars, collect_pins_and_vars: false})

    expr
  end

  defp collect_pattern_vars({:when, _, [pattern, _]}, known_vars) do
    collect_pattern_vars(pattern, known_vars)
  end

  defp collect_pattern_vars(expr, known_vars) do
    expr
    |> Macro.prewalk(known_vars, fn
      {:"::", _, [left, _right]}, acc ->
        {left, acc}

      {skip, _, [_ | _]}, acc when skip in [:^, :@, :quote] ->
        {:ok, acc}

      {:_, _, context}, acc when is_atom(context) ->
        {:ok, acc}

      {name, _meta, context}, acc when is_atom(name) and is_atom(context) ->
        {:ok, Map.put(acc, {name, context}, true)}

      node, acc ->
        {node, acc}
    end)
    |> elem(1)
  end

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

  @doc """
  String concatenation operator.

  Works with strings and series of strings.

  ## Examples

      DF.mutate(df, name: first_name <> " " <> last_name)

  If you want to convert concatenate non-string
  series, you can explicitly cast them to string
  before:

      DF.mutate(df, name: cast(year, :string) <> "-" <> cast(month, :string))

  Or use format:

      DF.mutate(df, name: format([year, "-", month]))
  """
  defmacro left <> right do
    parts = [left | extract_concatenations(right)]

    quote do
      unquote(__MODULE__).__concatenate__(unquote(parts))
    end
  end

  defp extract_concatenations({:<>, _, [left, right]}), do: [left | extract_concatenations(right)]
  defp extract_concatenations(other), do: [other]

  @doc false
  def __concatenate__(parts) do
    case validate_concatenation(parts, true) do
      true -> IO.iodata_to_binary(parts)
      false -> Explorer.Series.format(parts)
    end
  end

  @error_message "the string concatenation operator (<>) inside Explorer.Query expects either " <>
                   "an Elixir string or a Series with :string dtype, got: "

  defp validate_concatenation([%Explorer.Series{dtype: :string} | parts], _all_binary?) do
    validate_concatenation(parts, false)
  end

  defp validate_concatenation([%Explorer.Series{} = part | _parts], _all_binary?) do
    raise ArgumentError,
          <<@error_message, inspect(part)::binary,
            " (use cast(series, :string) to convert an existing series)"::binary>>
  end

  defp validate_concatenation([part | parts], all_binary?) when is_binary(part) do
    validate_concatenation(parts, all_binary?)
  end

  defp validate_concatenation([part | _parts], _all_binary?) do
    raise ArgumentError,
          <<@error_message, inspect(part)::binary,
            " (use Kernel.to_string(value) to convert an existing value to string)">>
  end

  defp validate_concatenation([], all_binary?), do: all_binary?

  @doc """
  Accesses a column by name.

  If your column name contains whitespace or start with
  uppercase letters, you can still access its name by
  using this macro:

      iex> df = Explorer.DataFrame.new("unusual nums": [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, col("unusual nums") > 2)
      #Explorer.DataFrame<
        Polars[1 x 1]
        unusual nums integer [3]
      >

  `name` must be an atom, a string, or an integer.
  It is equivalent to `df[name]` but inside a query.

  This can also be used if you want to access a column
  programatically, for example:

      iex> df = Explorer.DataFrame.new(nums: [1, 2, 3])
      iex> name = :nums
      iex> Explorer.DataFrame.filter(df, col(^name) > 2)
      #Explorer.DataFrame<
        Polars[1 x 1]
        nums integer [3]
      >

  For traversing multiple columns programatically,
  see `across/0` and `across/1`.
  """
  defmacro col(name) do
    quote do: Explorer.DataFrame.pull(unquote(df_var()), unquote(name))
  end

  @doc """
  Accesses all columns in the dataframe.

  This is the equivalent to `across(..)`.

  See the module docs for more information.
  """
  defmacro across() do
    quote do
      Explorer.Query.__across__(unquote(df_var()), ..)
    end
  end

  @doc """
  Accesses the columns given by `selector` in the dataframe.

  `across/1` is used as the generator inside for-comprehensions.

  See the module docs for more information.
  """
  defmacro across(selector) do
    quote do
      Explorer.Query.__across__(unquote(df_var()), unquote(selector))
    end
  end

  @doc false
  def __across__(df, selector) do
    df
    |> Explorer.Shared.to_existing_columns(selector)
    |> Enum.map(&%{Explorer.Shared.apply_impl(df, :pull, [&1]) | name: &1})
  end

  defp df_var(), do: quote(do: var!(df, Explorer.Query))
end
