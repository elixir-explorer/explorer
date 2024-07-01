defmodule Explorer.Query do
  @moduledoc ~S"""
  High-level query for Explorer.

  > #### Explorer.DataFrame vs DF {: .tip}
  >
  > All examples below assume you have defined aliased
  > `Explorer.DataFrame` to `DF` as shown below:
  >
  >     require Explorer.DataFrame, as: DF
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
        nums s64 [3]
      >

  If a column has unusual format, you can either rename it before-hand,
  or use `col/1` inside queries:

      iex> df = DF.new("unusual nums": [1, 2, 3])
      iex> DF.filter(df, col("unusual nums") > 2)
      #Explorer.DataFrame<
        Polars[1 x 1]
        unusual nums s64 [3]
      >

  All operations from `Explorer.Series` are imported inside queries.
  This module also provides operators to use in queries, which are
  also imported into queries.

  ## Supported operations

  Queries are supported in the following operations:

    * `Explorer.DataFrame.sort_by/2`
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
        nums s64 [3]
      >

      iex> min = 2
      iex> df = DF.new(strs: ["a", "b", "c"], nums: [1, 2, 3])
      iex> DF.filter(df, nums < ^if(min > 0, do: 10, else: -10))
      #Explorer.DataFrame<
        Polars[3 x 2]
        strs string ["a", "b", "c"]
        nums s64 [1, 2, 3]
      >

  `^` can be used with `col` to access columns dynamically:

      iex> df = DF.new("unusual nums": [1, 2, 3])
      iex> name = "unusual nums"
      iex> DF.filter(df, col(^name) > 2)
      #Explorer.DataFrame<
        Polars[1 x 1]
        unusual nums s64 [3]
      >

  ## Conditionals

  Queries support both `if/2` and `unless/2` operations inside queries.

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
        a s64 [10, 4, 6]
        b string ["Exceptional", "Failed", "Passed"]
      >

  ## Across and comprehensions

  `Explorer.Query` leverages the power behind Elixir for-comprehensions
  to provide a powerful syntax for traversing several columns in a dataframe
  at once. For example, imagine you want to standardize the data on the
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
        sepal_length f64 [-1.0840606189132322, -1.3757361217598405, -1.66741162460645, -1.8132493760297554, -1.2298983703365363, ...]
        sepal_width f64 [2.3722896125315045, -0.28722789030650403, 0.7765791108287005, 0.2446756102610982, 2.9041931130991068, ...]
        petal_length f64 [-0.7576391687443839, -0.7576391687443839, -0.7897606710936369, -0.7255176663951307, -0.7576391687443839, ...]
        petal_width f64 [-1.7147014356654708, -1.7147014356654708, -1.7147014356654708, -1.7147014356654708, -1.7147014356654708, ...]
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
        sepal_length f64 [-1.0840606189132322, -1.3757361217598405, -1.66741162460645, -1.8132493760297554, -1.2298983703365363, ...]
        sepal_width f64 [2.3722896125315045, -0.28722789030650403, 0.7765791108287005, 0.2446756102610982, 2.9041931130991068, ...]
        petal_length f64 [-0.7576391687443839, -0.7576391687443839, -0.7897606710936369, -0.7255176663951307, -0.7576391687443839, ...]
        petal_width f64 [-1.7147014356654708, -1.7147014356654708, -1.7147014356654708, -1.7147014356654708, -1.7147014356654708, ...]
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
      ...>   for col <- across(), col.dtype == {:f, 64} do
      ...>     {col.name, (col - mean(col)) / variance(col)}
      ...>   end
      ...> )
      #Explorer.DataFrame<
        Polars[150 x 5]
        sepal_length f64 [-1.0840606189132322, -1.3757361217598405, -1.66741162460645, -1.8132493760297554, -1.2298983703365363, ...]
        sepal_width f64 [2.3722896125315045, -0.28722789030650403, 0.7765791108287005, 0.2446756102610982, 2.9041931130991068, ...]
        petal_length f64 [-0.7576391687443839, -0.7576391687443839, -0.7897606710936369, -0.7255176663951307, -0.7576391687443839, ...]
        petal_width f64 [-1.7147014356654708, -1.7147014356654708, -1.7147014356654708, -1.7147014356654708, -1.7147014356654708, ...]
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
      ...>   for col <- across(), col.dtype == {:f, 64} do
      ...>     {"#{col.name}_mean", round(mean(col), 3)}
      ...>   end
      ...> )
      #Explorer.DataFrame<
        Polars[3 x 5]
        species string ["Iris-setosa", "Iris-versicolor", "Iris-virginica"]
        sepal_length_mean f64 [5.006, 5.936, 6.588]
        sepal_width_mean f64 [3.418, 2.77, 2.974]
        petal_length_mean f64 [1.464, 4.26, 5.552]
        petal_width_mean f64 [0.244, 1.326, 2.026]
      >

  `sort_by` expects a list of columns to sort by, while for-comprehensions
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
        sepal_length f64 [7.0, 6.4, 6.9, 6.5, 6.3, ...]
        sepal_width f64 [3.2, 3.2, 3.1, 2.8, 3.3, ...]
        petal_length f64 [4.7, 4.5, 4.9, 4.6, 4.7, ...]
        petal_width f64 [1.4, 1.5, 1.5, 1.5, 1.6, ...]
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

  alias Explorer.Series
  alias Explorer.Backend.LazySeries

  defstruct [:series_list]

  # `and` and `or` are sent as is to queries
  @binary_mapping [
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
  @binary_ops Keyword.keys(@binary_mapping)

  @series_ops_with_arity Explorer.Backend.Series.behaviour_info(:callbacks)
  @series_ops Keyword.keys(@series_ops_with_arity)

  def from_series(%Series{data: %LazySeries{}} = series) do
    series
  end

  def from_series(list) when is_list(list) do
    normalize_aliases(list)
  end

  def normalize_aliases(list) do
    Enum.map(list, fn
      {name, %Series{data: %LazySeries{}} = series} -> Series.rename(series, name)
      %Series{data: %LazySeries{}} = series -> series
    end)
  end

  defmacro new(expression) do
    quote do
      unquote(traverse_root(expression))
    end
  end

  defp traverse_root(ast) do
    if Keyword.keyword?(ast) do
      Enum.map(ast, fn {key, value} -> {key, traverse(value)} end)
    else
      traverse(ast)
    end
  end

  def traverse(ast) do
    lazy_series =
      ast
      |> Macro.prewalk(fn
        {op, meta, args} when op in @binary_ops ->
          {@binary_mapping[op], meta, args}

        node ->
          node
      end)
      |> Macro.postwalk(fn
        {op, _, _} = node when op in @series_ops ->
          lazy_series_ast(node)

        {op, meta, nil} ->
          lazy_series_ast({:col, meta, [to_string(op)]})

        node ->
          node
      end)

    quote do
      Explorer.Query.from_series(%Series{data: unquote(lazy_series), dtype: :unknown})
    end
  end

  def lazy_series_ast({op, _meta, args}) do
    quote do
      %LazySeries{op: unquote(op), args: unquote(args)}
    end
  end
end
