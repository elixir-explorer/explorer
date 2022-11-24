defmodule Explorer.Query do
  @moduledoc """
  High-level query for Explorer.

  Queries convert regular Elixir code which compile to efficient
  dataframes operations. Identifiers in queries, such as `strs`
  and `nums`, represent dataframe column names:

      iex> df = Explorer.DataFrame.new(strs: ["a", "b", "c"], nums: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, nums > 2)
      #Explorer.DataFrame<
        Polars[1 x 2]
        strs string ["c"]
        nums integer [3]
      >

  If you want to access variables defined outside of the query,
  you must escape them using `^`:

      iex> min = 2
      iex> df = Explorer.DataFrame.new(strs: ["a", "b", "c"], nums: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, nums > ^min)
      #Explorer.DataFrame<
        Polars[1 x 2]
        strs string ["c"]
        nums integer [3]
      >

  All operations from `Explorer.Series` are imported inside queries.
  This module also provides operators to use in queries, which are
  also imported into queries.

  ## Implementation details

  Queries simply become lazy dataframe operations at runtime.
  For example, the following query

      Explorer.DataFrame.filter(df, nums > 2)

  is equivalent to

      Explorer.DataFrame.filter_with(df, fn df -> df["nums"] > 2 end)

  This means that, whenever you want to generate queries programatically,
  you can fallback to the regular `_with` APIs.
  """

  @doc """
  Builds an anonymous function from a query.

  This is the entry point used by `Explorer.DataFrame.filter/2`
  and friends to convert queries into anonymous functions.
  See the moduledoc for more information.
  """
  defmacro query(expression) do
    df = Macro.unique_var(:df, __MODULE__)

    quote do
      fn unquote(df) ->
        import Kernel,
          except: [
            is_nil: 1,
            ==: 2,
            !=: 2,
            <: 2,
            <=: 2,
            >: 2,
            >=: 2,
            and: 2,
            or: 2,
            +: 2,
            -: 2,
            *: 2,
            /: 2,
            **: 2
          ]

        import Explorer.Query, except: [query: 1]
        import Explorer.Series
        unquote(traverse(expression, df))
      end
    end
  end

  defp traverse({:=, _, [_, _]}, _df) do
    raise "= is not currently supported in Explorer.Query"
  end

  defp traverse({:^, _, [expr]}, _df), do: expr

  defp traverse({var, meta, ctx}, df) when is_atom(var) and is_atom(ctx) do
    {{:., meta, [Explorer.DataFrame, :pull]}, meta, [df, var]}
  end

  defp traverse({left, meta, right}, df), do: {traverse(left, df), meta, traverse(right, df)}
  defp traverse({left, right}, df), do: {traverse(left, df), traverse(right, df)}
  defp traverse(list, df) when is_list(list), do: Enum.map(list, &traverse(&1, df))
  defp traverse(other, _df), do: other

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
end
