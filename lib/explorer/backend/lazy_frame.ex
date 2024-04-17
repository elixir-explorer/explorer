defmodule Explorer.Backend.LazyFrame do
  @moduledoc """
  Represents a lazy dataframe for building query expressions.

  The LazyFrame is available inside `filter_with`, `mutate_with`, and
  similar. You cannot perform any operation on them except accessing
  its underlying series.
  """

  alias Explorer.Backend
  alias Explorer.Backend.LazySeries

  defstruct dtypes: %{}, names: [], original_data: nil

  @type t :: %__MODULE__{
          original_data: Backend.DataFrame.t(),
          dtypes: Backend.DataFrame.dtypes(),
          names: Backend.DataFrame.column_name()
        }
  @behaviour Backend.DataFrame

  @doc false
  def new(df) do
    Explorer.Backend.DataFrame.new(
      %__MODULE__{names: df.names, dtypes: df.dtypes, original_data: df.data},
      df.names,
      df.dtypes
    )
  end

  @impl true
  def lazy, do: __MODULE__

  @impl true
  def lazy(ldf), do: ldf

  @impl true
  def inspect(ldf, opts) do
    import Inspect.Algebra

    open = color("[", :list, opts)
    close = color("]", :list, opts)

    dtypes = ldf.data.dtypes

    cols_algebra =
      for name <- ldf.data.names do
        concat([
          line(),
          color("#{name} ", :map, opts),
          color("#{Explorer.Shared.dtype_to_string(dtypes[name])}", :atom, opts)
        ])
      end

    concat([
      color("LazyFrame", :atom, opts),
      open,
      "??? x #{length(cols_algebra)}",
      close,
      groups_algebra(ldf.groups, opts) | cols_algebra
    ])
  end

  defp groups_algebra([_ | _] = groups, opts),
    do:
      Inspect.Algebra.concat([
        Inspect.Algebra.line(),
        Inspect.Algebra.color("Groups: ", :atom, opts),
        Inspect.Algebra.to_doc(groups, opts)
      ])

  defp groups_algebra([], _), do: ""

  @impl true
  def pull(df, column) do
    dtype_for_column = df.dtypes[column]
    series_backend = get_series_backend(df.data.original_data)

    data = LazySeries.new(:column, [column], dtype_for_column)
    data = %{data | backend: series_backend}

    Backend.Series.new(data, dtype_for_column)
  end

  defp get_series_backend(%module{}) do
    module
    |> Module.split()
    |> List.replace_at(-1, "Series")
    |> Module.concat()
  end

  funs =
    Backend.DataFrame.behaviour_info(:callbacks) --
      (Backend.DataFrame.behaviour_info(:optional_callbacks) ++
         Module.definitions_in(__MODULE__, :def))

  for {fun, arity} <- funs do
    args = Macro.generate_arguments(arity, __MODULE__)

    @impl true
    def unquote(fun)(unquote_splicing(args)) do
      raise """
      cannot perform operation #{unquote(fun)} on Explorer.Backend.LazyFrame.

      The LazyFrame is available inside filter_with, mutate_with, and \
      similar and they support only a limited subset of the Series API
      """
    end
  end
end
