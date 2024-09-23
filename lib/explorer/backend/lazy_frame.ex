defmodule Explorer.Backend.LazyFrame do
  @moduledoc """
  Represents a lazy dataframe for building query expressions.

  The LazyFrame is available inside `filter_with`, `mutate_with`, and
  similar. You cannot perform any operation on them except accessing
  its underlying series.
  """

  alias Explorer.Backend
  alias Explorer.Backend.LazySeries

  defstruct dtypes: %{}, names: [], backend: nil, resource: nil

  @type t :: %__MODULE__{
          backend: module(),
          dtypes: Backend.DataFrame.dtypes(),
          names: Backend.DataFrame.column_name(),
          resource: reference() | nil
        }

  @behaviour Access
  @behaviour Backend.DataFrame

  @doc false
  def new(df) do
    %module{} = df.data

    Explorer.Backend.DataFrame.new(
      %__MODULE__{
        names: df.names,
        dtypes: df.dtypes,
        backend: module,
        resource: module.owner_reference(df)
      },
      df.names,
      df.dtypes
    )
  end

  # We don't implement owner reference here because no
  # cross node operations happen at the lazy frame level.
  # Instead, we store the resource and we delegate them
  # to the underlying lazy series.
  @impl Backend.DataFrame
  def owner_reference(_), do: nil

  @impl Backend.DataFrame
  def lazy, do: __MODULE__

  @impl Backend.DataFrame
  def lazy(ldf), do: ldf

  @impl Backend.DataFrame
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

  @impl Backend.DataFrame
  def pull(%{data: data, dtypes: dtypes}, column) do
    dtype_for_column = dtypes[column]

    data = LazySeries.backed(:column, [column], dtype_for_column, data.resource, data.backend)

    Backend.Series.new(data, dtype_for_column)
  end

  funs =
    Backend.DataFrame.behaviour_info(:callbacks) --
      (Backend.DataFrame.behaviour_info(:optional_callbacks) ++
         Module.definitions_in(__MODULE__, :def))

  for {fun, arity} <- funs do
    args = Macro.generate_arguments(arity, __MODULE__)

    @impl Backend.DataFrame
    def unquote(fun)(unquote_splicing(args)) do
      raise """
      cannot perform operation #{unquote(fun)} on Explorer.Backend.LazyFrame.

      The LazyFrame is available inside filter_with, mutate_with, and \
      similar and they support only a limited subset of the Series API
      """
    end
  end

  @impl Access
  def fetch(%__MODULE__{} = lazy_frame, name) do
    case pull(lazy_frame, name) do
      %Explorer.Series{data: %Explorer.Backend.LazySeries{}} = lazy_series ->
        {:ok, lazy_series}

      _other ->
        :error
    end
  end

  @impl Access
  def get_and_update(%__MODULE__{}, _name, _callback) do
    raise "cannot update an `Explorer.Backend.LazyFrame`"
  end

  @impl Access
  def pop(%__MODULE__{}, _name) do
    raise "cannot delete from an `Explorer.Backend.LazyFrame`"
  end
end
