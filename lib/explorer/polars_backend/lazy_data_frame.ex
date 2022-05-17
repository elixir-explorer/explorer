defmodule Explorer.PolarsBackend.LazyDataFrame do
  @moduledoc false

  alias Explorer.DataFrame
  alias Explorer.PolarsBackend.Shared

  @type t :: %__MODULE__{resource: binary(), reference: reference()}

  defstruct resource: nil, reference: nil

  use Explorer.Backend.DataFrame, backend: "LazyPolars"

  # Conversion

  @impl true
  def lazy, do: __MODULE__

  @impl true
  def to_lazy(ldf), do: ldf

  @impl true
  def collect(ldf), do: Shared.apply_native(ldf, :lf_collect)

  # Introspection

  @impl true
  def names(ldf), do: Shared.apply_native(ldf, :lf_names)

  @impl true
  def dtypes(ldf),
    do:
      ldf
      |> Shared.apply_native(:lf_dtypes)
      |> Enum.map(&Shared.normalise_dtype/1)

  @impl true
  def n_columns(ldf), do: ldf |> names() |> length()

  @impl true

  # Single table verbs

  @impl true
  def head(ldf, rows), do: Shared.apply_native(ldf, :lf_head, [rows])

  @impl true
  def tail(ldf, rows), do: Shared.apply_native(ldf, :lf_tail, [rows])

  @impl true
  def select(ldf, columns, :keep) when is_list(columns),
    do: Shared.apply_native(ldf, :lf_select, [columns])

  def select(ldf, columns, :drop) when is_list(columns),
    do: Shared.apply_native(ldf, :lf_drop, [columns])

  @impl true
  def pull(ldf, name), do: Shared.apply_native(ldf, :lf_pull, [name])

  # Groups

  @impl true
  def group_by(%DataFrame{groups: groups} = ldf, new_groups),
    do: %DataFrame{ldf | groups: groups ++ new_groups}

  @impl true
  def ungroup(ldf, []), do: %DataFrame{ldf | groups: []}

  def ungroup(ldf, groups),
    do: %DataFrame{ldf | groups: Enum.filter(ldf.groups, &(&1 not in groups))}

  # TODO: Make the functions of non-implemented functions
  # explicit once the lazy interface is ready.
  funs =
    Explorer.Backend.DataFrame.behaviour_info(:callbacks) --
      (Explorer.Backend.DataFrame.behaviour_info(:optional_callbacks) ++
         Module.definitions_in(__MODULE__, :def) ++ [{:inspect, 2}])

  for {fun, arity} <- funs do
    args = Macro.generate_arguments(arity, __MODULE__)

    @impl true
    def unquote(fun)(unquote_splicing(args)) do
      raise "cannot perform operation on an Explorer.PolarsBackend.LazyDataFrame"
    end
  end
end
