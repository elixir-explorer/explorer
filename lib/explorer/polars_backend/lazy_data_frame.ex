defmodule Explorer.PolarsBackend.LazyDataFrame do
  @moduledoc false

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
  def shape(ldf), do: {nil, ldf |> names() |> length()}

  # Single table verbs

  @impl true
  def pull(ldf, name), do: Shared.apply_native(ldf, :lf_pull, [name])

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
