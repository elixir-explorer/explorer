defmodule Explorer.PolarsBackend.LazyDataFrame do
  @moduledoc false

  alias Explorer.DataFrame
  alias Explorer.PolarsBackend.Shared

  @type t :: %__MODULE__{resource: binary(), reference: reference()}

  defstruct resource: nil, reference: nil

  @behaviour Explorer.Backend.DataFrame

  # Conversion

  @impl true
  def lazy, do: __MODULE__

  @impl true
  def to_lazy(ldf), do: ldf

  @impl true
  def collect(ldf), do: Shared.apply_dataframe(ldf, :lf_collect)

  # Introspection

  @impl true
  def names(ldf), do: Shared.apply_dataframe(ldf, :lf_names)

  @impl true
  def dtypes(ldf),
    do:
      ldf
      |> Shared.apply_dataframe(:lf_dtypes)
      |> Enum.map(&Shared.normalise_dtype/1)

  @impl true
  def n_columns(ldf), do: ldf |> names() |> length()

  @impl true
  def inspect(ldf, opts) do
    df = Shared.apply_dataframe(ldf, :lf_fetch, [opts.limit])
    Explorer.Backend.DataFrame.inspect(df, "LazyPolars", nil, opts)
  end

  # Single table verbs

  @impl true
  def head(ldf, rows), do: Shared.apply_dataframe(ldf, :lf_head, [rows])

  @impl true
  def tail(ldf, rows), do: Shared.apply_dataframe(ldf, :lf_tail, [rows])

  @impl true
  def select(ldf, out_ldf), do: Shared.apply_dataframe(ldf, out_ldf, :lf_select, [out_ldf.names])

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
