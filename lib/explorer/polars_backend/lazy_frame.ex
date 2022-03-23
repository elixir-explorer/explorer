defmodule Explorer.PolarsBackend.LazyFrame do
  @moduledoc false

  alias Explorer.PolarsBackend.Shared

  @type t :: %__MODULE__{resource: binary(), reference: reference()}

  defstruct resource: nil, reference: nil

  @behaviour Explorer.Backend.DataFrame

  @impl true
  def select(df, columns, :keep) when is_list(columns),
    do: Shared.apply_native(df, :df_select, [columns])

  def collect(lf), do: Shared.apply_native(lf, :lf_collect)
end

defimpl Inspect, for: Explorer.PolarsBackend.LazyFrame do
  alias Explorer.PolarsBackend.Native

  def inspect(lf, _opts) do
    case Native.lf_describe_plan(lf) do
      {:ok, str} -> str
      {:error, error} -> raise "#{error}"
    end
  end
end
