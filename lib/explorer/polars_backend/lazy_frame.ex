defmodule Explorer.PolarsBackend.LazyFrame do
  @moduledoc false

  @type t :: %__MODULE__{resource: binary(), reference: reference()}

  defstruct resource: nil, reference: nil
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
