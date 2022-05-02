defmodule Explorer.PolarsBackend.LazyDataFrame do
  @type t :: %__MODULE__{resource: binary(), reference: reference()}

  defstruct resource: nil, reference: nil
end
