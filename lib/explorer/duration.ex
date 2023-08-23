defmodule Explorer.Duration do
  # Internal representation of a duration.
  @moduledoc false

  @enforce_keys [:value, :precision]
  defstruct [:value, :precision]
end
