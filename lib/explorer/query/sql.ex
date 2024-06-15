defmodule Explorer.Query.Sql do
  @moduledoc false
  defstruct [:string]

  @doc false
  def new(sql_string) when is_binary(sql_string) do
    %__MODULE__{string: sql_string}
  end

  @doc false
  def to_lazy_series(%__MODULE__{string: sql_string}) do
    aggregation? = true
    dtype_out = :unknown

    Explorer.Backend.LazySeries.new(
      :sql,
      [sql_string],
      dtype_out,
      aggregation?,
      Explorer.PolarsBackend
    )
  end
end
