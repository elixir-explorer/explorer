defmodule Explorer.Shared do
  # A collection of **private** helpers shared in Explorer.
  @moduledoc false

  def backend_from_options!(opts) do
    case Keyword.fetch(opts, :backend) do
      {:ok, backend} when is_atom(backend) ->
        backend

      {:ok, other} ->
        raise ArgumentError,
              ":backend must be an atom, got: #{inspect(other)}"

      :error ->
        nil
    end
  end

  @doc """
  Gets the implementation of a dataframe or series.
  """
  def impl!(%{data: %struct{}}), do: struct

  def impl!([%{data: %first_struct{}} | _] = dfs) when is_list(dfs),
    do: Enum.reduce(dfs, first_struct, fn %{data: %struct{}}, acc -> pick_struct(acc, struct) end)

  def impl!(%{data: %struct1{}}, %{data: %struct2{}}),
    do: pick_struct(struct1, struct2)

  @doc """
  Gets the implementation of a list of maybe dataframes or series.
  """
  def find_impl!(list) do
    Enum.reduce(list, fn
      %{data: %struct{}}, acc -> pick_struct(struct, acc)
      _, acc -> acc
    end)
  end

  defp pick_struct(struct, struct), do: struct

  defp pick_struct(struct1, struct2) do
    raise "cannot invoke Explorer function because it relies on two incompatible implementations: " <>
            "#{inspect(struct1)} and #{inspect(struct2)}. You may need to call Explorer.backend_transfer/1 " <>
            "(or Explorer.backend_copy/1) on one or both of them to transfer them to a common implementation"
  end
end
