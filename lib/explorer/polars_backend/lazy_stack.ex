defmodule Explorer.PolarsBackend.LazyStack do
  @moduledoc false

  def push_operation(ldf, {operation, args}, groups \\ nil) do
    stack = [{operation, args, groups || ldf.groups} | ldf.data.stack]

    %{ldf | data: %{ldf.data | stack: stack}}
  end

  def stack(ldf), do: Enum.reverse(ldf.data.stack)

  @doc """
  Chunk operations by groups.

  It removes the groups from the "triple" of each operation and group
  the operations that share the same groups.
  """
  def stack_by_groups(ldf) do
    chunk_fun = fn {op, args, groups}, acc ->
      case acc do
        nil -> {:cont, {groups, [{op, args}]}}
        {^groups, ops} -> {:cont, {groups, [{op, args} | ops]}}
        {other, ops} -> {:cont, {other, Enum.reverse(ops)}, {groups, [{op, args}]}}
      end
    end

    after_fun = fn acc ->
      case acc do
        nil -> {:cont, nil}
        {groups, ops} -> {:cont, {groups, Enum.reverse(ops)}, nil}
      end
    end

    ldf
    |> stack()
    |> Enum.chunk_while(nil, chunk_fun, after_fun)
  end
end
