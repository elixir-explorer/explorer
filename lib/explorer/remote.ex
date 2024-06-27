defmodule Explorer.Remote do
  @moduledoc """
  A module responsible for tracking remote dataframes
  and performing distributed garbage collection.

  A dataframe or a series must be manually placed in
  a remote node for this to work. From that moment,
  operations against said dataframe and series are
  executed in the remote node, until the dataframe
  is collected.

  If a remote reference is passed to a Series or
  DataFrame function, and they have not been placed,
  their data will be transferred to the current node.
  """

  # TODO: Make `collect` in dataframe transfer to the current node
  # TODO: Add `collect` to series
  # TODO: Add `compute` to dataframe

  @doc """
  Receives a data structure and traverses it looking
  for remote dataframes and series. If any is found,
  it spawns a process on the remote node and sets up
  a distributed garbage collector. This function only
  traverses maps, lists, and tuples, it does not support
  arbitrary structs (such as map sets).

  It returns the updated term and a list of remote PIDs
  spawned.
  """
  def place(term, acc \\ []) do
    place(term, acc, fn remote_ref, acc ->
      local_gc = Explorer.Remote.LocalGC.whereis!()
      {:ok, remote_pid} = Explorer.Remote.Holder.start_child(remote_ref, local_gc)
      local_ref = Explorer.Remote.LocalGC.track(local_gc, remote_pid, remote_ref)
      Explorer.Remote.Holder.hold(remote_pid, remote_ref, local_gc)
      {local_ref, remote_pid, [remote_pid | acc]}
    end)
  end

  def place(tuple, acc, fun) when is_tuple(tuple) do
    {list, acc} = tuple |> Tuple.to_list() |> place(acc, fun)
    {List.to_tuple(list), acc}
  end

  def place(list, acc, fun) when is_list(list) do
    Enum.map_reduce(list, acc, &place(&1, &2, fun))
  end

  def place(%Explorer.DataFrame{data: %struct{}} = df, acc, fun) do
    case struct.owner_reference(df) do
      remote_ref when is_reference(remote_ref) and node(remote_ref) != remote_ref ->
        {local_ref, remote_pid, acc} = fun.(remote_ref, acc)
        {%{df | remote: {local_ref, remote_pid, remote_ref}}, acc}

      _ ->
        {df, acc}
    end
  end

  def place(%Explorer.Series{data: %struct{}} = series, acc, fun) do
    case struct.owner_reference(series) do
      remote_ref when is_reference(remote_ref) and node(remote_ref) != remote_ref ->
        {local_ref, remote_pid, acc} = fun.(remote_ref, acc)
        {%{series | remote: {local_ref, remote_pid, remote_ref}}, acc}

      _ ->
        {series, acc}
    end
  end

  def place(%_{} = other, acc, _fun) do
    {other, acc}
  end

  def place(%{} = map, acc, fun) do
    {pairs, acc} =
      Enum.map_reduce(map, acc, fn {k, v}, acc ->
        {k, acc} = place(k, acc, fun)
        {v, acc} = place(v, acc, fun)
        {{k, v}, acc}
      end)

    {Map.new(pairs), acc}
  end

  def place(other, acc, _fun) do
    {other, acc}
  end
end
