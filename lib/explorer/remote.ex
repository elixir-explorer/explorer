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
  for remote references. If any is found, it spawns
  a process on the remote node and sets up a distributed
  garbage collector.
  """
  def place(remote_ref) do
    local_gc = Explorer.Remote.LocalGC.whereis!()
    {:ok, remote_pid} = Explorer.Remote.Holder.start_child(remote_ref, local_gc)
    local_ref = Explorer.Remote.LocalGC.track(local_gc, remote_pid, remote_ref)
    Explorer.Remote.Holder.hold(remote_pid, remote_ref, local_gc)
    {local_ref, remote_pid}
  end

  # def place(tuple, acc) when is_tuple(tuple) do
  #   tuple
  #   |> Tuple.to_list()
  #   |> place(acc)
  #   |> List.to_tuple()
  # end

  # def place(list) when is_list(list) do
  #   Enum.map(list, &place/1)
  # end
end
