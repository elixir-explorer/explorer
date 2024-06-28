defmodule Explorer.Remote do
  @moduledoc """
  A module responsible for tracking remote dataframes
  and garbage collect them.

  A dataframe or a series must be manually placed in
  a remote node for this to work. This is done by
  calling `place/2` on every node that receives a
  copy of the remote dataframe.

  From that moment, operations against said dataframe
  and series are executed in the remote node, until
  the dataframe is collected.

  If a remote reference is passed to a Series or
  DataFrame function, and they have not been placed,
  their data will be transferred to the current node.
  """

  # TODO: Make `collect` in dataframe transfer to the current node
  # TODO: Add `collect` to series
  # TODO: Add `compute` to dataframe
  # TODO: Handle dataframes (remove Shared.apply_impl)
  # TODO: Handle lazy series
  # TODO: Handle mixed arguments

  @doc """
  Receives a data structure and traverses it looking
  for remote dataframes and series.

  If any is found, it spawns a process on the remote node
  and sets up a distributed garbage collector. This function
  only traverses maps, lists, and tuples, it does not support
  arbitrary structs (such as map sets).

  It returns the updated term and a list of remote PIDs
  spawned.
  """
  def place(term, acc \\ []), do: place_on_node(term, acc)

  defp place_on_node(term, acc) do
    place(term, acc, &Explorer.Remote.Holder.start_child/2)
  end

  defp place_on_pid(term, remote_pid) do
    place(term, [], fn _remote_ref, _local_gc ->
      {:ok, remote_pid}
    end)
  end

  defp place(tuple, acc, fun) when is_tuple(tuple) do
    {list, acc} = tuple |> Tuple.to_list() |> place(acc, fun)
    {List.to_tuple(list), acc}
  end

  defp place(list, acc, fun) when is_list(list) do
    Enum.map_reduce(list, acc, &place(&1, &2, fun))
  end

  defp place(%Explorer.DataFrame{data: %struct{}} = df, acc, fun) do
    case struct.owner_reference(df) do
      remote_ref when is_reference(remote_ref) and node(remote_ref) != remote_ref ->
        {local_ref, remote_pid} = place_remote_ref(remote_ref, fun)
        {%{df | remote: {local_ref, remote_pid, remote_ref}}, [remote_pid | acc]}

      _ ->
        {df, acc}
    end
  end

  defp place(%Explorer.Series{data: %struct{}} = series, acc, fun) do
    case struct.owner_reference(series) do
      remote_ref when is_reference(remote_ref) and node(remote_ref) != remote_ref ->
        {local_ref, remote_pid} = place_remote_ref(remote_ref, fun)
        {%{series | remote: {local_ref, remote_pid, remote_ref}}, [remote_pid | acc]}

      _ ->
        {series, acc}
    end
  end

  defp place(%_{} = other, acc, _fun) do
    {other, acc}
  end

  defp place(%{} = map, acc, fun) do
    {pairs, acc} =
      Enum.map_reduce(map, acc, fn {k, v}, acc ->
        {k, acc} = place(k, acc, fun)
        {v, acc} = place(v, acc, fun)
        {{k, v}, acc}
      end)

    {Map.new(pairs), acc}
  end

  defp place(other, acc, _fun) do
    {other, acc}
  end

  defp place_remote_ref(remote_ref, fun) do
    local_gc = Explorer.Remote.LocalGC.whereis!()
    {:ok, remote_pid} = fun.(remote_ref, local_gc)
    local_ref = Explorer.Remote.LocalGC.track(local_gc, remote_pid, remote_ref)
    Explorer.Remote.Holder.hold(remote_pid, remote_ref, local_gc)
    {local_ref, remote_pid}
  end

  @doc false
  # Applies the given mod/fun/args in the remote node.
  # If a pid is given, we assume the pid will hold those references
  # and avoid spawning new ones. If a node is given, regular placement
  # occurs.
  def apply(pid, mod, fun, args) when is_pid(pid) do
    apply(node(pid), mod, fun, args, &place_on_pid(&1, pid))
  end

  def apply(node, mod, fun, args) when is_atom(node) do
    apply(node, mod, fun, args, &place_on_node(&1, []))
  end

  defp apply(node, mod, fun, args, placing_function) do
    message_ref = :erlang.make_ref()

    child =
      Node.spawn_link(node, __MODULE__, :remote_apply, [self(), message_ref, mod, fun, args])

    monitor_ref = Process.monitor(child)

    receive do
      {^message_ref, result} ->
        Process.demonitor(monitor_ref, [:flush])
        {term, _acc} = placing_function.(result)
        send(child, {message_ref, :ok})
        term

      {:DOWN, ^monitor_ref, _, _, reason} ->
        exit(reason)
    end
  end

  @doc false
  def remote_apply(parent, message_ref, mod, fun, args) do
    monitor_ref = Process.monitor(parent)
    result = apply(mod, fun, args)
    send(parent, {message_ref, result})

    receive do
      {^message_ref, :ok} -> Process.demonitor(monitor_ref, [:flush])
      {:DOWN, ^monitor_ref, _, _, reason} -> exit(reason)
    end
  end
end
