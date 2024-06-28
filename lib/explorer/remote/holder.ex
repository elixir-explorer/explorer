defmodule Explorer.Remote.Holder do
  # This process is spawned dynamically by a node
  # that wants to hold a reference to a resource
  # on this node. To avoid leaking resources, it
  # is tied to the local GC process of the spawned node.
  @moduledoc false

  use GenServer, restart: :transient

  def start_child(remote_ref, local_gc) when is_reference(remote_ref) and is_pid(local_gc) do
    DynamicSupervisor.start_child(
      {Explorer.Remote.Supervisor, node(remote_ref)},
      {__MODULE__, local_gc}
    )
  end

  def hold(holder, ref, pid) when is_pid(pid) and node(ref) == node(holder) do
    GenServer.call(holder, {:hold, ref, pid})
  end

  def start_link(pid) when is_pid(pid) do
    GenServer.start_link(__MODULE__, pid)
  end

  @impl true
  def init(pid) do
    ref = Process.monitor(pid)
    state = %{owner_ref: ref, refs: %{}, pids: %{}}
    {:ok, state}
  end

  @impl true
  def handle_info({:gc, ref, pid}, state) do
    refs = pop_from_list(state.refs, ref, pid)
    pids = pop_from_list(state.pids, pid, ref)
    noreply_or_stop(%{state | pids: pids, refs: refs})
  end

  def handle_info({:DOWN, owner_ref, _, _, reason}, %{owner_ref: owner_ref} = state) do
    {:stop, reason, state}
  end

  def handle_info({:DOWN, _, _, pid, _}, state) do
    {pid_refs, pids} = Map.pop(state.pids, pid, [])
    refs = Enum.reduce(pid_refs, state.refs, &pop_from_list(&2, &1, pid))
    noreply_or_stop(%{state | pids: pids, refs: refs})
  end

  defp noreply_or_stop(%{refs: refs} = state) when refs == %{}, do: {:stop, :shutdown, state}
  defp noreply_or_stop(state), do: {:noreply, state}

  defp pop_from_list(map, key, value) do
    case map do
      %{^key => list} ->
        case List.delete(list, value) do
          [] -> Map.delete(map, key)
          new_value -> Map.put(map, key, new_value)
        end

      %{} ->
        map
    end
  end
end
