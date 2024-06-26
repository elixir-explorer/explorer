defmodule Explorer.Remote.LocalGC do
  # This process is spawned on every node running Explorer.
  # It is responsible for tracking the local GC and reporting
  # them to the remote holder nodes.
  @moduledoc false

  use GenServer
  @name __MODULE__

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: @name)
  end

  @doc """
  An identity function to prevent GC.
  """
  def identity(data), do: data

  @doc """
  Locates the LocalGC process.
  """
  def whereis! do
    Process.whereis(@name) || exit({:noproc, {__MODULE__, :whereis!, []}})
  end

  @doc """
  This is a function that receives a process and a payload
  and it returns a NIF resource.

  Once the NIF resource is GCed, it sends a message `{:gc, pid, ref}`
  to the local GC process, which forwards it to the holder node.
  """
  def track(local_gc, remote_pid, remote_ref)
      when is_pid(local_gc) and is_pid(remote_pid) and is_reference(remote_ref) do
    Explorer.PolarsBackend.Native.message_on_gc(local_gc, {:gc, remote_pid, remote_ref})
  end

  @doc """
  Check is a local gc reference is alive.
  """
  def alive?(local_gc_ref) when is_reference(local_gc_ref) do
    Explorer.PolarsBackend.Native.is_message_on_gc(local_gc_ref)
  end

  ## Callbacks

  @impl true
  def init(:ok) do
    {:ok, :unused_state}
  end

  @impl true
  def handle_info({:gc, remote_pid, remote_ref}, state) do
    send(remote_pid, {:gc, remote_ref, self()})
    {:noreply, state}
  end
end
