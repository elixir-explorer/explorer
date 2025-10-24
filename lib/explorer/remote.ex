defmodule Explorer.Remote do
  @moduledoc """
  A module responsible for placing remote dataframes and
  garbage collect them.

  The functions in `Explorer.DataFrame` and `Explorer.Series`
  will automatically move operations on remote dataframes to
  the nodes they belong to. `Explorer` also integrates with
  `FLAME` and automatically tracks remote dataframes and
  series returned from `FLAME` calls when the `:track_resources`
  option is enabled.

  This module provides additional conveniences for manual placement.

  ## Implementation details

  In order to understand what this module does, we need
  to understand the challenges in working with remote series
  and dataframes.

  Series and dataframes are actually NIF resources: they are
  pointers to blobs of memory operated by low-level libraries.
  Those are represented in Erlang/Elixir as references (the
  same as the one returned by `make_ref/0`). Once the reference
  is garbage collected (based on refcounting), those NIF
  resources are garbage collected and the memory is reclaimed.

  When using Distributed Erlang, you may write this code:

      remote_series = :erpc.call(node, Explorer.Series, :from_list, [[1, 2, 3]])

  However, the code above will not work, because the series
  will be allocated in the remote node and the remote node
  won't hold a reference to said series! This means the series
  is garbage collected and if we attempt to read it later on,
  from the caller node, it will no longer exist. Therefore,
  we must explicitly place these resources in remote nodes
  by spawning processes to hold these references. That's what
  the `place/2` function in this module does.

  We also need to guarantee these resources are not kept
  forever by these remote nodes, so `place/2` creates a
  local NIF resource that notifies the remote resources
  they have been GCed, effectively implementing a remote
  garbage collector.
  """

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
    place_list(list, acc, [], fun)
  end

  defp place(%Explorer.Backend.LazySeries{args: args} = lazy_series, acc, fun) do
    {args, acc} = Enum.map_reduce(args, acc, &place(&1, &2, fun))
    {%{lazy_series | args: args}, acc}
  end

  defp place(%Explorer.Series{data: %Explorer.Backend.LazySeries{} = data} = series, acc, fun) do
    {data, acc} = place(data, acc, fun)
    {%{series | data: data}, acc}
  end

  defp place(%Explorer.Series{data: %impl{}} = series, acc, fun) do
    place_impl(impl, series, acc, fun)
  end

  defp place(%Explorer.DataFrame{data: %impl{}} = df, acc, fun) do
    place_impl(impl, df, acc, fun)
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

  defp place_list([head | tail], acc, rev, fun) do
    {head, acc} = place(head, acc, fun)
    place_list(tail, acc, [head | rev], fun)
  end

  defp place_list([], acc, rev, _fun) do
    {Enum.reverse(rev), acc}
  end

  defp place_list(tail, acc, rev, fun) do
    {tail, acc} = place(tail, acc, fun)
    {:lists.reverse(rev, tail), acc}
  end

  defp place_impl(impl, struct, acc, fun) do
    with remote_ref when is_reference(remote_ref) and node(remote_ref) != node() <-
           impl.owner_reference(struct),
         false <- alive?(remote_ref, struct) do
      {local_ref, remote_pid} = place_remote_ref(remote_ref, fun)
      {%{struct | remote: {local_ref, remote_pid, remote_ref}}, [remote_pid | acc]}
    else
      _ ->
        {struct, acc}
    end
  end

  defp alive?(remote_ref, %{remote: {local_gc_ref, _, remote_ref}})
       when is_reference(local_gc_ref) and node(local_gc_ref) == node(),
       do: Explorer.Remote.LocalGC.alive?(local_gc_ref)

  defp alive?(_remote_ref, _struct), do: false

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
  def apply(pid, mod, fun, resources, args_callback, place?) when is_pid(pid) do
    placing_callback = if place?, do: &place_on_pid(&1, pid), else: &{&1, []}
    do_apply(node(pid), mod, fun, resources, args_callback, placing_callback)
  end

  def apply(node, mod, fun, resources, args_callback, place?) when is_atom(node) do
    placing_callback = if place?, do: &place_on_node(&1, []), else: &{&1, []}
    do_apply(node, mod, fun, resources, args_callback, placing_callback)
  end

  defp do_apply(node, mod, fun, resources, args_callback, placing_callback) do
    resources =
      Enum.map(resources, fn
        {resource_data, resource_node} when resource_node == node or resource_node == nil ->
          {:local, resource_data}

        {%{data: %impl{}} = resource_data, resource_node} ->
          {:remote, impl, owner_export(resource_node, impl, resource_data)}
      end)

    message_ref = :erlang.make_ref()
    remote_args = [self(), message_ref, mod, fun, resources, args_callback]
    child = Node.spawn_link(node, __MODULE__, :remote_apply, remote_args)
    monitor_ref = Process.monitor(child)

    receive do
      {^message_ref, result} ->
        Process.demonitor(monitor_ref, [:flush])
        {term, _acc} = placing_callback.(result)
        send(child, {message_ref, :ok})
        term

      {:DOWN, ^monitor_ref, _, _, reason} ->
        exit(reason)
    end
  end

  defp owner_export(resource_node, impl, resource_data) do
    case :erpc.call(resource_node, impl, :owner_export, [resource_data]) do
      {:ok, val} -> val
      {:error, error} -> raise error
    end
  end

  defp owner_import(impl, exported_data) do
    case impl.owner_import(exported_data) do
      {:ok, val} -> val
      {:error, error} -> raise error
    end
  end

  @doc false
  def remote_apply(parent, message_ref, mod, fun, resources, args_callback) do
    resources =
      Enum.map(resources, fn
        {:local, value} -> value
        {:remote, impl, value} -> owner_import(impl, value)
      end)

    monitor_ref = Process.monitor(parent)
    result = apply(mod, fun, args_callback.(resources))
    send(parent, {message_ref, result})

    receive do
      {^message_ref, :ok} ->
        Process.demonitor(monitor_ref, [:flush])
        # Call a remote function to prevent garbage collection from happening until we are done
        Explorer.Remote.LocalGC.identity(result)

      {:DOWN, ^monitor_ref, _, _, reason} ->
        exit(reason)
    end
  end
end

if Code.ensure_loaded?(FLAME.Trackable) do
  defimpl FLAME.Trackable,
    for: [Explorer.DataFrame, Explorer.Series, Explorer.Backend.LazySeries] do
    def track(data, acc, _node) do
      Explorer.Remote.place(data, acc)
    end
  end
end
