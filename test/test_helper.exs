try_starting_epmd? = fn ->
  case :os.type() do
    {:unix, _} ->
      {"", 0} == System.cmd("epmd", ["-daemon"])

    _ ->
      true
  end
end

exclude =
  cond do
    :distributed in Keyword.get(ExUnit.configuration(), :exclude, []) ->
      []

    Code.ensure_loaded?(:peer) and try_starting_epmd?.() and
        match?({:ok, _}, Node.start(:"primary@127.0.0.1", :longnames)) ->
      {:ok, _pid, node2} = :peer.start(%{name: :"secondary@127.0.0.1"})
      {:ok, _pid, node3} = :peer.start(%{name: :"tertiary@127.0.0.1", args: ~w(-hidden)c})

      for node <- [node2, node3] do
        true = :erpc.call(node, :code, :set_path, [:code.get_path()])
        {:ok, _} = :erpc.call(node, :application, :ensure_all_started, [:explorer])
      end

      []

    true ->
      [:distributed]
  end

Calendar.put_time_zone_database(Tz.TimeZoneDatabase)
ExUnit.start(exclude: [cloud_integration: true, test_type: :property] ++ exclude)
