defmodule Explorer.PolarsBackend.ExperimentalIo do
  @moduledoc false

  # To try it:
  #
  # iex> df = Explorer.Datasets.wine()
  # iex> {:ok, pid} = GenServer.start_link(Explorer.PolarsBackend.ExperimentalIo, df)
  # iex> GenServer.cast(pid, :write_ipc)
  #
  alias Explorer.PolarsBackend.Native
  use GenServer

  @impl true
  def init(dataframe) do
    {:ok, dataframe}
  end

  @impl true
  def handle_cast(:write_ipc, dataframe) do
    _ = Native.df_to_ipc_via_pid(dataframe.data, self(), nil)
    {:noreply, dataframe}
  end

  @impl true
  def handle_info(msg, state) do
    IO.inspect(msg, label: "from rust")
    {:noreply, state}
  end
end
