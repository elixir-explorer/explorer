defprotocol Explorer.FSS do
  @moduledoc false

  @doc """
  Downloads a given entry to a path.
  """
  @spec download(t(), path :: Path.t()) :: :ok | {:error, term()}
  def download(entry, path)
end
