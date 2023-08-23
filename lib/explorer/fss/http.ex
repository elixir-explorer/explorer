defimpl Explorer.FSS, for: FSS.HTTP.Entry do
  alias Explorer.FSS.Utils

  def download(entry, path) do
    Utils.assert_regular_path!(path)

    headers = entry.config.headers
    collectable = File.stream!(path)

    case Utils.download(entry.url, collectable, headers: headers) do
      {:ok, _collectable} -> :ok
      {:error, _message, 404} -> {:error, ArgumentError.exception("resource not found (404)")}
      {:error, exception, _status} -> {:error, exception}
    end
  end
end
