defimpl Explorer.FSS, for: FSS.S3.Entry do
  alias Explorer.FSS.Utils

  def download(entry, path) do
    Utils.assert_regular_path!(path)

    url = url(entry)
    headers = headers(entry, :get, url, [])
    collectable = File.stream!(path)

    case Utils.download(url, collectable, headers: headers) do
      {:ok, _collectable} -> :ok
      {:error, _message, 404} -> {:error, ArgumentError.exception("resource not found (404)")}
      {:error, exception, _status} -> {:error, exception}
    end
  end

  defp url(%FSS.S3.Entry{} = entry) do
    config = entry.config

    uri = URI.parse(config.endpoint)

    uri =
      if is_nil(config.bucket) do
        uri
      else
        append_path(uri, "/" <> config.bucket)
      end

    uri
    |> append_path("/" <> entry.key)
    |> URI.to_string()
  end

  # Once we depend on Elixir ~> 1.15, we can use `URI.append_path/2`.
  defp append_path(%URI{}, "//" <> _ = path) do
    raise ArgumentError, ~s|path cannot start with "//", got: #{inspect(path)}|
  end

  defp append_path(%URI{path: path} = uri, "/" <> rest = all) do
    cond do
      path == nil -> %{uri | path: all}
      path != "" and :binary.last(path) == ?/ -> %{uri | path: path <> rest}
      true -> %{uri | path: path <> all}
    end
  end

  defp headers(%FSS.S3.Entry{} = entry, method, url, headers, body \\ nil) do
    now = NaiveDateTime.utc_now() |> NaiveDateTime.to_erl()
    %{host: host} = URI.parse(url)
    headers = [{"Host", host} | headers]

    :aws_signature.sign_v4(
      entry.config.access_key_id,
      entry.config.secret_access_key,
      entry.config.region,
      "s3",
      now,
      Atom.to_string(method),
      url,
      headers,
      body || "",
      uri_encode_path: false
    )
  end
end
