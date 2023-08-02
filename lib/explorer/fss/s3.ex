defimpl Explorer.FSS, for: FSS.S3.Entry do
  alias Explorer.FSS.Utils

  def download(entry, path) do
    Utils.assert_regular_path!(path)

    url = url(entry)
    headers = headers(entry, :get, url, [])
    collectable = File.stream!(path)

    case Utils.HTTP.download(url, collectable, headers: headers) do
      {:ok, _collectable} -> :ok
      {:error, _message, 404} -> Utils.posix_error(:enoent)
      {:error, message, _status} -> {:error, message}
    end
  end

  defp url(%FSS.S3.Entry{} = entry) do
    uri =
      if endpoint = entry.config.endpoint do
        URI.parse(endpoint)
      else
        URI.parse("https://s3.#{entry.config.region}.amazonaws.com")
      end

    uri
    |> URI.append_path("/" <> entry.bucket)
    |> URI.append_path("/" <> entry.key)
    |> URI.to_string()
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
