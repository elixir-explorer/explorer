defmodule Explorer.FSS do
  @moduledoc false

  # Internal module for parsing filesystem specifications.
  # Public APIs accept URIs like "s3://bucket/key", and this module
  # parses them into triplets {:backend, path, config} for internal use.

  @doc """
  Parses an S3 URL in the format `s3://bucket/key` and returns a triplet.

  ## Options

    * `:config` - A map with S3 configuration keys, or `nil` to read from env vars:
      - `AWS_ACCESS_KEY_ID`
      - `AWS_SECRET_ACCESS_KEY`
      - `AWS_REGION` or `AWS_DEFAULT_REGION`
      - `AWS_SESSION_TOKEN`

  ## Returns

  `{:ok, {:s3, key, config}}` where config is a map with:
    - `:access_key_id`
    - `:secret_access_key`
    - `:endpoint`
    - `:bucket` (can be nil if included in endpoint)
    - `:region` (can be nil for non-AWS S3)
    - `:token` (optional)
  """
  @spec parse_s3(String.t(), Keyword.t()) ::
          {:ok, {:s3, String.t(), map()}} | {:error, Exception.t()}
  def parse_s3(url, opts \\ []) do
    opts = Keyword.validate!(opts, config: nil)

    uri = URI.parse(url)

    case uri do
      %{scheme: "s3", host: bucket, path: "/" <> key} when is_binary(bucket) ->
        bucket = if bucket != "", do: bucket

        config =
          opts
          |> Keyword.fetch!(:config)
          |> normalize_s3_config!()
          |> validate_s3_config!()
          |> then(fn config ->
            config = %{config | bucket: bucket}

            if is_nil(config.endpoint) and not is_nil(bucket) do
              s3_host_suffix = "s3." <> config.region <> ".amazonaws.com"

              # We consume the bucket name in the endpoint if there are no dots in it.
              {endpoint, bucket} =
                if String.contains?(bucket, ".") do
                  {"https://" <> s3_host_suffix, bucket}
                else
                  {"https://" <> bucket <> "." <> s3_host_suffix, nil}
                end

              %{config | endpoint: endpoint, bucket: bucket}
            else
              config
            end
          end)

        if is_nil(config.endpoint) do
          {:error, ArgumentError.exception("endpoint is required when bucket is nil")}
        else
          {:ok, {:s3, key, config}}
        end

      _ ->
        {:error,
         ArgumentError.exception("expected s3://<bucket>/<key> URL, got: " <> URI.to_string(uri))}
    end
  end

  defp validate_s3_config!(config) do
    check_s3_field!(config, :access_key_id, "AWS_ACCESS_KEY_ID")
    check_s3_field!(config, :secret_access_key, "AWS_SECRET_ACCESS_KEY")
    check_s3_field!(config, :region, "AWS_REGION")

    config
  end

  defp normalize_s3_config!(nil), do: s3_config_from_env()

  defp normalize_s3_config!(config) when is_map(config) do
    Map.merge(s3_config_from_env(), config)
  end

  defp normalize_s3_config!(config) when is_list(config) do
    Map.merge(s3_config_from_env(), Map.new(config))
  end

  defp normalize_s3_config!(other) do
    raise ArgumentError,
          "expect S3 configuration to be a map or keyword list. Instead got #{inspect(other)}"
  end

  defp check_s3_field!(config, key, env) do
    if Map.get(config, key) in ["", nil] do
      raise ArgumentError,
            "missing #{inspect(key)} for S3 (set the key or the #{env} env var)"
    end
  end

  defp s3_config_from_env do
    %{
      access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
      region: System.get_env("AWS_REGION", System.get_env("AWS_DEFAULT_REGION")),
      token: System.get_env("AWS_SESSION_TOKEN"),
      endpoint: nil,
      bucket: nil
    }
  end

  @doc """
  Parses an HTTP or HTTPS URL and returns a triplet.

  ## Options

    * `:config` - A map or keyword list with `:headers` key, or `nil`

  ## Returns

  `{:ok, {:http, url, config}}` where config is a map with:
    - `:headers` - List of `{String.t(), String.t()}` tuples
  """
  @spec parse_http(String.t(), Keyword.t()) ::
          {:ok, {:http, String.t(), map()}} | {:error, Exception.t()}
  def parse_http(url, opts \\ []) do
    opts = Keyword.validate!(opts, config: nil)

    with {:ok, config} <- build_http_config(opts[:config]) do
      {:ok, {:http, url, config}}
    end
  end

  defp build_http_config(nil), do: {:ok, %{headers: []}}

  defp build_http_config(config) when is_map(config) do
    headers = Map.get(config, :headers, [])

    if valid_headers?(headers) do
      {:ok, %{headers: headers}}
    else
      {:error,
       ArgumentError.exception(
         "headers must be a list of {key, value} tuples where both are strings, got: #{inspect(headers)}"
       )}
    end
  end

  defp build_http_config(config) when is_list(config) do
    case Keyword.validate(config, headers: []) do
      {:ok, opts} ->
        if valid_headers?(opts[:headers]) do
          {:ok, %{headers: opts[:headers]}}
        else
          {:error,
           ArgumentError.exception(
             "headers must be a list of {key, value} tuples where both are strings, got: #{inspect(opts[:headers])}"
           )}
        end

      {:error, key} ->
        {:error,
         ArgumentError.exception(
           "the keys #{inspect(key)} are not valid keys for the HTTP configuration"
         )}
    end
  end

  defp build_http_config(other) do
    {:error,
     ArgumentError.exception(
       "config for HTTP entry is invalid. Expecting a map or keyword list with :headers, got #{inspect(other)}"
     )}
  end

  defp valid_headers?(headers) when is_list(headers) do
    Enum.all?(headers, fn
      {key, value} when is_binary(key) and is_binary(value) -> true
      _ -> false
    end)
  end

  defp valid_headers?(_), do: false

  @doc """
  Parses a local file path and returns a triplet.

  ## Returns

  `{:ok, {:local, path, %{}}}` where path is the filesystem path.
  """
  @spec parse_local(String.t()) :: {:ok, {:local, String.t(), map()}}
  def parse_local(path) when is_binary(path) do
    {:ok, {:local, path, %{}}}
  end

  @doc """
  Downloads a remote resource to a local path.

  Works with `:s3` and `:http` triplets.
  """
  @spec download({:s3 | :http, String.t(), map()}, Path.t()) ::
          :ok | {:error, Exception.t()}
  def download({:s3, key, config}, path) do
    with :ok <- assert_regular_path(path),
         {:ok, url} <- build_s3_url(key, config) do
      headers = build_s3_headers(config, :get, url, [])
      collectable = File.stream!(path)

      case download_url(url, collectable, headers: headers) do
        {:ok, _collectable} -> :ok
        {:error, _message, 404} -> {:error, ArgumentError.exception("resource not found (404)")}
        {:error, exception, _status} -> {:error, exception}
      end
    end
  end

  def download({:http, url, config}, path) do
    with :ok <- assert_regular_path(path) do
      headers = Map.get(config, :headers, [])
      collectable = File.stream!(path)

      case download_url(url, collectable, headers: headers) do
        {:ok, _collectable} -> :ok
        {:error, _message, 404} -> {:error, ArgumentError.exception("resource not found (404)")}
        {:error, exception, _status} -> {:error, exception}
      end
    end
  end

  # Copied from Livebook's code: https://github.com/livebook-dev/livebook/blob/481d39d7edbb7498c8372d0e35f143935e0aeb92/lib/livebook/file_system/utils.ex

  defp assert_regular_path(path) do
    if regular_path?(path) do
      :ok
    else
      {:error, ArgumentError.exception("expected a regular file path, got: #{inspect(path)}")}
    end
  end

  defp regular_path?(path) do
    not String.ends_with?(path, "/")
  end

  defp download_url(url, collectable, opts) do
    headers = build_download_headers(opts[:headers] || [])

    request = {url, headers}
    http_opts = [ssl: http_ssl_opts()]

    caller = self()

    receiver = fn reply_info ->
      request_id = elem(reply_info, 0)

      # Cancel the request if the caller terminates
      if Process.alive?(caller) do
        send(caller, {:http, reply_info})
      else
        :httpc.cancel_request(request_id)
      end
    end

    opts = [stream: :self, sync: false, receiver: receiver]

    {:ok, request_id} = :httpc.request(:get, request, http_opts, opts)

    try do
      {acc, collector} = Collectable.into(collectable)

      try do
        download_loop(%{
          request_id: request_id,
          total_size: nil,
          size: nil,
          acc: acc,
          collector: collector
        })
      catch
        kind, reason ->
          collector.(acc, :halt)
          :httpc.cancel_request(request_id)
          exception = Exception.normalize(kind, reason, __STACKTRACE__)
          {:error, exception, nil}
      else
        {:ok, state} ->
          acc = state.collector.(state.acc, :done)
          {:ok, acc}

        {:error, message, status} ->
          collector.(acc, :halt)
          :httpc.cancel_request(request_id)
          {:error, ArgumentError.exception(message), status}
      end
    catch
      kind, reason ->
        :httpc.cancel_request(request_id)
        exception = Exception.normalize(kind, reason, __STACKTRACE__)
        {:error, exception, nil}
    end
  end

  defp download_loop(state) do
    receive do
      {:http, reply_info} when elem(reply_info, 0) == state.request_id ->
        download_receive(state, reply_info)
    end
  end

  defp download_receive(_state, {_, {:error, error}}) do
    {:error, "reason: #{inspect(error)}", nil}
  end

  defp download_receive(state, {_, {{_, 200, _}, _headers, body}}) do
    acc = state.collector.(state.acc, {:cont, body})
    {:ok, %{state | acc: acc}}
  end

  defp download_receive(_state, {_, {{_, status, _}, _headers, _body}}) do
    {:error, "got HTTP status: #{status}", status}
  end

  defp download_receive(state, {_, :stream_start, headers}) do
    total_size = get_content_length(headers)
    download_loop(%{state | total_size: total_size, size: 0})
  end

  defp download_receive(state, {_, :stream, body_part}) do
    acc = state.collector.(state.acc, {:cont, body_part})
    state = %{state | acc: acc}

    part_size = byte_size(body_part)
    state = update_in(state.size, &(&1 + part_size))
    download_loop(state)
  end

  defp download_receive(state, {_, :stream_end, _headers}) do
    {:ok, state}
  end

  defp get_content_length(headers) do
    case List.keyfind(headers, ~c"content-length", 0) do
      {_, content_length} ->
        List.to_integer(content_length)

      _ ->
        nil
    end
  end

  defp build_download_headers(entries) do
    headers =
      Enum.map(entries, fn {key, value} ->
        {to_charlist(key), to_charlist(value)}
      end)

    [{~c"user-agent", ~c"elixir-explorer"} | headers]
  end

  defp http_ssl_opts do
    # TODO: Remove castore fallback when we require Elixir v1.17+
    # Use secure options, see https://gist.github.com/jonatanklosko/5e20ca84127f6b31bbe3906498e1a1d7
    [
      verify: :verify_peer,
      # We need to increase depth because the default value is 1.
      # See: https://erlef.github.io/security-wg/secure_coding_and_deployment_hardening/ssl
      depth: 3,
      customize_hostname_check: [
        match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
      ],
      cacerts: :public_key.cacerts_get()
    ]
  end

  defp build_s3_url(key, config) do
    endpoint = Map.fetch!(config, :endpoint)
    bucket = Map.get(config, :bucket)

    uri = URI.parse(endpoint)

    maybe_uri =
      if is_nil(bucket) do
        {:ok, uri}
      else
        append_path(uri, "/" <> bucket)
      end

    with {:ok, uri} <- maybe_uri,
         {:ok, uri_with_key} <- append_path(uri, "/" <> key) do
      {:ok, URI.to_string(uri_with_key)}
    end
  end

  # Once we depend on Elixir ~> 1.15, we can use `URI.append_path/2`.
  defp append_path(%URI{}, "//" <> _ = path) do
    {:error, ArgumentError.exception(~s|path cannot start with "//", got: #{inspect(path)}|)}
  end

  defp append_path(%URI{path: path} = uri, "/" <> rest = all) do
    updated_uri =
      cond do
        path == nil -> %{uri | path: all}
        path != "" and :binary.last(path) == ?/ -> %{uri | path: path <> rest}
        true -> %{uri | path: path <> all}
      end

    {:ok, updated_uri}
  end

  defp build_s3_headers(config, method, url, headers, body \\ nil) do
    now = NaiveDateTime.utc_now() |> NaiveDateTime.to_erl()
    %{host: host} = URI.parse(url)
    headers = [{"Host", host} | headers]

    headers =
      case Map.get(config, :token) do
        nil -> headers
        token -> [{"X-Amz-Security-Token", token} | headers]
      end

    :aws_signature.sign_v4(
      Map.fetch!(config, :access_key_id),
      Map.fetch!(config, :secret_access_key),
      Map.get(config, :region, ""),
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
