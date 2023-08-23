defmodule Explorer.FSS.Utils do
  # Copied from Livebook's code: https://github.com/livebook-dev/livebook/blob/481d39d7edbb7498c8372d0e35f143935e0aeb92/lib/livebook/file_system/utils.ex
  @moduledoc false

  @type path() :: String.t()
  @type status() :: non_neg_integer()

  @doc """
  Asserts that the given path is a regular file path.
  """
  @spec assert_regular_path!(path()) :: :ok
  def assert_regular_path!(path) do
    unless regular_path?(path) do
      raise ArgumentError, "expected a regular file path, got: #{inspect(path)}"
    end

    :ok
  end

  @doc """
  Checks if the given path describes a regular file.
  """
  @spec regular_path?(path()) :: boolean()
  def regular_path?(path) do
    not String.ends_with?(path, "/")
  end

  @doc """
  Downloads resource at the given URL into `collectable`.

  If collectable raises and error, it is rescued and an error tuple
  is returned.

  ## Options

    * `:headers` - request headers

  """
  @spec download(String.t(), Collectable.t(), keyword()) ::
          {:ok, Collectable.t()} | {:error, Exception.t(), status()}
  def download(url, collectable, opts \\ []) do
    headers = build_headers(opts[:headers] || [])

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
    total_size = total_size(headers)
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

  defp total_size(headers) do
    case List.keyfind(headers, ~c"content-length", 0) do
      {_, content_length} ->
        List.to_integer(content_length)

      _ ->
        nil
    end
  end

  defp build_headers(entries) do
    headers =
      Enum.map(entries, fn {key, value} ->
        {to_charlist(key), to_charlist(value)}
      end)

    [{~c"user-agent", ~c"elixir-explorer"} | headers]
  end

  # Load SSL certificates

  defp http_ssl_opts() do
    # Use secure options, see https://gist.github.com/jonatanklosko/5e20ca84127f6b31bbe3906498e1a1d7
    [
      verify: :verify_peer,
      cacertfile: CAStore.file_path() |> String.to_charlist(),
      # We need to increase depth because the default value is 1.
      # See: https://erlef.github.io/security-wg/secure_coding_and_deployment_hardening/ssl
      depth: 3,
      customize_hostname_check: [
        match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
      ]
    ]
  end
end
