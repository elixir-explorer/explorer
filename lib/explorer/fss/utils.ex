defmodule Explorer.FSS.Utils do
  # Copied from Livebook's code: https://github.com/livebook-dev/livebook/blob/481d39d7edbb7498c8372d0e35f143935e0aeb92/lib/livebook/file_system/utils.ex
  @moduledoc false

  @type path() :: String.t()

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
  Converts the given posix error atom into readable error tuple.
  """
  @spec posix_error(atom()) :: {:error, String.t()}
  def posix_error(error) do
    message = error |> :file.format_error() |> List.to_string()
    {:error, message}
  end
end
