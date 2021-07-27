defmodule Explorer do
  @moduledoc """
  Dataframes for Elixir.

  The `Explorer` library is a set of functions and data types to work with tabular data in Elixir.

  Most of the functionality in `Explorer` is in `Explorer.DataFrame` and `Explorer.Series`. This
  module handles the default backend for `Explorer`. As there is currently only one backend for
  `Explorer`, you should not use the functions in this module.
  """

  ## Backend API

  @backend_key {Explorer, :default_backend}
  @backend_default Explorer.PolarsBackend

  @doc """
  Sets the current process default backend to `backend`.

  The default backend is stored only in the process dictionary. This means if you start a 
  separate process, such as `Task`, the default backend must be set on the new process too.

  ## Examples
      iex> Explorer.default_backend(Lib.CustomBackend)
      Explorer.PolarsBackend
      iex> Explorer.default_backend()
      Lib.CustomBackend
  """
  def default_backend(backend) do
    Process.put(@backend_key, backend!(backend)) || @backend_default
  end

  @doc """
  Gets the default backend for the current process.
  """
  def default_backend do
    Process.get(@backend_key) || @backend_default
  end

  ## Helpers

  defp backend!(backend) when is_atom(backend),
    do: backend

  defp backend!(other) do
    raise ArgumentError,
          "backend must be an atom, got: #{inspect(other)}"
  end
end
