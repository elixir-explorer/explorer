defmodule Explorer.Backend do
  @moduledoc """
  The behaviour for Explorer backends and associated functions.

  Each backend is a module with `DataFrame` and `Series` submodules that match the 
  respective behaviours for each.

  The default backend is read from the application environment. Currently, the only 
  backend is an in-memory, eager one based on
  [`Polars`](https://github.com/pola-rs/polars). When alternatives are
  available, you can use them by configuring your runtime:

      # config/runtime.exs
      import Config
      config :explorer, default_backend: Lib.CustomBackend

  """

  @backend_key {Explorer, :default_backend}

  @doc """
  Sets the current process default backend to `backend`.

  The default backend is stored only in the process dictionary. This means if
  you start a separate process, such as `Task`, the default backend must be set
  on the new process too.

  ## Examples
      iex> Explorer.Backend.put(Lib.CustomBackend)
      Explorer.PolarsBackend
      iex> Explorer.Backend.get()
      Lib.CustomBackend
  """
  def put(backend) do
    Process.put(@backend_key, backend!(backend)) ||
      backend!(Application.fetch_env!(:explorer, :default_backend))
  end

  @doc """
  Gets the default backend for the current process.
  """
  def get do
    Process.get(@backend_key) || backend!(Application.fetch_env!(:explorer, :default_backend))
  end

  ## Helpers

  defp backend!(backend) when is_atom(backend),
    do: backend

  defp backend!(other) do
    raise ArgumentError,
          "backend must be an atom, got: #{inspect(other)}"
  end
end
