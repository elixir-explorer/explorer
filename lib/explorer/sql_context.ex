defmodule Explorer.SQLContext do
  @enforce_keys [:ctx]
  defstruct [:ctx]

  alias __MODULE__, as: SQLContext

  @type t :: %SQLContext{ctx: Explorer.Backend.SQLContext.t()}

  alias Explorer.Backend.SQLContext
  alias Explorer.Shared

  def new(args \\ [], opts \\ []), do: Shared.apply_init(backend(), :new, args, opts)

  def register(ctx, name, df, opts \\ []) do
    Shared.apply_init(backend(), :register, [ctx, name, df], opts)
  end

  def unregister(ctx, name, opts \\ []) do
    Shared.apply_init(backend(), :unregister, [ctx, name], opts)
  end

  def execute(ctx, query, opts \\ []) do
    Shared.apply_init(backend(), :execute, [ctx, query], opts)
  end

  def get_tables(ctx, opts \\ []) do
    Shared.apply_init(backend(), :get_tables, [ctx], opts)
  end

  defp backend do
    Module.concat([Explorer.Backend.get(), "SQLContext"])
  end
end
