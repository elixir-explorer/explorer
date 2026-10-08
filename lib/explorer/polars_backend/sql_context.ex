defmodule Explorer.PolarsBackend.SQLContext do
  @moduledoc false

  defstruct resource: nil

  alias Explorer.Native
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Shared

  @type t :: %__MODULE__{resource: reference()}

  @behaviour Explorer.Backend.SQLContext

  def new() do
    ctx = Native.sql_context_new()
    Explorer.Backend.SQLContext.new(ctx)
  end

  def register(%Explorer.SQLContext{ctx: ctx} = context, name, %Explorer.DataFrame{data: df}) do
    Native.sql_context_register(ctx, name, df)
    context
  end

  def unregister(%Explorer.SQLContext{ctx: ctx} = context, name) do
    Native.sql_context_unregister(ctx, name)
    context
  end

  def execute(%Explorer.SQLContext{ctx: ctx}, query) do
    case Native.sql_context_execute(ctx, query) do
      {:ok, polars_ldf} -> Shared.create_dataframe(polars_ldf)
      {:error, error} -> {:error, RuntimeError.exception(error)}
    end
  end

  def get_tables(%Explorer.SQLContext{ctx: ctx}), do: Native.sql_context_get_tables(ctx)
end
