defmodule Explorer.Backend.SQLContext do
  @type t :: struct()
  @type c :: Explorer.SQLContext.t()
  @type df :: Explorer.DataFrame.t()
  @type result(t) :: {:ok, t} | {:error, term()}

  @callback register(c, String.t(), df) :: c
  @callback unregister(c, String.t()) :: c
  @callback execute(c, String.t()) :: result(df)
  @callback get_tables(c) :: list(String.t())

  def new(ctx), do: %Explorer.SQLContext{ctx: ctx}
end
