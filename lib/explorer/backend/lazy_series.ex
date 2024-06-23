defmodule Explorer.Backend.LazySeries do
  @moduledoc """
  A lazy series represents (roughly) a tree of instructions to build a new
  series from one or more existing series.

  For example, if we wanted to create a new series from the addition of two
  existing series `a` and `b`:

      a + b

  We would represent that with the following `LazySeries`:

      sum =
        %LazySeries{op: :add, args: [
          %LazySeries{op: :col, args: ["a"]}
          %LazySeries{op: :col, args: ["b"]}
        ]}
  """
  alias Explorer.Backend

  defstruct op: nil, args: [], dtype: nil, aggregation: false, backend: nil

  @behaviour Backend.Series

  @type t :: %__MODULE__{
          op: atom(),
          args: list(),
          dtype: any(),
          aggregation: boolean(),
          backend: nil | module()
        }

  def new(op, args, dtype, aggregation \\ false, backend \\ nil) do
    %__MODULE__{op: op, args: args, dtype: dtype, aggregation: aggregation, backend: backend}
  end

  @series_ops_with_arity Backend.Series.behaviour_info(:callbacks) |> Enum.sort()

  for {op, arity} <- @series_ops_with_arity do
    args = Macro.generate_arguments(arity, __MODULE__)

    @impl Backend.Series
    def unquote(op)(unquote_splicing(args)) do
      %__MODULE__{op: unquote(op), args: unquote(args)}
    end
  end

  def operations, do: Keyword.keys(@series_ops_with_arity)
  def operations_with_arity, do: @series_ops_with_arity
end
