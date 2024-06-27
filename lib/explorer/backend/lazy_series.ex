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

  # Polars specific functions

  def rename(%__MODULE__{} = lazy_series, name) do
    %__MODULE__{op: :alias, args: [lazy_series, name]}
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(series, opts) do
      outer_doc(series, opts)
    end

    # I think this clause should be unnecessary...
    defp outer_doc(%Explorer.Series{data: data}, opts) do
      outer_doc(data, opts)
    end

    defp outer_doc(%Explorer.Backend.LazySeries{} = lazy_series, inspect_opts) do
      open = color("#Explorer.Backend.LazySeries<", :map, inspect_opts)
      separator = color(",", :map, inspect_opts)
      close = color(">", :map, inspect_opts)
      opts = [separator: separator, break: :strict]
      container_doc(open, [lazy_series], close, inspect_opts, &inner_doc/2, opts)
    end

    defp outer_doc(list, _opts) when is_list(list), do: list |> Enum.map(&to_string/1) |> concat()
    defp outer_doc(str, _opts) when is_binary(str), do: "\"" <> to_string(str) <> "\""
    defp outer_doc(atom, _opts) when is_atom(atom), do: ":" <> to_string(atom)
    defp outer_doc(other, _opts), do: to_string(other)

    defp inner_doc(%Explorer.Backend.LazySeries{} = lazy_series, opts) do
      concat([
        "op: :#{lazy_series.op}, ",
        args_doc(lazy_series, opts)
      ])
    end

    defp args_doc(%Explorer.Backend.LazySeries{} = lazy_series, inspect_opts) do
      open = color("args: [", :map, inspect_opts)
      separator = color(",", :map, inspect_opts)
      close = color("]", :map, inspect_opts)
      opts = [separator: separator, break: :strict]
      container_doc(open, lazy_series.args, close, inspect_opts, &outer_doc/2, opts)
    end
  end
end
