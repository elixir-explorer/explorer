defmodule Explorer.Backend.LazySeries do
  @moduledoc """
  A lazy series represents (roughly) a tree of instructions to build a new
  series from one or more existing series.

  For example, if we wanted to create a new series from the addition of two
  existing series `a` and `b`:

      sum = a + b

  We would represent that with the following `LazySeries`:

      sum =
        %LazySeries{op: :add, args: [
          %LazySeries{op: :col, args: ["a"]}
          %LazySeries{op: :col, args: ["b"]}
        ]}
  """
  alias Explorer.Backend
  alias Explorer.Series

  defstruct op: nil, args: [], dtype: :unknown, aggregation: false, backend: nil

  @type s :: Series.t()
  @type t :: %__MODULE__{
          op: atom(),
          args: list(),
          dtype: any(),
          aggregation: boolean(),
          backend: nil | module()
        }

  def new(op, args, dtype \\ :unknown, aggregation \\ false, backend \\ nil) do
    %__MODULE__{op: op, args: args, dtype: dtype, aggregation: aggregation, backend: backend}
  end

  @series_ops_with_arity Backend.Series.behaviour_info(:callbacks) |> Enum.sort()
  @custom_ops [:divide, :from_list, :inspect, :multiply]

  def operations, do: Keyword.keys(@series_ops_with_arity)
  def operations_with_arity, do: @series_ops_with_arity

  for {op, arity} <- @series_ops_with_arity, op not in @custom_ops do
    args = Macro.generate_arguments(arity, __MODULE__)

    def unquote(op)(unquote_splicing(args)) do
      Backend.LazySeries.__apply_lazy__(unquote(op), unquote(args))
    end
  end

  # These ops have an optional extra arg that defaults to `false`.
  @cumulative_ops [:cumulative_min, :cumulative_max, :cumulative_product, :cumulative_sum]
  for op <- @cumulative_ops do
    def unquote(op)(lazy_series) do
      __apply_lazy__(unquote(op), [lazy_series])
    end
  end

  @doc false
  def __apply_lazy__(op, args, dtype \\ :unknown) when is_atom(op) and is_list(args) do
    args =
      Enum.map(args, fn
        %Series{data: %__MODULE__{} = lazy_series} -> lazy_series
        other -> other
      end)

    %Explorer.Series{
      data: %Explorer.Backend.LazySeries{op: op, args: args},
      dtype: dtype
    }
  end

  # Polars specific functions

  def col(name) when is_atom(name), do: name |> Atom.to_string() |> col()
  def col(name) when is_binary(name), do: __apply_lazy__(:col, [name])

  def rename(%Series{data: %__MODULE__{} = lazy_series}, name) do
    __apply_lazy__(:alias, [lazy_series, name])
  end

  # Custom ops

  def divide(_out_dtype, left, right) do
    __apply_lazy__(:divide, [left, right])
  end

  def from_list(list, dtype \\ :unknown) when is_list(list) do
    case list do
      # [] -> TODO: figure out what do to here...
      [literal] -> lit(literal, dtype)
      [_ | _] -> __apply_lazy__(:from_list, [list, dtype])
    end
  end

  def lit(literal, dtype \\ :unknown) do
    __apply_lazy__(:lit, [literal], dtype)
  end

  def multiply(_out_dtype, left, right) do
    __apply_lazy__(:multiply, [left, right])
  end

  def inspect(series, opts) do
    alias Inspect.Algebra, as: A

    open = A.color("(", :list, opts)
    close = A.color(")", :list, opts)

    dtype =
      series
      |> Explorer.Series.dtype()
      |> Explorer.Shared.dtype_to_string()
      |> A.color(:atom, opts)

    A.concat([
      A.color("LazySeries[???]", :atom, opts),
      A.line(),
      dtype,
      " ",
      open,
      Code.quoted_to_algebra(to_elixir_ast(series.data)),
      close
    ])
  end

  @to_elixir_op %{
    add: :+,
    subtract: :-,
    multiply: :*,
    divide: :/,
    pow: :**,
    equal: :==,
    not_equal: :!=,
    greater: :>,
    greater_equal: :>=,
    less: :<,
    less_equal: :<=,
    binary_and: :and,
    binary_or: :or,
    binary_in: :in,
    unary_not: :not
  }

  defp to_elixir_ast(%__MODULE__{op: :from_list, args: [[single], _]}) do
    single
  end

  defp to_elixir_ast(%__MODULE__{op: op, args: args}) do
    {Map.get(@to_elixir_op, op, op), [], Enum.map(args, &to_elixir_ast/1)}
  end

  defp to_elixir_ast(%Explorer.PolarsBackend.Series{} = series) do
    series = Explorer.PolarsBackend.Shared.create_series(series)

    case Explorer.Series.size(series) do
      1 -> series[0]
      _ -> series
    end
  end

  defp to_elixir_ast(other), do: other

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
