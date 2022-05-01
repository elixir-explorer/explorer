defmodule Explorer.Inspect do
  # **Private** helpers for inspecting Explorer data structures.
  @moduledoc false

  alias Inspect.Algebra, as: IA

  def to_string(i, _opts) when is_nil(i), do: "nil"
  def to_string(i, _opts) when is_binary(i), do: "\"#{i}\""

  def to_string(i, opts) when is_list(i),
    do:
      IA.container_doc(
        open(opts),
        i,
        close(opts),
        opts,
        &to_string/2
      )

  def to_string(i, _opts), do: Kernel.to_string(i)

  def open(opts), do: IA.color("[", :list, opts)
  def close(opts), do: IA.color("]", :list, opts)

  def s_shape(size, opts), do: IA.concat([open(opts), Integer.to_string(size), close(opts)])

  def df_shape(rows, cols, [_ | _] = groups, opts),
    do:
      IA.nest(
        IA.concat([
          IA.line(),
          open(opts),
          IA.color("rows: ", :atom, opts),
          to_string(rows, opts),
          ", ",
          IA.color("columns: ", :atom, opts),
          to_string(cols, opts),
          ", ",
          IA.color("groups: ", :atom, opts),
          to_string(groups, opts),
          close(opts)
        ]),
        2
      )

  def df_shape(rows, cols, [] = _groups, opts),
    do:
      IA.nest(
        IA.concat([
          IA.line(),
          open(opts),
          IA.color("rows: ", :atom, opts),
          to_string(rows, opts),
          ", ",
          IA.color("columns: ", :atom, opts),
          to_string(cols, opts),
          close(opts)
        ]),
        2
      )

  def s_inner(dtype, size, values, opts) do
    data = format_data(values, opts)
    shape = s_shape(size, opts)
    dtype = IA.color(to_string(dtype), :atom, opts)
    IA.concat([IA.line(), dtype, shape, IA.line(), data])
  end

  def df_inner(name, dtype, values, opts) do
    name = IA.color(name, :map, opts)
    dtype = IA.color(to_string(dtype), :atom, opts)
    data = format_data(values, opts)
    IA.nest(IA.concat([IA.line(), name, " ", dtype, " ", data]), 2)
  end

  def format_data(values, opts) do
    IA.container_doc(open(opts), values, close(opts), opts, &to_string/2)
  end
end

defimpl Inspect, for: Explorer.Series do
  alias Explorer.Series
  import Inspect.Algebra

  @printable_limit 50

  def inspect(series, opts) do
    dtype = Series.dtype(series)
    size = Series.size(series)
    vals = series |> Series.slice(0, @printable_limit + 1) |> Series.to_list()

    inner = Explorer.Inspect.s_inner(dtype, size, vals, %{opts | limit: @printable_limit})

    color("#Explorer.Series<", :map, opts)
    |> concat(nest(inner, 2))
    |> concat(color("\n>", :map, opts))
  end
end

defimpl Inspect, for: Explorer.DataFrame do
  alias Explorer.DataFrame
  alias Explorer.Series
  import Inspect.Algebra

  @printable_limit 5

  def inspect(df, opts) do
    {rows, cols} = DataFrame.shape(df)
    groups = DataFrame.groups(df)
    shape = Explorer.Inspect.df_shape(rows, cols, groups, opts)

    cols_algebra =
      for name <- DataFrame.names(df) do
        series = df |> DataFrame.pull(name) |> Series.slice(0, @printable_limit + 1)
        dtype = Series.dtype(series)
        vals = Series.to_list(series)
        Explorer.Inspect.df_inner(name, dtype, vals, %{opts | limit: @printable_limit})
      end

    concat([
      color("#Explorer.DataFrame<", :map, opts),
      shape,
      concat(cols_algebra),
      line(),
      nest(color(">", :map, opts), 0)
    ])
  end
end
