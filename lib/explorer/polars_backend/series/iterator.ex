defmodule Explorer.PolarsBackend.Series.Iterator do
  @moduledoc false

  defstruct [:series, :size]

  alias Explorer.PolarsBackend.Series

  def new(series) do
    %__MODULE__{series: series, size: Series.size(series)}
  end

  defimpl Enumerable do
    alias Explorer.PolarsBackend.Series

    def count(iterator), do: {:ok, iterator.size}

    def member?(_iterator, _value), do: {:error, __MODULE__}

    def slice(iterator) do
      {:ok, iterator.size,
       fn start, size ->
         iterator.series
         |> Series.slice(start, size)
         |> Series.to_list()
       end}
    end

    def reduce(iterator, acc, fun) do
      reduce(iterator.series, iterator.size, 0, acc, fun)
    end

    defp reduce(_series, _size, _offset, {:halt, acc}, _fun), do: {:halted, acc}

    defp reduce(series, size, offset, {:suspend, acc}, fun) do
      {:suspended, acc, &reduce(series, size, offset, &1, fun)}
    end

    defp reduce(_series, size, size, {:cont, acc}, _fun), do: {:done, acc}

    defp reduce(series, size, offset, {:cont, acc}, fun) do
      value = Series.fetch!(series, offset)
      reduce(series, size, offset + 1, fun.(value, acc), fun)
    end
  end
end
