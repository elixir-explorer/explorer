defmodule Explorer.PolarsBackend.Series.Iterator do
  @moduledoc false

  defstruct [:series, :size]

  alias Explorer.PolarsBackend.Series

  def new(series) do
    %__MODULE__{series: series, size: Series.length(series)}
  end

  defimpl Enumerable do
    alias Explorer.PolarsBackend.Series

    def count(iterator), do: {:ok, iterator.size}

    def member?(_iterator, _value), do: {:error, __MODULE__}

    def slice(iterator) do
      {:ok, iterator.size,
       fn start, length ->
         iterator.series
         |> Series.slice(start, length)
         |> Series.to_list()
       end}
    end

    def reduce(iterator, acc, fun) do
      reduce(iterator, 0, acc, fun)
    end

    defp reduce(_iterator, _offset, {:halt, acc}, _fun), do: {:halted, acc}

    defp reduce(iterator, offset, {:suspend, acc}, fun) do
      {:suspended, acc, &reduce(iterator, offset, &1, fun)}
    end

    defp reduce(iterator, offset, {:cont, acc}, fun) do
      if iterator.size == offset do
        {:done, acc}
      else
        value = Series.get(iterator.series, offset)
        reduce(iterator, offset + 1, fun.(value, acc), fun)
      end
    end
  end
end
