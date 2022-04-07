defmodule Explorer.PolarsBackend.Series.Iterator do
  @moduledoc false

  defstruct [:series, :offset, :size]

  alias Explorer.PolarsBackend.Series

  def new(series) do
    %__MODULE__{series: series, offset: 0, size: Series.length(series)}
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

    def reduce(_iterator, {:halt, acc}, _fun), do: {:halted, acc}

    def reduce(iterator, {:suspend, acc}, fun) do
      {:suspended, acc, &reduce(iterator, &1, fun)}
    end

    def reduce(iterator, {:cont, acc}, fun) do
      case next(iterator) do
        :done -> {:done, acc}
        {value, iterator} -> reduce(iterator, fun.(value, acc), fun)
      end
    end

    defp next(%{offset: size, size: size}), do: :done

    defp next(iterator) do
      value = Series.get(iterator.series, iterator.offset)
      {value, update_in(iterator.offset, &(&1 + 1))}
    end
  end
end
