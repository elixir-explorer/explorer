defmodule Explorer.Shared do
  # A collection of **private** helpers shared in Explorer.
  @moduledoc false

  alias Explorer.Backend.LazySeries

  @doc """
  All supported dtypes.
  """
  def dtypes, do: [:float, :integer, :boolean, :string, :date, :datetime]

  @doc """
  Gets the backend from a `Keyword.t()` or `nil`.
  """
  def backend_from_options!(opts) do
    case Keyword.fetch(opts, :backend) do
      {:ok, backend} when is_atom(backend) ->
        backend

      {:ok, other} ->
        raise ArgumentError,
              ":backend must be an atom, got: #{inspect(other)}"

      :error ->
        nil
    end
  end

  @doc """
  Gets the implementation of a dataframe or series.
  """
  def impl!(%{data: %struct{}}), do: struct

  def impl!([%{data: %first_struct{}} | _] = dfs) when is_list(dfs),
    do: Enum.reduce(dfs, first_struct, fn %{data: %struct{}}, acc -> pick_struct(acc, struct) end)

  def impl!(%{data: %struct1{}}, %{data: %struct2{}}),
    do: pick_struct(struct1, struct2)

  @doc """
  Gets the implementation of maybe series.
  """
  def series_impl!(series_or_scalars) when is_list(series_or_scalars) do
    impl =
      Enum.reduce(series_or_scalars, nil, fn
        %{data: %struct{}}, nil -> struct
        %{data: %struct{}}, impl -> pick_series_impl(impl, struct)
        _scalar, impl -> impl
      end)

    if impl == nil do
      raise ArgumentError,
            "expected at least one series to be given as argument, got: #{inspect(series_or_scalars)}"
    end

    impl
  end

  @doc """
  Applies a function with args using the implementation of a dataframe or series.
  """
  def apply_impl(df_or_series, fun, args \\ []) do
    impl = impl!(df_or_series)
    apply(impl, fun, [df_or_series | args])
  end

  @doc """
  Applies a function using the implementation of maybe series.
  """
  def apply_series_impl(fun, series_or_scalars) when is_list(series_or_scalars) do
    impl = series_impl!(series_or_scalars)

    apply(impl, fun, series_or_scalars)
  end

  @doc """
  Gets the implementation of a list of maybe dataframes or series.
  """
  def find_impl!(list) do
    Enum.reduce(list, fn
      %{data: %struct{}}, acc -> pick_struct(struct, acc)
      _, acc -> acc
    end)
  end

  defp pick_struct(struct, struct), do: struct

  defp pick_struct(struct1, struct2) do
    raise "cannot invoke Explorer function because it relies on two incompatible implementations: " <>
            "#{inspect(struct1)} and #{inspect(struct2)}"
  end

  defp pick_series_impl(struct, struct), do: struct
  defp pick_series_impl(LazySeries, _), do: LazySeries
  defp pick_series_impl(_, LazySeries), do: LazySeries

  defp pick_series_impl(struct1, struct2) do
    raise "cannot invoke Explorer function because it relies on two incompatible implementations: " <>
            "#{inspect(struct1)} and #{inspect(struct2)}"
  end

  @doc """
  Gets the `dtype` of a list or raise error if not possible.
  """
  def check_types!(list) do
    type =
      Enum.reduce(list, nil, fn el, type ->
        new_type = type(el, type) || type

        cond do
          new_type == :numeric and type in [:float, :integer] ->
            new_type

          new_type != type and type != nil ->
            raise ArgumentError,
                  "the value #{inspect(el)} does not match the inferred series dtype #{inspect(type)}"

          true ->
            new_type
        end
      end)

    type || :float
  end

  defp type(item, type) when is_integer(item) and type == :float, do: :numeric
  defp type(item, type) when is_float(item) and type == :integer, do: :numeric
  defp type(item, type) when is_number(item) and type == :numeric, do: :numeric

  defp type(item, _type) when is_integer(item), do: :integer
  defp type(item, _type) when is_float(item), do: :float
  defp type(item, _type) when is_boolean(item), do: :boolean
  defp type(item, _type) when is_binary(item), do: :string
  defp type(%Date{} = _item, _type), do: :date
  defp type(%NaiveDateTime{} = _item, _type), do: :datetime
  defp type(item, _type) when is_nil(item), do: nil
  defp type(item, _type), do: raise(ArgumentError, "unsupported datatype: #{inspect(item)}")

  @doc """
  Downcasts lists of mixed numeric types (float and int) to float.
  """
  def cast_numerics(list, type) when type == :numeric do
    data =
      Enum.map(list, fn
        nil -> nil
        item -> item / 1
      end)

    {data, :float}
  end

  def cast_numerics(list, type), do: {list, type}

  @doc """
  Helper for shared behaviour in inspect.
  """
  def to_string(i, _opts) when is_nil(i), do: "nil"
  def to_string(i, _opts) when is_binary(i), do: "\"#{i}\""
  def to_string(i, _opts), do: Kernel.to_string(i)

  @doc """
  Raising helper for when a column is not found.
  """
  def raise_column_not_found!(name, names) do
    raise ArgumentError,
          List.to_string(["could not find column name \"#{name}\"" | did_you_mean(name, names)])
  end

  @threshold 0.77
  @max_suggestions 5
  defp did_you_mean(missing_key, available_keys) do
    suggestions =
      for key <- available_keys,
          distance = String.jaro_distance(missing_key, key),
          distance >= @threshold,
          do: {distance, key}

    case suggestions do
      [] -> []
      suggestions -> [". Did you mean:\n\n" | format_suggestions(suggestions)]
    end
  end

  defp format_suggestions(suggestions) do
    suggestions
    |> Enum.sort(&(elem(&1, 0) >= elem(&2, 0)))
    |> Enum.take(@max_suggestions)
    |> Enum.sort(&(elem(&1, 1) <= elem(&2, 1)))
    |> Enum.map(fn {_, key} -> ["      * ", inspect(key), ?\n] end)
  end
end
