defmodule Explorer.Shared do
  # A collection of **private** helpers shared in Explorer.
  @moduledoc false

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
  Applies a function with args using the implementation of a dataframe or series.
  """
  def apply_impl(df_or_series, fun, args \\ []) do
    impl = impl!(df_or_series)
    apply(impl, fun, [df_or_series | args])
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
            "#{inspect(struct1)} and #{inspect(struct2)}. You may need to call Explorer.backend_transfer/1 " <>
            "(or Explorer.backend_copy/1) on one or both of them to transfer them to a common implementation"
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
  Updates the names and dtypes map of a df in memory.

  The intention is to provide a "out_df" with updated names and dtypes map.
  Note that columns must be a subset of column names in the DF. No new columns
  will be added.
  """
  def update_names_and_dtypes(df, columns_to_keep) do
    %{df | names: columns_to_keep, dtypes: Map.take(df.dtypes, columns_to_keep)}
  end
end
