defmodule Explorer.Shared do
  # A collection of **private** helpers shared in Explorer.
  @moduledoc false

  @scalar_types [
    :binary,
    :boolean,
    :category,
    :date,
    {:f, 32},
    {:f, 64},
    {:s, 8},
    {:s, 16},
    {:s, 32},
    {:s, 64},
    {:u, 8},
    {:u, 16},
    {:u, 32},
    {:u, 64},
    # TODO: remove this integer
    :integer,
    :string,
    :time,
    {:datetime, :microsecond},
    {:datetime, :millisecond},
    {:datetime, :nanosecond},
    {:duration, :microsecond},
    {:duration, :millisecond},
    {:duration, :nanosecond}
  ]

  @doc """
  All supported dtypes.

  This list excludes recursive dtypes, such as lists
  within lists inside.
  """
  def dtypes do
    @scalar_types ++ [{:list, :any}, {:struct, :any}]
  end

  @doc """
  Normalise a given dtype and return nil if is invalid.
  """
  def normalise_dtype({:list, inner}) do
    if maybe_dtype = normalise_dtype(inner), do: {:list, maybe_dtype}
  end

  def normalise_dtype({:struct, inner_types}) do
    inner_types
    |> Enum.reduce_while(%{}, fn {key, dtype}, normalized_dtypes ->
      case normalise_dtype(dtype) do
        nil -> {:halt, nil}
        dtype -> {:cont, Map.put(normalized_dtypes, key, dtype)}
      end
    end)
    |> then(fn
      nil -> nil
      normalized_dtypes -> {:struct, normalized_dtypes}
    end)
  end

  def normalise_dtype(dtype) when dtype in @scalar_types, do: dtype
  def normalise_dtype(dtype) when dtype in [:float, :f64], do: {:f, 64}
  def normalise_dtype(dtype) when dtype in [:integer, :i64], do: {:s, 64}
  def normalise_dtype(:f32), do: {:f, 32}
  def normalise_dtype(:i8), do: {:s, 8}
  def normalise_dtype(:i16), do: {:s, 16}
  def normalise_dtype(:i32), do: {:s, 32}
  def normalise_dtype(:u8), do: {:u, 8}
  def normalise_dtype(:u16), do: {:u, 16}
  def normalise_dtype(:u32), do: {:u, 32}
  def normalise_dtype(:u64), do: {:u, 64}
  def normalise_dtype(_dtype), do: nil

  @doc """
  Normalise a given dtype, but raise error in case it's invalid.
  """
  def normalise_dtype!(dtype) do
    if maybe_dtype = normalise_dtype(dtype) do
      maybe_dtype
    else
      raise ArgumentError,
            "unsupported dtype #{inspect(dtype)}, expected one of #{inspect(dtypes())}"
    end
  end

  @doc """
  Supported datetime dtypes.
  """
  def datetime_types,
    do: [{:datetime, :nanosecond}, {:datetime, :microsecond}, {:datetime, :millisecond}]

  @doc """
  Supported duration dtypes.
  """
  def duration_types,
    do: [{:duration, :nanosecond}, {:duration, :microsecond}, {:duration, :millisecond}]

  @doc """
  Supported float dtypes.
  """
  def float_types, do: [{:f, 32}, {:f, 64}]

  @doc """
  Supported signed integer dtypes.
  """
  def signed_integer_types, do: [{:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, :integer]

  @doc """
  Supported unsigned integer dtypes.
  """
  def unsigned_integer_types, do: [{:u, 8}, {:u, 16}, {:u, 32}, {:u, 64}]

  @doc """
  All integer dtypes.
  """
  def integer_types, do: signed_integer_types() ++ unsigned_integer_types()

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
  Normalize column names and raise if column does not exist.
  """
  def to_existing_columns(df, columns) when is_list(columns) do
    {columns, _cache} =
      Enum.map_reduce(columns, nil, fn
        column, maybe_map when is_integer(column) ->
          map = maybe_map || column_index_map(df.names)
          existing_column = fetch_column_at!(map, column)
          {existing_column, map}

        column, maybe_map when is_atom(column) ->
          column = Atom.to_string(column)
          maybe_raise_column_not_found(df, column)
          {column, maybe_map}

        column, maybe_map when is_binary(column) ->
          maybe_raise_column_not_found(df, column)
          {column, maybe_map}
      end)

    columns
  end

  def to_existing_columns(%{names: names}, ..) do
    names
  end

  def to_existing_columns(%{names: names}, %Range{} = columns) do
    Enum.slice(names, columns)
  end

  def to_existing_columns(%{names: names}, %Regex{} = columns) do
    Enum.filter(names, &Regex.match?(columns, &1))
  end

  def to_existing_columns(%{names: names}, callback) when is_function(callback, 1) do
    Enum.filter(names, callback)
  end

  def to_existing_columns(%{names: names, dtypes: dtypes}, callback)
      when is_function(callback, 2) do
    Enum.filter(names, fn name -> callback.(name, dtypes[name]) end)
  end

  def to_existing_columns(_, other) do
    raise ArgumentError, """
    invalid columns specification. Columns may be specified as one of:

      * a list of columns indexes or names as atoms and strings

      * a range

      * a regex that keeps only the names matching the regex

      * a one-arity function that receives column names and returns
        true for column names to keep

      * a two-arity function that receives column names and types and
        returns true for column names to keep

    Got: #{inspect(other)}
    """
  end

  defp fetch_column_at!(map, index) do
    normalized = if index < 0, do: index + map_size(map), else: index

    case map do
      %{^normalized => column} -> column
      %{} -> raise ArgumentError, "no column exists at index #{index}"
    end
  end

  defp column_index_map(names),
    do: for({name, idx} <- Enum.with_index(names), into: %{}, do: {idx, name})

  @doc """
  Raises if a column is not found.
  """
  def maybe_raise_column_not_found(df, name) do
    unless Map.has_key?(df.dtypes, name) do
      raise ArgumentError,
            List.to_string([
              "could not find column name \"#{name}\"" | did_you_mean(name, df.names)
            ])
    end
  end

  @doc """
  Applies a function with args using the implementation of a dataframe or series.
  """
  def apply_impl(df_or_series_or_list, fun, args \\ []) do
    impl = impl!(df_or_series_or_list)
    apply(impl, fun, [df_or_series_or_list | args])
  end

  defp impl!(%{data: %struct{}}), do: struct

  defp impl!([%{data: %first_struct{}} | _] = dfs) when is_list(dfs),
    do: Enum.reduce(dfs, first_struct, fn %{data: %struct{}}, acc -> pick_impl(acc, struct) end)

  defp pick_impl(struct, struct), do: struct

  defp pick_impl(struct1, struct2) do
    raise "cannot invoke Explorer function because it relies on two incompatible implementations: " <>
            "#{inspect(struct1)} and #{inspect(struct2)}"
  end

  @doc """
  Gets the `dtype` of a list or raise error if not possible.

  It's possible to override the initial type by passing a preferable type.
  This is useful in cases where you want to build the series in a target type,
  without the need to cast it later.
  """
  def dtype_from_list!(list, preferable_type \\ nil) do
    initial_type =
      if leaf_dtype(preferable_type) in [
           :numeric,
           :binary,
           {:f, 32},
           {:f, 64},
           {:s, 8},
           {:s, 16},
           {:s, 32},
           {:s, 64},
           {:u, 8},
           {:u, 16},
           {:u, 32},
           {:u, 64},
           :integer,
           :category
         ],
         do: preferable_type

    type =
      Enum.reduce(list, initial_type, fn el, type ->
        new_type = type(el, type) || type

        if new_type_matches?(type, new_type) do
          new_type
        else
          raise ArgumentError,
                "the value #{inspect(el)} does not match the inferred series dtype #{inspect(type)}"
        end
      end)

    type || preferable_type || {:f, 64}
  end

  defp type(%Date{} = _item, _type), do: :date
  defp type(%Time{} = _item, _type), do: :time
  defp type(%NaiveDateTime{} = _item, _type), do: {:datetime, :microsecond}
  defp type(%Explorer.Duration{precision: precision} = _item, _type), do: {:duration, precision}

  defp type(item, type) when is_integer(item) and type in [{:f, 32}, {:f, 64}], do: :numeric
  defp type(item, type) when is_float(item) and type == :integer, do: :numeric
  defp type(item, type) when is_number(item) and type == :numeric, do: :numeric

  defp type(item, type)
       when item in [:nan, :infinity, :neg_infinity] and
              type in [:integer, {:f, 32}, {:f, 64}, :numeric],
       do: :numeric

  defp type(item, {:s, _} = integer_type) when is_integer(item), do: integer_type
  defp type(item, {:u, _} = integer_type) when is_integer(item) and item >= 0, do: integer_type
  defp type(item, _type) when is_integer(item), do: :integer
  defp type(item, {:f, _} = float_dtype) when is_float(item), do: float_dtype
  defp type(item, _type) when is_float(item), do: {:f, 64}
  defp type(item, _type) when item in [:nan, :infinity, :neg_infinity], do: {:f, 64}
  defp type(item, _type) when is_boolean(item), do: :boolean

  defp type(item, :binary) when is_binary(item), do: :binary
  defp type(item, :category) when is_binary(item), do: :category
  defp type(item, _type) when is_binary(item), do: :string

  defp type(item, _type) when is_nil(item), do: nil
  defp type([], _type), do: nil
  defp type([_item | _] = items, type), do: {:list, result_list_type(items, type)}

  defp type(%{} = item, type) do
    preferable_inner_types =
      case type do
        {:struct, %{} = inner_types} -> inner_types
        _ -> %{}
      end

    inferred_inner_types =
      for {key, value} <- item, into: %{} do
        key = to_string(key)
        inner_type = Map.get(preferable_inner_types, key)

        {key, type(value, inner_type) || Map.get(preferable_inner_types, key)}
      end

    {:struct, inferred_inner_types}
  end

  defp type(item, _type), do: raise(ArgumentError, "unsupported datatype: #{inspect(item)}")

  defp result_list_type(nil, _type), do: nil
  defp result_list_type([], _type), do: nil

  defp result_list_type([h | _tail] = items, type) when is_list(h) do
    # Enum.flat_map/2 is used here becase we want to remove one level of nesting per iteraction.
    {:list, result_list_type(Enum.flat_map(items, & &1), type)}
  end

  defp result_list_type(items, type) when is_list(items) do
    dtype_from_list!(items, leaf_dtype(type))
  end

  defp new_type_matches?(type, new_type)

  defp new_type_matches?(type, type), do: true

  defp new_type_matches?(nil, _new_type), do: true

  defp new_type_matches?({:struct, types}, {:struct, new_types}) do
    Enum.all?(types, fn {key, type} ->
      case Map.fetch(new_types, key) do
        {:ok, new_type} -> new_type_matches?(type, new_type)
        :error -> false
      end
    end)
  end

  defp new_type_matches?(type, new_type) do
    leaf_dtype(new_type) == :numeric and leaf_dtype(type) in [:integer, {:f, 32}, {:f, 64}]
  end

  @doc """
  Returns the leaf dtype from a {:list, _} dtype, or itself.
  """
  def leaf_dtype({:list, inner_dtype}), do: leaf_dtype(inner_dtype)
  def leaf_dtype(dtype), do: dtype

  @doc """
  Downcasts lists of mixed numeric types (float and int) to float.
  """
  def cast_numerics(list, dtype) do
    {cast_numerics_deep(list, dtype), cast_numeric_dtype_to_float(dtype)}
  end

  defp cast_numerics_deep(list, {:struct, dtypes}) when is_list(list) do
    Enum.map(list, fn item ->
      Map.new(item, fn {field, inner_value} ->
        inner_dtype = Map.fetch!(dtypes, to_string(field))
        [casted_value] = cast_numerics_deep([inner_value], inner_dtype)

        {field, casted_value}
      end)
    end)
  end

  defp cast_numerics_deep(list, {:list, inner_dtype}) when is_list(list) do
    Enum.map(list, fn item -> cast_numerics_deep(item, inner_dtype) end)
  end

  defp cast_numerics_deep(list, :numeric), do: cast_numerics_to_floats(list)

  defp cast_numerics_deep(list, _), do: list

  defp cast_numerics_to_floats(list) do
    Enum.map(list, fn
      item when item in [nil, :infinity, :neg_infinity, :nan] or is_float(item) -> item
      item -> item / 1
    end)
  end

  defp cast_numeric_dtype_to_float({:struct, dtypes}),
    do: {:struct, Map.new(dtypes, fn {f, inner} -> {f, cast_numeric_dtype_to_float(inner)} end)}

  defp cast_numeric_dtype_to_float({:list, inner}),
    do: {:list, cast_numeric_dtype_to_float(inner)}

  defp cast_numeric_dtype_to_float(:numeric), do: {:f, 64}
  defp cast_numeric_dtype_to_float(other), do: other

  @doc """
  Helper for shared behaviour in inspect.
  """
  def to_doc(item, opts) when is_list(item) do
    open = Inspect.Algebra.color("[", :list, opts)
    close = Inspect.Algebra.color("]", :list, opts)
    Inspect.Algebra.container_doc(open, item, close, opts, &to_doc/2)
  end

  def to_doc(item, opts) when is_map(item) and not is_struct(item) do
    open = Inspect.Algebra.color("%{", :map, opts)
    close = Inspect.Algebra.color("}", :map, opts)
    arrow = Inspect.Algebra.color(" => ", :map, opts)

    Inspect.Algebra.container_doc(open, Enum.to_list(item), close, opts, fn {key, value}, opts ->
      Inspect.Algebra.concat([
        Inspect.Algebra.color(inspect(key), :string, opts),
        arrow,
        to_doc(value, opts)
      ])
    end)
  end

  def to_doc(item, _opts) do
    case item do
      nil -> "nil"
      :nan -> "NaN"
      :infinity -> "Inf"
      :neg_infinity -> "-Inf"
      i when is_binary(i) -> inspect(i)
      _ -> Kernel.to_string(item)
    end
  end

  @doc """
  Converts a dtype to a binary type when possible.
  """
  def dtype_to_iotype!(dtype) do
    case dtype do
      {:f, _n} -> dtype
      :integer -> {:s, 64}
      :boolean -> {:u, 8}
      :date -> {:s, 32}
      :time -> {:s, 64}
      {:datetime, _} -> {:s, 64}
      {:duration, _} -> {:s, 64}
      _ -> raise ArgumentError, "cannot convert dtype #{dtype} into a binary/tensor type"
    end
  end

  @doc """
  Converts a binary type to dtype.
  """
  def iotype_to_dtype!(type) do
    case type do
      {:f, _} -> type
      {:s, 64} -> :integer
      {:u, 8} -> :boolean
      {:s, 32} -> :date
      _ -> raise ArgumentError, "cannot convert binary/tensor type #{inspect(type)} into dtype"
    end
  end

  @doc """
  Converts dtype to its string representation.
  """
  def dtype_to_string({:datetime, :millisecond}), do: "datetime[ms]"
  def dtype_to_string({:datetime, :microsecond}), do: "datetime[μs]"
  def dtype_to_string({:datetime, :nanosecond}), do: "datetime[ns]"
  def dtype_to_string({:duration, :millisecond}), do: "duration[ms]"
  def dtype_to_string({:duration, :microsecond}), do: "duration[μs]"
  def dtype_to_string({:duration, :nanosecond}), do: "duration[ns]"
  def dtype_to_string({:list, dtype}), do: "list[" <> dtype_to_string(dtype) <> "]"
  def dtype_to_string({:struct, fields}), do: "struct[#{map_size(fields)}]"
  def dtype_to_string({:f, size}), do: "f" <> Integer.to_string(size)
  def dtype_to_string({:s, size}), do: "s" <> Integer.to_string(size)
  def dtype_to_string({:u, size}), do: "u" <> Integer.to_string(size)
  def dtype_to_string(other) when is_atom(other), do: Atom.to_string(other)

  @threshold 0.77
  @max_suggestions 5

  @doc """
  Provides did_you_mean suggestions based on keys.
  """
  def did_you_mean(missing_key, available_keys) do
    suggestions =
      for key <- available_keys,
          distance = String.jaro_distance(missing_key, key),
          distance >= @threshold,
          do: {distance, key}

    case suggestions do
      [] -> [". The available entries are: #{inspect(available_keys)}"]
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
