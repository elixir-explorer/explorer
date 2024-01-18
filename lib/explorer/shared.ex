defmodule Explorer.Shared do
  # A collection of **private** helpers shared in Explorer.
  @moduledoc false

  @integer_types [
    {:s, 8},
    {:s, 16},
    {:s, 32},
    {:s, 64},
    {:u, 8},
    {:u, 16},
    {:u, 32},
    {:u, 64}
  ]

  @scalar_types @integer_types ++
                  [
                    :null,
                    :binary,
                    :boolean,
                    :category,
                    :date,
                    {:f, 32},
                    {:f, 64},
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
  def normalise_dtype(dtype) when dtype in [:integer, :s64], do: {:s, 64}
  def normalise_dtype(:f32), do: {:f, 32}
  def normalise_dtype(:s8), do: {:s, 8}
  def normalise_dtype(:s16), do: {:s, 16}
  def normalise_dtype(:s32), do: {:s, 32}
  def normalise_dtype(:u8), do: {:u, 8}
  def normalise_dtype(:u16), do: {:u, 16}
  def normalise_dtype(:u32), do: {:u, 32}
  def normalise_dtype(:u64), do: {:u, 64}
  def normalise_dtype({:s, -64}), do: {:s, 64}
  def normalise_dtype(_dtype), do: nil

  @doc """
  Normalise a given dtype, but raise error in case it's invalid.
  """
  def normalise_dtype!(dtype) do
    if maybe_dtype = normalise_dtype(dtype) do
      maybe_dtype
    else
      raise ArgumentError,
            "unsupported dtype #{inspect(dtype)}, expected one of #{inspect_dtypes(dtypes())}"
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
  def signed_integer_types, do: [{:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}]

  @doc """
  Supported unsigned integer dtypes.
  """
  def unsigned_integer_types, do: [{:u, 8}, {:u, 16}, {:u, 32}, {:u, 64}]

  @doc """
  All integer dtypes.
  """
  def integer_types, do: signed_integer_types() ++ unsigned_integer_types()

  @doc """
  Both integer and float dtypes.
  """
  def numeric_types, do: float_types() ++ integer_types()

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
            List.to_string(
              [
                "could not find column name \"#{name}\"" | did_you_mean(name, df.names)
              ] ++ ["\nIf you are attempting to interpolate a value, use ^#{name}.\n"]
            )
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
  """
  def dtype_from_list!(list) do
    list
    |> Enum.reduce(:null, &infer_type/2)
    |> normalise_dtype!()
  end

  @doc """
  Gets the dtype from the list according to the preferred type.

  The dtype from the list is first computed standalone.
  If the list can be directly instantiated as the preferred type,
  then the preferred type is returned. Otherwise, the inferred type
  is returned and a cast call to the preferred type is necessary.

  If no preferred type is given (nil), then the inferred type is returned.
  """
  def dtype_from_list!(_list, :null), do: :null

  def dtype_from_list!(list, nil), do: dtype_from_list!(list)

  def dtype_from_list!(list, preferred_type) do
    list
    |> Enum.reduce(:null, &infer_type/2)
    |> merge_preferred!(preferred_type, list)
  end

  @non_finite [:nan, :infinity, :neg_infinity]

  defp infer_type(nil, type), do: type
  defp infer_type(item, :null), do: infer_type(item)
  defp infer_type(integer, {:u, 64}) when is_integer(integer) and integer > 0, do: {:u, 64}
  defp infer_type(integer, {:f, 64}) when is_integer(integer), do: {:f, 64}
  defp infer_type(float, {:u, _}) when is_float(float) or float in @non_finite, do: {:f, 64}
  defp infer_type(float, {:s, _}) when is_float(float) or float in @non_finite, do: {:f, 64}
  defp infer_type(list, {:list, type}) when is_list(list), do: infer_list(list, type)
  defp infer_type(%{} = map, {:struct, inner}), do: infer_struct(map, inner)

  defp infer_type(item, type) do
    case {infer_type(item), type} do
      {{:u, 64}, {:s, 64}} ->
        {:u, 64}

      {{:s, m}, {:s, n}} when m < 0 or n < 0 ->
        type

      {type, type} ->
        type

      _ ->
        raise_infer_mismatch!(item, type)
    end
  end

  defp infer_type(%Date{} = _item), do: :date
  defp infer_type(%Time{} = _item), do: :time
  defp infer_type(%NaiveDateTime{} = _item), do: {:datetime, :microsecond}
  defp infer_type(%Explorer.Duration{precision: precision} = _item), do: {:duration, precision}

  defp infer_type(item) when is_integer(item) do
    cond do
      item < 0 -> {:s, -64}
      item > 9_223_372_036_854_775_807 -> {:u, 64}
      true -> {:s, 64}
    end
  end

  defp infer_type(item) when is_float(item) or item in @non_finite, do: {:f, 64}
  defp infer_type(item) when is_boolean(item), do: :boolean
  defp infer_type(item) when is_binary(item), do: :string
  defp infer_type(list) when is_list(list), do: infer_list(list, :null)
  defp infer_type(%{} = map), do: infer_struct(map, nil)
  defp infer_type(item), do: raise(ArgumentError, "unsupported datatype: #{inspect(item)}")

  defp infer_list(list, type) do
    {:list, Enum.reduce(list, type, &infer_type/2)}
  end

  defp infer_struct(%{} = map, types) do
    types =
      for {key, value} <- map, into: %{} do
        key = to_string(key)

        cond do
          types == nil ->
            {key, infer_type(value, :null)}

          type = types[key] ->
            {key, infer_type(value, type)}

          true ->
            raise_infer_mismatch!(map, {:struct, types})
        end
      end

    {:struct, types}
  end

  defp infer_as_uint_type!(nil, type), do: type

  defp infer_as_uint_type!(item, {:u, n} = type) do
    cond do
      item < 0 -> raise_infer_mismatch!(item, type)
      n == 8 and item < 256 -> type
      n == 16 and item < 2 ** 16 -> type
      n == 32 and item < 2 ** 32 -> type
      n == 64 and item < 2 ** 64 -> type
      true -> raise_infer_mismatch!(item, type)
    end
  end

  defp infer_as_int_type!(nil, type), do: type

  defp infer_as_int_type!(item, {:s, n} = type) do
    cond do
      item < -9_223_372_036_854_775_808 or item > 9_223_372_036_854_775_807 ->
        raise_infer_mismatch!(item, type)

      n == 8 and item > -129 and item < 128 ->
        type

      n == 16 and item > -32_769 and item < 32_768 ->
        type

      n == 32 and item > -2_147_483_649 and item < 2_147_483_648 ->
        type

      n == 64 and item > -9_223_372_036_854_775_809 and item < 9_223_372_036_854_775_808 ->
        type

      true ->
        raise_infer_mismatch!(item, type)
    end
  end

  defp merge_preferred!(type, type, _list), do: type
  defp merge_preferred!(:null, type, _list), do: type
  defp merge_preferred!({t, 64}, {:f, _} = type, _list) when t in [:s, :u], do: type

  defp merge_preferred!({:s, _}, {:s, _} = type, list) do
    Enum.reduce(list, type, &infer_as_int_type!/2)
  end

  defp merge_preferred!({:s, _}, {:u, _} = type, list) do
    Enum.reduce(list, type, &infer_as_uint_type!/2)
  end

  defp merge_preferred!({:u, _}, {:s, _} = type, list) do
    Enum.reduce(list, type, &infer_as_int_type!/2)
  end

  defp merge_preferred!({:u, _}, {:u, _} = type, list) do
    Enum.reduce(list, type, &infer_as_uint_type!/2)
  end

  defp merge_preferred!({:f, 64}, {:f, _} = type, _list), do: type
  defp merge_preferred!(:string, type, _list) when type in [:binary, :string, :category], do: type

  defp merge_preferred!({:list, {x, 64} = inferred}, {:list, {y, _} = preferred}, list)
       when x in [:s, :u] or y in [:s, :u] do
    result = Enum.reduce(list, preferred, fn l, acc -> merge_preferred!(inferred, acc, l) end)
    {:list, result}
  end

  defp merge_preferred!({:list, inferred}, {:list, preferred}, list) do
    {:list, merge_preferred!(inferred, preferred, list)}
  end

  defp merge_preferred!({:struct, inferred}, {:struct, preferred}, list) do
    result =
      Map.merge(inferred, preferred, fn k, v1, v2 ->
        v_list = Enum.map(list, & &1[k])
        merge_preferred!(v1, v2, v_list)
      end)

    {:struct, result}
  end

  defp merge_preferred!(inferred, _preferred, _list) do
    inferred
  end

  defp raise_infer_mismatch!(item, type) do
    type = if type == {:s, -64}, do: {:s, 64}, else: type

    raise ArgumentError,
          "the value #{inspect(item)} does not match the inferred dtype #{inspect(type)}"
  end

  @doc """
  Returns the leaf dtype from a {:list, _} dtype, or itself.
  """
  def leaf_dtype({:list, inner_dtype}), do: leaf_dtype(inner_dtype)
  def leaf_dtype(dtype), do: dtype

  @doc """
  Downcasts lists of mixed numeric types (float and int) to float.
  """
  def cast_numerics(list, {:struct, dtypes}) when is_list(list) do
    Enum.map(list, fn item ->
      Map.new(item, fn {field, inner_value} ->
        inner_dtype = Map.fetch!(dtypes, to_string(field))
        [casted_value] = cast_numerics([inner_value], inner_dtype)
        {field, casted_value}
      end)
    end)
  end

  def cast_numerics(list, {:list, inner_dtype}) when is_list(list) do
    Enum.map(list, fn item -> cast_numerics(item, inner_dtype) end)
  end

  def cast_numerics(list, {:f, _}) do
    Enum.map(list, fn
      item when item in [nil, :infinity, :neg_infinity, :nan] or is_float(item) -> item
      item -> item / 1
    end)
  end

  def cast_numerics(list, _), do: list

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
  Helper to inspect dtypes in a sentence.
  """
  def inspect_dtypes(dtypes, opts \\ []) do
    opts = Keyword.validate!(opts, with_prefix: false, backsticks: false)

    inspect_fun =
      if opts[:backsticks] do
        fn item -> "`" <> inspect(item) <> "`" end
      else
        &Kernel.inspect/1
      end

    case dtypes do
      [dtype] ->
        if opts[:with_prefix] do
          "dtype is #{inspect_fun.(dtype)}"
        else
          inspect_fun.(dtype)
        end

      [_ | _] = dtypes ->
        prefix =
          if opts[:with_prefix] do
            "dtypes are "
          else
            ""
          end

        {items, [last]} = Enum.split(Enum.sort(dtypes), -1)

        IO.iodata_to_binary([
          prefix,
          Enum.map_intersperse(items, ", ", inspect_fun),
          " and ",
          inspect_fun.(last)
        ])
    end
  end

  @doc """
  Converts a dtype to a binary type when possible.
  """
  def dtype_to_iotype!(dtype) do
    case dtype do
      {:f, n} when n in [32, 64] -> dtype
      {:s, n} when n in [8, 16, 32, 64] -> dtype
      {:u, n} when n in [8, 16, 32, 64] -> dtype
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
      {:f, n} when n in [32, 64] -> type
      {:s, n} when n in [8, 16, 32, 64] -> type
      {:u, n} when n in [8, 16, 32, 64] -> type
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
      [] -> [". The available columns are: #{inspect(available_keys)}."]
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

  @doc """
  Validate options to be used for the various sorting functions.
  """
  def validate_sort_options!(opts) do
    opts = Keyword.validate!(opts, [:direction, :nils, parallel: true, stable: false])

    direction =
      case Keyword.fetch(opts, :direction) do
        {:ok, d} when d in [:asc, :desc] ->
          d

        :error ->
          :not_provided

        {:ok, x} ->
          raise ArgumentError, "`:direction` must be `:asc` or `:desc`, found: #{inspect(x)}."
      end

    nils =
      case Keyword.fetch(opts, :nils) do
        {:ok, n} when n in [:first, :last] ->
          n

        :error ->
          case direction do
            :asc -> :last
            :desc -> :first
            :not_provided -> :last
          end

        {:ok, x} ->
          raise ArgumentError, "`:nils` must be `:first` or `:last`, found: #{inspect(x)}."
      end

    parallel =
      case Keyword.fetch!(opts, :parallel) do
        b when is_boolean(b) -> b
        x -> raise ArgumentError, "`:parallel` must be `true` or `false`, found: #{inspect(x)}."
      end

    stable =
      case Keyword.fetch!(opts, :stable) do
        b when is_boolean(b) -> b
        x -> raise ArgumentError, "`:stable` must be `true` or `false`, found: #{inspect(x)}."
      end

    descending? = direction == :desc
    maintain_order? = stable == true
    multithreaded? = parallel == true
    nulls_last? = nils == :last

    [descending?, maintain_order?, multithreaded?, nulls_last?]
  end
end
