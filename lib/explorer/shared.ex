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
    dtype_from_list!(list, nil, false)
  end

  @doc """
  Gets the dtype from the list according to the preferred type.

  The dtype from the list is first computed standalone.
  If the list can be directly instantiated as the preferred type,
  then the preferred type is returned. Otherwise, the inferred type
  is returned and a cast call to the preferred type is necessary.

  If no preferred type is given (nil), then the inferred type is returned.
  """
  def dtype_from_list!(list, preferred_type, strict \\ false)

  def dtype_from_list!(_list, :null, _strict), do: :null

  def dtype_from_list!(list, nil, strict) do
    list
    |> Enum.reduce(:null, &infer_type(&1, &2, nil, strict))
    |> normalise_dtype!()
  end

  def dtype_from_list!(list, preferred_type, strict) do
    list
    |> Enum.reduce(:null, &infer_type(&1, &2, preferred_type, strict))
    |> merge_preferred(preferred_type)
  end

  @non_finite [:nan, :infinity, :neg_infinity]

  defp infer_type(nil, type, _preferred, _strict), do: type
  defp infer_type(item, :null, preferred, strict), do: infer_type(item, preferred, strict)

  defp infer_type(integer, {:f, 64}, _preferred, _strict) when is_integer(integer), do: {:f, 64}

  defp infer_type(float, {:s, _}, _preferred, _strict)
       when is_float(float) or float in @non_finite,
       do: {:f, 64}

  defp infer_type(float, {:u, _}, _preferred, _strict)
       when is_float(float) or float in @non_finite,
       do: {:f, 64}

  defp infer_type(list, {:list, type}, preferred, strict) when is_list(list) do
    preferred =
      case preferred do
        {:list, preferred} -> preferred
        _ -> preferred
      end

    infer_list(list, type, preferred, strict)
  end

  defp infer_type(%{} = map, {:struct, inner}, preferred, strict) do
    preferred =
      case preferred do
        {:struct, preferred} -> preferred
        _ -> preferred
      end

    infer_struct(map, inner, preferred, strict)
  end

  defp infer_type(item, type, preferred, strict) do
    if infer_type(item, preferred, strict) == type do
      type
    else
      raise ArgumentError,
            "the value #{inspect(item)} does not match the inferred dtype #{inspect(type)}"
    end
  end

  defp infer_type(%Date{} = _item, _preferred, _strict), do: :date
  defp infer_type(%Time{} = _item, _preferred, _strict), do: :time
  defp infer_type(%NaiveDateTime{} = _item, _preferred, _strict), do: {:datetime, :microsecond}

  defp infer_type(%Explorer.Duration{precision: precision} = _item, _preferred, _strict),
    do: {:duration, precision}

  defp infer_type(item, _, false) when is_integer(item), do: {:s, 64}

  defp infer_type(item, nil, true) when is_integer(item) do
    if item < -9_223_372_036_854_775_808 or item > 9_223_372_036_854_775_807 do
      raise_mismatched_dtype!(item, {:s, 64})
    else
      {:s, 64}
    end
  end

  defp infer_type(item, preferred, true) when is_integer(item) do
    case preferred do
      {:s, 8} when item > -129 and item < 128 ->
        preferred

      {:s, 16} when item > -32_769 and item < 32_768 ->
        preferred

      {:s, 32} when item > -2_147_483_649 and item < 2_147_483_648 ->
        preferred

      {:s, 64} when item > -9_223_372_036_854_775_809 and item < 9_223_372_036_854_775_808 ->
        preferred

      {:s, _} ->
        raise_mismatched_dtype!(item, preferred)

      {:u, _} when item < 0 ->
        raise_mismatched_dtype!(item, preferred)

      {:u, 8} when item < 256 ->
        preferred

      {:u, 16} when item < 65_536 ->
        preferred

      {:u, 32} when item < 4_294_967_296 ->
        preferred

      {:u, 64} when item < 18_446_744_073_709_551_616 ->
        preferred

      {:u, _} ->
        raise_mismatched_dtype!(item, preferred)

      _ ->
        {:s, 64}
    end
  end

  defp infer_type(item, _preferred, _strict) when is_float(item) or item in @non_finite,
    do: {:f, 64}

  defp infer_type(item, _preferred, _strict) when is_boolean(item), do: :boolean
  defp infer_type(item, _preferred, _strict) when is_binary(item), do: :string

  defp infer_type(list, preferred, strict) when is_list(list),
    do: infer_list(list, :null, preferred, strict)

  defp infer_type(%{} = map, preferred, strict), do: infer_struct(map, nil, preferred, strict)

  defp infer_type(item, _preferred, _strict),
    do: raise(ArgumentError, "unsupported datatype: #{inspect(item)}")

  defp infer_list(list, type, preferred, strict) do
    preferred =
      case preferred do
        {:list, preferred} -> preferred
        _ -> preferred
      end

    {:list, Enum.reduce(list, type, &infer_type(&1, &2, preferred, strict))}
  end

  defp infer_struct(%{} = map, types, preferred, strict) do
    types =
      for {key, value} <- map, into: %{} do
        key = to_string(key)

        cond do
          types == nil ->
            {key, infer_type(value, :null, preferred, strict)}

          type = types[key] ->
            {key, infer_type(value, type, preferred, strict)}

          true ->
            raise_mismatched_dtype!(map, {:struct, types})
        end
      end

    {:struct, types}
  end

  defp raise_mismatched_dtype!(value, type) do
    raise ArgumentError,
          "the value #{inspect(value)} does not match the inferred dtype #{inspect(type)}"
  end

  defp merge_preferred(type, type), do: type
  defp merge_preferred(:null, type), do: type
  defp merge_preferred({:s, 64}, {:u, _} = type), do: type
  defp merge_preferred({:s, 64}, {:s, _} = type), do: type
  defp merge_preferred({:s, 64}, {:f, _} = type), do: type
  defp merge_preferred({:f, 64}, {:f, _} = type), do: type
  defp merge_preferred(:string, type) when type in [:binary, :string, :category], do: type

  defp merge_preferred({:list, inferred}, {:list, preferred}) do
    {:list, merge_preferred(inferred, preferred)}
  end

  defp merge_preferred({:struct, inferred}, {:struct, preferred}) do
    {:struct, Map.merge(inferred, preferred, fn _, v1, v2 -> merge_preferred(v1, v2) end)}
  end

  defp merge_preferred(inferred, _preferred) do
    inferred
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
  Merge two dtypes.
  """
  def merge_dtype(dtype, dtype), do: dtype
  def merge_dtype(:null, dtype), do: dtype
  def merge_dtype(dtype, :null), do: dtype
  def merge_dtype(ltype, rtype), do: merge_numeric_dtype(ltype, rtype)

  @doc """
  Merge two numeric dtypes to a valid precision.
  """
  def merge_numeric_dtype({int_type, left}, {int_type, right}) when int_type in [:s, :u],
    do: {int_type, max(left, right)}

  def merge_numeric_dtype({:s, s_size}, {:u, u_size}), do: {:s, max(min(64, u_size * 2), s_size)}
  def merge_numeric_dtype({:u, s_size}, {:s, u_size}), do: {:s, max(min(64, u_size * 2), s_size)}
  def merge_numeric_dtype({int_type, _}, {:f, _} = float) when int_type in [:s, :u], do: float
  def merge_numeric_dtype({:f, _} = float, {int_type, _}) when int_type in [:s, :u], do: float
  def merge_numeric_dtype({:f, left}, {:f, right}), do: {:f, max(left, right)}
  def merge_numeric_dtype({:f, _} = float, :null), do: float
  def merge_numeric_dtype(:null, {:f, _} = float), do: float
  def merge_numeric_dtype(_, _), do: nil

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
  Converts a dtype to an iotype.

  Note this is a subset of Series.iotype/1, given we can convert
  a category series to iotype but we cannot generally convert
  a category dtype to an iotype without the underlying categories.
  """
  def dtype_to_iotype(dtype) do
    case dtype do
      {:f, _} -> dtype
      {:s, _} -> dtype
      {:u, _} -> dtype
      :boolean -> {:u, 8}
      :date -> {:s, 32}
      :time -> {:s, 64}
      {:datetime, _} -> {:s, 64}
      {:duration, _} -> {:s, 64}
      _ -> :none
    end
  end

  @doc """
  Raising version of `dtype_to_iotype/1`.
  """
  def dtype_to_iotype!(dtype) do
    case dtype_to_iotype(dtype) do
      :none -> raise ArgumentError, "cannot convert dtype #{dtype} into a binary/tensor type"
      other -> other
    end
  end

  @doc """
  Converts an iotype to dtype.
  """
  def iotype_to_dtype!(type) do
    case type do
      {:f, _} -> type
      {:s, _} -> type
      {:u, _} -> type
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
