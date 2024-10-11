defmodule Explorer.Shared do
  # A collection of **private** helpers shared in Explorer.
  @moduledoc false

  require Logger

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

  @precisions [:millisecond, :microsecond, :nanosecond]

  @precision_types for d <- [:naive_datetime, :duration],
                       p <- @precisions,
                       do: {d, p}

  @scalar_types @integer_types ++
                  @precision_types ++
                  [
                    :null,
                    :binary,
                    :boolean,
                    :category,
                    :date,
                    {:f, 32},
                    {:f, 64},
                    :string,
                    :time
                  ]

  @doc """
  All supported dtypes.

  This list excludes recursive dtypes, such as lists
  within lists inside.
  """
  def dtypes do
    @scalar_types ++
      [{:list, :any}, {:struct, :any}, {:decimal, :pos_integer, :pos_integer}]
  end

  @doc """
  Normalise a given dtype and return nil if is invalid.
  """
  def normalise_dtype({:list, inner}) do
    if maybe_dtype = normalise_dtype(inner), do: {:list, maybe_dtype}
  end

  def normalise_dtype({:struct, inner_types}) do
    inner_types
    |> Enum.reduce_while([], fn {key, dtype}, normalized_dtypes ->
      case normalise_dtype(dtype) do
        nil ->
          {:halt, nil}

        dtype ->
          key = to_string(key)
          {:cont, List.keystore(normalized_dtypes, key, 0, {key, dtype})}
      end
    end)
    |> then(fn
      nil ->
        nil

      normalized_dtypes ->
        {:struct,
         if(is_map(inner_types), do: Enum.sort(normalized_dtypes), else: normalized_dtypes)}
    end)
  end

  def normalise_dtype({:datetime, p, tz} = dtype) when p in @precisions and is_binary(tz),
    do: dtype

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

  def normalise_dtype({:datetime, precision}) do
    :ok =
      Logger.warning("""
      The `{:datetime, _}` dtype has been deprecated.
      Please use `{:naive_datetime, _}` instead.
      """)

    {:naive_datetime, precision}
  end

  def normalise_dtype({:decimal, precision, scale} = dtype)
      when is_integer(scale) and (is_nil(precision) or is_integer(precision)),
      do: dtype

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
  Supported naive datetime dtypes.
  """
  def naive_datetime_types, do: for(p <- @precisions, do: {:naive_datetime, p})

  @doc """
  Supported duration dtypes.
  """
  def duration_types, do: for(p <- @precisions, do: {:duration, p})

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
  def to_existing_columns(df, columns, raise? \\ true)

  def to_existing_columns(df, columns, raise?) when is_list(columns) do
    {columns, _cache} =
      Enum.map_reduce(columns, nil, fn
        column, maybe_map when is_integer(column) ->
          map = maybe_map || column_index_map(df.names)
          existing_column = fetch_column_at!(map, column)
          {existing_column, map}

        column, maybe_map when is_atom(column) ->
          column = Atom.to_string(column)
          maybe_raise_column_not_found(df, column, raise?)
          {column, maybe_map}

        column, maybe_map when is_binary(column) ->
          maybe_raise_column_not_found(df, column, raise?)
          {column, maybe_map}
      end)

    columns
  end

  def to_existing_columns(%{names: names}, .., _raise?) do
    names
  end

  def to_existing_columns(%{names: names}, %Range{} = columns, _raise?) do
    Enum.slice(names, columns)
  end

  def to_existing_columns(%{names: names}, %Regex{} = columns, _raise?) do
    Enum.filter(names, &Regex.match?(columns, &1))
  end

  def to_existing_columns(%{names: names}, callback, _raise?) when is_function(callback, 1) do
    Enum.filter(names, callback)
  end

  def to_existing_columns(%{names: names, dtypes: dtypes}, callback, _raise?)
      when is_function(callback, 2) do
    Enum.filter(names, fn name -> callback.(name, dtypes[name]) end)
  end

  def to_existing_columns(_, other, _raise?) do
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
  def maybe_raise_column_not_found(df, name, raise? \\ true) do
    if raise? and Map.has_key?(df.dtypes, name) == false do
      raise ArgumentError,
            List.to_string(
              [
                "could not find column name \"#{name}\"" | did_you_mean(name, df.names)
              ] ++ ["\nIf you are attempting to interpolate a value, use ^#{name}.\n"]
            )
    end
  end

  @doc """
  Gets the `dtype` of a list or raise error if not possible.
  """
  def dtype_from_list!(list) do
    Enum.reduce(list, :null, &infer_type/2)
  end

  @doc """
  Gets the dtype from the list according to the preferred type.

  The dtype from the list is first computed standalone.
  If the list can be directly instantiated as the preferred type,
  then the preferred type is returned. Otherwise, the inferred type
  is returned and a cast call to the preferred type is necessary.

  If no preferred type is given (nil), then the inferred type is returned.
  """
  def dtype_from_list!(list, nil), do: dtype_from_list!(list)
  def dtype_from_list!(_list, preferred_type), do: preferred_type

  @non_finite [:nan, :infinity, :neg_infinity]

  defp infer_type(nil, type), do: type
  defp infer_type(item, :null), do: infer_type(item)
  defp infer_type(integer, {:f, 64}) when is_integer(integer), do: {:f, 64}
  defp infer_type(float, {:s, 64}) when is_float(float) or float in @non_finite, do: {:f, 64}

  defp infer_type(integer, {:decimal, _, _} = decimal) when is_integer(integer), do: decimal

  defp infer_type(float, {:decimal, _, _} = decimal) when is_float(float),
    do: infer_type(Decimal.from_float(float), decimal)

  defp infer_type(list, {:list, type}) when is_list(list), do: infer_list(list, type)
  defp infer_type(%{} = map, {:struct, inner}), do: infer_struct(map, inner)

  defp infer_type(%Decimal{} = item, {:decimal, precision, scale}) do
    {:decimal, precision, max(Decimal.scale(item), scale)}
  end

  defp infer_type(item, type) do
    if infer_type(item) == type do
      type
    else
      raise ArgumentError,
            "the value #{inspect(item)} does not match the inferred dtype #{inspect(type)}"
    end
  end

  defp infer_type(%Date{} = _item), do: :date
  defp infer_type(%Time{} = _item), do: :time
  defp infer_type(%DateTime{time_zone: tz} = _item), do: {:datetime, :microsecond, tz}
  defp infer_type(%NaiveDateTime{} = _item), do: {:naive_datetime, :microsecond}
  defp infer_type(%Explorer.Duration{precision: precision} = _item), do: {:duration, precision}
  defp infer_type(%Decimal{} = item), do: {:decimal, 38, Decimal.scale(item)}
  defp infer_type(%_{} = item), do: raise(ArgumentError, "unsupported datatype: #{inspect(item)}")
  defp infer_type(item) when is_integer(item), do: {:s, 64}
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
      for {key, value} <- map do
        key = to_string(key)

        cond do
          types == nil ->
            {key, infer_type(value, :null)}

          result = List.keyfind(types, key, 0) ->
            {^key, type} = result
            {key, infer_type(value, type)}

          true ->
            raise ArgumentError,
                  "the value #{inspect(map)} does not match the inferred dtype #{inspect({:struct, types})}"
        end
      end

    {:struct, Enum.sort(types)}
  end

  @doc """
  Returns the leaf dtype from a {:list, _} dtype, or itself.
  """
  def leaf_dtype({:list, inner_dtype}), do: leaf_dtype(inner_dtype)
  def leaf_dtype(dtype), do: dtype

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

  def merge_numeric_dtype({:decimal, _, _} = decimal, :null), do: decimal
  def merge_numeric_dtype(:null, {:decimal, _, _} = decimal), do: decimal

  # For now, float has priority over decimals due to Polars.
  def merge_numeric_dtype({:decimal, _, _}, {:f, _} = float), do: float
  def merge_numeric_dtype({:f, _} = float, {:decimal, _, _}), do: float

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
      {:naive_datetime, _} -> {:s, 64}
      {:datetime, _, _} -> {:s, 64}
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
  def dtype_to_string({:naive_datetime, p}), do: "naive_datetime[#{precision_string(p)}]"
  def dtype_to_string({:datetime, p, tz}), do: "datetime[#{precision_string(p)}, #{tz}]"
  def dtype_to_string({:duration, p}), do: "duration[#{precision_string(p)}]"
  def dtype_to_string({:list, dtype}), do: "list[" <> dtype_to_string(dtype) <> "]"
  def dtype_to_string({:struct, fields}), do: "struct[#{length(fields)}]"
  def dtype_to_string({:f, size}), do: "f" <> Integer.to_string(size)
  def dtype_to_string({:s, size}), do: "s" <> Integer.to_string(size)
  def dtype_to_string({:u, size}), do: "u" <> Integer.to_string(size)
  def dtype_to_string({:decimal, precision, scale}), do: "decimal[#{precision || "*"}, #{scale}]"
  def dtype_to_string(other) when is_atom(other), do: Atom.to_string(other)

  defp precision_string(:millisecond), do: "ms"
  defp precision_string(:microsecond), do: "μs"
  defp precision_string(:nanosecond), do: "ns"

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

  ## Apply

  @doc """
  Initializes a series or a dataframe with node placement.
  """
  def apply_init(impl, fun, args, opts) do
    if node = opts[:node] do
      Explorer.Remote.apply(node, impl, fun, [], fn _ -> args end, true)
    else
      apply(impl, fun, args)
    end
  end

  @doc """
  Applies a function with args using the implementation of a dataframe or series.
  """
  def apply_dataframe(dfs_or_df, fun, args \\ [], place? \\ true) do
    {df_nodes, {impl, remote}} =
      dfs_or_df
      |> List.wrap()
      |> Enum.map_reduce({nil, nil}, fn %{data: %impl{}} = df, {acc_impl, acc_remote} ->
        {node, remote} = remote_info(df, impl)
        {{df, node}, {pick_df_impl(acc_impl, impl), pick_remote(acc_remote, remote)}}
      end)

    if remote do
      callback = if is_list(dfs_or_df), do: &[&1 | args], else: &[hd(&1) | args]
      Explorer.Remote.apply(remote, impl, fun, df_nodes, callback, place?)
    else
      apply(impl, fun, [dfs_or_df | args])
    end
  end

  defp pick_df_impl(nil, struct), do: struct
  defp pick_df_impl(struct, struct), do: struct

  defp pick_df_impl(struct1, struct2) do
    raise "cannot invoke Explorer.DataFrame function because it relies on two incompatible implementations: " <>
            "#{inspect(struct1)} and #{inspect(struct2)}"
  end

  @doc """
  Applies a function to a series.
  """
  def apply_series(series, fun, args \\ [], place? \\ true) do
    apply_series_impl!([series], fun, &(&1 ++ args), place?)
  end

  @doc """
  Applies a function to a list of series.

  The list is typically static and it is passed as arguments.
  """
  def apply_series_list(fun, series_or_scalars) when is_list(series_or_scalars) do
    apply_series_impl!(series_or_scalars, fun, & &1, true)
  end

  @doc """
  Applies a function to a list of unknown size of series.

  The list is passed as a varargs to the backend.
  """
  def apply_series_varargs(fun, series_or_scalars) when is_list(series_or_scalars) do
    apply_series_impl!(series_or_scalars, fun, &[&1], true)
  end

  defp apply_series_impl!([_ | _] = series_or_scalars, fun, args_callback, place?) do
    {series_nodes, {impl, {_forced?, remote}, transfer?}} =
      Enum.map_reduce(series_or_scalars, {nil, {false, nil}, false}, fn
        # A lazy series without a resource is treated as a scalar
        %{data: %{__struct__: Explorer.Backend.LazySeries, resource: nil}} = series,
        {_acc_impl, acc_remote, acc_transfer?} ->
          {{series, nil}, {Explorer.Backend.LazySeries, acc_remote, acc_transfer?}}

        %{data: %impl{}} = series, {acc_impl, acc_remote, acc_transfer?} ->
          {node, remote} = remote_info(series, impl)

          {{series, node},
           {pick_series_impl(acc_impl, impl), pick_series_remote(acc_remote, impl, remote),
            acc_transfer? or remote != nil}}

        scalar, acc ->
          {{scalar, nil}, acc}
      end)

    if is_nil(impl) do
      raise ArgumentError,
            "expected a series as argument for #{fun}, got: #{inspect(series_or_scalars)}" <>
              maybe_bad_column_hint(series_or_scalars)
    end

    if transfer? do
      Explorer.Remote.apply(remote || node(), impl, fun, series_nodes, args_callback, place?)
    else
      apply(impl, fun, args_callback.(series_or_scalars))
    end
  end

  # The lazy series always wins the remote if it has one,
  # since we need to transfer it to the dataframe that started the lazy series.
  defp pick_series_remote(_acc_remote, Explorer.Backend.LazySeries, remote),
    do: {true, remote}

  defp pick_series_remote({true, acc_remote}, _impl, _remote),
    do: {true, acc_remote}

  defp pick_series_remote({false, acc_remote}, _impl, remote),
    do: {false, pick_remote(acc_remote, remote)}

  defp pick_series_impl(nil, impl), do: impl
  defp pick_series_impl(Explorer.Backend.LazySeries, _impl), do: Explorer.Backend.LazySeries
  defp pick_series_impl(_impl, Explorer.Backend.LazySeries), do: Explorer.Backend.LazySeries
  defp pick_series_impl(impl, impl), do: impl

  defp pick_series_impl(acc_impl, impl) do
    raise "cannot invoke Explorer function because it relies on two incompatible series: " <>
            "#{inspect(acc_impl)} and #{inspect(impl)}"
  end

  @doc """
  A hint in case there is a bad column from a query.
  """
  def maybe_bad_column_hint(values) do
    atom = Enum.find(values, &is_atom(&1))

    if Kernel.and(atom != nil, String.starts_with?(Atom.to_string(atom), "Elixir.")) do
      "\n\nHINT: we have noticed that one of the values is the atom #{inspect(atom)}. " <>
        "If you are inside Explorer.Query and you want to access a column starting in uppercase, " <>
        "you must write instead: col(\"#{inspect(atom)}\")"
    else
      ""
    end
  end

  # There is remote information and it is GCed by the current node.
  # Which means that it resides on the remote node given by remote_pid.
  defp remote_info(%{remote: {local_gc, remote_pid, _remote_ref}}, _impl)
       when node(local_gc) == node(),
       do: {node(remote_pid), remote_pid}

  # There is remote information but it is actually local. Treat it as one.
  defp remote_info(%{remote: {_local_gc, remote_pid, _remote_ref}}, _impl)
       when node(remote_pid) == node(),
       do: {node(), nil}

  # We don't know the remote information, let's ask the backend
  defp remote_info(df_or_series, impl) do
    case impl.owner_reference(df_or_series) do
      remote_ref when is_reference(remote_ref) and node(remote_ref) != node() ->
        {node(remote_ref), node(remote_ref)}

      _ ->
        {node(), nil}
    end
  end

  # We need to pick a remote, which one does not matter.
  # The important is to keep the PID reference around, if there is one.
  defp pick_remote(acc_remote, _remote) when is_pid(acc_remote), do: acc_remote
  defp pick_remote(acc_remote, nil), do: acc_remote
  defp pick_remote(_acc_remote, remote), do: remote
end
