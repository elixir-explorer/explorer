defmodule Explorer.Series do
  @moduledoc """
  The Series struct and API.

  A series can be of the following data types:

    * `:binary` - Binaries (sequences of bytes)
    * `:boolean` - Boolean
    * `:category` - Strings but represented internally as integers
    * `:date` - Date type that unwraps to `Elixir.Date`
    * `{:datetime, precision}` - DateTime type with millisecond/microsecond/nanosecond
      precision that unwraps to `Elixir.NaiveDateTime`
    * `{:duration, precision}` - Duration type with millisecond/microsecond/nanosecond
      precision that unwraps to `Explorer.Duration`
    * `{:f, size}` - a 64-bit or 32-bit floating point number
    * `{:s, size}` - a 8-bit or 16-bit or 32-bit or 64-bit signed integer number.
    * `{:u, size}` - a 8-bit or 16-bit or 32-bit or 64-bit unsigned integer number.
    * `:null` - `nil`s exclusively
    * `:string` - UTF-8 encoded binary
    * `:time` - Time type that unwraps to `Elixir.Time`
    * `{:list, dtype}` - A recursive dtype that can store lists. Examples: `{:list, :boolean}` or
      a nested list dtype like `{:list, {:list, :boolean}}`.
    * `{:struct, [{key, dtype}]}` - A recursive dtype that can store Arrow/Polars structs (not to be
      confused with Elixir's struct). This type unwraps to Elixir maps with string keys. Examples:
      `{:struct, [{"a", :string}]}` or a nested struct dtype like `{:struct, [{"a", {:struct, [{"b", :string}]}}]}`.

  When passing a dtype as argument, aliases are supported for convenience
  and compatibility with the Elixir ecosystem:

    * All numeric dtypes (signed integer, unsigned integer, and floats) can
      be specified as an atom in the form of `:s32`, `:u8`, `:f32` and o son
    * The atom `:float` as an alias for `{:f, 64}` to mirror Elixir's floats
    * The atom `:integer` as an alias for `{:s, 64}` to mirror Elixir's integers

  A series must consist of a single data type only. Series may have `nil` values in them.
  The series `dtype` can be retrieved via the `dtype/1` function or directly accessed as
  `series.dtype`. A `series.name` field is also available, but it is always `nil` unless
  the series is retrieved from a dataframe.

  Many functions only apply to certain dtypes. These functions may appear on distinct
  categories on the sidebar. Other functions may work on several datatypes, such as
  comparison functions. In such cases, a "Supported dtypes" section will be available
  in the function documentation.

  ## Creating series

  Series can be created using `from_list/2`, `from_binary/3`, and friends:

  Series can be made of numbers:

      iex> Explorer.Series.from_list([1, 2, 3])
      #Explorer.Series<
        Polars[3]
        s64 [1, 2, 3]
      >

  Series are nullable, so you may also include nils:

      iex> Explorer.Series.from_list([1.0, nil, 2.5, 3.1])
      #Explorer.Series<
        Polars[4]
        f64 [1.0, nil, 2.5, 3.1]
      >

  Any of the dtypes above are supported, such as strings:

      iex> Explorer.Series.from_list(["foo", "bar", "baz"])
      #Explorer.Series<
        Polars[3]
        string ["foo", "bar", "baz"]
      >

  ## Casting numeric series (type promotion)

  Series of integers and floats are automatically cast when executing certain
  operations. For example, adding a series of s64 with f64 will return a list
  of f64.

  Numeric casting works like this:

    * when working with the same numeric type but of different precisions,
      the higher precision wins

    * when working with unsigned integers and signed integers, unsigned integers
      are cast to signed integers using double of its precision (maximum of 64 bits)

    * when working with integers and floats, integers are always cast floats,
      keep the floating number precision

  ## Series queries

  DataFrames have named columns, so their queries use column names as variables:

      iex> require Explorer.DataFrame
      iex> df = Explorer.DataFrame.new(col_name: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, col_name > 2)
      #Explorer.DataFrame<
        Polars[1 x 1]
        col_name s64 [3]
      >

  Series have no named columns (a series constitutes a single column,
  so no name is required). This means their queries can't use column
  names as variables. Instead, series queries use the special `_` variable like so:

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.filter(s, _ > 2)
      #Explorer.Series<
        Polars[1]
        s64 [3]
      >

  """

  import Kernel, except: [and: 2, not: 1, in: 2]

  alias __MODULE__, as: Series
  alias Kernel, as: K
  alias Explorer.Duration
  alias Explorer.Shared

  @datetime_dtypes Explorer.Shared.datetime_types()
  @duration_dtypes Explorer.Shared.duration_types()
  @float_dtypes Explorer.Shared.float_types()
  @integer_types Explorer.Shared.integer_types()

  @date_or_datetime_dtypes [:date | @datetime_dtypes]
  @temporal_dtypes [:time | @date_or_datetime_dtypes ++ @duration_dtypes]
  @numeric_dtypes Explorer.Shared.numeric_types()
  @numeric_or_temporal_dtypes @numeric_dtypes ++ @temporal_dtypes

  @io_dtypes Shared.dtypes() -- [:binary, :string, {:list, :any}, {:struct, :any}]

  @type dtype ::
          :null
          | :binary
          | :boolean
          | :category
          | :date
          | :time
          | :string
          | datetime_dtype
          | duration_dtype
          | float_dtype
          | list_dtype
          | signed_integer_dtype
          | struct_dtype
          | unsigned_integer_dtype

  @type time_unit :: :nanosecond | :microsecond | :millisecond
  @type datetime_dtype :: {:datetime, time_unit}
  @type duration_dtype :: {:duration, time_unit}
  @type list_dtype :: {:list, dtype()}
  @type struct_dtype :: {:struct, [{String.t(), dtype()}]}

  @type signed_integer_dtype :: {:s, 8} | {:s, 16} | {:s, 32} | {:s, 64}
  @type unsigned_integer_dtype :: {:u, 8} | {:u, 16} | {:u, 32} | {:u, 64}
  @type float_dtype :: {:f, 32} | {:f, 64}

  @type float_dtype_alias :: :float | :f32 | :f64
  @type integer_dtype_alias :: :integer | :u8 | :u16 | :u32 | :u64 | :s8 | :s16 | :s32 | :s64

  @type t :: %Series{data: Explorer.Backend.Series.t(), dtype: dtype()}
  @type lazy_t :: %Series{data: Explorer.Backend.LazySeries.t(), dtype: dtype()}

  @type non_finite :: :nan | :infinity | :neg_infinity
  @type inferable_scalar ::
          number()
          | non_finite()
          | boolean()
          | map()
          | String.t()
          | Date.t()
          | Time.t()
          | NaiveDateTime.t()

  @doc false
  @enforce_keys [:data, :dtype]
  defstruct [:data, :dtype, :name]

  @behaviour Access
  @compile {:no_warn_undefined, Nx}

  defguardp is_numeric(n) when K.or(is_number(n), K.in(n, [:nan, :infinity, :neg_infinity]))

  defguardp is_io_dtype(dtype) when K.in(dtype, @io_dtypes)

  defguardp is_numeric_dtype(dtype) when K.in(dtype, @numeric_dtypes)

  defguardp is_numeric_or_bool_dtype(dtype)
            when K.in(dtype, [:boolean | @numeric_dtypes])

  defguardp is_numeric_or_temporal_dtype(dtype)
            when K.in(dtype, @numeric_or_temporal_dtypes)

  @impl true
  def fetch(series, idx) when is_integer(idx), do: {:ok, fetch!(series, idx)}
  def fetch(series, indices) when is_list(indices), do: {:ok, slice(series, indices)}
  def fetch(series, %Range{} = range), do: {:ok, slice(series, range)}

  @impl true
  def pop(series, idx) when is_integer(idx) do
    mask = 0..(size(series) - 1) |> Enum.map(&(&1 != idx)) |> from_list()
    value = fetch!(series, idx)
    series = mask(series, mask)
    {value, series}
  end

  def pop(series, indices) when is_list(indices) do
    mask = 0..(size(series) - 1) |> Enum.map(&K.not(Enum.member?(indices, &1))) |> from_list()
    value = slice(series, indices)
    series = mask(series, mask)
    {value, series}
  end

  def pop(series, %Range{} = range) do
    mask = 0..(size(series) - 1) |> Enum.map(&K.not(Enum.member?(range, &1))) |> from_list()
    value = slice(series, range)
    series = mask(series, mask)
    {value, series}
  end

  @impl true
  def get_and_update(series, idx, fun) when is_integer(idx) do
    value = fetch!(series, idx)
    {current_value, new_value} = fun.(value)
    new_data = series |> to_list() |> List.replace_at(idx, new_value) |> from_list()
    {current_value, new_data}
  end

  defp fetch!(series, idx) do
    size = size(series)
    idx = if idx < 0, do: idx + size, else: idx

    if K.or(idx < 0, idx > size),
      do: raise(ArgumentError, "index #{idx} out of bounds for series of size #{size}")

    apply_series(series, :at, [idx])
  end

  # Conversion

  @doc """
  Creates a new series from a list.

  The list must consist of a single data type and nils. It is possible to have
  a list of only nil values. In this case, the list will have the `:dtype` of `:null`.

  ## Options

    * `:backend` - The backend to allocate the series on.
    * `:dtype` - Cast the series to a given `:dtype`. By default this is `nil`, which means
      that Explorer will infer the type from the values in the list.
      See the module docs for the list of valid dtypes and aliases.

  ## Examples

  Explorer will infer the type from the values in the list.
  Integers are always treated as `s64` and floats are always
  treated as `f64`:

      iex> Explorer.Series.from_list([1, 2, 3])
      #Explorer.Series<
        Polars[3]
        s64 [1, 2, 3]
      >
      iex> Explorer.Series.from_list([1.0, 2.0, 3.0])
      #Explorer.Series<
        Polars[3]
        f64 [1.0, 2.0, 3.0]
      >

  Series are nullable, so you may also include nils:

      iex> Explorer.Series.from_list([1.0, nil, 2.5, 3.1])
      #Explorer.Series<
        Polars[4]
        f64 [1.0, nil, 2.5, 3.1]
      >

  A mix of integers and floats will be cast to a float:

      iex> Explorer.Series.from_list([1, 2.0])
      #Explorer.Series<
        Polars[2]
        f64 [1.0, 2.0]
      >

  Floats series can accept NaN, Inf, and -Inf values:

      iex> Explorer.Series.from_list([1.0, 2.0, :nan, 4.0])
      #Explorer.Series<
        Polars[4]
        f64 [1.0, 2.0, NaN, 4.0]
      >

      iex> Explorer.Series.from_list([1.0, 2.0, :infinity, 4.0])
      #Explorer.Series<
        Polars[4]
        f64 [1.0, 2.0, Inf, 4.0]
      >

      iex> Explorer.Series.from_list([1.0, 2.0, :neg_infinity, 4.0])
      #Explorer.Series<
        Polars[4]
        f64 [1.0, 2.0, -Inf, 4.0]
      >

  Trying to create an empty series or a series of nils will, by default,
  result in a series of `:null` type:

      iex> Explorer.Series.from_list([])
      #Explorer.Series<
        Polars[0]
        null []
      >
      iex> Explorer.Series.from_list([nil, nil])
      #Explorer.Series<
        Polars[2]
        null [nil, nil]
      >

  A list of `Date`, `Time`, `NaiveDateTime`, and `Explorer.Duration` structs
  are also supported, and they will become series with the respective dtypes:
  `:date`, `:time`, `{:datetime, :microsecond}`, and `{:duration, precision}`.
  For example:

      iex> Explorer.Series.from_list([~D[0001-01-01], ~D[1970-01-01], ~D[1986-10-13]])
      #Explorer.Series<
        Polars[3]
        date [0001-01-01, 1970-01-01, 1986-10-13]
      >

  You can specify the desired `dtype` for a series with the `:dtype` option.

      iex> Explorer.Series.from_list([nil, nil], dtype: :integer)
      #Explorer.Series<
        Polars[2]
        s64 [nil, nil]
      >

      iex> Explorer.Series.from_list([1, nil], dtype: :string)
      #Explorer.Series<
        Polars[2]
        string ["1", nil]
      >

      iex> Explorer.Series.from_list([1, 2], dtype: :f32)
      #Explorer.Series<
        Polars[2]
        f32 [1.0, 2.0]
      >

      iex> Explorer.Series.from_list([1, nil, 2], dtype: :float)
      #Explorer.Series<
        Polars[3]
        f64 [1.0, nil, 2.0]
      >

  The `dtype` option is particulary important if a `:binary` series
  is desired, because by default binary series will have the dtype
  of `:string`:

      iex> Explorer.Series.from_list([<<228, 146, 51>>, <<42, 209, 236>>], dtype: :binary)
      #Explorer.Series<
        Polars[2]
        binary [<<228, 146, 51>>, <<42, 209, 236>>]
      >

  A series mixing UTF8 strings and binaries is possible:

      iex> Explorer.Series.from_list([<<228, 146, 51>>, "Elixir"], dtype: :binary)
      #Explorer.Series<
        Polars[2]
        binary [<<228, 146, 51>>, "Elixir"]
      >

  Another option is to create a categorical series from a list of strings:

      iex> Explorer.Series.from_list(["EUA", "Brazil", "Poland"], dtype: :category)
      #Explorer.Series<
        Polars[3]
        category ["EUA", "Brazil", "Poland"]
      >

  It is possible to create a series of `:datetime` from a list of microseconds since Unix Epoch.

      iex> Explorer.Series.from_list([1649883642 * 1_000 * 1_000], dtype: {:datetime, :microsecond})
      #Explorer.Series<
        Polars[1]
        datetime[μs] [2022-04-13 21:00:42.000000]
      >

  It is possible to create a series of `:time` from a list of nanoseconds since midnight.

      iex> Explorer.Series.from_list([123 * 1_000 * 1_000 * 1_000], dtype: :time)
      #Explorer.Series<
        Polars[1]
        time [00:02:03.000000]
      >

  Mixing non-numeric data types will raise an ArgumentError:

      iex> Explorer.Series.from_list([1, "a"])
      ** (ArgumentError) the value "a" does not match the inferred dtype {:s, 64}
  """
  @doc type: :conversion
  @spec from_list(list :: list(), opts :: Keyword.t()) :: Series.t()
  def from_list(list, opts \\ []) do
    opts = Keyword.validate!(opts, [:dtype, :backend])
    backend = backend_from_options!(opts)

    normalised_dtype = if opts[:dtype], do: Shared.normalise_dtype!(opts[:dtype])

    type = Shared.dtype_from_list!(list, normalised_dtype)
    list = Shared.cast_series(list, type)

    series = backend.from_list(list, type)

    case normalised_dtype do
      nil -> series
      ^type -> series
      other -> cast(series, other)
    end
  end

  defp from_same_value(%{data: %backend{}}, value) do
    backend.from_list([value], Shared.dtype_from_list!([value]))
  end

  defp from_same_value(%{data: %backend{}}, value, dtype) do
    backend.from_list([value], dtype)
  end

  @doc """
  Builds a series of `dtype` from `binary`.

  All binaries must be in native endianness.

  ## Options

    * `:backend` - The backend to allocate the series on.

  ## Examples

  Integers and floats follow their native encoding:

      iex> Explorer.Series.from_binary(<<1.0::float-64-native, 2.0::float-64-native>>, {:f, 64})
      #Explorer.Series<
        Polars[2]
        f64 [1.0, 2.0]
      >

      iex> Explorer.Series.from_binary(<<-1::signed-64-native, 1::signed-64-native>>, :integer)
      #Explorer.Series<
        Polars[2]
        s64 [-1, 1]
      >

  Booleans are unsigned integers:

      iex> Explorer.Series.from_binary(<<1, 0, 1>>, :boolean)
      #Explorer.Series<
        Polars[3]
        boolean [true, false, true]
      >

  Dates are encoded as s32 representing days from the Unix epoch (1970-01-01):

      iex> binary = <<-719162::signed-32-native, 0::signed-32-native, 6129::signed-32-native>>
      iex> Explorer.Series.from_binary(binary, :date)
      #Explorer.Series<
        Polars[3]
        date [0001-01-01, 1970-01-01, 1986-10-13]
      >

  Times are encoded as s64 representing nanoseconds from midnight:

      iex> binary = <<0::signed-64-native, 86399999999000::signed-64-native>>
      iex> Explorer.Series.from_binary(binary, :time)
      #Explorer.Series<
        Polars[2]
        time [00:00:00.000000, 23:59:59.999999]
      >

  Datetimes are encoded as s64 representing microseconds from the Unix epoch (1970-01-01):

      iex> binary = <<0::signed-64-native, 529550625987654::signed-64-native>>
      iex> Explorer.Series.from_binary(binary, {:datetime, :microsecond})
      #Explorer.Series<
        Polars[2]
        datetime[μs] [1970-01-01 00:00:00.000000, 1986-10-13 01:23:45.987654]
      >

  """
  @doc type: :conversion
  @spec from_binary(
          binary,
          :boolean
          | :date
          | :time
          | datetime_dtype
          | duration_dtype
          | float_dtype
          | float_dtype_alias
          | integer_dtype_alias
          | signed_integer_dtype
          | unsigned_integer_dtype,
          keyword
        ) ::
          Series.t()
  def from_binary(binary, dtype, opts \\ []) when K.and(is_binary(binary), is_list(opts)) do
    opts = Keyword.validate!(opts, [:backend])
    dtype = Shared.normalise_dtype!(dtype)

    {_type, alignment} = dtype |> Shared.dtype_to_iotype!()

    if rem(bit_size(binary), alignment) != 0 do
      raise ArgumentError,
            "binary for dtype #{Shared.dtype_to_string(dtype)} is expected to be #{alignment}-bit aligned"
    end

    backend = backend_from_options!(opts)
    backend.from_binary(binary, dtype)
  end

  @doc """
  Converts a `t:Nx.Tensor.t/0` to a series.

  > #### Warning {: .warning}
  >
  > `Nx` is an optional dependency. You will need to ensure it's installed to use this function.

  ## Options

    * `:backend` - The backend to allocate the series on.
    * `:dtype` - The dtype of the series that must match the underlying tensor type.

      The series can have a different dtype if the tensor is compatible with it.
      For example, a tensor of `{:u, 8}` can represent a series of `:boolean` dtype.

      Here are the list of compatible tensor types and dtypes:

      * `{:u, 8}` tensor as a `:boolean` series.
      * `{:s, 32}` tensor as a `:date` series.
      * `{:s, 64}` tensor as a `:time` series.
      * `{:s, 64}` tensor as a `{:datetime, unit}` or `{:duration, unit}` series.

  ## Examples

  Integers and floats:

      iex> tensor = Nx.tensor([1, 2, 3])
      iex> Explorer.Series.from_tensor(tensor)
      #Explorer.Series<
        Polars[3]
        s64 [1, 2, 3]
      >

      iex> tensor = Nx.tensor([1.0, 2.0, 3.0], type: :f64)
      iex> Explorer.Series.from_tensor(tensor)
      #Explorer.Series<
        Polars[3]
        f64 [1.0, 2.0, 3.0]
      >

      iex> tensor = Nx.tensor([1, 0, 1], type: :u8)
      iex> Explorer.Series.from_tensor(tensor)
      #Explorer.Series<
        Polars[3]
        u8 [1, 0, 1]
      >

      iex> tensor = Nx.tensor([-719162, 0, 6129], type: :s32)
      iex> Explorer.Series.from_tensor(tensor)
      #Explorer.Series<
        Polars[3]
        s32 [-719162, 0, 6129]
      >

  Booleans can be read from a tensor of `{:u, 8}` type if the dtype is explicitly given:

      iex> tensor = Nx.tensor([1, 0, 1], type: :u8)
      iex> Explorer.Series.from_tensor(tensor, dtype: :boolean)
      #Explorer.Series<
        Polars[3]
        boolean [true, false, true]
      >

  Times are signed 64-bit representing nanoseconds from midnight and
  therefore must have their dtype explicitly given:

      iex> tensor = Nx.tensor([0, 86399999999000])
      iex> Explorer.Series.from_tensor(tensor, dtype: :time)
      #Explorer.Series<
        Polars[2]
        time [00:00:00.000000, 23:59:59.999999]
      >

  Datetimes are signed 64-bit and therefore must have their dtype explicitly given:

      iex> tensor = Nx.tensor([0, 529550625987654])
      iex> Explorer.Series.from_tensor(tensor, dtype: {:datetime, :microsecond})
      #Explorer.Series<
        Polars[2]
        datetime[μs] [1970-01-01 00:00:00.000000, 1986-10-13 01:23:45.987654]
      >
  """
  @doc type: :conversion
  @spec from_tensor(tensor :: Nx.Tensor.t(), opts :: Keyword.t()) :: Series.t()
  def from_tensor(tensor, opts \\ []) when is_struct(tensor, Nx.Tensor) do
    opts = Keyword.validate!(opts, [:dtype, :backend])
    type = Nx.type(tensor)
    {dtype, opts} = Keyword.pop_lazy(opts, :dtype, fn -> Shared.iotype_to_dtype!(type) end)

    dtype = Shared.normalise_dtype!(dtype)

    if Shared.dtype_to_iotype!(dtype) != type do
      raise ArgumentError,
            "dtype #{inspect(dtype)} expects a tensor of type #{inspect(Shared.dtype_to_iotype!(dtype))} " <>
              "but got type #{inspect(type)}"
    end

    backend = backend_from_options!(opts)
    tensor |> Nx.to_binary() |> backend.from_binary(dtype)
  end

  @doc """
  Replaces the contents of the given series by the one given in
  a tensor or list.

  The new series will have the same dtype and backend as the current
  series, but the size may not necessarily match.

  ## Tensor examples

      iex> s = Explorer.Series.from_list([0, 1, 2])
      iex> Explorer.Series.replace(s, Nx.tensor([1, 2, 3]))
      #Explorer.Series<
        Polars[3]
        s64 [1, 2, 3]
      >

  This is particularly useful for categorical columns:

      iex> s = Explorer.Series.from_list(["foo", "bar", "baz"], dtype: :category)
      iex> Explorer.Series.replace(s, Nx.tensor([2, 1, 0]))
      #Explorer.Series<
        Polars[3]
        category ["baz", "bar", "foo"]
      >

  ## List examples

  Similar to tensors, we can also replace by lists:

      iex> s = Explorer.Series.from_list([0, 1, 2])
      iex> Explorer.Series.replace(s, [1, 2, 3, 4, 5])
      #Explorer.Series<
        Polars[5]
        s64 [1, 2, 3, 4, 5]
      >

  The same considerations as above apply.
  """
  @doc type: :conversion
  @spec replace(Series.t(), Nx.Tensor.t() | list()) :: Series.t()
  def replace(series, tensor_or_list)

  def replace(series, tensor) when is_struct(tensor, Nx.Tensor) do
    replace_tensor_or_list(series, :from_tensor, tensor)
  end

  def replace(series, list) when is_list(list) do
    replace_tensor_or_list(series, :from_list, list)
  end

  defp replace_tensor_or_list(series, fun, arg) do
    backend_series_string = Atom.to_string(series.data.__struct__)
    backend_string = binary_part(backend_series_string, 0, byte_size(backend_series_string) - 7)
    backend = String.to_atom(backend_string)

    case series.dtype do
      :category ->
        Series
        |> apply(fun, [arg, [dtype: {:s, 64}, backend: backend]])
        |> categorise(series)

      dtype ->
        apply(Series, fun, [arg, [dtype: dtype, backend: backend]])
    end
  end

  @doc """
  Converts a series to a list.

  > #### Warning {: .warning}
  >
  > You must avoid converting a series to list, as that requires copying
  > the whole series in memory. Prefer to use the operations in this module
  > rather than the ones in `Enum` whenever possible, as this module is
  > optimized for large series.

  ## Examples

      iex> series = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.to_list(series)
      [1, 2, 3]
  """
  @doc type: :conversion
  @spec to_list(series :: Series.t()) :: list()
  def to_list(series), do: apply_series(series, :to_list)

  @doc """
  Converts a series to an enumerable.

  The enumerable will lazily traverse the series.

  > #### Warning {: .warning}
  >
  > You must avoid converting a series to enum, as that will copy the whole
  > series in memory as you traverse it. Prefer to use the operations in this
  > module rather than the ones in `Enum` whenever possible, as this module is
  > optimized for large series.

  ## Examples

      iex> series = Explorer.Series.from_list([1, 2, 3])
      iex> series |> Explorer.Series.to_enum() |> Enum.to_list()
      [1, 2, 3]
  """
  @doc type: :conversion
  @spec to_enum(series :: Series.t()) :: Enumerable.t()
  def to_enum(series), do: Explorer.Series.Iterator.new(series)

  @doc """
  Returns a series as a list of fixed-width binaries.

  An io vector (`iovec`) is the Erlang VM term for a flat list of binaries.
  This is typically a reference to the in-memory representation of the series.
  If the whole series in contiguous in memory, then the list will have a single
  element. All binaries are in native endianness.

  This operation fails if the series has `nil` values.
  Use `fill_missing/1` to handle them accordingly.

  To retrieve the type of the underlying io vector, use `iotype/1`.
  To convert an iovec to a binary, you can use `IO.iodata_to_binary/1`.

  ## Examples

  Integers and floats follow their native encoding:

      iex> series = Explorer.Series.from_list([-1, 0, 1])
      iex> Explorer.Series.to_iovec(series)
      [<<-1::signed-64-native, 0::signed-64-native, 1::signed-64-native>>]

      iex> series = Explorer.Series.from_list([1.0, 2.0, 3.0])
      iex> Explorer.Series.to_iovec(series)
      [<<1.0::float-64-native, 2.0::float-64-native, 3.0::float-64-native>>]

  Booleans are encoded as 0 and 1:

      iex> series = Explorer.Series.from_list([true, false, true])
      iex> Explorer.Series.to_iovec(series)
      [<<1, 0, 1>>]

  Dates are encoded as s32 representing days from the Unix epoch (1970-01-01):

      iex> series = Explorer.Series.from_list([~D[0001-01-01], ~D[1970-01-01], ~D[1986-10-13]])
      iex> Explorer.Series.to_iovec(series)
      [<<-719162::signed-32-native, 0::signed-32-native, 6129::signed-32-native>>]

  Times are encoded as s64 representing nanoseconds from midnight:

      iex> series = Explorer.Series.from_list([~T[00:00:00.000000], ~T[23:59:59.999999]])
      iex> Explorer.Series.to_iovec(series)
      [<<0::signed-64-native, 86399999999000::signed-64-native>>]

  Datetimes are encoded as s64 representing their precision from the Unix epoch (1970-01-01):

      iex> series = Explorer.Series.from_list([~N[0001-01-01 00:00:00], ~N[1970-01-01 00:00:00], ~N[1986-10-13 01:23:45.987654]])
      iex> Explorer.Series.to_iovec(series)
      [<<-62135596800000000::signed-64-native, 0::signed-64-native, 529550625987654::signed-64-native>>]

  The operation raises for binaries and strings, as they do not provide a fixed-width
  binary representation:

      iex> s = Explorer.Series.from_list(["a", "b", "c", "b"])
      iex> Explorer.Series.to_iovec(s)
      ** (ArgumentError) cannot convert series of dtype :string into iovec

  However, if appropriate, you can convert them to categorical types,
  which will then return the index of each category:

      iex> series = Explorer.Series.from_list(["a", "b", "c", "b"], dtype: :category)
      iex> Explorer.Series.to_iovec(series)
      [<<0::unsigned-32-native, 1::unsigned-32-native, 2::unsigned-32-native, 1::unsigned-32-native>>]

  """
  @doc type: :conversion
  @spec to_iovec(series :: Series.t()) :: [binary]
  def to_iovec(%Series{dtype: dtype} = series) do
    if is_io_dtype(dtype) do
      apply_series(series, :to_iovec)
    else
      raise ArgumentError, "cannot convert series of dtype #{inspect(dtype)} into iovec"
    end
  end

  @doc """
  Returns a series as a fixed-width binary.

  This is a shortcut around `to_iovec/1`. If possible, prefer
  to use `to_iovec/1` as that avoids copying binaries.

  ## Examples

      iex> series = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.to_binary(series)
      <<1::signed-64-native, 2::signed-64-native, 3::signed-64-native>>

      iex> series = Explorer.Series.from_list([true, false, true])
      iex> Explorer.Series.to_binary(series)
      <<1, 0, 1>>

  """
  @doc type: :conversion
  @spec to_binary(series :: Series.t()) :: binary
  def to_binary(series), do: series |> to_iovec() |> IO.iodata_to_binary()

  @doc """
  Converts a series to a `t:Nx.Tensor.t/0`.

  Note that `Explorer.Series` are automatically converted
  to tensors when passed to numerical definitions.
  The tensor type is given by `iotype/1`.

  > #### Warning {: .warning}
  >
  > `Nx` is an optional dependency. You will need to ensure it's installed to use this function.

  ## Options

    * `:backend` - the Nx backend to allocate the tensor on

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.to_tensor(s)
      #Nx.Tensor<
        s64[3]
        [1, 2, 3]
      >

      iex> s = Explorer.Series.from_list([true, false, true])
      iex> Explorer.Series.to_tensor(s)
      #Nx.Tensor<
        u8[3]
        [1, 0, 1]
      >

  """
  @doc type: :conversion
  @spec to_tensor(series :: Series.t(), tensor_opts :: Keyword.t()) :: Nx.Tensor.t()
  def to_tensor(%Series{dtype: dtype} = series, tensor_opts \\ []) do
    case iotype(series) do
      {_, _} = type ->
        Nx.from_binary(to_binary(series), type, tensor_opts)

      :none when Kernel.in(dtype, [:string, :binary]) ->
        raise ArgumentError,
              "cannot convert #{inspect(dtype)} series to tensor (consider casting the series to a :category type before)"

      :none ->
        raise ArgumentError, "cannot convert #{inspect(dtype)} series to tensor"
    end
  end

  @doc """
  Cast the series to another type.

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.cast(s, :string)
      #Explorer.Series<
        Polars[3]
        string ["1", "2", "3"]
      >

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.cast(s, {:f, 64})
      #Explorer.Series<
        Polars[3]
        f64 [1.0, 2.0, 3.0]
      >

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.cast(s, :date)
      #Explorer.Series<
        Polars[3]
        date [1970-01-02, 1970-01-03, 1970-01-04]
      >

  Note that `time` is represented as an integer of nanoseconds since midnight.
  In Elixir we can't represent nanoseconds, only microseconds. So be aware that
  information can be lost if a conversion is needed (e.g. calling `to_list/1`).

      iex> s = Explorer.Series.from_list([1_000, 2_000, 3_000])
      iex> Explorer.Series.cast(s, :time)
      #Explorer.Series<
        Polars[3]
        time [00:00:00.000001, 00:00:00.000002, 00:00:00.000003]
      >

      iex> s = Explorer.Series.from_list([86399 * 1_000 * 1_000 * 1_000])
      iex> Explorer.Series.cast(s, :time)
      #Explorer.Series<
        Polars[1]
        time [23:59:59.000000]
      >

  Note that `datetime` is represented as an integer of microseconds since Unix Epoch (1970-01-01 00:00:00).

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.cast(s, {:datetime, :microsecond})
      #Explorer.Series<
        Polars[3]
        datetime[μs] [1970-01-01 00:00:00.000001, 1970-01-01 00:00:00.000002, 1970-01-01 00:00:00.000003]
      >

      iex> s = Explorer.Series.from_list([1649883642 * 1_000 * 1_000])
      iex> Explorer.Series.cast(s, {:datetime, :microsecond})
      #Explorer.Series<
        Polars[1]
        datetime[μs] [2022-04-13 21:00:42.000000]
      >

  You can also use `cast/2` to categorise a string:

      iex> s = Explorer.Series.from_list(["apple", "banana",  "apple", "lemon"])
      iex> Explorer.Series.cast(s, :category)
      #Explorer.Series<
        Polars[4]
        category ["apple", "banana", "apple", "lemon"]
      >

  `cast/2` will return the series as a no-op if you try to cast to the same dtype.

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.cast(s, :integer)
      #Explorer.Series<
        Polars[3]
        s64 [1, 2, 3]
      >
  """
  @doc type: :element_wise
  @spec cast(series :: Series.t(), dtype :: dtype()) :: Series.t()
  def cast(%Series{dtype: original_dtype} = series, dtype) do
    if normalised = Shared.normalise_dtype(dtype) do
      if normalised == original_dtype do
        series
      else
        apply_series(series, :cast, [normalised])
      end
    else
      dtype_error("cast/2", dtype, Shared.dtypes())
    end
  end

  @doc """
  Converts a string series to a datetime series with a given `format_string`.

  For the format string specification, refer to the
  [chrono crate documentation](https://docs.rs/chrono/latest/chrono/format/strftime/).

  Use `cast(series, :datetime)` if you prefer the format to be inferred (if possible).

  ## Examples

      iex> s = Explorer.Series.from_list(["2023-01-05 12:34:56", "XYZ", nil])
      iex> Explorer.Series.strptime(s, "%Y-%m-%d %H:%M:%S")
      #Explorer.Series<
        Polars[3]
        datetime[μs] [2023-01-05 12:34:56.000000, nil, nil]
      >
  """
  @doc type: :element_wise
  @spec strptime(series :: Series.t(), format_string :: String.t()) :: Series.t()
  def strptime(%Series{dtype: dtype} = series, format_string) when K.in(dtype, [:string]),
    do: apply_series(series, :strptime, [format_string])

  def strptime(%Series{dtype: dtype}, _format_string),
    do: dtype_error("strptime/2", dtype, [:string])

  @doc """
  Converts a datetime series to a string series.

  For the format string specification, refer to the
  [chrono crate documentation](https://docs.rs/chrono/latest/chrono/format/strftime/index.html).

  Use `cast(series, :string)` for the default `"%Y-%m-%d %H:%M:%S%.6f"` format.

  ## Examples

      iex> s = Explorer.Series.from_list([~N[2023-01-05 12:34:56], nil])
      iex> Explorer.Series.strftime(s, "%Y/%m/%d %H:%M:%S")
      #Explorer.Series<
        Polars[2]
        string ["2023/01/05 12:34:56", nil]
      >
  """
  @doc type: :element_wise
  @spec strftime(series :: Series.t(), format_string :: String.t()) :: Series.t()
  def strftime(%Series{dtype: dtype} = series, format_string) when K.in(dtype, @datetime_dtypes),
    do: apply_series(series, :strftime, [format_string])

  def strftime(%Series{dtype: dtype}, _format_string),
    do: dtype_error("strftime/2", dtype, @datetime_dtypes)

  @doc """
  Clip (or clamp) the values in a series.

  Values that fall outside of the interval defined by the `min` and `max`
  bounds are clipped to the bounds.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  Clipping other dtypes are possible using `select/3`.

  ## Examples

      iex> s = Explorer.Series.from_list([-50, 5, nil, 50])
      iex> Explorer.Series.clip(s, 1, 10)
      #Explorer.Series<
        Polars[4]
        s64 [1, 5, nil, 10]
      >

      iex> s = Explorer.Series.from_list([-50, 5, nil, 50])
      iex> Explorer.Series.clip(s, 1.5, 10.5)
      #Explorer.Series<
        Polars[4]
        f64 [1.5, 5.0, nil, 10.5]
      >
  """
  @doc type: :element_wise
  @spec clip(series :: Series.t(), min :: number(), max :: number()) :: Series.t()
  def clip(%Series{dtype: dtype} = series, min, max) when is_numeric_dtype(dtype) do
    if !K.and(is_number(min), is_number(max)) do
      raise ArgumentError,
            "Explorer.Series.clip/3 expects both the min and max bounds to be numbers"
    end

    if min > max do
      raise ArgumentError,
            "Explorer.Series.clip/3 expects the max bound to be greater than the min bound"
    end

    apply_series(series, :clip, [min, max])
  end

  def clip(%Series{dtype: dtype}, _min, _max),
    do: dtype_error("clip/3", dtype, @numeric_dtypes)

  # Introspection

  @doc """
  Returns the data type of the series.

  See the moduledoc for all supported dtypes.

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.dtype(s)
      {:s, 64}

      iex> s = Explorer.Series.from_list(["a", nil, "b", "c"])
      iex> Explorer.Series.dtype(s)
      :string
  """
  @doc type: :introspection
  @spec dtype(series :: Series.t()) :: dtype()
  def dtype(%Series{dtype: dtype}), do: dtype

  @doc """
  Returns the number of elements in the series.

  See also:

    * `count/1` - counts only the non-`nil` elements.
    * `nil_count/1` - counts all `nil` elements.

  ## Examples

  Basic example:

      iex> s = Explorer.Series.from_list([~D[1999-12-31], ~D[1989-01-01]])
      iex> Explorer.Series.size(s)
      2

  With lists:

      iex> s = Explorer.Series.from_list([[1, 2, 3], [4, 5]])
      iex> Explorer.Series.size(s)
      2
  """
  @doc type: :introspection
  @spec size(series :: Series.t()) :: non_neg_integer() | lazy_t()
  def size(series), do: apply_series(series, :size)

  @doc """
  Returns the type of the underlying fixed-width binary representation.

  It returns something in the shape of `{atom(), bits_size}` or `:none`.
  It is often used in conjunction with `to_iovec/1` and `to_binary/1`.

  The possible iotypes are:

  * `:u` for unsigned integers.
  * `:s` for signed integers.
  * `:f` for floats.

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 3, 4])
      iex> Explorer.Series.iotype(s)
      {:s, 64}

      iex> s = Explorer.Series.from_list([~D[1999-12-31], ~D[1989-01-01]])
      iex> Explorer.Series.iotype(s)
      {:s, 32}

      iex> s = Explorer.Series.from_list([~T[00:00:00.000000], ~T[23:59:59.999999]])
      iex> Explorer.Series.iotype(s)
      {:s, 64}

      iex> s = Explorer.Series.from_list([1.2, 2.3, 3.5, 4.5])
      iex> Explorer.Series.iotype(s)
      {:f, 64}

      iex> s = Explorer.Series.from_list([true, false, true])
      iex> Explorer.Series.iotype(s)
      {:u, 8}

  The operation returns `:none` for strings and binaries, as they do not
  provide a fixed-width binary representation:

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.iotype(s)
      :none

  However, if appropriate, you can convert them to categorical types,
  which will then return the index of each category:

      iex> s = Explorer.Series.from_list(["a", "b", "c"], dtype: :category)
      iex> Explorer.Series.iotype(s)
      {:u, 32}

  """
  @doc type: :introspection
  @spec iotype(series :: Series.t()) :: {:s | :u | :f, non_neg_integer()} | :none
  def iotype(%Series{dtype: dtype}) do
    case dtype do
      :category -> {:u, 32}
      other -> Shared.dtype_to_iotype(other)
    end
  end

  @doc """
  Return a series with the category names of a categorical series.

  Each category has the index equal to its position.
  No order for the categories is guaranteed.

  ## Examples

      iex> s = Explorer.Series.from_list(["a", "b", "c", nil, "a", "c"], dtype: :category)
      iex> Explorer.Series.categories(s)
      #Explorer.Series<
        Polars[3]
        string ["a", "b", "c"]
      >

      iex> s = Explorer.Series.from_list(["c", "a", "b"], dtype: :category)
      iex> Explorer.Series.categories(s)
      #Explorer.Series<
        Polars[3]
        string ["c", "a", "b"]
      >

  """
  @doc type: :introspection
  @spec categories(series :: Series.t()) :: Series.t()
  def categories(%Series{dtype: :category} = series), do: apply_series(series, :categories)
  def categories(%Series{dtype: dtype}), do: dtype_error("categories/1", dtype, [:category])

  @doc """
  Categorise a series of integers or strings according to `categories`.

  This function receives a series of integers or strings and convert them
  into the categories specified by the second argument.
  The second argument can be one of:

    * a series with dtype `:category`. The integers will be indexes into
      the categories of the given series (returned by `categories/1`)

    * a series with dtype `:string`. The integers will be indexes into
      the series itself

    * a list of strings. The integers will be indexes into the list

  This is going to essentially "copy" the source of categories from the left series
  to the right. All members from the left that are not present in the right hand-side
  are going to be `nil`.

  If you have a series of strings and you want to convert them into categories,
  invoke `cast(series, :category)` instead.

  ## Examples

  If a categorical series is given as second argument, we will extract its
  categories and map the integers into it:

      iex> categories = Explorer.Series.from_list(["a", "b", "c", nil, "a"], dtype: :category)
      iex> indexes = Explorer.Series.from_list([0, 2, 1, 0, 2])
      iex> Explorer.Series.categorise(indexes, categories)
      #Explorer.Series<
        Polars[5]
        category ["a", "c", "b", "a", "c"]
      >

  Otherwise, if a list of strings or a series of strings is given, they are
  considered to be the categories series itself:

      iex> categories = Explorer.Series.from_list(["a", "b", "c"])
      iex> indexes = Explorer.Series.from_list([0, 2, 1, 0, 2])
      iex> Explorer.Series.categorise(indexes, categories)
      #Explorer.Series<
        Polars[5]
        category ["a", "c", "b", "a", "c"]
      >

      iex> indexes = Explorer.Series.from_list([0, 2, 1, 0, 2])
      iex> Explorer.Series.categorise(indexes, ["a", "b", "c"])
      #Explorer.Series<
        Polars[5]
        category ["a", "c", "b", "a", "c"]
      >

  Elements that are not mapped to a category will become `nil`:

      iex> indexes = Explorer.Series.from_list([0, 2, nil, 0, 2, 7])
      iex> Explorer.Series.categorise(indexes, ["a", "b", "c"])
      #Explorer.Series<
        Polars[6]
        category ["a", "c", nil, "a", "c", nil]
      >

  Strings can be used as "indexes" to create a categorical series
  with the intersection of members:

      iex> strings = Explorer.Series.from_list(["a", "c", nil, "c", "b", "d"])
      iex> Explorer.Series.categorise(strings, ["a", "b", "c"])
      #Explorer.Series<
        Polars[6]
        category ["a", "c", nil, "c", "b", nil]
      >

  """
  @doc type: :element_wise
  def categorise(%Series{dtype: l_dtype} = series, %Series{dtype: :category} = categories)
      when K.in(l_dtype, [:string | @integer_types]),
      do: apply_series(series, :categorise, [categories])

  def categorise(%Series{dtype: l_dtype} = series, %Series{dtype: :string} = categories)
      when K.in(l_dtype, [:string | @integer_types]) do
    if nil_count(categories) != 0 do
      raise(ArgumentError, "categories as strings cannot have nil values")
    end

    if count(categories) != n_distinct(categories) do
      raise(ArgumentError, "categories as strings cannot have duplicated values")
    end

    categories = cast(categories, :category)
    apply_series(series, :categorise, [categories])
  end

  def categorise(%Series{dtype: l_dtype} = series, [head | _] = categories)
      when K.and(K.in(l_dtype, [:string | @integer_types]), is_binary(head)),
      do: categorise(series, from_list(categories, dtype: :string))

  # Slice and dice

  @doc """
  Returns the first N elements of the series.

  ## Examples

      iex> s = 1..100 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.head(s)
      #Explorer.Series<
        Polars[10]
        s64 [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      >
  """
  @doc type: :shape
  @spec head(series :: Series.t(), n_elements :: integer()) :: Series.t()
  def head(series, n_elements \\ 10), do: apply_series(series, :head, [n_elements])

  @doc """
  Returns the last N elements of the series.

  ## Examples

      iex> s = 1..100 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.tail(s)
      #Explorer.Series<
        Polars[10]
        s64 [91, 92, 93, 94, 95, 96, 97, 98, 99, 100]
      >
  """
  @doc type: :shape
  @spec tail(series :: Series.t(), n_elements :: integer()) :: Series.t()
  def tail(series, n_elements \\ 10), do: apply_series(series, :tail, [n_elements])

  @doc """
  Returns the first element of the series.

  ## Examples

      iex> s = 1..100 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.first(s)
      1
  """
  @doc type: :shape
  @spec first(series :: Series.t()) :: any()
  def first(series), do: apply_series(series, :first, [])

  @doc """
  Returns the last element of the series.

  ## Examples

      iex> s = 1..100 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.last(s)
      100
  """
  @doc type: :shape
  @spec last(series :: Series.t()) :: any()
  def last(series), do: apply_series(series, :last, [])

  @doc """
  Shifts `series` by `offset` with `nil` values.

  Positive offset shifts from first, negative offset shifts from last.

  ## Examples

      iex> s = 1..5 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.shift(s, 2)
      #Explorer.Series<
        Polars[5]
        s64 [nil, nil, 1, 2, 3]
      >

      iex> s = 1..5 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.shift(s, -2)
      #Explorer.Series<
        Polars[5]
        s64 [3, 4, 5, nil, nil]
      >
  """
  @doc type: :shape
  @spec shift(series :: Series.t(), offset :: integer()) :: Series.t()
  def shift(series, offset)
      when is_integer(offset),
      do: apply_series(series, :shift, [offset, nil])

  @doc """
  Returns a series from two series, based on a predicate.

  The resulting series is built by evaluating each element of
  `predicate` and returning either the corresponding element from
  `on_true` or `on_false`.

  `predicate` must be a boolean series. `on_true` and `on_false` must be
  a series of the same size as `predicate` or a series of size 1.

  It is possible to mix numeric series in the `on_true` and `on_false`,
  and the resultant series will have the dtype of the greater side.
  For example, `:u8` and `:s16` is going to result in `:s16` series.
  """
  @doc type: :element_wise
  @spec select(
          predicate :: Series.t(),
          on_true :: Series.t() | inferable_scalar(),
          on_false :: Series.t() | inferable_scalar()
        ) ::
          Series.t()
  def select(%Series{dtype: predicate_dtype} = predicate, on_true, on_false) do
    if predicate_dtype != :boolean do
      raise ArgumentError,
            "Explorer.Series.select/3 expect the first argument to be a series of booleans, got: #{inspect(predicate_dtype)}"
    end

    %Series{dtype: on_true_dtype} = on_true = maybe_from_list(on_true)
    %Series{dtype: on_false_dtype} = on_false = maybe_from_list(on_false)

    cond do
      K.and(is_numeric_dtype(on_true_dtype), is_numeric_dtype(on_false_dtype)) ->
        apply_series_list(:select, [predicate, on_true, on_false])

      on_true_dtype == on_false_dtype ->
        apply_series_list(:select, [predicate, on_true, on_false])

      true ->
        dtype_mismatch_error("select/3", on_true_dtype, on_false_dtype)
    end
  end

  defp maybe_from_list(%Series{} = series), do: series
  defp maybe_from_list(other), do: from_list([other])

  @doc """
  Returns a random sample of the series.

  If given an integer as the second argument, it will return N samples. If given a float, it will
  return that proportion of the series.

  Can sample with or without replace.

  ## Options

    * `:replace` - If set to `true`, each sample will be independent and therefore values may repeat.
      Required to be `true` for `n` greater then the number of rows in the series or `frac` > 1.0. (default: `false`)
    * `:seed` - An integer to be used as a random seed. If nil, a random value between 0 and 2^64 − 1 will be used. (default: nil)
    * `:shuffle` - In case the sample is equal to the size of the series, shuffle tells if the resultant
      series should be shuffled or if it should return the same series. (default: `false`).

  ## Examples

      iex> s = 1..100 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.sample(s, 10, seed: 100)
      #Explorer.Series<
        Polars[10]
        s64 [57, 9, 54, 62, 50, 77, 35, 88, 1, 69]
      >

      iex> s = 1..100 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.sample(s, 0.05, seed: 100)
      #Explorer.Series<
        Polars[5]
        s64 [9, 56, 79, 28, 54]
      >

      iex> s = 1..5 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.sample(s, 7, seed: 100, replace: true)
      #Explorer.Series<
        Polars[7]
        s64 [4, 1, 3, 4, 3, 4, 2]
      >

      iex> s = 1..5 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.sample(s, 1.2, seed: 100, replace: true)
      #Explorer.Series<
        Polars[6]
        s64 [4, 1, 3, 4, 3, 4]
      >

      iex> s = 0..9 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.sample(s, 1.0, seed: 100, shuffle: false)
      #Explorer.Series<
        Polars[10]
        s64 [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
      >

      iex> s = 0..9 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.sample(s, 1.0, seed: 100, shuffle: true)
      #Explorer.Series<
        Polars[10]
        s64 [7, 9, 2, 0, 4, 1, 3, 8, 5, 6]
      >

  """
  @doc type: :shape
  @spec sample(series :: Series.t(), n_or_frac :: number(), opts :: Keyword.t()) :: Series.t()
  def sample(series, n_or_frac, opts \\ []) when is_number(n_or_frac) do
    opts = Keyword.validate!(opts, replace: false, shuffle: false, seed: nil)

    size = size(series)

    # In case the series is lazy, we don't perform this check here.
    if K.and(
         is_integer(size),
         K.and(opts[:replace] == false, invalid_size_for_sample?(n_or_frac, size))
       ) do
      raise ArgumentError,
            "in order to sample more elements than are in the series (#{size}), sampling " <>
              "`replace` must be true"
    end

    apply_series(series, :sample, [n_or_frac, opts[:replace], opts[:shuffle], opts[:seed]])
  end

  defp invalid_size_for_sample?(n, size) when is_integer(n), do: n > size

  defp invalid_size_for_sample?(frac, size) when is_float(frac),
    do: invalid_size_for_sample?(round(frac * size), size)

  @doc """
  Change the elements order randomly.

  ## Options

    * `:seed` - An integer to be used as a random seed. If nil,
      a random value between 0 and 2^64 − 1 will be used. (default: nil)

  ## Examples

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.shuffle(s, seed: 100)
      #Explorer.Series<
        Polars[10]
        s64 [8, 10, 3, 1, 5, 2, 4, 9, 6, 7]
      >

  """
  @doc type: :shape
  @spec shuffle(series :: Series.t(), opts :: Keyword.t()) :: Series.t()
  def shuffle(series, opts \\ [])

  def shuffle(series, opts) do
    opts = Keyword.validate!(opts, seed: nil)

    sample(series, 1.0, seed: opts[:seed], shuffle: true)
  end

  @doc """
  Takes every *n*th value in this series, returned as a new series.

  ## Examples

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.at_every(s, 2)
      #Explorer.Series<
        Polars[5]
        s64 [1, 3, 5, 7, 9]
      >

  If *n* is bigger than the size of the series, the result is a new series with only the first value of the supplied series.

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.at_every(s, 20)
      #Explorer.Series<
        Polars[1]
        s64 [1]
      >
  """
  @doc type: :shape
  @spec at_every(series :: Series.t(), every_n :: integer()) :: Series.t()
  def at_every(series, every_n), do: apply_series(series, :at_every, [every_n])

  # Macros

  @doc """
  Picks values based on an `Explorer.Query`.

  The query is compiled and runs efficiently against the series.
  The query must return a boolean expression or a list of boolean expressions.
  When a list is returned, they are joined as `and` expressions.

  > #### Notice {: .notice}
  >
  > This is a macro.
  >
  >   * You must `require Explorer.Series` before using it.
  >   * You must use the special `_` syntax. See the moduledoc for details.

  Besides element-wise series operations, you can also use window functions
  and aggregations inside comparisons.

  See `filter_with/2` for a callback version of this function without
  `Explorer.Query`.
  See `mask/2` if you want to filter values based on another series.

  ## Examples

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.filter(s, _ == "b")
      #Explorer.Series<
        Polars[1]
        string ["b"]
      >

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.filter(s, remainder(_, 2) == 1)
      #Explorer.Series<
        Polars[2]
        s64 [1, 3]
      >

  Returning a non-boolean expression errors:

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.filter(s, cumulative_max(_))
      ** (ArgumentError) expecting the function to return a boolean LazySeries, but instead it returned a LazySeries of type {:s, 64}

  Which can be addressed by converting it to boolean:

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.filter(s, cumulative_max(_) == 1)
      #Explorer.Series<
        Polars[1]
        s64 [1]
      >
  """
  @doc type: :element_wise
  defmacro filter(series, query) do
    quote do
      require Explorer.Query

      Explorer.DataFrame.new(_: unquote(series))
      |> Explorer.DataFrame.filter_with(Explorer.Query.query(unquote(query)))
      |> Explorer.DataFrame.pull(:_)
    end
  end

  @doc """
  Filters a series with a callback function.

  See `mask/2` if you want to filter values based on another series.

  ## Examples

      iex> series = Explorer.Series.from_list([1, 2, 3])
      iex> is_odd = fn s -> s |> Explorer.Series.remainder(2) |> Explorer.Series.equal(1) end
      iex> Explorer.Series.filter_with(series, is_odd)
      #Explorer.Series<
        Polars[2]
        s64 [1, 3]
      >
  """
  @doc type: :element_wise
  @spec filter_with(
          series :: Series.t(),
          fun :: (Series.t() -> Series.lazy_t())
        ) :: Series.t()
  def filter_with(%Series{} = series, fun) when is_function(fun, 1) do
    Explorer.DataFrame.new(series: series)
    |> Explorer.DataFrame.filter_with(&fun.(&1[:series]))
    |> Explorer.DataFrame.pull(:series)
  end

  @doc """
  Maps values based on an `Explorer.Query`.

  The query is compiled and runs efficiently against the series.

  > #### Notice {: .notice}
  >
  > This is a macro.
  >
  >   * You must `require Explorer.Series` before using it.
  >   * You must use the special `_` syntax. See the moduledoc for details.

  See `map_with/2` for a callback version of this function without `Explorer.Query`.

  This function only works with lazy computations.
  See `transform/2` for a version that works with any Elixir function.

  ## Examples

  Basic example:

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.map(s, _ * 2)
      #Explorer.Series<
        Polars[3]
        s64 [2, 4, 6]
      >

  You can also use window functions and aggregations:

      iex> s = Explorer.Series.from_list([2, 3, 4])
      iex> Explorer.Series.map(s, _ - min(_))
      #Explorer.Series<
        Polars[3]
        s64 [0, 1, 2]
      >
  """
  @doc type: :element_wise
  defmacro map(series, query) do
    quote do
      require Explorer.DataFrame

      Explorer.DataFrame.new(_: unquote(series))
      |> Explorer.DataFrame.mutate(_: unquote(query))
      |> Explorer.DataFrame.pull(:_)
    end
  end

  @doc """
  Maps a series with a callback function.

  This function only works with lazy computations.
  See `transform/2` for a version that works with any Elixir function.

  ## Examples

      iex> series = Explorer.Series.from_list([2, 3, 4])
      iex> shift_left = fn s -> Explorer.Series.subtract(s, Explorer.Series.min(s)) end
      iex> Explorer.Series.map_with(series, shift_left)
      #Explorer.Series<
        Polars[3]
        s64 [0, 1, 2]
      >
  """
  @doc type: :element_wise
  def map_with(%Series{} = series, fun) when is_function(fun, 1) do
    Explorer.DataFrame.new(series: series)
    |> Explorer.DataFrame.mutate_with(&[series: fun.(&1[:series])])
    |> Explorer.DataFrame.pull(:series)
  end

  @doc """
  Sorts the series based on an expression.

  > #### Notice {: .notice}
  >
  > This is a macro.
  >
  >   * You must `require Explorer.Series` before using it.
  >   * You must use the special `_` syntax. See the moduledoc for details.

  See `sort_with/3` for the callback-based version of this function.

  ## Options

    * `:direction` - `:asc` or `:desc`, meaning "ascending" or "descending", respectively.
      By default it sorts in ascending order.

    * `:nils` - `:first` or `:last`.
      By default it is `:last` if direction is `:asc`, and `:first` otherwise.

    * `:parallel` - boolean.
      Whether to parallelize the sorting.
      By default it is `true`.

      Parallel sort isn't available on certain lazy operations.
      In those situations this option is ignored.

    * `:stable` - boolean.
      Determines if the sorting is stable (ties are guaranteed to maintain their order) or not.
      Unstable sorting may be more performant.
      By default it is `false`.

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.sort_by(s, remainder(_, 3))
      #Explorer.Series<
        Polars[3]
        s64 [3, 1, 2]
      >

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.sort_by(s, remainder(_, 3), direction: :desc)
      #Explorer.Series<
        Polars[3]
        s64 [2, 1, 3]
      >

      iex> s = Explorer.Series.from_list([1, nil, 2, 3])
      iex> Explorer.Series.sort_by(s, -2 * _, nils: :first)
      #Explorer.Series<
        Polars[4]
        s64 [nil, 3, 2, 1]
      >
  """
  @doc type: :shape
  defmacro sort_by(series, query, opts \\ []) do
    {direction, opts} = Keyword.pop(opts, :direction, :asc)

    quote do
      require Explorer.DataFrame

      Explorer.DataFrame.new(_: unquote(series))
      |> Explorer.DataFrame.sort_by([{unquote(direction), unquote(query)}], unquote(opts))
      |> Explorer.DataFrame.pull(:_)
    end
  end

  @doc """
  Sorts the series based on a callback that returns a lazy series.

  See `sort_by/3` for the expression-based version of this function.

  ## Options

    * `:direction` - `:asc` or `:desc`, meaning "ascending" or "descending", respectively.
      By default it sorts in ascending order.

    * `:nils` - `:first` or `:last`.
      By default it is `:last` if direction is `:asc`, and `:first` otherwise.

    * `:parallel` - boolean.
      Whether to parallelize the sorting.
      By default it is `true`.

      Parallel sort isn't available on certain lazy operations.
      In those situations this option is ignored.

    * `:stable` - boolean.
      Determines if the sorting is stable (ties are guaranteed to maintain their order) or not.
      Unstable sorting may be more performant.
      By default it is `false`.

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.sort_with(s, &Explorer.Series.remainder(&1, 3))
      #Explorer.Series<
        Polars[3]
        s64 [3, 1, 2]
      >

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.sort_with(s, &Explorer.Series.remainder(&1, 3), direction: :desc)
      #Explorer.Series<
        Polars[3]
        s64 [2, 1, 3]
      >

      iex> s = Explorer.Series.from_list([1, nil, 2, 3])
      iex> Explorer.Series.sort_with(s, &Explorer.Series.multiply(-2, &1), nils: :first)
      #Explorer.Series<
        Polars[4]
        s64 [nil, 3, 2, 1]
      >
  """
  @doc type: :shape
  def sort_with(%Series{} = series, fun, opts \\ []) do
    {direction, opts} = Keyword.pop(opts, :direction, :asc)

    Explorer.DataFrame.new(series: series)
    |> Explorer.DataFrame.sort_with(&[{direction, fun.(&1[:series])}], opts)
    |> Explorer.DataFrame.pull(:series)
  end

  @doc """
  Filters a series with a mask.

  ## Examples

      iex> s1 = Explorer.Series.from_list([1,2,3])
      iex> s2 = Explorer.Series.from_list([true, false, true])
      iex> Explorer.Series.mask(s1, s2)
      #Explorer.Series<
        Polars[2]
        s64 [1, 3]
      >
  """
  @doc type: :element_wise
  @spec mask(series :: Series.t(), mask :: Series.t()) :: Series.t()
  def mask(series, %Series{} = mask), do: apply_series(series, :mask, [mask])

  @doc """
  Assign ranks to data with appropriate handling of tied values.

  ## Options

  * `:method` - Determine how ranks are assigned to tied elements. The following methods are available:
    - `:average` : Each value receives the average rank that would be assigned to all tied values. (default)
    - `:min` : Tied values are assigned the minimum rank. Also known as "competition" ranking.
    - `:max` : Tied values are assigned the maximum of their ranks.
    - `:dense` : Similar to `:min`, but the rank of the next highest element is assigned the rank immediately after those assigned to the tied elements.
    - `:ordinal` : Each value is given a distinct rank based on its occurrence in the series.
    - `:random` : Similar to `:ordinal`, but the rank for ties is not dependent on the order that the values occur in the Series.
  * `:descending` - Rank in descending order.
  * `:seed` - An integer to be used as a random seed. If nil, a random value between 0 and 2^64 − 1 will be used. (default: nil)

  ## Examples

      iex> s = Explorer.Series.from_list([3, 6, 1, 1, 6])
      iex> Explorer.Series.rank(s)
      #Explorer.Series<
        Polars[5]
        f64 [3.0, 4.5, 1.5, 1.5, 4.5]
      >

      iex> s = Explorer.Series.from_list([1.1, 2.4, 3.2])
      iex> Explorer.Series.rank(s, method: :ordinal)
      #Explorer.Series<
        Polars[3]
        u32 [1, 2, 3]
      >

      iex> s = Explorer.Series.from_list([ ~N[2022-07-07 17:44:13.020548], ~N[2022-07-07 17:43:08.473561], ~N[2022-07-07 17:45:00.116337] ])
      iex> Explorer.Series.rank(s, method: :average)
      #Explorer.Series<
        Polars[3]
        f64 [2.0, 1.0, 3.0]
      >

      iex> s = Explorer.Series.from_list([3, 6, 1, 1, 6])
      iex> Explorer.Series.rank(s, method: :min)
      #Explorer.Series<
        Polars[5]
        s64 [3, 4, 1, 1, 4]
      >

      iex> s = Explorer.Series.from_list([3, 6, 1, 1, 6])
      iex> Explorer.Series.rank(s, method: :dense)
      #Explorer.Series<
        Polars[5]
        s64 [2, 3, 1, 1, 3]
      >


      iex> s = Explorer.Series.from_list([3, 6, 1, 1, 6])
      iex> Explorer.Series.rank(s, method: :random, seed: 42)
      #Explorer.Series<
        Polars[5]
        s64 [3, 4, 2, 1, 5]
      >
  """
  @doc type: :element_wise
  @spec rank(series :: Series.t(), opts :: Keyword.t()) :: Series.t()
  def rank(series, opts \\ [])

  def rank(series, opts) do
    opts = Keyword.validate!(opts, method: :average, descending: false, seed: nil)

    if K.not(K.in(opts[:method], [:average, :min, :max, :dense, :ordinal, :random])),
      do: raise(ArgumentError, "unsupported rank method #{inspect(opts[:method])}")

    apply_series(series, :rank, [opts[:method], opts[:descending], opts[:seed]])
  end

  @doc """
  Returns a slice of the series, with `size` elements starting at `offset`.

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 3, 4, 5])
      iex> Explorer.Series.slice(s, 1, 2)
      #Explorer.Series<
        Polars[2]
        s64 [2, 3]
      >

  Negative offsets count from the end of the series:

      iex> s = Explorer.Series.from_list([1, 2, 3, 4, 5])
      iex> Explorer.Series.slice(s, -3, 2)
      #Explorer.Series<
        Polars[2]
        s64 [3, 4]
      >

  If the offset runs past the end of the series,
  the series is empty:

      iex> s = Explorer.Series.from_list([1, 2, 3, 4, 5])
      iex> Explorer.Series.slice(s, 10, 3)
      #Explorer.Series<
        Polars[0]
        s64 []
      >

  If the size runs past the end of the series,
  the result may be shorter than the size:

      iex> s = Explorer.Series.from_list([1, 2, 3, 4, 5])
      iex> Explorer.Series.slice(s, -3, 4)
      #Explorer.Series<
        Polars[3]
        s64 [3, 4, 5]
      >
  """
  @doc type: :shape
  @spec slice(series :: Series.t(), offset :: integer(), size :: integer()) :: Series.t()
  def slice(series, offset, size), do: apply_series(series, :slice, [offset, size])

  @doc """
  Slices the elements at the given indices as a new series.

  The indices may be either a list (or series) of indices or a range.
  A list or series of indices does not support negative numbers.
  Ranges may be negative on either end, which are then
  normalized. Note ranges in Elixir are inclusive.

  ## Examples

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.slice(s, [0, 2])
      #Explorer.Series<
        Polars[2]
        string ["a", "c"]
      >

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.slice(s, 1..2)
      #Explorer.Series<
        Polars[2]
        string ["b", "c"]
      >

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.slice(s, -2..-1)
      #Explorer.Series<
        Polars[2]
        string ["b", "c"]
      >

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.slice(s, 3..2//1)
      #Explorer.Series<
        Polars[0]
        string []
      >

  """
  @doc type: :shape
  @spec slice(series :: Series.t(), indices :: [integer()] | Range.t() | Series.t()) :: Series.t()
  def slice(series, indices) when is_list(indices),
    do: apply_series(series, :slice, [indices])

  def slice(series, %Series{dtype: int_dtype} = indices) when K.in(int_dtype, @integer_types),
    do: apply_series(series, :slice, [indices])

  def slice(_series, %Series{dtype: invalid_dtype}),
    do: dtype_error("slice/2", invalid_dtype, @integer_types)

  def slice(series, first..last//1) do
    first = if first < 0, do: first + size(series), else: first
    last = if last < 0, do: last + size(series), else: last
    size = last - first + 1

    if K.and(first >= 0, size >= 0) do
      apply_series(series, :slice, [first, size])
    else
      apply_series(series, :slice, [[]])
    end
  end

  def slice(series, %Range{} = range),
    do: slice(series, Enum.slice(0..(size(series) - 1)//1, range))

  @doc """
  Returns the value of the series at the given index.

  This function will raise an error in case the index
  is out of bounds.

  ## Examples

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.at(s, 2)
      "c"

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.at(s, 4)
      ** (ArgumentError) index 4 out of bounds for series of size 3
  """
  @doc type: :shape
  @spec at(series :: Series.t(), idx :: integer()) :: any()
  def at(series, idx), do: fetch!(series, idx)

  @doc """
  Returns a string series with all values concatenated.

  ## Examples

      iex> s1 = Explorer.Series.from_list(["a", "b", "c"])
      iex> s2 = Explorer.Series.from_list(["d", "e", "f"])
      iex> s3 = Explorer.Series.from_list(["g", "h", "i"])
      iex> Explorer.Series.format([s1, s2, s3])
      #Explorer.Series<
        Polars[3]
        string ["adg", "beh", "cfi"]
      >

      iex> s1 = Explorer.Series.from_list(["a", "b", "c", "d"])
      iex> s2 = Explorer.Series.from_list([1, 2, 3, 4])
      iex> s3 = Explorer.Series.from_list([1.5, :nan, :infinity, :neg_infinity])
      iex> Explorer.Series.format([s1, "/", s2, "/", s3])
      #Explorer.Series<
        Polars[4]
        string ["a/1/1.5", "b/2/NaN", "c/3/inf", "d/4/-inf"]
      >

      iex> s1 = Explorer.Series.from_list([<<1>>, <<239, 191, 19>>], dtype: :binary)
      iex> s2 = Explorer.Series.from_list([<<3>>, <<4>>], dtype: :binary)
      iex> Explorer.Series.format([s1, s2])
      ** (RuntimeError) Polars Error: invalid utf8
  """
  @doc type: :shape
  @spec format([Series.t() | String.t()]) :: Series.t()
  def format([_ | _] = list) do
    list = cast_to_string(list)

    if impl = impl!(list) do
      impl.format(list)
    else
      [hd | rest] = list
      s = Series.from_list([hd], dtype: :string)
      impl!([s]).format([s | rest])
    end
  end

  defp cast_to_string(list) do
    Enum.map(list, fn
      %Series{dtype: :string} = s ->
        s

      %Series{} = s ->
        cast(s, :string)

      value when K.or(is_binary(value), K.is_nil(value)) ->
        value

      other ->
        raise ArgumentError,
              "format/1 expects a list of series or strings, got: #{inspect(other)}"
    end)
  end

  @doc """
  Concatenate one or more series.

  Type promotion may happen between numeric series or
  null series. All other dtypes must match.

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> s2 = Explorer.Series.from_list([4, 5, 6])
      iex> Explorer.Series.concat([s1, s2])
      #Explorer.Series<
        Polars[6]
        s64 [1, 2, 3, 4, 5, 6]
      >

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> s2 = Explorer.Series.from_list([4.0, 5.0, 6.4])
      iex> Explorer.Series.concat([s1, s2])
      #Explorer.Series<
        Polars[6]
        f64 [1.0, 2.0, 3.0, 4.0, 5.0, 6.4]
      >
  """
  @doc type: :shape
  @spec concat([Series.t()]) :: Series.t()
  def concat([%Series{} | _t] = series) do
    dtypes = series |> Enum.map(& &1.dtype) |> Enum.uniq()

    series =
      case List.delete(dtypes, :null) do
        [] ->
          series

        [dtype] ->
          if Enum.member?(dtypes, :null), do: Enum.map(series, &cast(&1, dtype)), else: series

        dtypes ->
          dtype =
            Enum.reduce(dtypes, fn left, right ->
              Shared.merge_numeric_dtype(left, right) ||
                raise ArgumentError,
                      "cannot concatenate series with mismatched dtypes: #{inspect(dtypes)}. " <>
                        "First cast the series to the desired dtype."
            end)

          Enum.map(series, &cast(&1, dtype))
      end

    impl!(series).concat(series)
  end

  @doc """
  Concatenate two series.

  `concat(s1, s2)` is equivalent to `concat([s1, s2])`.
  """
  @doc type: :shape
  @spec concat(s1 :: Series.t(), s2 :: Series.t()) :: Series.t()
  def concat(%Series{} = s1, %Series{} = s2),
    do: concat([s1, s2])

  @doc """
  Finds the first non-missing element at each position.

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, nil, nil])
      iex> s2 = Explorer.Series.from_list([1, 2, nil, 4])
      iex> s3 = Explorer.Series.from_list([nil, nil, 3, 4])
      iex> Explorer.Series.coalesce([s1, s2, s3])
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, 3, 4]
      >
  """
  @doc type: :element_wise
  @spec coalesce([Series.t()]) :: Series.t()
  def coalesce([%Series{} = h | t]),
    do: Enum.reduce(t, h, &coalesce(&2, &1))

  @doc """
  Finds the first non-missing element at each position.

  `coalesce(s1, s2)` is equivalent to `coalesce([s1, s2])`.

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, nil, 3, nil])
      iex> s2 = Explorer.Series.from_list([1, 2, nil, 4])
      iex> Explorer.Series.coalesce(s1, s2)
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, 3, 4]
      >

      iex> s1 = Explorer.Series.from_list(["foo", nil, "bar", nil])
      iex> s2 = Explorer.Series.from_list([1, 2, nil, 4])
      iex> Explorer.Series.coalesce(s1, s2)
      ** (ArgumentError) cannot invoke Explorer.Series.coalesce/2 with mismatched dtypes: :string and {:s, 64}
  """
  @doc type: :element_wise
  @spec coalesce(s1 :: Series.t(), s2 :: Series.t()) :: Series.t()
  def coalesce(s1, s2) do
    :ok = check_dtypes_for_coalesce!(s1, s2)
    apply_series_list(:coalesce, [s1, s2])
  end

  # Aggregation

  @doc """
  Gets the sum of the series.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:boolean`

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 3])
      iex> Explorer.Series.sum(s)
      6

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 3.0])
      iex> Explorer.Series.sum(s)
      6.0

      iex> s = Explorer.Series.from_list([true, false, true])
      iex> Explorer.Series.sum(s)
      2

      iex> s = Explorer.Series.from_list([~D[2021-01-01], ~D[1999-12-31]])
      iex> Explorer.Series.sum(s)
      ** (ArgumentError) Explorer.Series.sum/1 not implemented for dtype :date. Valid dtypes are :boolean, {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :aggregation
  @spec sum(series :: Series.t()) :: number() | non_finite() | nil
  def sum(%Series{dtype: dtype} = series) when is_numeric_or_bool_dtype(dtype),
    do: apply_series(series, :sum)

  def sum(%Series{dtype: dtype}),
    do: dtype_error("sum/1", dtype, [:boolean | @numeric_dtypes])

  @doc """
  Gets the minimum value of the series.

  ## Supported dtypes


    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:date`
    * `:time`
    * `:datetime`
    * `:duration`

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 3])
      iex> Explorer.Series.min(s)
      1

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 3.0])
      iex> Explorer.Series.min(s)
      1.0

      iex> s = Explorer.Series.from_list([~D[2021-01-01], ~D[1999-12-31]])
      iex> Explorer.Series.min(s)
      ~D[1999-12-31]

      iex> s = Explorer.Series.from_list([~N[2021-01-01 00:00:00], ~N[1999-12-31 00:00:00]])
      iex> Explorer.Series.min(s)
      ~N[1999-12-31 00:00:00.000000]

      iex> s = Explorer.Series.from_list([~T[00:02:03.000451], ~T[00:05:04.000134]])
      iex> Explorer.Series.min(s)
      ~T[00:02:03.000451]

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.min(s)
      ** (ArgumentError) Explorer.Series.min/1 not implemented for dtype :string. Valid dtypes are :date, :time, {:datetime, :microsecond}, {:datetime, :millisecond}, {:datetime, :nanosecond}, {:duration, :microsecond}, {:duration, :millisecond}, {:duration, :nanosecond}, {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :aggregation
  @spec min(series :: Series.t()) ::
          number() | non_finite() | Date.t() | Time.t() | NaiveDateTime.t() | nil
  def min(%Series{dtype: dtype} = series) when is_numeric_or_temporal_dtype(dtype),
    do: apply_series(series, :min)

  def min(%Series{dtype: dtype}), do: dtype_error("min/1", dtype, @numeric_or_temporal_dtypes)

  @doc """
  Gets the maximum value of the series.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:date`
    * `:time`
    * `:datetime`
    * `:duration`

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 3])
      iex> Explorer.Series.max(s)
      3

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 3.0])
      iex> Explorer.Series.max(s)
      3.0

      iex> s = Explorer.Series.from_list([~D[2021-01-01], ~D[1999-12-31]])
      iex> Explorer.Series.max(s)
      ~D[2021-01-01]

      iex> s = Explorer.Series.from_list([~N[2021-01-01 00:00:00], ~N[1999-12-31 00:00:00]])
      iex> Explorer.Series.max(s)
      ~N[2021-01-01 00:00:00.000000]

      iex> s = Explorer.Series.from_list([~T[00:02:03.000212], ~T[00:05:04.000456]])
      iex> Explorer.Series.max(s)
      ~T[00:05:04.000456]

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.max(s)
      ** (ArgumentError) Explorer.Series.max/1 not implemented for dtype :string. Valid dtypes are :date, :time, {:datetime, :microsecond}, {:datetime, :millisecond}, {:datetime, :nanosecond}, {:duration, :microsecond}, {:duration, :millisecond}, {:duration, :nanosecond}, {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :aggregation
  @spec max(series :: Series.t()) ::
          number() | non_finite() | Date.t() | Time.t() | NaiveDateTime.t() | nil
  def max(%Series{dtype: dtype} = series) when is_numeric_or_temporal_dtype(dtype),
    do: apply_series(series, :max)

  def max(%Series{dtype: dtype}), do: dtype_error("max/1", dtype, @numeric_or_temporal_dtypes)

  @doc """
  Gets the index of the maximum value of the series.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:date`
    * `:time`
    * `:datetime`
    * `:duration`

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 3])
      iex> Explorer.Series.argmax(s)
      3

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 3.0])
      iex> Explorer.Series.argmax(s)
      3

      iex> s = Explorer.Series.from_list([~D[2021-01-01], ~D[1999-12-31]])
      iex> Explorer.Series.argmax(s)
      0

      iex> s = Explorer.Series.from_list([~N[2021-01-01 00:00:00], ~N[1999-12-31 00:00:00]])
      iex> Explorer.Series.argmax(s)
      0

      iex> s = Explorer.Series.from_list([~T[00:02:03.000212], ~T[00:05:04.000456]])
      iex> Explorer.Series.argmax(s)
      1

      iex> s = Explorer.Series.from_list([], dtype: :integer)
      iex> Explorer.Series.argmax(s)
      nil

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.argmax(s)
      ** (ArgumentError) Explorer.Series.argmax/1 not implemented for dtype :string. Valid dtypes are :date, :time, {:datetime, :microsecond}, {:datetime, :millisecond}, {:datetime, :nanosecond}, {:duration, :microsecond}, {:duration, :millisecond}, {:duration, :nanosecond}, {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :aggregation
  @spec argmax(series :: Series.t()) :: number() | non_finite() | nil
  def argmax(%Series{dtype: dtype} = series) when is_numeric_or_temporal_dtype(dtype),
    do: apply_series(series, :argmax)

  def argmax(%Series{dtype: dtype}),
    do: dtype_error("argmax/1", dtype, @numeric_or_temporal_dtypes)

  @doc """
  Gets the index of the minimum value of the series.

  Note that `nil` is ignored. In case an empty list
  or a series whose all elements are `nil` is used,
  the result will be `nil`.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:date`
    * `:time`
    * `:datetime`
    * `:duration`

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 3])
      iex> Explorer.Series.argmin(s)
      0

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 3.0])
      iex> Explorer.Series.argmin(s)
      0

      iex> s = Explorer.Series.from_list([~D[2021-01-01], ~D[1999-12-31]])
      iex> Explorer.Series.argmin(s)
      1

      iex> s = Explorer.Series.from_list([~N[2021-01-01 00:00:00], ~N[1999-12-31 00:00:00]])
      iex> Explorer.Series.argmin(s)
      1

      iex> s = Explorer.Series.from_list([~T[00:02:03.000212], ~T[00:05:04.000456]])
      iex> Explorer.Series.argmin(s)
      0

      iex> s = Explorer.Series.from_list([], dtype: :integer)
      iex> Explorer.Series.argmin(s)
      nil

      iex> s = Explorer.Series.from_list([nil], dtype: :integer)
      iex> Explorer.Series.argmin(s)
      nil

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.argmin(s)
      ** (ArgumentError) Explorer.Series.argmin/1 not implemented for dtype :string. Valid dtypes are :date, :time, {:datetime, :microsecond}, {:datetime, :millisecond}, {:datetime, :nanosecond}, {:duration, :microsecond}, {:duration, :millisecond}, {:duration, :nanosecond}, {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :aggregation
  @spec argmin(series :: Series.t()) :: number() | non_finite() | nil
  def argmin(%Series{dtype: dtype} = series) when is_numeric_or_temporal_dtype(dtype),
    do: apply_series(series, :argmin)

  def argmin(%Series{dtype: dtype}),
    do: dtype_error("argmin/1", dtype, @numeric_or_temporal_dtypes)

  @doc """
  Gets the mean value of the series.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 3])
      iex> Explorer.Series.mean(s)
      2.0

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 3.0])
      iex> Explorer.Series.mean(s)
      2.0

      iex> s = Explorer.Series.from_list([~D[2021-01-01], ~D[1999-12-31]])
      iex> Explorer.Series.mean(s)
      ** (ArgumentError) Explorer.Series.mean/1 not implemented for dtype :date. Valid dtypes are {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :aggregation
  @spec mean(series :: Series.t()) :: float() | non_finite() | nil
  def mean(%Series{dtype: dtype} = series) when is_numeric_dtype(dtype),
    do: apply_series(series, :mean)

  def mean(%Series{dtype: dtype}),
    do: dtype_error("mean/1", dtype, @numeric_dtypes)

  @doc """
  Gets the most common value(s) of the series.

  This function will return multiple values when there's a tie.

  ## Supported dtypes

  All except `:list` and `:struct`.

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 2, nil])
      iex> Explorer.Series.mode(s)
      #Explorer.Series<
        Polars[1]
        s64 [2]
      >

      iex> s = Explorer.Series.from_list(["a", "b", "b", "c"])
      iex> Explorer.Series.mode(s)
      #Explorer.Series<
        Polars[1]
        string ["b"]
      >

  This function can return multiple entries, but the order is not guaranteed.
  You may sort the series if desired.

      iex> s = Explorer.Series.from_list([1.0, 2.0, 2.0, 3.0, 3.0])
      iex> Explorer.Series.mode(s) |> Explorer.Series.sort()
      #Explorer.Series<
        Polars[2]
        f64 [2.0, 3.0]
      >
  """
  @doc type: :aggregation
  @spec mode(series :: Series.t()) :: Series.t() | nil
  def mode(%Series{dtype: {composite, _} = dtype}) when K.in(composite, [:list, :struct]),
    do: dtype_error("mode/1", dtype, Shared.dtypes() -- [{:list, :any}, {:struct, :any}])

  def mode(%Series{} = series),
    do: Shared.apply_impl(series, :mode)

  @doc """
  Gets the median value of the series.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 3])
      iex> Explorer.Series.median(s)
      2.0

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 3.0])
      iex> Explorer.Series.median(s)
      2.0

      iex> s = Explorer.Series.from_list([~D[2021-01-01], ~D[1999-12-31]])
      iex> Explorer.Series.median(s)
      ** (ArgumentError) Explorer.Series.median/1 not implemented for dtype :date. Valid dtypes are {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :aggregation
  @spec median(series :: Series.t()) :: float() | non_finite() | nil
  def median(%Series{dtype: dtype} = series) when is_numeric_dtype(dtype),
    do: apply_series(series, :median)

  def median(%Series{dtype: dtype}),
    do: dtype_error("median/1", dtype, @numeric_dtypes)

  @doc """
  Gets the variance of the series.

  By default, this is the sample variance. This function also takes an optional
  delta degrees of freedom (ddof). Setting this to zero corresponds to the population
  variance.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 3])
      iex> Explorer.Series.variance(s)
      1.0

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 3.0])
      iex> Explorer.Series.variance(s)
      1.0

      iex> s = Explorer.Series.from_list([~N[2021-01-01 00:00:00], ~N[1999-12-31 00:00:00]])
      iex> Explorer.Series.variance(s)
      ** (ArgumentError) Explorer.Series.variance/1 not implemented for dtype {:datetime, :microsecond}. Valid dtypes are {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :aggregation
  @spec variance(series :: Series.t(), ddof :: non_neg_integer()) :: float() | non_finite() | nil
  def variance(series, ddof \\ 1)

  def variance(%Series{dtype: dtype} = series, ddof) when is_numeric_dtype(dtype),
    do: apply_series(series, :variance, [ddof])

  def variance(%Series{dtype: dtype}, _), do: dtype_error("variance/1", dtype, @numeric_dtypes)

  @doc """
  Gets the standard deviation of the series.

  By default, this is the sample standard deviation. This function also takes an optional
  delta degrees of freedom (ddof). Setting this to zero corresponds to the population
  sample standard deviation.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 3])
      iex> Explorer.Series.standard_deviation(s)
      1.0

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 3.0])
      iex> Explorer.Series.standard_deviation(s)
      1.0

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.standard_deviation(s)
      ** (ArgumentError) Explorer.Series.standard_deviation/1 not implemented for dtype :string. Valid dtypes are {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :aggregation
  @spec standard_deviation(series :: Series.t(), ddof :: non_neg_integer()) ::
          float() | non_finite() | nil
  def standard_deviation(series, ddof \\ 1)

  def standard_deviation(%Series{dtype: dtype} = series, ddof) when is_numeric_dtype(dtype),
    do: apply_series(series, :standard_deviation, [ddof])

  def standard_deviation(%Series{dtype: dtype}, _),
    do: dtype_error("standard_deviation/1", dtype, @numeric_dtypes)

  @doc """
  Reduce this Series to the product value.

  Note that an empty series is going to result in a
  product of `1`. Values that are `nil` are ignored.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.product(s)
      6

      iex> s = Explorer.Series.from_list([], dtype: :float)
      iex> Explorer.Series.product(s)
      1.0

      iex> s = Explorer.Series.from_list([true, false, true])
      iex> Explorer.Series.product(s)
      ** (ArgumentError) Explorer.Series.product/1 not implemented for dtype :boolean. Valid dtypes are {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :aggregation
  @spec product(series :: Series.t()) :: float() | non_finite() | nil
  def product(%Series{dtype: dtype} = series) when is_numeric_dtype(dtype),
    do: at(apply_series(series, :product), 0)

  def product(%Series{dtype: dtype}),
    do: dtype_error("product/1", dtype, @numeric_dtypes)

  @doc """
  Gets the given quantile of the series.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:date`
    * `:time`
    * `:datetime`
    * `:duration`

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 3])
      iex> Explorer.Series.quantile(s, 0.2)
      1

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 3.0])
      iex> Explorer.Series.quantile(s, 0.5)
      2.0

      iex> s = Explorer.Series.from_list([~D[2021-01-01], ~D[1999-12-31]])
      iex> Explorer.Series.quantile(s, 0.5)
      ~D[2021-01-01]

      iex> s = Explorer.Series.from_list([~N[2021-01-01 00:00:00], ~N[1999-12-31 00:00:00]])
      iex> Explorer.Series.quantile(s, 0.5)
      ~N[2021-01-01 00:00:00.000000]

      iex> s = Explorer.Series.from_list([~T[01:55:00], ~T[15:35:00], ~T[23:00:00]])
      iex> Explorer.Series.quantile(s, 0.5)
      ~T[15:35:00]

      iex> s = Explorer.Series.from_list([true, false, true])
      iex> Explorer.Series.quantile(s, 0.5)
      ** (ArgumentError) Explorer.Series.quantile/2 not implemented for dtype :boolean. Valid dtypes are :date, :time, {:datetime, :microsecond}, {:datetime, :millisecond}, {:datetime, :nanosecond}, {:duration, :microsecond}, {:duration, :millisecond}, {:duration, :nanosecond}, {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :aggregation
  @spec quantile(series :: Series.t(), quantile :: float()) :: any()
  def quantile(%Series{dtype: dtype} = series, quantile)
      when is_numeric_or_temporal_dtype(dtype),
      do: apply_series(series, :quantile, [quantile])

  def quantile(%Series{dtype: dtype}, _),
    do: dtype_error("quantile/2", dtype, @numeric_or_temporal_dtypes)

  @doc """
  Compute the sample skewness of a series.

  For normally distributed data, the skewness should be about zero.

  For unimodal continuous distributions, a skewness value greater
  than zero means that there is more weight in the right tail of the
  distribution.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 3, 4, 5, 23])
      iex> Explorer.Series.skew(s)
      1.6727687946848508

      iex> s = Explorer.Series.from_list([1, 2, 3, 4, 5, 23])
      iex> Explorer.Series.skew(s, bias: false)
      2.2905330058490514

      iex> s = Explorer.Series.from_list([1, 2, 3, nil, 1])
      iex> Explorer.Series.skew(s, bias: false)
      0.8545630383279712

      iex> s = Explorer.Series.from_list([1, 2, 3, nil, 1])
      iex> Explorer.Series.skew(s)
      0.49338220021815865

      iex> s = Explorer.Series.from_list([true, false, true])
      iex> Explorer.Series.skew(s, false)
      ** (ArgumentError) Explorer.Series.skew/2 not implemented for dtype :boolean. Valid dtypes are {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :aggregation
  @spec skew(series :: Series.t(), opts :: Keyword.t()) :: float() | non_finite() | nil
  def skew(series, opts \\ [])

  def skew(%Series{dtype: dtype} = series, opts) when is_numeric_dtype(dtype) do
    opts = Keyword.validate!(opts, bias: true)
    apply_series(series, :skew, [opts[:bias]])
  end

  def skew(%Series{dtype: dtype}, _),
    do: dtype_error("skew/2", dtype, @numeric_dtypes)

  @doc """
  Compute the correlation between two series.

  The parameter `ddof` refers to the 'delta degrees of freedom' - the divisor
  used in the correlation calculation. Defaults to 1.

  The parameter `:method` refers to the correlation method. The following methods are available:
    - `:pearson` : Standard correlation coefficient. (default)
    - `:spearman` : Spearman rank correlation.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s1 = Series.from_list([1, 8, 3])
      iex> s2 = Series.from_list([4, 5, 2])
      iex> Series.correlation(s1, s2)
      0.5447047794019219
  """
  @doc type: :aggregation
  @spec correlation(
          left :: Series.t() | number(),
          right :: Series.t() | number(),
          opts :: Keyword.t()
        ) ::
          float() | non_finite() | nil
  def correlation(left, right, opts \\ []) do
    opts = Keyword.validate!(opts, ddof: 1, method: :pearson)

    if K.not(K.in(opts[:method], [:pearson, :spearman])),
      do: raise(ArgumentError, "unsupported correlation method #{inspect(opts[:method])}")

    basic_numeric_operation(:correlation, left, right, [opts[:ddof], opts[:method]])
  end

  @doc """
  Compute the covariance between two series.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s1 = Series.from_list([1, 8, 3])
      iex> s2 = Series.from_list([4, 5, 2])
      iex> Series.covariance(s1, s2)
      3.0
  """
  @doc type: :aggregation
  @spec covariance(
          left :: Series.t() | number(),
          right :: Series.t() | number(),
          ddof :: non_neg_integer()
        ) ::
          float() | non_finite() | nil
  def covariance(left, right, ddof \\ 1) do
    basic_numeric_operation(:covariance, left, right, [ddof])
  end

  @doc """
  Returns if all the values in a boolean series are true.

  ## Supported dtypes

    * `:boolean`

  ## Examples

      iex> s = Series.from_list([true, true, true])
      iex> Series.all?(s)
      true

      iex> s = Series.from_list([true, false, true])
      iex> Series.all?(s)
      false

      iex> s = Series.from_list([1, 2, 3])
      iex> Series.all?(s)
      ** (ArgumentError) Explorer.Series.all?/1 not implemented for dtype {:s, 64}. Valid dtype is :boolean

  An empty series will always return true:

      iex> s = Series.from_list([], dtype: :boolean)
      iex> Series.all?(s)
      true

  Opposite to Elixir but similar to databases, `nil` values are ignored:

      iex> s = Series.from_list([nil, true, true])
      iex> Series.all?(s)
      true

      iex> s = Series.from_list([nil, nil, nil], dtype: :boolean)
      iex> Series.all?(s)
      true
  """
  @doc type: :aggregation
  @spec all?(series :: Series.t()) :: boolean()
  def all?(%Series{dtype: :boolean} = series), do: apply_series(series, :all?)
  def all?(%Series{dtype: dtype}), do: dtype_error("all?/1", dtype, [:boolean])

  @doc """
  Returns if any of the values in a boolean series are true.

  ## Supported dtypes

    * `:boolean`

  ## Examples

      iex> s = Series.from_list([true, true, true])
      iex> Series.any?(s)
      true

      iex> s = Series.from_list([true, false, true])
      iex> Series.any?(s)
      true

      iex> s = Series.from_list([1, 2, 3])
      iex> Series.any?(s)
      ** (ArgumentError) Explorer.Series.any?/1 not implemented for dtype {:s, 64}. Valid dtype is :boolean

  An empty series will always return `false`:

      iex> s = Series.from_list([], dtype: :boolean)
      iex> Series.any?(s)
      false

  Opposite to Elixir but similar to databases, `nil` values are ignored:

      iex> s = Series.from_list([nil, true, true])
      iex> Series.any?(s)
      true

      iex> s = Series.from_list([nil, nil, nil], dtype: :boolean)
      iex> Series.any?(s)
      false
  """
  @doc type: :aggregation
  @spec any?(series :: Series.t()) :: boolean()
  def any?(%Series{dtype: :boolean} = series), do: apply_series(series, :any?)
  def any?(%Series{dtype: dtype}), do: dtype_error("any?/1", dtype, [:boolean])

  @doc """
  Returns a series of indexes for each item (row) in the series, starting from 0.

  ## Examples

      iex> s = Series.from_list([nil, true, true])
      iex> Series.row_index(s)
      #Explorer.Series<
        Polars[3]
        u32 [0, 1, 2]
      >

  This function can be used to add a row index as the first column of a dataframe.
  The resulting column is a regular column of type `:u32`.

      iex> require Explorer.DataFrame, as: DF
      iex> df = DF.new(a: [1, 3, 5], b: [2, 4, 6])
      iex> DF.mutate(df, index: row_index(a)) |> DF.relocate("index", before: 0)
      #Explorer.DataFrame<
        Polars[3 x 3]
        index u32 [0, 1, 2]
        a s64 [1, 3, 5]
        b s64 [2, 4, 6]
      >
      iex> df = DF.new(a: [1, 3, 5], b: [2, 4, 6])
      iex> DF.mutate(df, id: row_index(a) + 1000)
      #Explorer.DataFrame<
        Polars[3 x 3]
        a s64 [1, 3, 5]
        b s64 [2, 4, 6]
        id s64 [1000, 1001, 1002]
      >
  """
  @doc type: :element_wise
  @spec row_index(Series.t()) :: Series.t()
  def row_index(%Series{} = series), do: apply_series(series, :row_index)

  # Cumulative

  @doc """
  Calculates the cumulative maximum of the series.

  Optionally, can accumulate in reverse.

  Does not fill nil values. See `fill_missing/2`.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:date`
    * `:time`
    * `:datetime`
    * `:duration`

  ## Examples

      iex> s = [1, 2, 3, 4] |> Explorer.Series.from_list()
      iex> Explorer.Series.cumulative_max(s)
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, 3, 4]
      >

      iex> s = [1, 2, nil, 4] |> Explorer.Series.from_list()
      iex> Explorer.Series.cumulative_max(s)
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, nil, 4]
      >

      iex> s = [~T[03:00:02.000000], ~T[02:04:19.000000], nil, ~T[13:24:56.000000]] |> Explorer.Series.from_list()
      iex> Explorer.Series.cumulative_max(s)
      #Explorer.Series<
        Polars[4]
        time [03:00:02.000000, 03:00:02.000000, nil, 13:24:56.000000]
      >
  """
  @doc type: :window
  @spec cumulative_max(series :: Series.t(), opts :: Keyword.t()) :: Series.t()
  def cumulative_max(series, opts \\ [])

  def cumulative_max(%Series{dtype: dtype} = series, opts)
      when is_numeric_or_temporal_dtype(dtype) do
    opts = Keyword.validate!(opts, reverse: false)
    apply_series(series, :cumulative_max, [opts[:reverse]])
  end

  def cumulative_max(%Series{dtype: dtype}, _),
    do: dtype_error("cumulative_max/2", dtype, @numeric_or_temporal_dtypes)

  @doc """
  Calculates the cumulative minimum of the series.

  Optionally, can accumulate in reverse.

  Does not fill nil values. See `fill_missing/2`.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:date`
    * `:time`
    * `:datetime`
    * `:duration`

  ## Examples

      iex> s = [1, 2, 3, 4] |> Explorer.Series.from_list()
      iex> Explorer.Series.cumulative_min(s)
      #Explorer.Series<
        Polars[4]
        s64 [1, 1, 1, 1]
      >

      iex> s = [1, 2, nil, 4] |> Explorer.Series.from_list()
      iex> Explorer.Series.cumulative_min(s)
      #Explorer.Series<
        Polars[4]
        s64 [1, 1, nil, 1]
      >

      iex> s = [~T[03:00:02.000000], ~T[02:04:19.000000], nil, ~T[13:24:56.000000]] |> Explorer.Series.from_list()
      iex> Explorer.Series.cumulative_min(s)
      #Explorer.Series<
        Polars[4]
        time [03:00:02.000000, 02:04:19.000000, nil, 02:04:19.000000]
      >
  """
  @doc type: :window
  @spec cumulative_min(series :: Series.t(), opts :: Keyword.t()) :: Series.t()
  def cumulative_min(series, opts \\ [])

  def cumulative_min(%Series{dtype: dtype} = series, opts)
      when is_numeric_or_temporal_dtype(dtype) do
    opts = Keyword.validate!(opts, reverse: false)
    apply_series(series, :cumulative_min, [opts[:reverse]])
  end

  def cumulative_min(%Series{dtype: dtype}, _),
    do: dtype_error("cumulative_min/2", dtype, @numeric_or_temporal_dtypes)

  @doc """
  Calculates the cumulative sum of the series.

  Optionally, can accumulate in reverse.

  Does not fill nil values. See `fill_missing/2`.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:boolean`

  ## Examples

      iex> s = [1, 2, 3, 4] |> Explorer.Series.from_list()
      iex> Explorer.Series.cumulative_sum(s)
      #Explorer.Series<
        Polars[4]
        s64 [1, 3, 6, 10]
      >

      iex> s = [1, 2, nil, 4] |> Explorer.Series.from_list()
      iex> Explorer.Series.cumulative_sum(s)
      #Explorer.Series<
        Polars[4]
        s64 [1, 3, nil, 7]
      >
  """
  @doc type: :window
  @spec cumulative_sum(series :: Series.t(), opts :: Keyword.t()) :: Series.t()
  def cumulative_sum(series, opts \\ [])

  def cumulative_sum(%Series{dtype: dtype} = series, opts)
      when is_numeric_dtype(dtype) do
    opts = Keyword.validate!(opts, reverse: false)
    apply_series(series, :cumulative_sum, [opts[:reverse]])
  end

  def cumulative_sum(%Series{dtype: dtype}, _),
    do: dtype_error("cumulative_sum/2", dtype, @numeric_dtypes)

  @doc """
  Calculates the cumulative product of the series.

  Optionally, can accumulate in reverse.

  Does not fill nil values. See `fill_missing/2`.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s = [1, 2, 3, 2] |> Explorer.Series.from_list()
      iex> Explorer.Series.cumulative_product(s)
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, 6, 12]
      >

      iex> s = [1, 2, nil, 4] |> Explorer.Series.from_list()
      iex> Explorer.Series.cumulative_product(s)
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, nil, 8]
      >
  """
  @doc type: :window
  @spec cumulative_product(series :: Series.t(), opts :: Keyword.t()) :: Series.t()
  def cumulative_product(series, opts \\ [])

  def cumulative_product(%Series{dtype: dtype} = series, opts)
      when is_numeric_dtype(dtype) do
    opts = Keyword.validate!(opts, reverse: false)
    apply_series(series, :cumulative_product, [opts[:reverse]])
  end

  def cumulative_product(%Series{dtype: dtype}, _),
    do: dtype_error("cumulative_product/2", dtype, @numeric_dtypes)

  # Local minima/maxima

  @doc """
  Returns a boolean mask with `true` where the 'peaks' (series max or min, default max) are.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:date`
    * `:time`
    * `:datetime`
    * `:duration`

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 4, 1, 4])
      iex> Explorer.Series.peaks(s)
      #Explorer.Series<
        Polars[5]
        boolean [false, false, true, false, true]
      >

      iex> s = [~T[03:00:02.000000], ~T[13:24:56.000000], ~T[02:04:19.000000]] |> Explorer.Series.from_list()
      iex> Explorer.Series.peaks(s)
      #Explorer.Series<
        Polars[3]
        boolean [false, true, false]
      >
  """
  @doc type: :element_wise
  @spec peaks(series :: Series.t(), max_or_min :: :max | :min) :: Series.t()
  def peaks(series, max_or_min \\ :max)

  def peaks(%Series{dtype: dtype} = series, max_or_min)
      when is_numeric_or_temporal_dtype(dtype),
      do: apply_series(series, :peaks, [max_or_min])

  def peaks(%Series{dtype: dtype}, _),
    do: dtype_error("peaks/2", dtype, @numeric_or_temporal_dtypes)

  # Arithmetic

  defp cast_for_arithmetic(function, [_, _] = args) do
    args
    |> case do
      [%Series{}, %Series{}] -> args
      [left, %Series{} = right] -> [from_list([left]), right]
      [%Series{} = left, right] -> [left, from_list([right])]
      [left, right] -> no_series_error(function, left, right)
    end
    |> enforce_highest_precision()
  end

  # TODO: maybe we can move this casting to Rust.
  defp enforce_highest_precision([
         %Series{dtype: {left_base, left_timeunit}} = left,
         %Series{dtype: {right_base, right_timeunit}} = right
       ])
       when K.and(is_atom(left_timeunit), is_atom(right_timeunit)) do
    # Higher precision wins, otherwise information is lost.
    case {left_timeunit, right_timeunit} do
      {equal, equal} -> [left, right]
      {:nanosecond, _} -> [left, cast(right, {right_base, :nanosecond})]
      {_, :nanosecond} -> [cast(left, {left_base, :nanosecond}), right]
      {:microsecond, _} -> [left, cast(right, {right_base, :microsecond})]
      {_, :microsecond} -> [cast(left, {left_base, :microsecond}), right]
    end
  end

  defp enforce_highest_precision(args), do: args

  @doc """
  Adds right to left, element-wise.

  When mixing floats and integers, the resulting series will have dtype `{:f, 64}`.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> s2 = Explorer.Series.from_list([4, 5, 6])
      iex> Explorer.Series.add(s1, s2)
      #Explorer.Series<
        Polars[3]
        s64 [5, 7, 9]
      >

  You can also use scalar values on both sides:

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.add(s1, 2)
      #Explorer.Series<
        Polars[3]
        s64 [3, 4, 5]
      >

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.add(2, s1)
      #Explorer.Series<
        Polars[3]
        s64 [3, 4, 5]
      >
  """
  @doc type: :element_wise
  @spec add(
          left :: Series.t() | number() | Date.t() | NaiveDateTime.t() | Duration.t(),
          right :: Series.t() | number() | Date.t() | NaiveDateTime.t() | Duration.t()
        ) :: Series.t()
  def add(left, right) do
    [left, right] = cast_for_arithmetic("add/2", [left, right])

    if out_dtype = cast_to_add(dtype(left), dtype(right)) do
      apply_series_list(:add, [out_dtype, left, right])
    else
      dtype_mismatch_error("add/2", left, right)
    end
  end

  defp cast_to_add(:date, {:duration, _}), do: :date
  defp cast_to_add({:duration, _}, :date), do: :date
  defp cast_to_add({:datetime, p}, {:duration, p}), do: {:datetime, p}
  defp cast_to_add({:duration, p}, {:datetime, p}), do: {:datetime, p}
  defp cast_to_add({:duration, p}, {:duration, p}), do: {:duration, p}
  defp cast_to_add(left, right), do: Shared.merge_numeric_dtype(left, right)

  @doc """
  Subtracts right from left, element-wise.

  When mixing floats and integers, the resulting series will have dtype `{:f, 64}`.
  In case both series are of unsigned integers, we will try to subtract,
  but an exception is raised if overflow occurs.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> s2 = Explorer.Series.from_list([4, 5, 6])
      iex> Explorer.Series.subtract(s1, s2)
      #Explorer.Series<
        Polars[3]
        s64 [-3, -3, -3]
      >

  You can also use scalar values on both sides:

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.subtract(s1, 2)
      #Explorer.Series<
        Polars[3]
        s64 [-1, 0, 1]
      >

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.subtract(2, s1)
      #Explorer.Series<
        Polars[3]
        s64 [1, 0, -1]
      >
  """
  @doc type: :element_wise
  @spec subtract(
          left :: Series.t() | number() | Date.t() | NaiveDateTime.t() | Duration.t(),
          right :: Series.t() | number() | Date.t() | NaiveDateTime.t() | Duration.t()
        ) :: Series.t()
  def subtract(left, right) do
    [left, right] = cast_for_arithmetic("subtract/2", [left, right])

    if out_dtype = cast_to_subtract(dtype(left), dtype(right)) do
      apply_series_list(:subtract, [out_dtype, left, right])
    else
      dtype_mismatch_error("subtract/2", left, right)
    end
  end

  defp cast_to_subtract(:date, :date), do: {:duration, :millisecond}
  defp cast_to_subtract(:date, {:duration, _}), do: :date
  defp cast_to_subtract({:datetime, p}, {:datetime, p}), do: {:duration, p}
  defp cast_to_subtract({:datetime, p}, {:duration, p}), do: {:datetime, p}
  defp cast_to_subtract({:duration, p}, {:duration, p}), do: {:duration, p}
  defp cast_to_subtract(left, right), do: Shared.merge_numeric_dtype(left, right)

  @doc """
  Multiplies left and right, element-wise.

  When mixing floats and integers, the resulting series will have dtype `{:f, 64}`.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s1 = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> s2 = 11..20 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.multiply(s1, s2)
      #Explorer.Series<
        Polars[10]
        s64 [11, 24, 39, 56, 75, 96, 119, 144, 171, 200]
      >

      iex> s1 = 1..5 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.multiply(s1, 2)
      #Explorer.Series<
        Polars[5]
        s64 [2, 4, 6, 8, 10]
      >
  """
  @doc type: :element_wise
  @spec multiply(
          left :: Series.t() | number() | Duration.t(),
          right :: Series.t() | number() | Duration.t()
        ) :: Series.t()
  def multiply(left, right) do
    [left, right] = cast_for_arithmetic("multiply/2", [left, right])

    if out_dtype = cast_to_multiply(dtype(left), dtype(right)) do
      apply_series_list(:multiply, [out_dtype, left, right])
    else
      dtype_mismatch_error("multiply/2", left, right)
    end
  end

  defp cast_to_multiply({:s, _}, {:duration, p}), do: {:duration, p}
  defp cast_to_multiply({:duration, p}, {:s, _}), do: {:duration, p}
  defp cast_to_multiply({:f, _}, {:duration, p}), do: {:duration, p}
  defp cast_to_multiply({:duration, p}, {:f, _}), do: {:duration, p}
  defp cast_to_multiply(left, right), do: Shared.merge_numeric_dtype(left, right)

  @doc """
  Divides left by right, element-wise.

  The resulting series will have the dtype as `{:f, 64}`.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s1 = [10, 10, 10] |> Explorer.Series.from_list()
      iex> s2 = [2, 2, 2] |> Explorer.Series.from_list()
      iex> Explorer.Series.divide(s1, s2)
      #Explorer.Series<
        Polars[3]
        f64 [5.0, 5.0, 5.0]
      >

      iex> s1 = [10, 10, 10] |> Explorer.Series.from_list()
      iex> Explorer.Series.divide(s1, 2)
      #Explorer.Series<
        Polars[3]
        f64 [5.0, 5.0, 5.0]
      >

      iex> s1 = [10, 52 ,10] |> Explorer.Series.from_list()
      iex> Explorer.Series.divide(s1, 2.5)
      #Explorer.Series<
        Polars[3]
        f64 [4.0, 20.8, 4.0]
      >

      iex> s1 = [10, 10, 10] |> Explorer.Series.from_list()
      iex> s2 = [2, 0, 2] |> Explorer.Series.from_list()
      iex> Explorer.Series.divide(s1, s2)
      #Explorer.Series<
        Polars[3]
        f64 [5.0, Inf, 5.0]
      >
  """
  @doc type: :element_wise
  @spec divide(
          left :: Series.t() | number() | Duration.t(),
          right :: Series.t() | number()
        ) :: Series.t()
  def divide(left, right) do
    [left, right] = cast_for_arithmetic("divide/2", [left, right])

    if out_dtype = cast_to_divide(dtype(left), dtype(right)) do
      apply_series_list(:divide, [out_dtype, left, right])
    else
      case dtype(right) do
        {:duration, _} -> raise(ArgumentError, "cannot divide by duration")
        _ -> dtype_mismatch_error("divide/2", left, right)
      end
    end
  end

  # Review the size needed for this operation.
  defp cast_to_divide({int_type, _}, {int_type, _}) when K.in(int_type, [:s, :u]), do: {:f, 64}
  defp cast_to_divide({:s, _}, {:u, _}), do: {:f, 64}
  defp cast_to_divide({:u, _}, {:s, _}), do: {:f, 64}
  defp cast_to_divide({int_type, _}, {:f, _} = float) when K.in(int_type, [:s, :u]), do: float
  defp cast_to_divide({:f, _} = float, {int_type, _}) when K.in(int_type, [:s, :u]), do: float
  defp cast_to_divide({:f, left}, {:f, right}), do: {:f, max(left, right)}
  defp cast_to_divide({:duration, p}, {:s, _}), do: {:duration, p}
  defp cast_to_divide({:duration, p}, {:f, _}), do: {:duration, p}
  defp cast_to_divide(_, _), do: nil

  @doc """
  Raises a numeric series to the power of the exponent.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s = [8, 16, 32] |> Explorer.Series.from_list()
      iex> Explorer.Series.pow(s, 2.0)
      #Explorer.Series<
        Polars[3]
        f64 [64.0, 256.0, 1024.0]
      >

      iex> s = [2, 4, 6] |> Explorer.Series.from_list()
      iex> Explorer.Series.pow(s, 3)
      #Explorer.Series<
        Polars[3]
        f64 [8.0, 64.0, 216.0]
      >

      iex> s = [2, 4, 6] |> Explorer.Series.from_list()
      iex> Explorer.Series.pow(s, -3.0)
      #Explorer.Series<
        Polars[3]
        f64 [0.125, 0.015625, 0.004629629629629629]
      >

      iex> s = [1.0, 2.0, 3.0] |> Explorer.Series.from_list()
      iex> Explorer.Series.pow(s, 3.0)
      #Explorer.Series<
        Polars[3]
        f64 [1.0, 8.0, 27.0]
      >

      iex> s = [2.0, 4.0, 6.0] |> Explorer.Series.from_list()
      iex> Explorer.Series.pow(s, 2)
      #Explorer.Series<
        Polars[3]
        f64 [4.0, 16.0, 36.0]
      >
  """
  @doc type: :element_wise
  @spec pow(left :: Series.t() | number(), right :: Series.t() | number()) :: Series.t()
  def pow(left, right) do
    [left, right] = cast_for_arithmetic("pow/2", [left, right])

    if out_dtype = cast_to_pow(dtype(left), dtype(right)) do
      apply_series_list(:pow, [out_dtype, left, right])
    else
      dtype_mismatch_error("pow/2", left, right)
    end
  end

  defp cast_to_pow({:u, l}, {:u, r}), do: {:u, max(l, r)}
  defp cast_to_pow({:s, s}, {:u, u}), do: {:s, min(64, max(2 * u, s))}
  defp cast_to_pow({:f, l}, {:f, r}), do: {:f, max(l, r)}
  defp cast_to_pow({:f, l}, {n, _}) when K.in(n, [:u, :s]), do: {:f, l}
  defp cast_to_pow({n, _}, {:f, r}) when K.in(n, [:u, :s]), do: {:f, r}
  defp cast_to_pow({n, _}, {:s, _}) when K.in(n, [:u, :s]), do: {:f, 64}
  defp cast_to_pow(_, _), do: nil

  @doc """
  Calculates the natural logarithm.

  The resultant series is going to be of dtype `{:f, 64}`.
  See `log/2` for passing a custom base.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 3, nil, 4])
      iex> Explorer.Series.log(s)
      #Explorer.Series<
        Polars[5]
        f64 [0.0, 0.6931471805599453, 1.0986122886681098, nil, 1.3862943611198906]
      >

  """
  @doc type: :element_wise
  @spec log(argument :: Series.t()) :: Series.t()
  def log(%Series{} = s), do: apply_series(s, :log, [])

  @doc """
  Calculates the logarithm on a given base.

  The resultant series is going to be of dtype `{:f, 64}`.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s = Explorer.Series.from_list([8, 16, 32])
      iex> Explorer.Series.log(s, 2)
      #Explorer.Series<
        Polars[3]
        f64 [3.0, 4.0, 5.0]
      >

  """
  @doc type: :element_wise
  @spec log(argument :: Series.t(), base :: number()) :: Series.t()
  def log(%Series{dtype: dtype} = series, base)
      when K.and(is_numeric_dtype(dtype), is_number(base)) do
    if base <= 0, do: raise(ArgumentError, "base must be a positive number")
    if base == 1, do: raise(ArgumentError, "base cannot be equal to 1")

    base = if is_integer(base), do: base / 1.0, else: base
    apply_series(series, :log, [base])
  end

  @doc """
  Calculates the exponential of all elements.
  """
  @doc type: :element_wise
  @spec exp(Series.t()) :: Series.t()
  def exp(%Series{} = series), do: apply_series(series, :exp, [])

  @doc """
  Element-wise integer division.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Supported dtype

    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  Returns `nil` if there is a zero in the right-hand side.

  ## Examples

      iex> s1 = [10, 11, 10] |> Explorer.Series.from_list()
      iex> s2 = [2, 2, 2] |> Explorer.Series.from_list()
      iex> Explorer.Series.quotient(s1, s2)
      #Explorer.Series<
        Polars[3]
        s64 [5, 5, 5]
      >

      iex> s1 = [10, 11, 10] |> Explorer.Series.from_list()
      iex> s2 = [2, 2, 0] |> Explorer.Series.from_list()
      iex> Explorer.Series.quotient(s1, s2)
      #Explorer.Series<
        Polars[3]
        s64 [5, 5, nil]
      >

      iex> s1 = [10, 12, 15] |> Explorer.Series.from_list()
      iex> Explorer.Series.quotient(s1, 3)
      #Explorer.Series<
        Polars[3]
        s64 [3, 4, 5]
      >

  """
  @doc type: :element_wise
  @spec quotient(left :: Series.t(), right :: Series.t() | integer()) :: Series.t()
  def quotient(%Series{dtype: l_dtype} = left, %Series{dtype: r_dtype} = right)
      when K.and(K.in(l_dtype, @integer_types), K.in(r_dtype, @integer_types)),
      do: apply_series_list(:quotient, [left, right])

  def quotient(%Series{dtype: l_dtype} = left, right)
      when K.and(K.in(l_dtype, @integer_types), is_integer(right)),
      do: apply_series_list(:quotient, [left, from_list([right])])

  def quotient(left, %Series{dtype: r_dtype} = right)
      when K.and(K.in(r_dtype, @integer_types), is_integer(left)),
      do: apply_series_list(:quotient, [from_list([left]), right])

  @doc """
  Computes the remainder of an element-wise integer division.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Supported dtype

    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  Returns `nil` if there is a zero in the right-hand side.

  ## Examples

      iex> s1 = [10, 11, 10] |> Explorer.Series.from_list()
      iex> s2 = [2, 2, 2] |> Explorer.Series.from_list()
      iex> Explorer.Series.remainder(s1, s2)
      #Explorer.Series<
        Polars[3]
        s64 [0, 1, 0]
      >

      iex> s1 = [10, 11, 10] |> Explorer.Series.from_list()
      iex> s2 = [2, 2, 0] |> Explorer.Series.from_list()
      iex> Explorer.Series.remainder(s1, s2)
      #Explorer.Series<
        Polars[3]
        s64 [0, 1, nil]
      >

      iex> s1 = [10, 11, 9] |> Explorer.Series.from_list()
      iex> Explorer.Series.remainder(s1, 3)
      #Explorer.Series<
        Polars[3]
        s64 [1, 2, 0]
      >

  """
  @doc type: :element_wise
  @spec remainder(left :: Series.t(), right :: Series.t() | integer()) :: Series.t()
  def remainder(%Series{dtype: l_dtype} = left, %Series{dtype: r_dtype} = right)
      when K.and(K.in(l_dtype, @integer_types), K.in(r_dtype, @integer_types)),
      do: apply_series_list(:remainder, [left, right])

  def remainder(%Series{dtype: l_dtype} = left, right)
      when K.and(K.in(l_dtype, @integer_types), is_integer(right)),
      do: apply_series_list(:remainder, [left, from_list([right])])

  def remainder(left, %Series{dtype: r_dtype} = right)
      when K.and(K.in(r_dtype, @integer_types), is_integer(left)),
      do: apply_series_list(:remainder, [from_list([left]), right])

  @doc """
  Computes the the sine of a number (in radians).
  The resultant series is going to be of dtype `{:f, 64}`, with values between 1 and -1.

  ## Supported dtype

    * `{:f, 32}`
    * `{:f, 64}`

  ## Examples

      iex> pi = :math.pi()
      iex> s = [-pi * 3/2, -pi, -pi / 2, -pi / 4, 0, pi / 4, pi / 2, pi, pi * 3/2] |> Explorer.Series.from_list()
      iex> Explorer.Series.sin(s)
      #Explorer.Series<
        Polars[9]
        f64 [1.0, -1.2246467991473532e-16, -1.0, -0.7071067811865475, 0.0, 0.7071067811865475, 1.0, 1.2246467991473532e-16, -1.0]
      >
  """
  @doc type: :float_wise
  @spec sin(series :: Series.t()) :: Series.t()
  def sin(%Series{dtype: dtype} = series) when K.in(dtype, @float_dtypes),
    do: apply_series(series, :sin)

  def sin(%Series{dtype: dtype}),
    do: dtype_error("sin/1", dtype, [{:f, 32}, {:f, 64}])

  @doc """
  Computes the the cosine of a number (in radians).
  The resultant series is going to be of dtype `{:f, 64}`, with values between 1 and -1.

  ## Supported dtype

    * `{:f, 32}`
    * `{:f, 64}`

  ## Examples

      iex> pi = :math.pi()
      iex> s = [-pi * 3/2, -pi, -pi / 2, -pi / 4, 0, pi / 4, pi / 2, pi, pi * 3/2] |> Explorer.Series.from_list()
      iex> Explorer.Series.cos(s)
      #Explorer.Series<
        Polars[9]
        f64 [-1.8369701987210297e-16, -1.0, 6.123233995736766e-17, 0.7071067811865476, 1.0, 0.7071067811865476, 6.123233995736766e-17, -1.0, -1.8369701987210297e-16]
      >
  """
  @doc type: :float_wise
  @spec cos(series :: Series.t()) :: Series.t()
  def cos(%Series{dtype: dtype} = series) when K.in(dtype, @float_dtypes),
    do: apply_series(series, :cos)

  def cos(%Series{dtype: dtype}),
    do: dtype_error("cos/1", dtype, [{:f, 32}, {:f, 64}])

  @doc """
  Computes the tangent of a number (in radians).
  The resultant series is going to be of dtype `{:f, 64}`.

  ## Supported dtype

    * `{:f, 32}`
    * `{:f, 64}`

  ## Examples

      iex> pi = :math.pi()
      iex> s = [-pi * 3/2, -pi, -pi / 2, -pi / 4, 0, pi / 4, pi / 2, pi, pi * 3/2] |> Explorer.Series.from_list()
      iex> Explorer.Series.tan(s)
      #Explorer.Series<
        Polars[9]
        f64 [-5443746451065123.0, 1.2246467991473532e-16, -1.633123935319537e16, -0.9999999999999999, 0.0, 0.9999999999999999, 1.633123935319537e16, -1.2246467991473532e-16, 5443746451065123.0]
      >
  """
  @doc type: :float_wise
  @spec tan(series :: Series.t()) :: Series.t()
  def tan(%Series{dtype: dtype} = series) when K.in(dtype, @float_dtypes),
    do: apply_series(series, :tan)

  def tan(%Series{dtype: dtype}),
    do: dtype_error("tan/1", dtype, [{:f, 32}, {:f, 64}])

  @doc """
  Computes the the arcsine of a number.
  The resultant series is going to be of dtype `{:f, 64}`, in radians, with values between -pi/2 and pi/2.

  ## Supported dtype

    * `{:f, 32}`
    * `{:f, 64}`

  ## Examples

      iex> s = [1.0, 0.0, -1.0, -0.7071067811865475, 0.7071067811865475] |> Explorer.Series.from_list()
      iex> Explorer.Series.asin(s)
      #Explorer.Series<
        Polars[5]
        f64 [1.5707963267948966, 0.0, -1.5707963267948966, -0.7853981633974482, 0.7853981633974482]
      >
  """
  @doc type: :float_wise
  @spec asin(series :: Series.t()) :: Series.t()
  def asin(%Series{dtype: dtype} = series) when K.in(dtype, @float_dtypes),
    do: apply_series(series, :asin)

  def asin(%Series{dtype: dtype}),
    do: dtype_error("asin/1", dtype, [{:f, 32}, {:f, 64}])

  @doc """
  Computes the the arccosine of a number.
  The resultant series is going to be of dtype `{:f, 64}`, in radians, with values between 0 and pi.

  ## Supported dtype

    * `{:f, 32}`
    * `{:f, 64}`

  ## Examples

      iex> s = [1.0, 0.0, -1.0, -0.7071067811865475, 0.7071067811865475] |> Explorer.Series.from_list()
      iex> Explorer.Series.acos(s)
      #Explorer.Series<
        Polars[5]
        f64 [0.0, 1.5707963267948966, 3.141592653589793, 2.356194490192345, 0.7853981633974484]
      >
  """
  @doc type: :float_wise
  @spec acos(series :: Series.t()) :: Series.t()
  def acos(%Series{dtype: dtype} = series) when K.in(dtype, @float_dtypes),
    do: apply_series(series, :acos)

  def acos(%Series{dtype: dtype}),
    do: dtype_error("acos/1", dtype, [{:f, 32}, {:f, 64}])

  @doc """
  Computes the the arctangent of a number.
  The resultant series is going to be of dtype `{:f, 64}`, in radians, with values between -pi/2 and pi/2.

  ## Supported dtype

    * `{:f, 32}`
    * `{:f, 64}`

  ## Examples

      iex> s = [1.0, 0.0, -1.0, -0.7071067811865475, 0.7071067811865475] |> Explorer.Series.from_list()
      iex> Explorer.Series.atan(s)
      #Explorer.Series<
        Polars[5]
        f64 [0.7853981633974483, 0.0, -0.7853981633974483, -0.6154797086703873, 0.6154797086703873]
      >
  """
  @doc type: :float_wise
  @spec atan(series :: Series.t()) :: Series.t()
  def atan(%Series{dtype: dtype} = series) when K.in(dtype, @float_dtypes),
    do: apply_series(series, :atan)

  def atan(%Series{dtype: dtype}),
    do: dtype_error("atan/1", dtype, [{:f, 32}, {:f, 64}])

  defp basic_numeric_operation(operation, %Series{} = left, right, args) when is_numeric(right),
    do: basic_numeric_operation(operation, left, from_same_value(left, right), args)

  defp basic_numeric_operation(operation, left, %Series{} = right, args) when is_numeric(left),
    do: basic_numeric_operation(operation, from_same_value(right, left), right, args)

  defp basic_numeric_operation(
         operation,
         %Series{dtype: left_dtype} = left,
         %Series{dtype: right_dtype} = right,
         args
       )
       when K.and(is_numeric_dtype(left_dtype), is_numeric_dtype(right_dtype)),
       do: apply_series_list(operation, [left, right | args])

  defp basic_numeric_operation(operation, %Series{} = left, %Series{} = right, args),
    do: dtype_mismatch_error("#{operation}/#{length(args) + 2}", left, right)

  defp basic_numeric_operation(operation, _, %Series{dtype: dtype}, args),
    do: dtype_error("#{operation}/#{length(args) + 2}", dtype, @numeric_dtypes)

  defp basic_numeric_operation(operation, %Series{dtype: dtype}, _, args),
    do: dtype_error("#{operation}/#{length(args) + 2}", dtype, @numeric_dtypes)

  defp basic_numeric_operation(operation, left, right, args)
       when K.and(is_numeric(left), is_numeric(right)),
       do: no_series_error("#{operation}/#{length(args) + 2}", left, right)

  defp no_series_error(function, left, right) do
    raise ArgumentError,
          "#{function} expects a series as one of its arguments, " <>
            "instead got two scalars: #{inspect(left)} and #{inspect(right)}"
  end

  # Comparisons

  @doc """
  Returns boolean mask of `left == right`, element-wise.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> s2 = Explorer.Series.from_list([1, 2, 4])
      iex> Explorer.Series.equal(s1, s2)
      #Explorer.Series<
        Polars[3]
        boolean [true, true, false]
      >

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.equal(s, 1)
      #Explorer.Series<
        Polars[3]
        boolean [true, false, false]
      >

      iex> s = Explorer.Series.from_list([true, true, false])
      iex> Explorer.Series.equal(s, true)
      #Explorer.Series<
        Polars[3]
        boolean [true, true, false]
      >

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.equal(s, "a")
      #Explorer.Series<
        Polars[3]
        boolean [true, false, false]
      >

      iex> s = Explorer.Series.from_list([~D[2021-01-01], ~D[1999-12-31]])
      iex> Explorer.Series.equal(s, ~D[1999-12-31])
      #Explorer.Series<
        Polars[2]
        boolean [false, true]
      >

      iex> s = Explorer.Series.from_list([~N[2022-01-01 00:00:00], ~N[2022-01-01 23:00:00]])
      iex> Explorer.Series.equal(s, ~N[2022-01-01 00:00:00])
      #Explorer.Series<
        Polars[2]
        boolean [true, false]
      >

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.equal(s, false)
      ** (ArgumentError) cannot invoke Explorer.Series.equal/2 with mismatched dtypes: :string and false
  """
  @doc type: :element_wise
  @spec equal(
          left :: Series.t() | number() | Date.t() | NaiveDateTime.t() | boolean() | String.t(),
          right :: Series.t() | number() | Date.t() | NaiveDateTime.t() | boolean() | String.t()
        ) :: Series.t()
  def equal(left, right) do
    if args = cast_for_comparable_operation(left, right) do
      apply_series_list(:equal, args)
    else
      dtype_mismatch_error("equal/2", left, right)
    end
  end

  @doc """
  Returns boolean mask of `left != right`, element-wise.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> s2 = Explorer.Series.from_list([1, 2, 4])
      iex> Explorer.Series.not_equal(s1, s2)
      #Explorer.Series<
        Polars[3]
        boolean [false, false, true]
      >

      iex> s = Explorer.Series.from_list([1, 2, 3])
      iex> Explorer.Series.not_equal(s, 1)
      #Explorer.Series<
        Polars[3]
        boolean [false, true, true]
      >

      iex> s = Explorer.Series.from_list([true, true, false])
      iex> Explorer.Series.not_equal(s, true)
      #Explorer.Series<
        Polars[3]
        boolean [false, false, true]
      >

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.not_equal(s, "a")
      #Explorer.Series<
        Polars[3]
        boolean [false, true, true]
      >

      iex> s = Explorer.Series.from_list([~D[2021-01-01], ~D[1999-12-31]])
      iex> Explorer.Series.not_equal(s, ~D[1999-12-31])
      #Explorer.Series<
        Polars[2]
        boolean [true, false]
      >

      iex> s = Explorer.Series.from_list([~N[2022-01-01 00:00:00], ~N[2022-01-01 23:00:00]])
      iex> Explorer.Series.not_equal(s, ~N[2022-01-01 00:00:00])
      #Explorer.Series<
        Polars[2]
        boolean [false, true]
      >

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.not_equal(s, false)
      ** (ArgumentError) cannot invoke Explorer.Series.not_equal/2 with mismatched dtypes: :string and false
  """
  @doc type: :element_wise
  @spec not_equal(
          left :: Series.t() | number() | Date.t() | NaiveDateTime.t() | boolean() | String.t(),
          right :: Series.t() | number() | Date.t() | NaiveDateTime.t() | boolean() | String.t()
        ) :: Series.t()
  def not_equal(left, right) do
    if args = cast_for_comparable_operation(left, right) do
      apply_series_list(:not_equal, args)
    else
      dtype_mismatch_error("not_equal/2", left, right)
    end
  end

  @doc """
  Returns boolean mask of `left > right`, element-wise.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:date`
    * `:time`
    * `:datetime`
    * `:duration`

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> s2 = Explorer.Series.from_list([1, 2, 4])
      iex> Explorer.Series.greater(s1, s2)
      #Explorer.Series<
        Polars[3]
        boolean [false, false, false]
      >
  """
  @doc type: :element_wise
  @spec greater(
          left :: Series.t() | number() | Date.t() | NaiveDateTime.t(),
          right :: Series.t() | number() | Date.t() | NaiveDateTime.t()
        ) :: Series.t()
  def greater(left, right) do
    if args = cast_for_ordered_operation(left, right) do
      apply_series_list(:greater, args)
    else
      dtype_mismatch_error("greater/2", left, right, @numeric_or_temporal_dtypes)
    end
  end

  @doc """
  Returns boolean mask of `left >= right`, element-wise.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:date`
    * `:time`
    * `:datetime`
    * `:duration`

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> s2 = Explorer.Series.from_list([1, 2, 4])
      iex> Explorer.Series.greater_equal(s1, s2)
      #Explorer.Series<
        Polars[3]
        boolean [true, true, false]
      >
  """
  @doc type: :element_wise
  @spec greater_equal(
          left :: Series.t() | number() | Date.t() | NaiveDateTime.t(),
          right :: Series.t() | number() | Date.t() | NaiveDateTime.t()
        ) :: Series.t()
  def greater_equal(left, right) do
    if args = cast_for_ordered_operation(left, right) do
      apply_series_list(:greater_equal, args)
    else
      dtype_mismatch_error("greater_equal/2", left, right, @numeric_or_temporal_dtypes)
    end
  end

  @doc """
  Returns boolean mask of `left < right`, element-wise.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:date`
    * `:time`
    * `:datetime`
    * `:duration`

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> s2 = Explorer.Series.from_list([1, 2, 4])
      iex> Explorer.Series.less(s1, s2)
      #Explorer.Series<
        Polars[3]
        boolean [false, false, true]
      >
  """
  @doc type: :element_wise
  @spec less(
          left :: Series.t() | number() | Date.t() | NaiveDateTime.t(),
          right :: Series.t() | number() | Date.t() | NaiveDateTime.t()
        ) :: Series.t()
  def less(left, right) do
    if args = cast_for_ordered_operation(left, right) do
      apply_series_list(:less, args)
    else
      dtype_mismatch_error("less/2", left, right, @numeric_or_temporal_dtypes)
    end
  end

  @doc """
  Returns boolean mask of `left <= right`, element-wise.

  At least one of the arguments must be a series. If both
  sizes are series, the series must have the same size or
  at last one of them must have size of 1.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}
    * `:date`
    * `:time`
    * `:datetime`
    * `:duration`

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> s2 = Explorer.Series.from_list([1, 2, 4])
      iex> Explorer.Series.less_equal(s1, s2)
      #Explorer.Series<
        Polars[3]
        boolean [true, true, true]
      >
  """
  @doc type: :element_wise
  @spec less_equal(
          left :: Series.t() | number() | Date.t() | NaiveDateTime.t(),
          right :: Series.t() | number() | Date.t() | NaiveDateTime.t()
        ) :: Series.t()
  def less_equal(left, right) do
    if args = cast_for_ordered_operation(left, right) do
      apply_series_list(:less_equal, args)
    else
      dtype_mismatch_error("less_equal/2", left, right, @numeric_or_temporal_dtypes)
    end
  end

  @doc """
  Checks if each element of the series in the left exists in the series
  on the right, returning a boolean mask.

  The series sizes do not have to match.

  See `member?/2` if you want to check if a literal belongs to a list.

  ## Examples

      iex> left = Explorer.Series.from_list([1, 2, 3])
      iex> right = Explorer.Series.from_list([1, 2])
      iex> Series.in(left, right)
      #Explorer.Series<
        Polars[3]
        boolean [true, true, false]
      >

      iex> left = Explorer.Series.from_list([~D[1970-01-01], ~D[2000-01-01], ~D[2010-04-17]])
      iex> right = Explorer.Series.from_list([~D[1970-01-01], ~D[2010-04-17]])
      iex> Series.in(left, right)
      #Explorer.Series<
        Polars[3]
        boolean [true, false, true]
      >
  """
  @doc type: :element_wise
  def (%Series{} = left) in (%Series{} = right) do
    if args = cast_for_comparable_operation(left, right) do
      apply_series_list(:binary_in, args)
    else
      dtype_mismatch_error("in/2", left, right)
    end
  end

  def (%Series{data: %backend{}} = left) in right when is_list(right),
    do: left in backend.from_list(right, Shared.dtype_from_list!(right))

  ## Comparable (a superset of ordered)

  defp cast_for_comparable_operation(
         %Series{dtype: left_dtype} = left,
         %Series{dtype: right_dtype} = right
       ) do
    if valid_comparable_series?(left_dtype, right_dtype) do
      [left, right]
    else
      nil
    end
  end

  defp cast_for_comparable_operation(%Series{dtype: dtype} = series, value) do
    if dtype = cast_to_comparable_series(dtype, value) do
      [series, from_same_value(series, value, dtype)]
    else
      nil
    end
  end

  defp cast_for_comparable_operation(value, %Series{dtype: dtype} = series) do
    if dtype = cast_to_comparable_series(dtype, value) do
      [from_same_value(series, value, dtype), series]
    else
      nil
    end
  end

  defp cast_for_comparable_operation(_left, _right),
    do: nil

  defp valid_comparable_series?(:category, :string), do: true
  defp valid_comparable_series?(:string, :category), do: true

  defp valid_comparable_series?(left_dtype, right_dtype),
    do: valid_ordered_series?(left_dtype, right_dtype)

  defp cast_to_comparable_series(:category, value) when is_binary(value), do: :string
  defp cast_to_comparable_series(:string, value) when is_binary(value), do: :string
  defp cast_to_comparable_series(:binary, value) when is_binary(value), do: :binary
  defp cast_to_comparable_series(:boolean, value) when is_boolean(value), do: :boolean
  defp cast_to_comparable_series(dtype, value), do: cast_to_ordered_series(dtype, value)

  ## Ordered

  defp cast_for_ordered_operation(
         %Series{dtype: left_dtype} = left,
         %Series{dtype: right_dtype} = right
       ) do
    if valid_ordered_series?(left_dtype, right_dtype) do
      [left, right]
    else
      nil
    end
  end

  defp cast_for_ordered_operation(%Series{dtype: dtype} = series, value) do
    if dtype = cast_to_ordered_series(dtype, value) do
      [series, from_same_value(series, value, dtype)]
    else
      nil
    end
  end

  defp cast_for_ordered_operation(value, %Series{dtype: dtype} = series) do
    if dtype = cast_to_ordered_series(dtype, value) do
      [from_same_value(series, value, dtype), series]
    else
      nil
    end
  end

  defp cast_for_ordered_operation(_left, _right),
    do: nil

  defp valid_ordered_series?(dtype, dtype),
    do: true

  defp valid_ordered_series?(left_dtype, right_dtype)
       when K.and(is_numeric_dtype(left_dtype), is_numeric_dtype(right_dtype)),
       do: true

  defp valid_ordered_series?(_, _),
    do: false

  defp cast_to_ordered_series(dtype, value)
       when K.and(is_numeric_dtype(dtype), is_integer(value)),
       do: {:s, 64}

  defp cast_to_ordered_series(dtype, value)
       when K.and(is_numeric_dtype(dtype), is_numeric(value)),
       do: {:f, 64}

  defp cast_to_ordered_series(:date, %Date{}), do: :date
  defp cast_to_ordered_series(:time, %Time{}), do: :time

  defp cast_to_ordered_series({:datetime, _}, %NaiveDateTime{}),
    do: {:datetime, :microsecond}

  defp cast_to_ordered_series({:duration, _}, value)
       when is_integer(value),
       do: {:s, 64}

  defp cast_to_ordered_series({:duration, _}, %Explorer.Duration{}),
    do: :duration

  defp cast_to_ordered_series(_dtype, _value),
    do: nil

  @doc """
  Returns a boolean mask of `left and right`, element-wise.

  Both sizes must be series, the series must have the same
  size or at last one of them must have size of 1.

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> mask1 = Explorer.Series.greater(s1, 1)
      iex> mask2 = Explorer.Series.less(s1, 3)
      iex> Explorer.Series.and(mask1, mask2)
      #Explorer.Series<
        Polars[3]
        boolean [false, true, false]
      >

  """
  @doc type: :element_wise
  def (%Series{dtype: :boolean} = left) and (%Series{dtype: :boolean} = right),
    do: apply_series_list(:binary_and, [left, right])

  def (%Series{} = left) and (%Series{} = right),
    do: dtype_mismatch_error("and/2", left, right, [:boolean])

  @doc """
  Returns a boolean mask of `left or right`, element-wise.

  Both sizes must be series, the series must have the same
  size or at last one of them must have size of 1.

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 3])
      iex> mask1 = Explorer.Series.less(s1, 2)
      iex> mask2 = Explorer.Series.greater(s1, 2)
      iex> Explorer.Series.or(mask1, mask2)
      #Explorer.Series<
        Polars[3]
        boolean [true, false, true]
      >

  """
  @doc type: :element_wise
  def (%Series{dtype: :boolean} = left) or (%Series{dtype: :boolean} = right),
    do: apply_series_list(:binary_or, [left, right])

  def (%Series{} = left) or (%Series{} = right),
    do: dtype_mismatch_error("or/2", left, right, [:boolean])

  @doc """
  Checks equality between two entire series.

  ## Examples

      iex> s1 = Explorer.Series.from_list(["a", "b"])
      iex> s2 = Explorer.Series.from_list(["a", "b"])
      iex> Explorer.Series.all_equal(s1, s2)
      true

      iex> s1 = Explorer.Series.from_list(["a", "b"])
      iex> s2 = Explorer.Series.from_list(["a", "c"])
      iex> Explorer.Series.all_equal(s1, s2)
      false

      iex> s1 = Explorer.Series.from_list(["a", "b"])
      iex> s2 = Explorer.Series.from_list([1, 2])
      iex> Explorer.Series.all_equal(s1, s2)
      false
  """
  @doc type: :element_wise
  def all_equal(%Series{dtype: dtype} = left, %Series{dtype: dtype} = right),
    do: apply_series_list(:all_equal, [left, right])

  def all_equal(%Series{dtype: left_dtype}, %Series{dtype: right_dtype})
      when left_dtype !=
             right_dtype,
      do: false

  @doc """
  Negate the elements of a boolean series.

  ## Examples

      iex> s1 = Explorer.Series.from_list([true, false, false])
      iex> Explorer.Series.not(s1)
      #Explorer.Series<
        Polars[3]
        boolean [false, true, true]
      >

  """
  @doc type: :element_wise
  def not (%Series{dtype: :boolean} = series), do: apply_series(series, :unary_not, [])
  def not %Series{dtype: dtype}, do: dtype_error("not/1", dtype, [:boolean])

  # Sort

  @doc """
  Sorts the series.

  See `sort_by/3` for an expression-based sorting function.
  See `sort_with/3` for a callback-based sorting function.

  ## Options

    * `:direction` - `:asc` or `:desc`, meaning "ascending" or "descending", respectively.
      By default it sorts in ascending order.

    * `:nils` - `:first` or `:last`.
      By default it is `:last` if direction is `:asc`, and `:first` otherwise.

    * `:parallel` - boolean.
      Whether to parallelize the sorting.
      By default it is `true`.

    * `:stable` - boolean.
      Determines if the sorting is stable (ties are guaranteed to maintain their order) or not.
      Unstable sorting may be more performant.
      By default it is `false`.

  ## Examples

      iex> s = Explorer.Series.from_list([9, 3, 7, 1])
      iex> Explorer.Series.sort(s)
      #Explorer.Series<
        Polars[4]
        s64 [1, 3, 7, 9]
      >

      iex> s = Explorer.Series.from_list([9, 3, 7, 1])
      iex> Explorer.Series.sort(s, direction: :desc)
      #Explorer.Series<
        Polars[4]
        s64 [9, 7, 3, 1]
      >

  """
  @doc type: :shape
  def sort(series, opts \\ []) do
    apply_series(series, :sort, Shared.validate_sort_options!(opts))
  end

  @doc """
  Returns the indices that would sort the series.

  The resultant series is going to have the `{:u, 32}` dtype.

  ## Options

    * `:direction` - `:asc` or `:desc`, meaning "ascending" or "descending", respectively.
      By default it sorts in ascending order.

    * `:nils` - `:first` or `:last`.
      By default it is `:last` if direction is `:asc`, and `:first` otherwise.

    * `:parallel` - boolean.
      Whether to parallelize the sorting.
      By default it is `true`.

    * `:stable` - boolean.
      Determines if the sorting is stable (ties are guaranteed to maintain their order) or not.
      Unstable sorting may be more performant.
      By default it is `false`.

  ## Examples

      iex> s = Explorer.Series.from_list([9, 3, 7, 1])
      iex> Explorer.Series.argsort(s)
      #Explorer.Series<
        Polars[4]
        u32 [3, 1, 2, 0]
      >

      iex> s = Explorer.Series.from_list([9, 3, 7, 1])
      iex> Explorer.Series.argsort(s, direction: :desc)
      #Explorer.Series<
        Polars[4]
        u32 [0, 2, 1, 3]
      >

  """
  @doc type: :shape
  def argsort(series, opts \\ []) do
    apply_series(series, :argsort, Shared.validate_sort_options!(opts))
  end

  @doc """
  Reverses the series order.

  ## Example

      iex> s = [1, 2, 3] |> Explorer.Series.from_list()
      iex> Explorer.Series.reverse(s)
      #Explorer.Series<
        Polars[3]
        s64 [3, 2, 1]
      >
  """
  @doc type: :shape
  def reverse(series), do: apply_series(series, :reverse)

  # Distinct

  @doc """
  Returns the unique values of the series.

  ## Examples

      iex> s = [1, 1, 2, 2, 3, 3] |> Explorer.Series.from_list()
      iex> Explorer.Series.distinct(s)
      #Explorer.Series<
        Polars[3]
        s64 [1, 2, 3]
      >
  """
  @doc type: :shape
  def distinct(series), do: apply_series(series, :distinct)

  @doc """
  Returns the unique values of the series, but does not maintain order.

  Faster than `distinct/1`.

  ## Examples

      iex> s = [1, 1, 2, 2, 3, 3] |> Explorer.Series.from_list()
      iex> Explorer.Series.unordered_distinct(s)
  """
  @doc type: :shape
  def unordered_distinct(series), do: apply_series(series, :unordered_distinct)

  @doc """
  Returns the number of unique values in the series.

  ## Examples

      iex> s = Explorer.Series.from_list(["a", "b", "a", "b"])
      iex> Explorer.Series.n_distinct(s)
      2
  """
  @doc type: :aggregation
  def n_distinct(series), do: apply_series(series, :n_distinct)

  @doc """
  Creates a new dataframe with unique values and the frequencies of each.

  ## Examples

      iex> s = Explorer.Series.from_list(["a", "a", "b", "c", "c", "c"])
      iex> Explorer.Series.frequencies(s)
      #Explorer.DataFrame<
        Polars[3 x 2]
        values string ["c", "a", "b"]
        counts u32 [3, 2, 1]
      >
  """
  @doc type: :aggregation
  def frequencies(series), do: apply_series(series, :frequencies)

  @doc """
  Bins values into discrete values.

  Given a `bins` length of N, there will be N+1 categories.

  ## Options

    * `:labels` - The labels assigned to the bins. Given `bins` of
      length N, `:labels` must be of length N+1. Defaults to the bin
      bounds (e.g. `(-inf -1.0]`, `(-1.0, 1.0]`, `(1.0, inf]`)

    * `:break_point_label` - The name given to the breakpoint column.
      Defaults to `break_point`.

    * `:category_label` - The name given to the category column.
      Defaults to `category`.

  ## Examples

      iex> s = Explorer.Series.from_list([1.0, 2.0, 3.0])
      iex> Explorer.Series.cut(s, [1.5, 2.5])
      #Explorer.DataFrame<
        Polars[3 x 3]
        values f64 [1.0, 2.0, 3.0]
        break_point f64 [1.5, 2.5, Inf]
        category category ["(-inf, 1.5]", "(1.5, 2.5]", "(2.5, inf]"]
      >
  """
  @doc type: :aggregation
  def cut(series, bins, opts \\ []) do
    apply_series(series, :cut, [
      Enum.map(bins, &(&1 / 1.0)),
      Keyword.get(opts, :labels),
      Keyword.get(opts, :break_point_label),
      Keyword.get(opts, :category_label)
    ])
  end

  @doc """
  Bins values into discrete values base on their quantiles.

  Given a `quantiles` length of N, there will be N+1 categories. Each
  element of `quantiles` is expected to be between 0.0 and 1.0.

  ## Options

    * `:labels` - The labels assigned to the bins. Given `bins` of
      length N, `:labels` must be of length N+1. Defaults to the bin
      bounds (e.g. `(-inf -1.0]`, `(-1.0, 1.0]`, `(1.0, inf]`)

    * `:break_point_label` - The name given to the breakpoint column.
      Defaults to `break_point`.

    * `:category_label` - The name given to the category column.
      Defaults to `category`.

  ## Examples

      iex> s = Explorer.Series.from_list([1.0, 2.0, 3.0, 4.0, 5.0])
      iex> Explorer.Series.qcut(s, [0.25, 0.75])
      #Explorer.DataFrame<
        Polars[5 x 3]
        values f64 [1.0, 2.0, 3.0, 4.0, 5.0]
        break_point f64 [2.0, 2.0, 4.0, 4.0, Inf]
        category category ["(-inf, 2]", "(-inf, 2]", "(2, 4]", "(2, 4]", "(4, inf]"]
      >
  """
  @doc type: :aggregation
  def qcut(series, quantiles, opts \\ []) do
    apply_series(series, :qcut, [
      Enum.map(quantiles, &(&1 / 1.0)),
      Keyword.get(opts, :labels),
      Keyword.get(opts, :break_point_label),
      Keyword.get(opts, :category_label)
    ])
  end

  @doc """
  Counts the number of non-`nil` elements in a series.

  See also:

    * `count_nil/1` - counts only the `nil` elements.
    * `size/1` - counts all elements.

  ## Examples

  Without `nil`:

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.count(s)
      3

  With `nil`:

      iex> s = Explorer.Series.from_list(["a", nil, "c"])
      iex> Explorer.Series.count(s)
      2

  With `:nan` (`:nan` does not count as `nil`):

      iex> s = Explorer.Series.from_list([1, :nan, 3])
      iex> Explorer.Series.count(s)
      3
  """
  @doc type: :aggregation
  def count(series), do: apply_series(series, :count)

  @doc """
  Counts the number of `nil` elements in a series.

  When used in a query on grouped data, `count_nil/1` is a per-group operation.

  See also:

    * `count/1` - counts only the non-`nil` elements.
    * `size/1` - counts all elements.

  ## Examples

  Without `nil`s:

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.nil_count(s)
      0

  With `nil`s:

      iex> s = Explorer.Series.from_list(["a", nil, "c"])
      iex> Explorer.Series.nil_count(s)
      1

  With `:nan`s (`:nan` does not count as `nil`):

      iex> s = Explorer.Series.from_list([1, :nan, 3])
      iex> Explorer.Series.nil_count(s)
      0
  """
  @doc type: :aggregation
  def nil_count(series), do: apply_series(series, :nil_count)

  # Window

  @doc """
  Calculate the rolling sum, given a window size and optional list of weights.

  ## Options

    * `:weights` - An optional list of weights with the same length as the window
      that will be multiplied elementwise with the values in the window. Defaults to `nil`.

    * `:min_periods` - The number of values in the window that should be non-nil
      before computing a result. If `nil`, it will be set equal to window size. Defaults to `1`.

    * `:center` - Set the labels at the center of the window. Defaults to `false`.

  ## Examples

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.window_sum(s, 4)
      #Explorer.Series<
        Polars[10]
        s64 [1, 3, 6, 10, 14, 18, 22, 26, 30, 34]
      >

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.window_sum(s, 2, weights: [1.0, 2.0])
      #Explorer.Series<
        Polars[10]
        f64 [1.0, 5.0, 8.0, 11.0, 14.0, 17.0, 20.0, 23.0, 26.0, 29.0]
      >
  """
  @doc type: :window
  def window_sum(series, window_size, opts \\ []),
    do: apply_series(series, :window_sum, [window_size | window_args(opts)])

  @doc """
  Calculate the rolling mean, given a window size and optional list of weights.

  ## Options

    * `:weights` - An optional list of weights with the same length as the window
      that will be multiplied elementwise with the values in the window. Defaults to `nil`.

    * `:min_periods` - The number of values in the window that should be non-nil
      before computing a result. If `nil`, it will be set equal to window size. Defaults to `1`.

    * `:center` - Set the labels at the center of the window. Defaults to `false`.

  ## Examples

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.window_mean(s, 4)
      #Explorer.Series<
        Polars[10]
        f64 [1.0, 1.5, 2.0, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5]
      >

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.window_mean(s, 2, weights: [0.25, 0.75])
      #Explorer.Series<
        Polars[10]
        f64 [0.25, 1.75, 2.75, 3.75, 4.75, 5.75, 6.75, 7.75, 8.75, 9.75]
      >

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.window_mean(s, 2, weights: [0.25, 0.75], min_periods: nil)
      #Explorer.Series<
        Polars[10]
        f64 [nil, 1.75, 2.75, 3.75, 4.75, 5.75, 6.75, 7.75, 8.75, 9.75]
      >
  """
  @doc type: :window
  def window_mean(series, window_size, opts \\ []),
    do: apply_series(series, :window_mean, [window_size | window_args(opts)])

  @doc """
  Calculate the rolling median, given a window size and optional list of weights.

  ## Options

    * `:weights` - An optional list of weights with the same length as the window
      that will be multiplied elementwise with the values in the window. Defaults to `nil`.

    * `:min_periods` - The number of values in the window that should be non-nil
      before computing a result. If `nil`, it will be set equal to window size. Defaults to `1`.

    * `:center` - Set the labels at the center of the window. Defaults to `false`.

  ## Examples

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.window_median(s, 4)
      #Explorer.Series<
        Polars[10]
        f64 [1.0, 1.5, 2.0, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5]
      >

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.window_median(s, 2, weights: [0.25, 0.75])
      #Explorer.Series<
        Polars[10]
        f64 [1.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5]
      >

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.window_median(s, 2, weights: [0.25, 0.75], min_periods: nil)
      #Explorer.Series<
        Polars[10]
        f64 [nil, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5]
      >
  """
  @doc type: :window
  def window_median(series, window_size, opts \\ []),
    do: apply_series(series, :window_median, [window_size | window_args(opts)])

  @doc """
  Calculate the rolling min, given a window size and optional list of weights.

  ## Options

    * `:weights` - An optional list of weights with the same length as the window
      that will be multiplied elementwise with the values in the window. Defaults to `nil`.

    * `:min_periods` - The number of values in the window that should be non-nil
      before computing a result. If `nil`, it will be set equal to window size. Defaults to `1`.

    * `:center` - Set the labels at the center of the window. Defaults to `false`.

  ## Examples

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.window_min(s, 4)
      #Explorer.Series<
        Polars[10]
        s64 [1, 1, 1, 1, 2, 3, 4, 5, 6, 7]
      >

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.window_min(s, 2, weights: [1.0, 2.0])
      #Explorer.Series<
        Polars[10]
        f64 [1.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
      >
  """
  @doc type: :window
  def window_min(series, window_size, opts \\ []),
    do: apply_series(series, :window_min, [window_size | window_args(opts)])

  @doc """
  Calculate the rolling max, given a window size and optional list of weights.

  ## Options

    * `:weights` - An optional list of weights with the same length as the window
      that will be multiplied elementwise with the values in the window. Defaults to `nil`.

    * `:min_periods` - The number of values in the window that should be non-nil
      before computing a result. If `nil`, it will be set equal to window size. Defaults to `1`.

    * `:center` - Set the labels at the center of the window. Defaults to `false`.

  ## Examples

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.window_max(s, 4)
      #Explorer.Series<
        Polars[10]
        s64 [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      >

      iex> s = 1..10 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.window_max(s, 2, weights: [1.0, 2.0])
      #Explorer.Series<
        Polars[10]
        f64 [1.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0]
      >
  """
  @doc type: :window
  def window_max(series, window_size, opts \\ []),
    do: apply_series(series, :window_max, [window_size | window_args(opts)])

  @doc """
  Calculate the rolling standard deviation, given a window size and optional list of weights.

  ## Options

    * `:weights` - An optional list of weights with the same length as the window
      that will be multiplied elementwise with the values in the window. Defaults to `nil`.

    * `:min_periods` - The number of values in the window that should be non-nil
      before computing a result. If `nil`, it will be set equal to window size. Defaults to `1`.

    * `:center` - Set the labels at the center of the window. Defaults to `false`.

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, 3, 4, 1])
      iex> Explorer.Series.window_standard_deviation(s, 2)
      #Explorer.Series<
        Polars[5]
        f64 [0.0, 0.7071067811865476, 0.7071067811865476, 0.7071067811865476, 2.1213203435596424]
      >

      iex> s = Explorer.Series.from_list([1, 2, 3, 4, 5, 6])
      iex> Explorer.Series.window_standard_deviation(s, 2, weights: [0.25, 0.75])
      #Explorer.Series<
        Polars[6]
        f64 [0.4330127018922193, 0.4330127018922193, 0.4330127018922193, 0.4330127018922193, 0.4330127018922193, 0.4330127018922193]
      >
  """
  @doc type: :window
  def window_standard_deviation(series, window_size, opts \\ []) do
    apply_series(series, :window_standard_deviation, [window_size | window_args(opts)])
  end

  defp window_args(opts) do
    opts = Keyword.validate!(opts, weights: nil, min_periods: 1, center: false)
    [opts[:weights], opts[:min_periods], opts[:center]]
  end

  @doc """
  Calculate the exponentially weighted moving average, given smoothing factor alpha.

  ## Options

    * `:alpha` - Optional smoothing factor which specifies the importance given
      to most recent observations. It is a value such that, 0 < alpha <= 1. Defaults to 0.5.

    * `:adjust` - If set to true, it corrects the bias introduced by smoothing process.
      Defaults to `true`.

    * `:min_periods` - The number of values in the window that should be non-nil
      before computing a result. Defaults to `1`.

    * `:ignore_nils` - If set to true, it ignore nulls in the calculation. Defaults to `true`.

  ## Examples

      iex> s = 1..5 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.ewm_mean(s)
      #Explorer.Series<
        Polars[5]
        f64 [1.0, 1.6666666666666667, 2.4285714285714284, 3.2666666666666666, 4.161290322580645]
      >

      iex> s = 1..5 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.ewm_mean(s, alpha: 0.1)
      #Explorer.Series<
        Polars[5]
        f64 [1.0, 1.5263157894736843, 2.070110701107011, 2.6312881651642916, 3.2097140484969833]
      >
  """
  @doc type: :window
  def ewm_mean(series, opts \\ []) do
    opts = Keyword.validate!(opts, alpha: 0.5, adjust: true, min_periods: 1, ignore_nils: true)

    apply_series(series, :ewm_mean, [
      opts[:alpha],
      opts[:adjust],
      opts[:min_periods],
      opts[:ignore_nils]
    ])
  end

  @doc """
  Calculate the exponentially weighted moving standard deviation, given smoothing factor alpha.

  ## Options

    * `:alpha` - Optional smoothing factor which specifies the importance given
      to most recent observations. It is a value such that, 0 < alpha <= 1. Defaults to 0.5.

    * `:adjust` - If set to true, it corrects the bias introduced by smoothing process.
      Defaults to `true`.

    * `:bias` - If set to false, it corrects the estimate to be statistically unbiased.
      Defaults to `false`.

    * `:min_periods` - The number of values in the window that should be non-nil
      before computing a result. Defaults to `1`.

    * `:ignore_nils` - If set to true, it ignore nulls in the calculation. Defaults to `true`.

  ## Examples

      iex> s = 1..5 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.ewm_standard_deviation(s)
      #Explorer.Series<
        Polars[5]
        f64 [0.0, 0.7071067811865476, 0.9636241116594314, 1.1771636613972951, 1.3452425132127066]
      >

      iex> s = 1..5 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.ewm_standard_deviation(s, alpha: 0.1)
      #Explorer.Series<
        Polars[5]
        f64 [0.0, 0.7071067811865476, 0.9990770648702808, 1.2879021599718157, 1.5741638698820746]
      >
  """
  @doc type: :window
  def ewm_standard_deviation(series, opts \\ []) do
    opts =
      Keyword.validate!(opts,
        alpha: 0.5,
        adjust: true,
        bias: false,
        min_periods: 1,
        ignore_nils: true
      )

    apply_series(series, :ewm_standard_deviation, [
      opts[:alpha],
      opts[:adjust],
      opts[:bias],
      opts[:min_periods],
      opts[:ignore_nils]
    ])
  end

  @doc """
  Calculate the exponentially weighted moving variance, given smoothing factor alpha.

  ## Options

    * `:alpha` - Optional smoothing factor which specifies the importance given
      to most recent observations. It is a value such that, 0 < alpha <= 1. Defaults to 0.5.

    * `:adjust` - If set to true, it corrects the bias introduced by smoothing process.
      Defaults to `true`.

    * `:bias` - If set to false, it corrects the estimate to be statistically unbiased.
      Defaults to `false`.

    * `:min_periods` - The number of values in the window that should be non-nil
      before computing a result. Defaults to `1`.

    * `:ignore_nils` - If set to true, it ignore nulls in the calculation. Defaults to `true`.

  ## Examples

      iex> s = 1..5 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.ewm_variance(s)
      #Explorer.Series<
        Polars[5]
        f64 [0.0, 0.5, 0.9285714285714284, 1.385714285714286, 1.8096774193548393]
      >

      iex> s = 1..5 |> Enum.to_list() |> Explorer.Series.from_list()
      iex> Explorer.Series.ewm_variance(s, alpha: 0.1)
      #Explorer.Series<
        Polars[5]
        f64 [0.0, 0.5, 0.9981549815498153, 1.6586919736600685, 2.4779918892421087]
      >
  """
  @doc type: :window
  def ewm_variance(series, opts \\ []) do
    opts =
      Keyword.validate!(opts,
        alpha: 0.5,
        adjust: true,
        bias: false,
        min_periods: 1,
        ignore_nils: true
      )

    apply_series(series, :ewm_variance, [
      opts[:alpha],
      opts[:adjust],
      opts[:bias],
      opts[:min_periods],
      opts[:ignore_nils]
    ])
  end

  # Missing values

  @doc """
  Fill missing values with the given strategy. If a scalar value is provided instead of a strategy
  atom, `nil` will be replaced with that value. It must be of the same `dtype` as the series.

  ## Strategies

    * `:forward` - replace nil with the previous value
    * `:backward` - replace nil with the next value
    * `:max` - replace nil with the series maximum
    * `:min` - replace nil with the series minimum
    * `:mean` - replace nil with the series mean
    * `:nan` (float only) - replace nil with `NaN`

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 4])
      iex> Explorer.Series.fill_missing(s, :forward)
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, 2, 4]
      >

      iex> s = Explorer.Series.from_list([1, 2, nil, 4])
      iex> Explorer.Series.fill_missing(s, :backward)
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, 4, 4]
      >

      iex> s = Explorer.Series.from_list([1, 2, nil, 4])
      iex> Explorer.Series.fill_missing(s, :max)
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, 4, 4]
      >

      iex> s = Explorer.Series.from_list([1, 2, nil, 4])
      iex> Explorer.Series.fill_missing(s, :min)
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, 1, 4]
      >

      iex> s = Explorer.Series.from_list([1, 2, nil, 4])
      iex> Explorer.Series.fill_missing(s, :mean)
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, 2, 4]
      >

  Values that belong to the series itself can also be added as missing:

      iex> s = Explorer.Series.from_list([1, 2, nil, 4])
      iex> Explorer.Series.fill_missing(s, 3)
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, 3, 4]
      >

      iex> s = Explorer.Series.from_list(["a", "b", nil, "d"])
      iex> Explorer.Series.fill_missing(s, "c")
      #Explorer.Series<
        Polars[4]
        string ["a", "b", "c", "d"]
      >

  Mismatched types will raise:

      iex> s = Explorer.Series.from_list([1, 2, nil, 4])
      iex> Explorer.Series.fill_missing(s, "foo")
      ** (ArgumentError) cannot invoke Explorer.Series.fill_missing/2 with mismatched dtypes: {:s, 64} and "foo"

  Floats in particular accept missing values to be set to NaN, Inf, and -Inf:

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 4.0])
      iex> Explorer.Series.fill_missing(s, :nan)
      #Explorer.Series<
        Polars[4]
        f64 [1.0, 2.0, NaN, 4.0]
      >

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 4.0])
      iex> Explorer.Series.fill_missing(s, :infinity)
      #Explorer.Series<
        Polars[4]
        f64 [1.0, 2.0, Inf, 4.0]
      >

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, 4.0])
      iex> Explorer.Series.fill_missing(s, :neg_infinity)
      #Explorer.Series<
        Polars[4]
        f64 [1.0, 2.0, -Inf, 4.0]
      >

  """
  @doc type: :window
  @spec fill_missing(
          Series.t(),
          :forward
          | :backward
          | :max
          | :min
          | :mean
          | :nan
          | :infinity
          | :neg_infinity
          | Explorer.Backend.Series.valid_types()
        ) :: Series.t()
  def fill_missing(%Series{} = series, value)
      when K.in(value, [:nan, :infinity, :neg_infinity]) do
    if K.not(K.in(series.dtype, [{:f, 32}, {:f, 64}])) do
      raise ArgumentError,
            "fill_missing with :#{value} values require a float series, got #{inspect(series.dtype)}"
    end

    apply_series(series, :fill_missing_with_value, [value])
  end

  def fill_missing(%Series{} = series, strategy)
      when K.in(strategy, [:forward, :backward, :min, :max, :mean]),
      do: apply_series(series, :fill_missing_with_strategy, [strategy])

  def fill_missing(%Series{dtype: dtype} = series, value) do
    if cast_to_comparable_series(dtype, value) do
      apply_series(series, :fill_missing_with_value, [value])
    else
      dtype_mismatch_error("fill_missing/2", series, value)
    end
  end

  @doc """
  Returns a mask of nil values.

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 4])
      iex> Explorer.Series.is_nil(s)
      #Explorer.Series<
        Polars[4]
        boolean [false, false, true, false]
      >
  """
  @doc type: :element_wise
  @spec is_nil(Series.t()) :: Series.t()
  def is_nil(series), do: apply_series(series, :is_nil)

  @doc """
  Returns a mask of not nil values.

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, nil, 4])
      iex> Explorer.Series.is_not_nil(s)
      #Explorer.Series<
        Polars[4]
        boolean [true, true, false, true]
      >
  """
  @doc type: :element_wise
  @spec is_not_nil(Series.t()) :: Series.t()
  def is_not_nil(series), do: apply_series(series, :is_not_nil)

  @doc """
  Gets the series absolute values.

  ## Supported dtypes

    * floats: #{Shared.inspect_dtypes(@float_dtypes, backsticks: true)}
    * integers: #{Shared.inspect_dtypes(@integer_types, backsticks: true)}

  ## Examples

      iex> s = Explorer.Series.from_list([1, 2, -1, -3])
      iex> Explorer.Series.abs(s)
      #Explorer.Series<
        Polars[4]
        s64 [1, 2, 1, 3]
      >

      iex> s = Explorer.Series.from_list([1.0, 2.0, -1.0, -3.0])
      iex> Explorer.Series.abs(s)
      #Explorer.Series<
        Polars[4]
        f64 [1.0, 2.0, 1.0, 3.0]
      >

      iex> s = Explorer.Series.from_list([1.0, 2.0, nil, -3.0])
      iex> Explorer.Series.abs(s)
      #Explorer.Series<
        Polars[4]
        f64 [1.0, 2.0, nil, 3.0]
      >

      iex> s = Explorer.Series.from_list(["a", "b", "c"])
      iex> Explorer.Series.abs(s)
      ** (ArgumentError) Explorer.Series.abs/1 not implemented for dtype :string. Valid dtypes are {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}
  """
  @doc type: :element_wise
  @spec abs(series :: Series.t()) :: Series.t()
  def abs(%Series{dtype: dtype} = series) when is_numeric_dtype(dtype),
    do: apply_series(series, :abs)

  def abs(%Series{dtype: dtype}),
    do: dtype_error("abs/1", dtype, @numeric_dtypes)

  # Strings

  @doc """
  Detects whether a string contains a substring.

  ## Examples

      iex> s = Explorer.Series.from_list(["abc", "def", "bcd"])
      iex> Explorer.Series.contains(s, "bc")
      #Explorer.Series<
        Polars[3]
        boolean [true, false, true]
      >
  """
  @doc type: :string_wise
  @spec contains(Series.t(), String.t()) :: Series.t()
  def contains(%Series{dtype: :string} = series, pattern)
      when K.is_binary(pattern),
      do: apply_series(series, :contains, [pattern])

  def contains(%Series{dtype: dtype}, _), do: dtype_error("contains/2", dtype, [:string])

  @doc """
  Converts all characters to uppercase.

  ## Examples

      iex> s = Explorer.Series.from_list(["abc", "def", "bcd"])
      iex> Explorer.Series.upcase(s)
      #Explorer.Series<
        Polars[3]
        string ["ABC", "DEF", "BCD"]
      >
  """
  @doc type: :string_wise
  @spec upcase(Series.t()) :: Series.t()
  def upcase(%Series{dtype: :string} = series),
    do: apply_series(series, :upcase)

  def upcase(%Series{dtype: dtype}), do: dtype_error("upcase/1", dtype, [:string])

  @doc """
  Converts all characters to lowercase.

  ## Examples

      iex> s = Explorer.Series.from_list(["ABC", "DEF", "BCD"])
      iex> Explorer.Series.downcase(s)
      #Explorer.Series<
        Polars[3]
        string ["abc", "def", "bcd"]
      >
  """
  @doc type: :string_wise
  @spec downcase(Series.t()) :: Series.t()
  def downcase(%Series{dtype: :string} = series),
    do: apply_series(series, :downcase)

  def downcase(%Series{dtype: dtype}), do: dtype_error("downcase/1", dtype, [:string])

  @doc """
  Replaces all occurences of pattern with replacement in string series.

  Both pattern and replacement must be of type string.

  ## Examples

      iex> series = Explorer.Series.from_list(["1,200", "1,234,567", "asdf", nil])
      iex> Explorer.Series.replace(series, ",", "")
      #Explorer.Series<
        Polars[4]
        string ["1200", "1234567", "asdf", nil]
      >
  """
  @doc type: :string_wise
  @spec replace(Series.t(), binary(), binary()) :: Series.t()
  def replace(%Series{dtype: :string} = series, pattern, replacement)
      when K.and(is_binary(pattern), is_binary(replacement)),
      do: apply_series(series, :replace, [pattern, replacement])

  def replace(%Series{dtype: :string}, _, _),
    do: raise(ArgumentError, "pattern and replacement in replace/3 need to be a string")

  def replace(%Series{dtype: dtype}, _, _), do: dtype_error("replace/3", dtype, [:string])

  @doc """
  Returns a string series where all leading and trailing Unicode whitespaces
  have been removed.

  ## Examples

      iex> s = Explorer.Series.from_list(["  abc", "def  ", "  bcd   "])
      iex> Explorer.Series.strip(s)
      #Explorer.Series<
        Polars[3]
        string ["abc", "def", "bcd"]
      >

  """
  @doc type: :string_wise
  @spec strip(Series.t()) :: Series.t()
  def strip(%Series{dtype: :string} = series),
    do: apply_series(series, :strip, [nil])

  @doc """
  Returns a string series where all leading and trailing examples of the provided string
  have been removed.

  Where multiple characters are provided, all combinations of this set of characters will be stripped

  ## Examples

      iex> s = Explorer.Series.from_list(["£123", "1.00£", "£1.00£"])
      iex> Explorer.Series.strip(s, "£")
      #Explorer.Series<
        Polars[3]
        string ["123", "1.00", "1.00"]
      >

      iex> s = Explorer.Series.from_list(["abc", "adefa", "bcda"])
      iex> Explorer.Series.strip(s, "ab")
      #Explorer.Series<
        Polars[3]
        string ["c", "def", "cd"]
      >

  """
  @doc type: :string_wise
  @spec strip(Series.t(), String.t()) :: Series.t()
  def strip(%Series{dtype: :string} = series, string) when is_binary(string),
    do: apply_series(series, :strip, [string])

  def strip(%Series{dtype: dtype}, _string), do: dtype_error("strip/2", dtype, [:string])

  @doc """
  Returns a string series where all leading Unicode whitespaces have been removed.

  ## Examples

      iex> s = Explorer.Series.from_list(["  abc", "def  ", "  bcd"])
      iex> Explorer.Series.lstrip(s)
      #Explorer.Series<
        Polars[3]
        string ["abc", "def  ", "bcd"]
      >
  """
  @doc type: :string_wise
  @spec lstrip(Series.t()) :: Series.t()
  def lstrip(%Series{dtype: :string} = series),
    do: apply_series(series, :lstrip, [nil])

  @doc """
  Returns a string series where all leading examples of the provided string
  have been removed.

  Where multiple characters are provided, all combinations of this set of characters will be stripped
  ## Examples

      iex> s = Explorer.Series.from_list(["$1", "$$200$$", "$$$3000$"])
      iex> Explorer.Series.lstrip(s, "$")
      #Explorer.Series<
        Polars[3]
        string ["1", "200$$", "3000$"]
      >

      iex> s = Explorer.Series.from_list(["abc", "adefa", "bcda"])
      iex> Explorer.Series.lstrip(s, "ab")
      #Explorer.Series<
        Polars[3]
        string ["c", "defa", "cda"]
      >
  """
  @doc type: :string_wise
  @spec lstrip(Series.t(), String.t()) :: Series.t()
  def lstrip(%Series{dtype: :string} = series, string) when is_binary(string),
    do: apply_series(series, :lstrip, [string])

  def lstrip(%Series{dtype: dtype}, _string),
    do: dtype_error("lstrip/2", dtype, [:string])

  @doc """
  Returns a string series where all trailing Unicode whitespaces have been removed.

  ## Examples

      iex> s = Explorer.Series.from_list(["  abc", "def  ", "  bcd"])
      iex> Explorer.Series.rstrip(s)
      #Explorer.Series<
        Polars[3]
        string ["  abc", "def", "  bcd"]
      >
  """
  @doc type: :string_wise
  @spec rstrip(Series.t()) :: Series.t()
  def rstrip(%Series{dtype: :string} = series),
    do: apply_series(series, :rstrip, [nil])

  @doc """
  Returns a string series where all trailing examples of the provided string
  have been removed.

  Where multiple characters are provided, all combinations of this set of characters will be stripped

  ## Examples

      iex> s = Explorer.Series.from_list(["__abc__", "def_", "__bcd_"])
      iex> Explorer.Series.rstrip(s, "_")
      #Explorer.Series<
        Polars[3]
        string ["__abc", "def", "__bcd"]
      >

      iex> s = Explorer.Series.from_list(["abc", "adefa", "bcdabaaa"])
      iex> Explorer.Series.rstrip(s, "ab")
      #Explorer.Series<
        Polars[3]
        string ["abc", "adef", "bcd"]
      >
  """
  @doc type: :string_wise
  @spec rstrip(Series.t(), String.t()) :: Series.t()
  def rstrip(%Series{dtype: :string} = series, string) when is_binary(string),
    do: apply_series(series, :rstrip, [string])

  def rstrip(%Series{dtype: dtype}, _string),
    do: dtype_error("rstrip/2", dtype, [:string])

  @doc """
  Returns a string sliced from the offset to the end of the string, supporting
  negative indexing

  ## Examples

      iex> s = Explorer.Series.from_list(["earth", "mars", "neptune"])
      iex> Explorer.Series.substring(s, -3)
      #Explorer.Series<
        Polars[3]
        string ["rth", "ars", "une"]
      >

      iex> s = Explorer.Series.from_list(["earth", "mars", "neptune"])
      iex> Explorer.Series.substring(s, 1)
      #Explorer.Series<
        Polars[3]
        string ["arth", "ars", "eptune"]
      >
  """
  @doc type: :string_wise
  @spec substring(Series.t(), integer()) :: Series.t()
  def substring(%Series{dtype: :string} = series, offset) when is_integer(offset),
    do: apply_series(series, :substring, [offset, nil])

  @doc """
  Returns a string sliced from the offset to the length provided, supporting
  negative indexing

  ## Examples

      iex> s = Explorer.Series.from_list(["earth", "mars", "neptune"])
      iex> Explorer.Series.substring(s, -3, 2)
      #Explorer.Series<
        Polars[3]
        string ["rt", "ar", "un"]
      >

      iex> s = Explorer.Series.from_list(["earth", "mars", "neptune"])
      iex> Explorer.Series.substring(s, 1, 5)
      #Explorer.Series<
        Polars[3]
        string ["arth", "ars", "eptun"]
      >

      iex> d = Explorer.Series.from_list(["こんにちは世界", "مرحبًا", "안녕하세요"])
      iex> Explorer.Series.substring(d, 1, 3)
      #Explorer.Series<
        Polars[3]
        string ["んにち", "رحب", "녕하세"]
      >
  """
  @doc type: :string_wise
  @spec substring(Series.t(), integer(), integer()) :: Series.t()
  def substring(%Series{dtype: :string} = series, offset, length)
      when is_integer(offset)
      when is_integer(length)
      when length >= 0,
      do: apply_series(series, :substring, [offset, length])

  def substring(%Series{dtype: dtype}, _offset, _length),
    do: dtype_error("substring/3", dtype, [:string])

  @doc """
  Split the string by a substring.

  ## Examples

      iex> s = Series.from_list(["1", "1|2"])
      iex> Series.split(s, "|")
      #Explorer.Series<
        Polars[2]
        list[string] [["1"], ["1", "2"]]
      >

  """
  @doc type: :string_wise
  @spec split(Series.t(), String.t()) :: Series.t()
  def split(%Series{dtype: :string} = series, by)
      when is_binary(by),
      do: apply_series(series, :split, [by])

  def split(%Series{dtype: dtype}, _by),
    do: dtype_error("split/2", dtype, [:string])

  # Float

  @doc """
  Round floating point series to given decimal places.

  ## Examples

      iex> s = Explorer.Series.from_list([1.124993, 2.555321, 3.995001])
      iex> Explorer.Series.round(s, 2)
      #Explorer.Series<
        Polars[3]
        f64 [1.12, 2.56, 4.0]
      >
  """
  @doc type: :float_wise
  @spec round(Series.t(), non_neg_integer()) :: Series.t()
  def round(%Series{dtype: {:f, _}} = series, decimals)
      when K.and(is_integer(decimals), decimals >= 0),
      do: apply_series(series, :round, [decimals])

  def round(%Series{dtype: {:f, _}}, _),
    do: raise(ArgumentError, "second argument to round/2 must be a non-negative integer")

  def round(%Series{dtype: dtype}, _), do: dtype_error("round/2", dtype, @float_dtypes)

  @doc """
  Floor floating point series to lowest integers smaller or equal to the float value.

  ## Examples

      iex> s = Explorer.Series.from_list([1.124993, 2.555321, 3.995001])
      iex> Explorer.Series.floor(s)
      #Explorer.Series<
        Polars[3]
        f64 [1.0, 2.0, 3.0]
      >
  """
  @doc type: :float_wise
  @spec floor(Series.t()) :: Series.t()
  def floor(%Series{dtype: dtype} = series) when K.in(dtype, @float_dtypes),
    do: apply_series(series, :floor)

  def floor(%Series{dtype: dtype}), do: dtype_error("floor/1", dtype, @float_dtypes)

  @doc """
  Ceil floating point series to highest integers smaller or equal to the float value.

  ## Examples

      iex> s = Explorer.Series.from_list([1.124993, 2.555321, 3.995001])
      iex> Explorer.Series.ceil(s)
      #Explorer.Series<
        Polars[3]
        f64 [2.0, 3.0, 4.0]
      >
  """
  @doc type: :float_wise
  @spec ceil(Series.t()) :: Series.t()
  def ceil(%Series{dtype: dtype} = series) when K.in(dtype, @float_dtypes),
    do: apply_series(series, :ceil)

  def ceil(%Series{dtype: dtype}), do: dtype_error("ceil/1", dtype, @float_dtypes)

  @doc """
  Returns a mask of finite values.

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 0, nil])
      iex> s2 = Explorer.Series.from_list([0, 2, 0, nil])
      iex> s3 = Explorer.Series.divide(s1, s2)
      iex> Explorer.Series.is_finite(s3)
      #Explorer.Series<
        Polars[4]
        boolean [false, true, false, nil]
      >
  """
  @doc type: :float_wise
  @spec is_finite(Series.t()) :: Series.t()
  def is_finite(%Series{dtype: dtype} = series) when K.in(dtype, @float_dtypes),
    do: apply_series(series, :is_finite)

  def is_finite(%Series{dtype: dtype}), do: dtype_error("is_finite/1", dtype, @float_dtypes)

  @doc """
  Returns a mask of infinite values.

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, -1, 2, 0, nil])
      iex> s2 = Explorer.Series.from_list([0, 0, 2, 0, nil])
      iex> s3 = Explorer.Series.divide(s1, s2)
      iex> Explorer.Series.is_infinite(s3)
      #Explorer.Series<
        Polars[5]
        boolean [true, true, false, false, nil]
      >
  """
  @doc type: :float_wise
  @spec is_infinite(Series.t()) :: Series.t()
  def is_infinite(%Series{dtype: dtype} = series) when K.in(dtype, @float_dtypes),
    do: apply_series(series, :is_infinite)

  def is_infinite(%Series{dtype: dtype}),
    do: dtype_error("is_infinite/1", dtype, @float_dtypes)

  @doc """
  Returns a mask of nan values.

  ## Examples

      iex> s1 = Explorer.Series.from_list([1, 2, 0, nil])
      iex> s2 = Explorer.Series.from_list([0, 2, 0, nil])
      iex> s3 = Explorer.Series.divide(s1, s2)
      iex> Explorer.Series.is_nan(s3)
      #Explorer.Series<
        Polars[4]
        boolean [false, false, true, nil]
      >
  """
  @doc type: :float_wise
  @spec is_nan(Series.t()) :: Series.t()
  def is_nan(%Series{dtype: dtype} = series) when K.in(dtype, @float_dtypes),
    do: apply_series(series, :is_nan)

  def is_nan(%Series{dtype: dtype}), do: dtype_error("is_nan/1", dtype, @float_dtypes)

  # Date / DateTime

  @doc """
  Returns the month number starting from 1. The return value ranges from 1 to 12.

  ## Examples

      iex> s = Explorer.Series.from_list([~D[2023-01-15], ~D[2023-02-16], ~D[2023-03-20], nil])
      iex> Explorer.Series.month(s)
      #Explorer.Series<
        Polars[4]
        s8 [1, 2, 3, nil]
      >

  It can also be called on a datetime series.

      iex> s = Explorer.Series.from_list([~N[2023-01-15 00:00:00], ~N[2023-02-16 23:59:59.999999], ~N[2023-03-20 12:00:00], nil])
      iex> Explorer.Series.month(s)
      #Explorer.Series<
        Polars[4]
        s8 [1, 2, 3, nil]
      >
  """
  @doc type: :datetime_wise
  @spec month(Series.t()) :: Series.t()
  def month(%Series{dtype: dtype} = series) when K.in(dtype, @date_or_datetime_dtypes),
    do: apply_series_list(:month, [series])

  def month(%Series{dtype: dtype}),
    do: dtype_error("month/1", dtype, @date_or_datetime_dtypes)

  @doc """
  Returns the year number in the calendar date.

  ## Examples

      iex> s = Explorer.Series.from_list([~D[2023-01-15], ~D[2022-02-16], ~D[2021-03-20], nil])
      iex> Explorer.Series.year(s)
      #Explorer.Series<
        Polars[4]
        s32 [2023, 2022, 2021, nil]
      >

  It can also be called on a datetime series.

      iex> s = Explorer.Series.from_list([~N[2023-01-15 00:00:00], ~N[2022-02-16 23:59:59.999999], ~N[2021-03-20 12:00:00], nil])
      iex> Explorer.Series.year(s)
      #Explorer.Series<
        Polars[4]
        s32 [2023, 2022, 2021, nil]
      >
  """
  @doc type: :datetime_wise
  @spec year(Series.t()) :: Series.t()
  def year(%Series{dtype: dtype} = series) when K.in(dtype, @date_or_datetime_dtypes),
    do: apply_series_list(:year, [series])

  def year(%Series{dtype: dtype}),
    do: dtype_error("year/1", dtype, @date_or_datetime_dtypes)

  @doc """
  Returns the hour number from 0 to 23.

  ## Examples

      iex> s = Explorer.Series.from_list([~N[2023-01-15 00:00:00], ~N[2022-02-16 23:59:59.999999], ~N[2021-03-20 12:00:00], nil])
      iex> Explorer.Series.hour(s)
      #Explorer.Series<
        Polars[4]
        s8 [0, 23, 12, nil]
      >
  """
  @doc type: :datetime_wise
  @spec hour(Series.t()) :: Series.t()
  def hour(%Series{dtype: dtype} = series) when K.in(dtype, @datetime_dtypes),
    do: apply_series_list(:hour, [series])

  def hour(%Series{dtype: dtype}),
    do: dtype_error("hour/1", dtype, @datetime_dtypes)

  @doc """
  Returns the minute number from 0 to 59.

  ## Examples

      iex> s = Explorer.Series.from_list([~N[2023-01-15 00:00:00], ~N[2022-02-16 23:59:59.999999], ~N[2021-03-20 12:00:00], nil])
      iex> Explorer.Series.minute(s)
      #Explorer.Series<
        Polars[4]
        s8 [0, 59, 0, nil]
      >
  """
  @doc type: :datetime_wise
  @spec minute(Series.t()) :: Series.t()
  def minute(%Series{dtype: dtype} = series) when K.in(dtype, @datetime_dtypes),
    do: apply_series_list(:minute, [series])

  def minute(%Series{dtype: dtype}),
    do: dtype_error("minute/1", dtype, @datetime_dtypes)

  @doc """
  Returns the second number from 0 to 59.

  ## Examples

      iex> s = Explorer.Series.from_list([~N[2023-01-15 00:00:00], ~N[2022-02-16 23:59:59.999999], ~N[2021-03-20 12:00:00], nil])
      iex> Explorer.Series.second(s)
      #Explorer.Series<
        Polars[4]
        s8 [0, 59, 0, nil]
      >
  """
  @doc type: :datetime_wise
  @spec second(Series.t()) :: Series.t()
  def second(%Series{dtype: dtype} = series) when K.in(dtype, @datetime_dtypes),
    do: apply_series_list(:second, [series])

  def second(%Series{dtype: dtype}),
    do: dtype_error("minute/1", dtype, @datetime_dtypes)

  @doc """
  Returns a day-of-week number starting from Monday = 1. (ISO 8601 weekday number)

  ## Examples

      iex> s = Explorer.Series.from_list([~D[2023-01-15], ~D[2023-01-16], ~D[2023-01-20], nil])
      iex> Explorer.Series.day_of_week(s)
      #Explorer.Series<
        Polars[4]
        s8 [7, 1, 5, nil]
      >

  It can also be called on a datetime series.

      iex> s = Explorer.Series.from_list([~N[2023-01-15 00:00:00], ~N[2023-01-16 23:59:59.999999], ~N[2023-01-20 12:00:00], nil])
      iex> Explorer.Series.day_of_week(s)
      #Explorer.Series<
        Polars[4]
        s8 [7, 1, 5, nil]
      >
  """

  @doc type: :datetime_wise
  @spec day_of_week(Series.t()) :: Series.t()
  def day_of_week(%Series{dtype: dtype} = series) when K.in(dtype, @date_or_datetime_dtypes),
    do: apply_series_list(:day_of_week, [series])

  def day_of_week(%Series{dtype: dtype}),
    do: dtype_error("day_of_week/1", dtype, @date_or_datetime_dtypes)

  @doc """
  Returns the day-of-year number starting from 1.

  The return value ranges from 1 to 366 (the last day of year differs by years).

  ## Examples

      iex> s = Explorer.Series.from_list([~D[2023-01-01], ~D[2023-01-02], ~D[2023-02-01], nil])
      iex> Explorer.Series.day_of_year(s)
      #Explorer.Series<
        Polars[4]
        s16 [1, 2, 32, nil]
      >

  It can also be called on a datetime series.

      iex> s = Explorer.Series.from_list(
      ...>   [~N[2023-01-01 00:00:00], ~N[2023-01-02 00:00:00], ~N[2023-02-01 23:59:59], nil]
      ...> )
      iex> Explorer.Series.day_of_year(s)
      #Explorer.Series<
        Polars[4]
        s16 [1, 2, 32, nil]
      >
  """
  @doc type: :datetime_wise
  @spec day_of_year(Series.t()) :: Series.t()
  def day_of_year(%Series{dtype: dtype} = series) when K.in(dtype, @date_or_datetime_dtypes),
    do: apply_series_list(:day_of_year, [series])

  def day_of_year(%Series{dtype: dtype}),
    do: dtype_error("day_of_year/1", dtype, @date_or_datetime_dtypes)

  @doc """
  Returns the week-of-year number.

  The return value ranges from 1 to 53 (the last week of year differs by years).

  Weeks start on Monday and end on Sunday. If the final week of a year does not end on Sunday, the
  first days of the following year will have a week number of 52 (or 53 for a leap year).

  ## Examples

      iex> s = Explorer.Series.from_list([~D[2023-01-01], ~D[2023-01-02], ~D[2023-02-01], nil])
      iex> Explorer.Series.week_of_year(s)
      #Explorer.Series<
        Polars[4]
        s8 [52, 1, 5, nil]
      >

  It can also be called on a datetime series.

      iex> s = Explorer.Series.from_list(
      ...>   [~N[2023-01-01 00:00:00], ~N[2023-01-02 00:00:00], ~N[2023-02-01 23:59:59], nil]
      ...> )
      iex> Explorer.Series.week_of_year(s)
      #Explorer.Series<
        Polars[4]
        s8 [52, 1, 5, nil]
      >
  """
  @doc type: :datetime_wise
  @spec week_of_year(Series.t()) :: Series.t()
  def week_of_year(%Series{dtype: dtype} = series) when K.in(dtype, @date_or_datetime_dtypes),
    do: apply_series_list(:week_of_year, [series])

  def week_of_year(%Series{dtype: dtype}),
    do: dtype_error("week_of_year/1", dtype, @date_or_datetime_dtypes)

  @deprecated "Use cast(:date) instead"
  @doc type: :deprecated
  def to_date(%Series{dtype: dtype} = series) when K.in(dtype, @datetime_dtypes),
    do: cast(series, :date)

  def to_date(%Series{dtype: dtype}),
    do: dtype_error("to_date/1", dtype, @datetime_dtypes)

  @deprecated "Use cast(:time) instead"
  @doc type: :deprecated
  def to_time(%Series{dtype: dtype} = series) when K.in(dtype, @datetime_dtypes),
    do: cast(series, :time)

  def to_time(%Series{dtype: dtype}),
    do: dtype_error("to_time/1", dtype, @datetime_dtypes)

  @doc """
  Join all string items in a sublist and place a separator between them.

  ## Examples

      iex> s = Series.from_list([["1"], ["1", "2"]])
      iex> Series.join(s, "|")
      #Explorer.Series<
        Polars[2]
        string ["1", "1|2"]
      >

  """
  @doc type: :list_wise
  @spec join(Series.t(), String.t()) :: Series.t()
  def join(%Series{dtype: {:list, :string}} = series, separator)
      when is_binary(separator),
      do: apply_series(series, :join, [separator])

  def join(%Series{dtype: dtype}, _separator),
    do: dtype_error("join/2", dtype, [{:list, :string}])

  @doc """
  Calculates the length of each list in a list series.

  ## Examples

      iex> s = Series.from_list([[1], [1, 2]])
      iex> Series.lengths(s)
      #Explorer.Series<
        Polars[2]
        u32 [1, 2]
      >

  """
  @doc type: :list_wise
  @spec lengths(Series.t()) :: Series.t()
  def lengths(%Series{dtype: {:list, _}} = series),
    do: apply_series(series, :lengths)

  def lengths(%Series{dtype: dtype}),
    do: dtype_error("lengths/1", dtype, [{:list, :_}])

  @doc """
  Checks for the presence of a value in a list series.

  ## Examples

      iex> s = Series.from_list([[1], [1, 2]])
      iex> Series.member?(s, 2)
      #Explorer.Series<
        Polars[2]
        boolean [false, true]
      >

  """
  @doc type: :list_wise
  @spec member?(Series.t(), Explorer.Backend.Series.valid_types()) :: Series.t()
  def member?(%Series{dtype: {:list, dtype}} = series, value) do
    if cast_to_comparable_series(dtype, value) do
      apply_series(series, :member?, [value])
    else
      dtype_mismatch_error("member?/2", series, value)
    end
  end

  def member?(%Series{dtype: dtype}, _value),
    do: dtype_error("member?/2", dtype, [{:list, :_}])

  # Escape hatch

  @doc """
  Returns an `Explorer.Series` where each element is the result of invoking `fun` on each
  corresponding element of `series`.

  This is an expensive operation meant to enable the use of arbitrary Elixir functions against
  any backend. The implementation will vary by backend but in most (all?) cases will require
  converting to an `Elixir.List`, applying `Enum.map/2`, and then converting back to an
  `Explorer.Series`.

  ## Examples

      iex> s = Explorer.Series.from_list(["this ", " is", "great   "])
      iex> Explorer.Series.transform(s, &String.trim/1)
      #Explorer.Series<
        Polars[3]
        string ["this", "is", "great"]
      >

      iex> s = Explorer.Series.from_list(["this", "is", "great"])
      iex> Explorer.Series.transform(s, &String.length/1)
      #Explorer.Series<
        Polars[3]
        s64 [4, 2, 5]
      >
  """
  @doc type: :element_wise
  def transform(series, fun) do
    apply_series(series, :transform, [fun])
  end

  @doc """
  Extracts a field from a struct series

  The field can be an atom or string.

  ## Examples

      iex> s = Series.from_list([%{a: 1}, %{a: 2}])
      iex> Series.field(s, :a)
      #Explorer.Series<
        Polars[2]
        s64 [1, 2]
      >

      iex> s = Series.from_list([%{a: 1}, %{a: 2}])
      iex> Series.field(s, "a")
      #Explorer.Series<
        Polars[2]
        s64 [1, 2]
      >
  """
  @doc type: :struct_wise
  @spec field(Series.t(), String.t() | atom()) :: Series.t()
  def field(%Series{dtype: {:struct, dtypes}} = series, name)
      when K.or(is_binary(name), is_atom(name)) do
    name = to_string(name)

    if List.keymember?(dtypes, name, 0) do
      apply_series(series, :field, [name])
    else
      raise ArgumentError,
            "field #{inspect(name)} not found in fields #{inspect(Enum.map(dtypes, &elem(&1, 0)))}"
    end
  end

  @doc """
  Decodes a string series containing valid JSON according to `dtype`.

  ## Examples

      iex> s = Series.from_list(["1"])
      iex> Series.json_decode(s, {:s, 64})
      #Explorer.Series<
        Polars[1]
        s64 [1]
      >

      iex> s = Series.from_list(["{\\"a\\":1}"])
      iex> Series.json_decode(s, {:struct, [{"a", {:s, 64}}]})
      #Explorer.Series<
        Polars[1]
        struct[1] [%{"a" => 1}]
      >

  If the decoded value does not match the given `dtype`,
  nil is returned for the given entry:

      iex> s = Series.from_list(["\\"1\\""])
      iex> Series.json_decode(s, {:s, 64})
      #Explorer.Series<
        Polars[1]
        s64 [nil]
      >

  It raises an exception if the string is invalid JSON.
  """
  @doc type: :string_wise
  @spec json_decode(Series.t(), dtype()) :: Series.t()
  def json_decode(%Series{dtype: :string} = series, dtype) do
    dtype = Shared.normalise_dtype!(dtype)

    apply_series(series, :json_decode, [dtype])
  end

  @doc """
  Extracts a string series using a [`json_path`](https://goessner.net/articles/JsonPath/) from a series.

  ## Examples

      iex> s = Series.from_list(["{\\"a\\":1}", "{\\"a\\":2}"])
      iex> Series.json_path_match(s, "$.a")
      #Explorer.Series<
        Polars[2]
        string [\"1\", \"2\"]
      >

  If `json_path` is not found or if the string is invalid JSON or `nil`,
  nil is returned for the given entry:

      iex> s = Series.from_list(["{\\"a\\":1}", nil, "{\\"a\\":2}"])
      iex> Series.json_path_match(s, "$.b")
      #Explorer.Series<
        Polars[3]
        string [nil, nil, nil]
      >

  It raises an exception if the `json_path` is invalid.
  """
  @doc type: :string_wise
  @spec json_path_match(Series.t(), String.t()) :: Series.t()
  def json_path_match(%Series{dtype: :string} = series, json_path) do
    apply_series(series, :json_path_match, [json_path])
  end

  # Helpers

  defp apply_series(series, fun, args \\ []) do
    if impl = impl!([series]) do
      apply(impl, fun, [series | args])
    else
      raise ArgumentError,
            "expected a series as argument for #{fun}, got: #{inspect(series)}" <>
              maybe_hint([series])
    end
  end

  defp apply_series_list(fun, series_or_scalars) when is_list(series_or_scalars) do
    impl = impl!(series_or_scalars)
    apply(impl, fun, series_or_scalars)
  end

  defp impl!([_ | _] = series_or_scalars) do
    Enum.reduce(series_or_scalars, nil, fn
      %{data: %struct{}}, nil -> struct
      %{data: %struct{}}, impl -> pick_series_impl(impl, struct)
      _scalar, impl -> impl
    end)
  end

  defp pick_series_impl(struct, struct), do: struct
  defp pick_series_impl(Explorer.Backend.LazySeries, _), do: Explorer.Backend.LazySeries
  defp pick_series_impl(_, Explorer.Backend.LazySeries), do: Explorer.Backend.LazySeries

  defp pick_series_impl(struct1, struct2) do
    raise "cannot invoke Explorer function because it relies on two incompatible series: " <>
            "#{inspect(struct1)} and #{inspect(struct2)}"
  end

  defp backend_from_options!(opts) do
    backend = Explorer.Shared.backend_from_options!(opts) || Explorer.Backend.get()

    :"#{backend}.Series"
  end

  defp dtype_error(function, dtype, valid_dtypes) when is_list(valid_dtypes) do
    raise(
      ArgumentError,
      "Explorer.Series.#{function} not implemented for dtype #{inspect(dtype)}. " <>
        "Valid " <> Shared.inspect_dtypes(valid_dtypes, with_prefix: true)
    )
  end

  @spec dtype_mismatch_error(String.t(), any(), any(), [any()]) :: no_return()
  defp dtype_mismatch_error(function, left, right, valid) do
    left_series? = match?(%Series{}, left)
    right_series? = match?(%Series{}, right)

    cond do
      Kernel.and(left_series?, Kernel.not(Enum.member?(valid, left.dtype))) ->
        dtype_error(function, left.dtype, valid)

      Kernel.and(right_series?, Kernel.not(Enum.member?(valid, right.dtype))) ->
        dtype_error(function, right.dtype, valid)

      Kernel.or(left_series?, right_series?) ->
        dtype_mismatch_error(function, left, right)

      true ->
        raise(
          ArgumentError,
          "expecting series for one of the sides, but got: " <>
            "#{dtype_or_inspect(left)} (lhs) and #{dtype_or_inspect(right)} (rhs)" <>
            maybe_hint([left, right])
        )
    end
  end

  defp dtype_mismatch_error(function, left, right) do
    raise(
      ArgumentError,
      "cannot invoke Explorer.Series.#{function} with mismatched dtypes: #{dtype_or_inspect(left)} and " <>
        "#{dtype_or_inspect(right)}" <> maybe_hint([left, right])
    )
  end

  defp dtype_or_inspect(%Series{dtype: dtype}), do: inspect(dtype)
  defp dtype_or_inspect(value), do: inspect(value)

  defp maybe_hint(values) do
    atom = Enum.find(values, &is_atom(&1))

    if Kernel.and(atom != nil, String.starts_with?(Atom.to_string(atom), "Elixir.")) do
      "\n\nHINT: we have noticed that one of the values is the atom #{inspect(atom)}. " <>
        "If you are inside Explorer.Query and you want to access a column starting in uppercase, " <>
        "you must write instead: col(\"#{inspect(atom)}\")"
    else
      ""
    end
  end

  defp check_dtypes_for_coalesce!(%Series{} = s1, %Series{} = s2) do
    # TODO: consider the unsigned types here.
    case {s1.dtype, s2.dtype} do
      {dtype, dtype} -> :ok
      {{:s, _}, {:f, _}} -> :ok
      {{:f, _}, {:s, _}} -> :ok
      {left, right} -> dtype_mismatch_error("coalesce/2", left, right)
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(series, opts) do
      force_unfit(
        concat([
          color("#Explorer.Series<", :map, opts),
          nest(
            concat([line(), Shared.apply_impl(series, :inspect, [opts])]),
            2
          ),
          line(),
          color(">", :map, opts)
        ])
      )
    end
  end
end

defmodule Explorer.Series.Iterator do
  @moduledoc false
  defstruct [:series, :size, :impl]

  def new(%{data: %impl{}} = series) do
    %__MODULE__{series: series, size: impl.size(series), impl: impl}
  end

  defimpl Enumerable do
    def count(iterator), do: {:ok, iterator.size}

    def member?(_iterator, _value), do: {:error, __MODULE__}

    def slice(%{size: size, series: series, impl: impl}) do
      {:ok, size,
       fn start, size ->
         series
         |> impl.slice(start, size)
         |> impl.to_list()
       end}
    end

    def reduce(%{series: series, size: size, impl: impl}, acc, fun) do
      reduce(series, impl, size, 0, acc, fun)
    end

    defp reduce(_series, _impl, _size, _offset, {:halt, acc}, _fun), do: {:halted, acc}

    defp reduce(series, impl, size, offset, {:suspend, acc}, fun) do
      {:suspended, acc, &reduce(series, impl, size, offset, &1, fun)}
    end

    defp reduce(_series, _impl, size, size, {:cont, acc}, _fun), do: {:done, acc}

    defp reduce(series, impl, size, offset, {:cont, acc}, fun) do
      value = impl.at(series, offset)
      reduce(series, impl, size, offset + 1, fun.(value, acc), fun)
    end
  end
end
