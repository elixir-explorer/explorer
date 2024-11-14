defmodule Explorer.Generator do
  @moduledoc """
  `StreamData` generators

  ## Notes

    * A default fixed/max length of <= 3 used quite a bit. This is intentional
      because issues usually stem from empty lists and `nil`s, not long lists.
      By keeping list sizes small, we can iterate much quicker through the input
      space.
  """

  import Bitwise, only: [<<<: 2]
  import StreamData

  @type gen(a) :: StreamData.t(a)

  @type field_name() :: atom() | String.t()
  @type column_name() :: atom() | String.t()
  @type key() :: field_name() | column_name()
  @type val() :: any()

  @type dtype() :: Explorer.Series.dtype()
  @type dtypes() :: [{column_name(), dtype()}] | %{column_name() => dtype()}

  @type row() :: [{column_name(), val()}] | %{column_name() => val()}

  @type time_unit() :: Explorer.Series.time_unit()

  @type dtype_opt() :: {:include, [dtype()]} | {:exclude, [dtype()]}
  @type dtype_opts() :: [dtype_opt()]

  @type dtypes_opt() ::
          {:min_length, non_neg_integer()}
          | {:max_length, non_neg_integer()}
          | {:length, non_neg_integer()}
  @type dtypes_opts() :: [dtypes_opt() | dtype_opt()]

  @doc """
  Generate row for dtypes

  The shape of the generated `row` will match that of `dtypes`: either a map or
  a list of 2-tuples.

  ## Examples

  Dtypes as list:

      iex> dtypes_list = [{"name", :string}, {"age", {:s, 64}}]
      iex> [row] = Explorer.Generator.row(dtypes_list) |> Enum.take(1)
      iex> row
      [{"name", "John"}, {"age", 20}]

  Dtypes as map:

      iex> dtypes_map = %{"name" => :string, "age" => {:s, 64}}
      iex> [row] = Explorer.Generator.row(dtypes_map) |> Enum.take(1)
      iex> row
      [%{"name" => "John", "age" => 20}]

  """
  @spec row(dtypes()) :: gen(row())
  def row(dtypes) do
    kv(dtypes, &value/1)
  end

  @doc """
  Generate a list of rows for dtypes

  ## Options

  Accepts any valid option to `StreamData.list_of/2` like `:length`.

  ## Examples

      iex> dtypes = [{"name", :string}, {"age", {:s, 64}}]
      iex> [rows] = Explorer.Generator.rows(dtypes) |> Enum.take(1)
      iex> rows
      [
        [{"name", "John"}, {"age", 20}],
        [{"name", "Sarah"}, {"age", 30}],
      ]

  """
  @spec rows(dtypes()) :: gen([row()])
  def rows(dtypes, opts \\ []) do
    opts = Keyword.put_new(opts, :max_length, 3)

    list_of(row(dtypes), opts)
  end

  @doc """
  Generate a column of dtype as a list

  ## Options

    * `:as` (`atom()`) - Set the return type to either a list or series. Must be
      one of `:list` or `:series`.

  Also accepts any valid option to `StreamData.list_of/2` like `:length`.

  ## Examples

      iex> [col] = Explorer.Generator.column(:string) |> Enum.take(1)
      iex> col
      ["a", "", nil]
  """
  @spec column(dtype(), Keyword.t()) :: gen([val()])
  def column(dtype, opts \\ []) do
    opts = Keyword.put_new(opts, :length, 3)

    column_as_list = list_of(value(dtype), opts)

    case Keyword.get(opts, :as, :list) do
      :list -> column_as_list
      :series -> map(column_as_list, &Explorer.Series.from_list/1)
      _ -> raise ArgumentError, "`:as` must be `:list` or `:series`"
    end
  end

  @doc """
  Generate columns for dtypes

  ## Options

  See `column/2` for valid options.

  ## Examples

      iex> dtypes = [{"name", :string}, {"age", {:s, 64}}]
      iex> [columns] = Explorer.Generator.columns(dtypes) |> Enum.take(1)
      iex> columns
      [
        {"name", ["a", "", nil]},
        {"age", [-1, 2, nil]}
      ]

  Generate columns where each value is a series instead of a list:

      iex> dtypes = [{"name", :string}, {"age", {:s, 64}}]
      iex> generator = Explorer.Generator.columns(dtypes, as: :series)
      iex> [columns] = generator |> Enum.take(1)
      iex> columns
      [
        {"name",
        #Explorer.Series<
          Polars[3]
          string ["a", "", nil]
        >},
        {"age",
        #Explorer.Series<
          Polars[3]
          s64 [-1, 2, nil]
        >}
      ]
  """
  def columns(dtypes, opts \\ []) do
    kv(dtypes, &column(&1, opts))
  end

  @spec kv(dtypes(), (dtype -> gen(a))) :: gen([{key(), a}] | %{key() => a}) when a: any()
  defp kv(dtypes, dtype_fun) do
    case dtypes do
      map when is_map(map) ->
        map
        |> Enum.map(fn {key, dtype} -> {key, dtype_fun.(dtype)} end)
        |> fixed_map()

      list when is_list(list) ->
        list
        |> Enum.map(fn {key, dtype} -> {constant(key), dtype_fun.(dtype)} end)
        |> fixed_list()
    end
  end

  @doc """
  Generate DataFrame dtypes

  ## Examples

      iex> Explorer.Generator.dtypes() |> Enum.take(1)
      [
        {"name", :string},
        {"age", {:s, 64}},
        {"is_active", :boolean}
      ]

  """
  @spec dtypes(opts :: dtypes_opts()) :: gen(dtypes())
  def dtypes(opts \\ []) do
    {list_of_opts, dtype_opts} = Keyword.split(opts, [:min_length, :max_length, :length])

    list_of_opts =
      list_of_opts
      |> Keyword.put_new(:min_length, 1)
      |> Keyword.put_new(:max_length, 2)
      |> Keyword.put(:uniq_fun, &elem(&1, 0))

    uniq_list_of(tuple({column_name(), dtype(dtype_opts)}), list_of_opts)
  end

  # For clarity, column names and field names are built from different halves of
  # the alphabet so there are no collisions. Columns are also prefixed with
  # "col_" to make it even clearer which is which.
  @spec column_name() :: gen(column_name())
  defp column_name(), do: map(string(?a..?m, min_length: 1, max_length: 1), &"col_#{&1}")

  @spec field_name() :: gen(field_name())
  defp field_name(), do: string(?n..?z, min_length: 1, max_length: 1)

  @doc """
  Generate Series dtype

  ## Options

    * `:exclude` (`[atom()]`) - A list of dtype aliases to _not_ include. The
      base dtype generator will return any dtype except those in this list. See
      below for details on aliases.

    * `:include` (`[atom()]`) - A list of dtype aliases to include. The base
      dtype generator will return one of the dtypes included in this list of
      aliases. See below for details on aliases.

    * `:scalar` (`StreamData.t()`) - A custom scalar dtype generator. This
      generator will be used as the base for building composite dtypes.

  You may not pass both of `:include` and `:exclude`.

  ### Aliases

  The following aliases are available for the `:include`/`:exclude` options. If
  you need more control over which scalar dtypes are included, use the `:scalar`
  option to pass a custom generator.

    * `:binary`
    * `:boolean`
    * `:category`
    * `:date`
    * `:datetime`
    * `:decimal` - any precision/scale
    * `:duration` - any precision/timezone
    * `:float` - any bit size
    * `:integer` - any bit size, signed or unsigned
    * `:naive_datetime` - any precision
    * `:null`
    * `:string`
    * `:time`

  ## Examples

      iex> Explorer.Generator.dtype() |> Enum.take(2)
      [
        :string,
        {:list, {:struct, [{"name", :string}, {"age", {:s, 64}}]}}
      ]

  Pass a custom scalar dtype generator:

      iex> Explorer.Generator.dtype(scalar: constant(:string)) |> Enum.take(3)
      [
        :string,
        {:list, :string}
        {:struct, [{"n", :string}, {"z", :string}]}
      ]

  """
  @spec dtype(ops :: dtype_opts()) :: gen(dtype())
  def dtype(opts \\ []) do
    {scalar, opts} = Keyword.pop(opts, :scalar)

    scalar = scalar || scalar_dtype(opts)

    tree(scalar, fn node ->
      struct =
        uniq_list_of(tuple({field_name(), node}),
          uniq_fun: &elem(&1, 0),
          min_length: 1,
          max_length: 2
        )

      one_of([
        tuple({constant(:list), node}),
        tuple({constant(:struct), struct})
      ])
    end)
  end

  @spec scalar_dtype(keyword()) :: gen(dtype())
  defp scalar_dtype(opts) do
    scalars_by_alias =
      %{
        binary: constant(:binary),
        boolean: constant(:boolean),
        category: constant(:category),
        date: constant(:date),
        datetime: tuple({constant(:datetime), time_unit(), constant("Etc/UTC")}),
        decimal:
          bind(integer(0..37), fn scale ->
            bind(integer((scale + 1)..38), fn precision ->
              tuple({constant(:decimal), constant(precision), constant(scale)})
            end)
          end),
        duration: tuple({constant(:duration), time_unit()}),
        float: tuple({constant(:f), one_of([constant(32), constant(64)])}),
        integer:
          tuple(
            {one_of([constant(:s), constant(:u)]),
             one_of([constant(8), constant(16), constant(32), constant(64)])}
          ),
        naive_datetime: tuple({constant(:naive_datetime), time_unit()}),
        null: constant(:null),
        string: constant(:string),
        time: constant(:time)
      }

    if Keyword.has_key?(opts, :include) and Keyword.has_key?(opts, :exclude) do
      raise ArgumentError, "can pass at most one of `:include` and `:exclude`"
    end

    keys =
      case Map.new(opts) do
        %{include: include} -> List.wrap(include)
        %{exclude: exclude} -> Map.keys(scalars_by_alias) -- List.wrap(exclude)
        %{} -> Map.keys(scalars_by_alias)
      end

    scalars_by_alias
    |> Map.take(keys)
    |> Map.values()
    |> one_of()
  end

  @spec time_unit() :: gen(time_unit())
  def time_unit do
    one_of([
      constant(:millisecond),
      constant(:microsecond),
      constant(:nanosecond)
    ])
  end

  @spec value(dtype()) :: gen(val())
  defp value(:null), do: constant(nil)
  defp value(dtype), do: one_of([constant(nil), non_nil_value(dtype)])

  # Each value in a series can be nil. This generates a non-nil value for the dtype.
  defp non_nil_value(:binary), do: binary(max_length: 2)
  defp non_nil_value(:boolean), do: boolean()
  defp non_nil_value(:category), do: string(:utf8, max_length: 2)
  defp non_nil_value(:string), do: string(:utf8, max_length: 2)
  defp non_nil_value(:date), do: date()
  defp non_nil_value(:time), do: time()
  defp non_nil_value({:naive_datetime, time_unit}), do: naive_datetime(time_unit)
  defp non_nil_value({:datetime, time_unit, timezone}), do: datetime(time_unit, timezone)
  defp non_nil_value({:duration, time_unit}), do: duration(time_unit)
  defp non_nil_value({:decimal, precision, scale}), do: decimal(precision, scale)
  defp non_nil_value({:f, 32}), do: f32()
  defp non_nil_value({:f, 64}), do: float()
  defp non_nil_value({:u, bits}), do: integer(0..((1 <<< bits) - 1))
  defp non_nil_value({:s, bits}), do: integer((-1 <<< (bits - 1))..((1 <<< (bits - 1)) - 1))
  defp non_nil_value({:list, dtype}), do: list_of(value(dtype), max_length: 2)
  defp non_nil_value({:struct, fields}), do: struct_of(fields)

  @spec date() :: gen(Date.t())
  defp date do
    {integer(1..9999), integer(1..12), integer(1..31)}
    |> tuple()
    |> map(fn {year, month, day} -> Date.new(year, month, day) end)
    |> filter(&match?({:ok, _date}, &1))
    |> map(&elem(&1, 1))
  end

  @spec time() :: gen(Time.t())
  defp time do
    {integer(0..23), integer(0..59), integer(0..59), microsecond()}
    |> tuple()
    |> map(fn {hour, minute, second, microseconds} ->
      Time.new!(hour, minute, second, microseconds)
    end)
  end

  @spec naive_datetime(time_unit()) :: gen(NaiveDateTime.t())
  defp naive_datetime(_time_unit) do
    {date(), time()}
    |> tuple()
    |> map(fn {date, time} -> NaiveDateTime.new(date, time) end)
    |> filter(&match?({:ok, _naive_datetime}, &1))
    |> map(&elem(&1, 1))
  end

  @spec datetime(time_unit(), String.t()) :: gen(DateTime.t())
  defp datetime(time_unit, timezone) do
    time_unit
    |> naive_datetime()
    |> map(fn naive_datetime -> DateTime.from_naive(naive_datetime, timezone) end)
    |> filter(&match?({:ok, _datetime}, &1))
    |> map(&elem(&1, 1))
  end

  @spec datetime(pos_integer(), pos_integer()) :: gen(Decimal.t())
  defp decimal(precision, scale) do
    tuple({
      one_of([constant("-"), constant("+")]),
      string(?0..?9, min_length: precision - scale, max_length: precision - scale),
      string(?0..?9, min_length: scale, max_length: scale)
    })
    |> map(fn {sign, integer_part, fractional_part} ->
      Decimal.new(sign <> integer_part <> "." <> fractional_part)
    end)
  end

  @spec f32() :: gen(float())
  defp f32 do
    filter(float(), fn
      float when float in [0.0, -0.0] -> true
      float -> float |> abs() |> :math.log10() |> trunc() < 38
    end)
  end

  @spec duration(time_unit()) :: gen(Explorer.Duration.t())
  defp duration(time_unit) do
    map(integer(), fn value ->
      %Explorer.Duration{value: value, precision: time_unit}
    end)
  end

  @spec microsecond() :: gen(Calendar.microsecond())
  defp microsecond(), do: tuple({integer(0..999_999), constant(6)})

  @spec struct_of(%{field_name() => dtype()} | [{field_name(), dtype()}]) ::
          gen(%{field_name() => val()})
  defp struct_of(fields) do
    fields |> Map.new() |> kv(&value/1)
  end
end
