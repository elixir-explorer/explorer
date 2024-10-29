defmodule Explorer.Generator do
  @moduledoc """
  `StreamData` generators
  """

  import Bitwise, only: [<<<: 2]
  import StreamData

  @type dtypes() :: [{String.t(), Explorer.Series.dtype()}]

  @type dtype_opt :: {:include, [Explorer.Series.dtype()]} | {:exclude, [Explorer.Series.dtype()]}
  @type dtype_opts() :: [dtype_opt()]

  @type dtypes_opt ::
          {:min_length, non_neg_integer()}
          | {:max_length, non_neg_integer()}
          | {:length, non_neg_integer()}
  @type dtypes_opts() :: [dtypes_opt() | dtype_opt()]

  @doc """
  Generate DataFrame dtypes

  ## Examples

      iex> Explorer.Generator.dtypes() |> Enum.take(1)
      [
        {"name", :string},
        {"age", :integer},
        {"is_active", :boolean}
      ]

  """
  @spec dtypes(opts :: dtypes_opts()) :: Enumerable.t(dtypes())
  def dtypes(opts \\ []) do
    {list_opts, dtype_opts} = Keyword.split(opts, [:min_length, :max_length, :length])

    list_opts =
      list_opts
      |> Keyword.put_new(:min_length, 1)
      |> Keyword.put(:uniq_fun, &elem(&1, 0))

    uniq_list_of(
      tuple({
        string(:utf8, min_length: 1),
        dtype(dtype_opts)
      }),
      list_opts
    )
  end

  @doc """
  Generate Series dtype

  ## Examples

      iex> Explorer.Generator.dtype() |> Enum.take(2)
      [
        :string,
        {:list, {:struct, [{"name", :string}, {"age", :integer}]}}
      ]

  """
  @spec dtype(opts :: dtype_opts()) :: Enumerable.t(Explorer.Series.dtype())
  def dtype(opts \\ []) do
    opts
    |> scalar_dtype()
    |> tree(fn nested_generator ->
      one_of([
        tuple({
          constant(:list),
          nested_generator
        }),
        tuple({
          constant(:struct),
          uniq_list_of(
            tuple({
              string(:utf8, min_length: 1),
              nested_generator
            }),
            uniq_fun: &elem(&1, 0),
            min_length: 1
          )
        })
      ])
    end)
    |> filter_dtype_inclusion(opts)
  end

  defp scalar_dtype(opts) do
    time_unit =
      one_of([
        constant(:nanosecond),
        constant(:microsecond),
        constant(:millisecond)
      ])

    one_of([
      constant(:null),
      constant(:binary),
      constant(:boolean),
      constant(:category),
      constant(:date),
      constant(:time),
      constant(:string),
      tuple({
        constant(:datetime),
        time_unit,
        one_of([
          constant("Etc/UTC")
        ])
      }),
      bind(integer(0..37), fn scale ->
        bind(integer((scale + 1)..38), fn precision ->
          tuple({constant(:decimal), constant(precision), constant(scale)})
        end)
      end),
      tuple({
        constant(:duration),
        time_unit
      }),
      tuple({
        constant(:f),
        one_of([
          constant(32),
          constant(64)
        ])
      }),
      tuple({
        constant(:naive_datetime),
        time_unit
      }),
      tuple({
        one_of([constant(:u), constant(:s)]),
        one_of([constant(8), constant(16), constant(32), constant(64)])
      })
    ])
    |> filter_dtype_inclusion(opts)
  end

  defp filter_dtype_inclusion(dtype, opts) do
    dtype =
      case Keyword.fetch(opts, :include) do
        {:ok, include} -> filter(dtype, &Enum.member?(include, &1))
        :error -> dtype
      end

    case Keyword.fetch(opts, :exclude) do
      {:ok, exclude} -> filter(dtype, &(!Enum.member?(exclude, &1)))
      :error -> dtype
    end
  end

  @doc """
  Generate row for dtypes

  ## Examples

      iex> Explorer.Generator.row([{"name", :string}, {"age", :integer}]) |> Enum.take(1)
      [
        {"name", "John", "age", 20}
      ]

  """
  @spec row(dtypes :: dtypes()) :: %{String.t() => term()}
  def row(dtypes) do
    fixed_map(Enum.map(dtypes, fn {name, dtype} -> {name, field(dtype)} end))
  end

  @doc """
  Generate field for dtype

  ## Examples

      iex> Explorer.Generator.field(:string) |> Enum.take(1)
      ["John"]

  """
  @spec field(dtype :: Explorer.Series.dtype()) :: term()
  def field(:null), do: constant(nil)
  def field(dtype), do: one_of([constant(nil), non_nil(dtype)])

  # Each term in a series can be nil. This generates a non-nil term for the dtype.
  defp non_nil(:binary), do: binary(max_length: 2)
  defp non_nil(:boolean), do: boolean()
  defp non_nil(:category), do: string(:utf8, max_length: 2)
  defp non_nil(:string), do: string(:utf8, max_length: 2)
  defp non_nil(:date), do: date()
  defp non_nil(:time), do: time()
  defp non_nil({:naive_datetime, time_unit}), do: naive_datetime(time_unit)
  defp non_nil({:datetime, time_unit, timezone}), do: datetime(time_unit, timezone)
  defp non_nil({:duration, time_unit}), do: duration(time_unit)
  defp non_nil({:decimal, precision, scale}), do: decimal(precision, scale)
  defp non_nil({:f, 32}), do: f32()
  defp non_nil({:f, 64}), do: float()
  defp non_nil({:u, bits}), do: integer(0..((1 <<< bits) - 1))
  defp non_nil({:s, bits}), do: integer((-1 <<< (bits - 1))..((1 <<< (bits - 1)) - 1))
  defp non_nil({:list, dtype}), do: list_of(field(dtype))
  defp non_nil({:struct, fields}), do: struct_of(fields)

  defp date do
    {integer(1..9999), integer(1..12), integer(1..31)}
    |> tuple()
    |> map(fn {year, month, day} -> Date.new(year, month, day) end)
    |> filter(&match?({:ok, _date}, &1))
    |> map(&elem(&1, 1))
  end

  defp time do
    {integer(0..23), integer(0..59), integer(0..59), microsecond(:microsecond)}
    |> tuple()
    |> map(fn {hour, minute, second, microseconds} ->
      Time.new!(hour, minute, second, microseconds)
    end)
  end

  defp naive_datetime(_time_unit) do
    {date(), time()}
    |> tuple()
    |> map(fn {date, time} -> NaiveDateTime.new(date, time) end)
    |> filter(&match?({:ok, _naive_datetime}, &1))
    |> map(&elem(&1, 1))
  end

  defp datetime(time_unit, timezone) do
    time_unit
    |> naive_datetime()
    |> map(fn naive_datetime -> DateTime.from_naive(naive_datetime, timezone) end)
    |> filter(&match?({:ok, _datetime}, &1))
    |> map(&elem(&1, 1))
  end

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

  defp f32 do
    filter(float(), fn
      float when float in [0.0, -0.0] -> true
      float -> float |> abs() |> :math.log10() |> trunc() < 38
    end)
  end

  defp duration(time_unit) do
    map(integer(), &%Explorer.Duration{value: &1, precision: time_unit})
  end

  defp microsecond(time_unit)
  defp microsecond(:microsecond), do: tuple({integer(0..999_999), constant(6)})
  defp microsecond(:millisecond), do: tuple({integer(0..999), constant(3)})
  # Nanoseconds is not supported
  defp microsecond(:nanosecond), do: microsecond(:microsecond)

  defp struct_of(fields) do
    fields
    |> Enum.map(fn {name, dtype} -> {name, field(dtype)} end)
    |> fixed_map()
  end
end
