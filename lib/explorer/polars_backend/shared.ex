defmodule Explorer.PolarsBackend.Shared do
  # A collection of **private** helpers shared in Explorer.PolarsBackend.
  @moduledoc false

  alias Explorer.DataFrame, as: DataFrame
  alias Explorer.PolarsBackend.DataFrame, as: PolarsDataFrame
  alias Explorer.PolarsBackend.LazyFrame, as: PolarsLazyFrame
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries
  alias Explorer.Series, as: Series

  @valid_dtypes Explorer.Shared.dtypes()
  @polars_df [PolarsDataFrame, PolarsLazyFrame]

  def apply(fun, args \\ []) do
    case apply(Native, fun, args) do
      {:ok, value} -> value
      {:error, error} -> raise runtime_error(error)
    end
  end

  # Applies to a series. Expects a series or a value back.
  def apply_series(%Series{} = series, fun, args \\ []) do
    case apply(Native, fun, [series.data | args]) do
      {:ok, %PolarsSeries{} = new_series} -> create_series(new_series)
      {:ok, value} -> value
      {:error, error} -> raise runtime_error(error)
    end
  end

  # Applies to a dataframe. Expects a series or a value back.
  def apply_dataframe(%DataFrame{} = df, fun, args \\ []) do
    case apply(Native, fun, [df.data | args]) do
      {:ok, %PolarsSeries{} = new_series} -> create_series(new_series)
      {:ok, value} -> value
      {:error, error} -> raise runtime_error(error)
    end
  end

  @check_frames Application.compile_env(:explorer, :check_polars_frames, false)

  # Applies to a dataframe. Expects a dataframe back.
  def apply_dataframe(%DataFrame{} = df, %DataFrame{} = out_df, fun, args) do
    case apply(Native, fun, [df.data | args]) do
      {:ok, %module{} = new_df} when module in @polars_df ->
        if @check_frames do
          check_df = create_dataframe(new_df)

          if Enum.sort(out_df.names) != Enum.sort(check_df.names) or
               out_df.dtypes != check_df.dtypes do
            raise """
            DataFrame mismatch.

            expected:

                names: #{inspect(out_df.names)}
                dtypes: #{inspect(out_df.dtypes)}

            got:

                names: #{inspect(check_df.names)}
                dtypes: #{inspect(check_df.dtypes)}
            """
          end
        end

        %{out_df | data: new_df}

      {:error, error} ->
        raise runtime_error(error)
    end
  end

  def create_series(%PolarsSeries{} = polars_series) do
    {:ok, dtype} = Native.s_dtype(polars_series)
    Explorer.Backend.Series.new(polars_series, normalise_dtype(dtype))
  end

  def create_dataframe(polars_df) do
    Explorer.Backend.DataFrame.new(polars_df, df_names(polars_df), df_dtypes(polars_df))
  end

  defp df_names(%PolarsDataFrame{} = polars_df) do
    {:ok, names} = Native.df_names(polars_df)
    names
  end

  defp df_names(%PolarsLazyFrame{} = polars_df) do
    {:ok, names} = Native.lf_names(polars_df)
    names
  end

  defp df_dtypes(%PolarsDataFrame{} = polars_df) do
    {:ok, dtypes} = Native.df_dtypes(polars_df)
    Enum.map(dtypes, &normalise_dtype/1)
  end

  defp df_dtypes(%PolarsLazyFrame{} = polars_df) do
    {:ok, dtypes} = Native.lf_dtypes(polars_df)
    Enum.map(dtypes, &normalise_dtype/1)
  end

  def from_list(list, dtype, name \\ "") when is_list(list) and dtype in @valid_dtypes do
    case dtype do
      :integer -> Native.s_from_list_i64(name, list)
      :float -> Native.s_from_list_f64(name, list)
      :boolean -> Native.s_from_list_bool(name, list)
      :string -> Native.s_from_list_str(name, list)
      :category -> Native.s_from_list_categories(name, list)
      :date -> Native.s_from_list_date(name, list)
      :time -> Native.s_from_list_time(name, list)
      {:datetime, _} -> Native.s_from_list_datetime(name, list)
      :binary -> Native.s_from_list_binary(name, list)
    end
  end

  def from_binary(binary, dtype, name \\ "") when is_binary(binary) do
    case dtype do
      :boolean ->
        Native.s_from_binary_u8(name, binary) |> Native.s_cast("boolean") |> ok()

      :date ->
        Native.s_from_binary_i32(name, binary) |> Native.s_cast("date") |> ok()

      :time ->
        Native.s_from_binary_i64(name, binary) |> Native.s_cast("time") |> ok()

      {:datetime, :milli_seconds} ->
        Native.s_from_binary_i64(name, binary) |> Native.s_cast("datetime[ms]") |> ok()

      {:datetime, :micro_seconds} ->
        Native.s_from_binary_i64(name, binary) |> Native.s_cast("datetime[μs]") |> ok()

      {:datetime, :nano_seconds} ->
        Native.s_from_binary_i64(name, binary) |> Native.s_cast("datetime[ns]") |> ok()

      :integer ->
        Native.s_from_binary_i64(name, binary)

      :float ->
        Native.s_from_binary_f64(name, binary)
    end
  end

  defp ok({:ok, value}), do: value

  def normalise_dtype("binary"), do: :binary
  def normalise_dtype("bool"), do: :boolean
  def normalise_dtype("cat"), do: :category
  def normalise_dtype("date"), do: :date
  def normalise_dtype("time"), do: :time
  def normalise_dtype("datetime[ms]"), do: {:datetime, :milli_seconds}
  def normalise_dtype("datetime[ns]"), do: {:datetime, :nano_seconds}
  def normalise_dtype("datetime[μs]"), do: {:datetime, :micro_seconds}
  def normalise_dtype("f64"), do: :float
  def normalise_dtype("i64"), do: :integer
  def normalise_dtype("list[u32]"), do: :integer
  def normalise_dtype("str"), do: :string

  def internal_from_dtype(:binary), do: "binary"
  def internal_from_dtype(:boolean), do: "bool"
  def internal_from_dtype(:category), do: "cat"
  def internal_from_dtype(:date), do: "date"
  def internal_from_dtype(:time), do: "time"
  def internal_from_dtype({:datetime, :milli_seconds}), do: "datetime[ms]"
  def internal_from_dtype({:datetime, :nano_seconds}), do: "datetime[ns]"
  def internal_from_dtype({:datetime, :micro_seconds}), do: "datetime[μs]"
  def internal_from_dtype(:float), do: "f64"
  def internal_from_dtype(:integer), do: "i64"
  def internal_from_dtype(:string), do: "str"

  defp runtime_error(error) when is_binary(error), do: RuntimeError.exception(error)

  def parquet_compression(nil, _), do: :uncompressed

  def parquet_compression(algorithm, level) when algorithm in ~w(gzip brotli zstd)a,
    do: {algorithm, level}

  def parquet_compression(algorithm, _) when algorithm in ~w(snappy lz4raw)a, do: algorithm
end
