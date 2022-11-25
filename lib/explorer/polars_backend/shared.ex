defmodule Explorer.PolarsBackend.Shared do
  # A collection of **private** helpers shared in Explorer.PolarsBackend.
  @moduledoc false

  alias Explorer.DataFrame, as: DataFrame
  alias Explorer.PolarsBackend.DataFrame, as: PolarsDataFrame
  alias Explorer.PolarsBackend.LazyDataFrame, as: PolarsLazyFrame
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries
  alias Explorer.Series, as: Series

  @polars_df [PolarsDataFrame, PolarsLazyFrame]

  def apply_series(series, fun, args \\ [])

  def apply_series(%Series{} = series, fun, args) do
    case apply(Native, fun, [series.data | args]) do
      {:ok, %PolarsSeries{} = new_series} -> create_series(new_series)
      {:ok, %module{} = new_df} when module in @polars_df -> create_dataframe(new_df)
      {:ok, value} -> value
      {:error, error} -> raise error_message(error)
    end
  end

  def apply_dataframe(df_or_s, fun, args \\ [])

  def apply_dataframe(%DataFrame{} = df, fun, args) do
    case apply(Native, fun, [df.data | args]) do
      {:ok, %module{} = new_df} when module in @polars_df ->
        # TODO: this currently assumes we want to preserve the groups
        # but, if that's the case, we should be using apply_df/4.
        # In other words, we should find every caller of this clause
        # and make it use apply_dataframe/4 instead.
        %{create_dataframe(new_df) | groups: df.groups}

      {:ok, %PolarsSeries{} = new_series} ->
        create_series(new_series)

      {:ok, value} ->
        value

      {:error, error} ->
        raise error_message(error)
    end
  end

  @check_frames Application.compile_env(:explorer, :check_polars_frames, false)

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
        raise error_message(error)
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

  def from_list(list, dtype, name \\ "") when is_list(list) and is_atom(dtype) do
    case dtype do
      :integer -> Native.s_from_list_i64(name, list)
      :float -> Native.s_from_list_f64(name, list)
      :boolean -> Native.s_from_list_bool(name, list)
      :string -> Native.s_from_list_str(name, list)
      :date -> Native.s_from_list_date(name, list)
      :datetime -> Native.s_from_list_datetime(name, list)
    end
  end

  def from_binary(binary, bintype, alignment, name \\ "") when is_binary(binary) do
    case {bintype, alignment} do
      {:u, 8} ->
        Native.s_from_binary_u8(name, binary)

      {:s, 32} ->
        Native.s_from_binary_i32(name, binary)

      {:s, 64} ->
        Native.s_from_binary_i64(name, binary)

      {:f, 64} ->
        Native.s_from_binary_f64(name, binary)

      _ ->
        raise ArgumentError,
              "Polars backend does not support loading #{bintype}#{alignment} from binary"
    end
  end

  def normalise_dtype("u8"), do: :integer
  def normalise_dtype("u32"), do: :integer
  def normalise_dtype("i32"), do: :integer
  def normalise_dtype("i64"), do: :integer
  def normalise_dtype("f64"), do: :float
  def normalise_dtype("bool"), do: :boolean
  def normalise_dtype("str"), do: :string
  def normalise_dtype("date"), do: :date
  def normalise_dtype("datetime[ms]"), do: :datetime
  def normalise_dtype("datetime[μs]"), do: :datetime
  def normalise_dtype("datetime[ns]"), do: :datetime
  def normalise_dtype("list[u32]"), do: :integer

  def internal_from_dtype(:integer), do: "i64"
  def internal_from_dtype(:float), do: "f64"
  def internal_from_dtype(:boolean), do: "bool"
  def internal_from_dtype(:string), do: "str"
  def internal_from_dtype(:date), do: "date"
  def internal_from_dtype(:datetime), do: "datetime[μs]"

  defp error_message({_err_type, error}) when is_binary(error), do: error
  defp error_message(error), do: inspect(error)
end
