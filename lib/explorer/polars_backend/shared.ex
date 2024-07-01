defmodule Explorer.PolarsBackend.Shared do
  # A collection of **private** helpers shared in Explorer.PolarsBackend.
  @moduledoc false

  alias Explorer.DataFrame, as: DataFrame
  alias Explorer.PolarsBackend.DataFrame, as: PolarsDataFrame
  alias Explorer.PolarsBackend.LazyFrame, as: PolarsLazyFrame
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries
  alias Explorer.Series, as: Series
  import Kernel, except: [apply: 2]

  @polars_df [PolarsDataFrame, PolarsLazyFrame]

  def apply(fun, args \\ []) do
    case Kernel.apply(Native, fun, args) do
      {:ok, value} -> value
      {:error, error} -> raise runtime_error(error)
    end
  end

  # Applies to a series. Expects a series or a value back.
  def apply_series(%Series{} = series, fun, args \\ []) do
    args =
      Enum.map([series | args], fn
        %Series{data: %PolarsSeries{} = polars_series} -> polars_series
        other -> other
      end)

    case apply(Native, fun, args) do
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
          # We need to collect here, because the lazy frame may not have
          # the full picture of the result yet.
          check_df =
            if match?(%PolarsLazyFrame{}, new_df) do
              case Native.lf_collect(new_df) do
                {:ok, new_df} -> create_dataframe(new_df)
                {:error, error} -> raise runtime_error(error)
              end
            else
              create_dataframe(new_df)
            end

          if Enum.sort(out_df.names) != Enum.sort(check_df.names) or
               not dtypes_match?(check_df.dtypes, out_df.dtypes) do
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

  defp dtypes_match?(check_dtypes, out_dtypes) do
    if map_size(check_dtypes) != map_size(out_dtypes) do
      false
    else
      # The dtypes for each key must either be the same or `:unknown`.
      Enum.all?(check_dtypes, fn {key, check_value} ->
        case Map.fetch(out_dtypes, key) do
          {:ok, ^check_value} -> true
          {:ok, :unknown} -> true
          _ -> false
        end
      end)
    end
  end

  def create_series(%PolarsSeries{} = polars_series) do
    dtype =
      case Native.s_dtype(polars_series) do
        {:ok, dtype} ->
          dtype

        {:error, reason} ->
          raise ArgumentError, reason
      end

    Explorer.Backend.Series.new(polars_series, dtype)
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
    dtypes
  end

  defp df_dtypes(%PolarsLazyFrame{} = polars_df) do
    {:ok, dtypes} = Native.lf_dtypes(polars_df)
    dtypes
  end

  def from_list(list, dtype), do: from_list(list, dtype, "")

  def from_list([], {:list, _} = dtype, name) do
    polars_series = Native.s_from_list_of_series(name, [])
    {:ok, casted} = Native.s_cast(polars_series, dtype)
    casted
  end

  def from_list(list, {:list, inner_dtype} = _dtype, name) when is_list(list) do
    series =
      Enum.map(list, fn maybe_inner_list ->
        if is_list(maybe_inner_list), do: from_list(maybe_inner_list, inner_dtype, name)
      end)

    Native.s_from_list_of_series(name, series)
  end

  def from_list([], {:struct, _} = dtype, name) do
    polars_series = Native.s_from_list_of_series_as_structs(name, [])
    {:ok, casted} = Native.s_cast(polars_series, dtype)
    casted
  end

  def from_list(list, {:struct, fields}, name) when is_list(list) do
    columns = Map.new(fields, fn {k, _v} -> {k, []} end)

    columns =
      Enum.reduce(list, columns, fn
        nil, columns ->
          Enum.reduce(fields, columns, fn {field, _}, columns ->
            Map.update!(columns, field, &[nil | &1])
          end)

        row, columns ->
          Enum.reduce(row, columns, fn {field, value}, columns ->
            Map.update!(columns, to_string(field), &[value | &1])
          end)
      end)

    series =
      for {field, inner_dtype} <- fields do
        columns
        |> Map.fetch!(field)
        |> Enum.reverse()
        |> from_list(inner_dtype, field)
      end

    Native.s_from_list_of_series_as_structs(name, series)
  end

  def from_list(list, dtype, name) when is_list(list) do
    case dtype do
      # Signed integers
      {:s, 8} -> Native.s_from_list_s8(name, list)
      {:s, 16} -> Native.s_from_list_s16(name, list)
      {:s, 32} -> Native.s_from_list_s32(name, list)
      {:s, 64} -> Native.s_from_list_s64(name, list)
      # Unsigned integers
      {:u, 8} -> Native.s_from_list_u8(name, list)
      {:u, 16} -> Native.s_from_list_u16(name, list)
      {:u, 32} -> Native.s_from_list_u32(name, list)
      {:u, 64} -> Native.s_from_list_u64(name, list)
      # Floats
      {:f, 32} -> Native.s_from_list_f32(name, list)
      {:f, 64} -> Native.s_from_list_f64(name, list)
      :boolean -> Native.s_from_list_bool(name, list)
      :string -> Native.s_from_list_str(name, list)
      :category -> Native.s_from_list_categories(name, list)
      :date -> apply(:s_from_list_date, [name, list])
      :time -> apply(:s_from_list_time, [name, list])
      {:naive_datetime, precision} -> apply(:s_from_list_naive_datetime, [name, list, precision])
      {:datetime, precision, tz} -> apply(:s_from_list_datetime, [name, list, precision, tz])
      {:duration, precision} -> apply(:s_from_list_duration, [name, list, precision])
      :binary -> Native.s_from_list_binary(name, list)
      :null -> Native.s_from_list_null(name, length(list))
    end
  end

  def from_binary(binary, dtype, name \\ "") when is_binary(binary) do
    case dtype do
      :boolean ->
        Native.s_from_binary_u8(name, binary) |> Native.s_cast(dtype) |> ok()

      :date ->
        Native.s_from_binary_s32(name, binary) |> Native.s_cast(dtype) |> ok()

      :time ->
        Native.s_from_binary_s64(name, binary) |> Native.s_cast(dtype) |> ok()

      {:naive_datetime, :millisecond} ->
        Native.s_from_binary_s64(name, binary) |> Native.s_cast(dtype) |> ok()

      {:naive_datetime, :microsecond} ->
        Native.s_from_binary_s64(name, binary) |> Native.s_cast(dtype) |> ok()

      {:naive_datetime, :nanosecond} ->
        Native.s_from_binary_s64(name, binary) |> Native.s_cast(dtype) |> ok()

      {:duration, :millisecond} ->
        Native.s_from_binary_s64(name, binary) |> Native.s_cast(dtype) |> ok()

      {:duration, :microsecond} ->
        Native.s_from_binary_s64(name, binary) |> Native.s_cast(dtype) |> ok()

      {:duration, :nanosecond} ->
        Native.s_from_binary_s64(name, binary) |> Native.s_cast(dtype) |> ok()

      {:s, 8} ->
        Native.s_from_binary_s8(name, binary)

      {:s, 16} ->
        Native.s_from_binary_s16(name, binary)

      {:s, 32} ->
        Native.s_from_binary_s32(name, binary)

      {:s, 64} ->
        Native.s_from_binary_s64(name, binary)

      {:u, 8} ->
        Native.s_from_binary_u8(name, binary)

      {:u, 16} ->
        Native.s_from_binary_u16(name, binary)

      {:u, 32} ->
        Native.s_from_binary_u32(name, binary)

      {:u, 64} ->
        Native.s_from_binary_u64(name, binary)

      {:f, 32} ->
        Native.s_from_binary_f32(name, binary)

      {:f, 64} ->
        Native.s_from_binary_f64(name, binary)
    end
  end

  defp ok({:ok, value}), do: value

  defp runtime_error(error) when is_binary(error), do: RuntimeError.exception(error)

  def parquet_compression(nil, _), do: :uncompressed

  def parquet_compression(algorithm, level) when algorithm in ~w(gzip brotli zstd)a,
    do: {algorithm, level}

  def parquet_compression(algorithm, _) when algorithm in ~w(snappy lz4raw)a, do: algorithm

  @doc """
  Builds and returns a path for a new file.

  It saves in a directory called "elixir-explorer-datasets" inside
  the `System.tmp_dir()`.
  """
  def build_path_for_entry(%FSS.S3.Entry{} = entry) do
    bucket = entry.config.bucket || "default-explorer-bucket"

    hash =
      :crypto.hash(:sha256, entry.config.endpoint <> "/" <> bucket <> "/" <> entry.key)
      |> Base.url_encode64(padding: false)

    id = "s3-file-#{hash}"

    build_tmp_path(id)
  end

  def build_path_for_entry(%FSS.HTTP.Entry{} = entry) do
    hash = :crypto.hash(:sha256, entry.url) |> Base.url_encode64(padding: false)

    id = "http-file-#{hash}"

    build_tmp_path(id)
  end

  defp build_tmp_path(id) do
    base_dir = Path.join([System.tmp_dir!(), "elixir-explorer-datasets"])
    File.mkdir_p!(base_dir)

    Path.join([base_dir, id])
  end
end
