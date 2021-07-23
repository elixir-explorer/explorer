defmodule Explorer.PolarsBackend.Shared do
  # A collection of **private** helpers shared in Explorer.PolarsBackend.
  @moduledoc false

  alias Explorer.DataFrame, as: DataFrame
  alias Explorer.PolarsBackend.DataFrame, as: PolarsDataFrame
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries
  alias Explorer.Series, as: Series

  def apply_native(df_or_s, fun, args \\ [])

  def apply_native(%Series{data: series}, fun, args) do
    result = apply(Native, fun, [series | args])
    unwrap(result)
  end

  def apply_native(%DataFrame{} = df, fun, args) do
    df = to_polars_df(df)
    result = apply(Native, fun, [df | args])
    unwrap(result)
  end

  def to_polars_df(%DataFrame{data: %PolarsDataFrame{} = polars_df}), do: polars_df
  def to_polars_df(%PolarsDataFrame{} = polars_df), do: polars_df
  def to_dataframe(%DataFrame{} = df), do: df
  def to_dataframe(%PolarsDataFrame{} = polars_df), do: %DataFrame{data: polars_df, groups: []}
  def to_polars_s(%Series{data: %PolarsSeries{} = polars_s}), do: polars_s
  def to_polars_s(%PolarsSeries{} = polars_s), do: polars_s

  def to_series(%PolarsSeries{} = polars_s) do
    {:ok, dtype} = Native.s_dtype(polars_s)
    dtype = normalise_dtype(dtype)
    %Series{data: polars_s, dtype: dtype}
  end

  def to_series(%Series{} = series), do: series

  def unwrap({:ok, %PolarsSeries{} = series}), do: to_series(series)
  def unwrap({:ok, %PolarsDataFrame{} = df}), do: to_dataframe(df)
  def unwrap({:ok, value}), do: value
  def unwrap({:error, error}), do: raise("#{error}")

  def normalise_dtype("u32"), do: :integer
  def normalise_dtype("i32"), do: :integer
  def normalise_dtype("i64"), do: :integer
  def normalise_dtype("f64"), do: :float
  def normalise_dtype("bool"), do: :boolean
  def normalise_dtype("str"), do: :string
  def normalise_dtype("date32(days)"), do: :date
  def normalise_dtype("date64(ms)"), do: :datetime
end
