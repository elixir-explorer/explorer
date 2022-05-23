defmodule Explorer.PolarsBackend.Shared do
  # A collection of **private** helpers shared in Explorer.PolarsBackend.
  @moduledoc false

  alias Explorer.DataFrame, as: DataFrame
  alias Explorer.PolarsBackend.DataFrame, as: PolarsDataFrame
  alias Explorer.PolarsBackend.LazyDataFrame, as: PolarsLazyFrame
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries
  alias Explorer.Series, as: Series

  def apply_native(df_or_s, fun, args \\ [])

  def apply_native(%Series{} = series, fun, args) do
    case apply(Native, fun, [series.data | args]) do
      {:ok, %PolarsDataFrame{} = new_df} -> create_dataframe(new_df)
      {:ok, %PolarsLazyFrame{} = new_df} -> create_dataframe(new_df)
      {:ok, %PolarsSeries{} = new_series} -> create_series(new_series)
      {:ok, value} -> value
      {:error, error} -> raise "#{error}"
    end
  end

  def apply_native(%DataFrame{} = df, fun, args) do
    case apply(Native, fun, [df.data | args]) do
      {:ok, %PolarsDataFrame{} = new_df} -> update_dataframe(new_df, df)
      {:ok, %PolarsLazyFrame{} = new_df} -> update_dataframe(new_df, df)
      {:ok, %PolarsSeries{} = new_series} -> create_series(new_series)
      {:ok, value} -> value
      {:error, error} -> raise "#{error}"
    end
  end

  def create_dataframe(%module{} = polars_df) when module in [PolarsDataFrame, PolarsLazyFrame],
    do: %DataFrame{data: polars_df, groups: []}

  def update_dataframe(%module{} = polars_df, %DataFrame{} = df)
      when module in [PolarsDataFrame, PolarsLazyFrame],
      do: %DataFrame{df | data: polars_df}

  def create_series(%PolarsSeries{} = polars_series) do
    {:ok, dtype} = Native.s_dtype(polars_series)
    %Series{data: polars_series, dtype: normalise_dtype(dtype)}
  end

  def normalise_dtype("u32"), do: :integer
  def normalise_dtype("i32"), do: :integer
  def normalise_dtype("i64"), do: :integer
  def normalise_dtype("f64"), do: :float
  def normalise_dtype("bool"), do: :boolean
  def normalise_dtype("str"), do: :string
  def normalise_dtype("date"), do: :date
  def normalise_dtype("datetime"), do: :datetime
  def normalise_dtype("datetime[ms]"), do: :datetime
  def normalise_dtype("datetime[μs]"), do: :datetime

  def internal_from_dtype(:integer), do: "i64"
  def internal_from_dtype(:float), do: "f64"
  def internal_from_dtype(:boolean), do: "bool"
  def internal_from_dtype(:string), do: "str"
  def internal_from_dtype(:date), do: "date"
  def internal_from_dtype(:datetime), do: "datetime[μs]"
end
