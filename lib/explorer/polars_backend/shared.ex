defmodule Explorer.PolarsBackend.Shared do
  # A collection of **private** helpers shared in Explorer.PolarsBackend.
  @moduledoc false

  alias Explorer.DataFrame, as: DataFrame
  alias Explorer.PolarsBackend.DataFrame, as: PolarsDataFrame
  alias Explorer.PolarsBackend.LazyDataFrame, as: PolarsLazyFrame
  alias Explorer.PolarsBackend.Native
  alias Explorer.PolarsBackend.Series, as: PolarsSeries
  alias Explorer.Series, as: Series

  defguardp is_polars_df(df) when is_struct(df, PolarsDataFrame) or is_struct(df, PolarsLazyFrame)
  defguardp is_polars_series(series) when is_struct(series, PolarsSeries)

  def apply_series(series, fun, args \\ [])

  def apply_series(%Series{} = series, fun, args) do
    case apply(Native, fun, [series.data | args]) do
      {:ok, new_series} when is_polars_series(new_series) -> create_series(new_series)
      {:ok, new_df} when is_polars_df(new_df) -> create_dataframe(new_df)
      {:ok, value} -> value
      {:error, error} -> raise "#{error}"
    end
  end

  def apply_dataframe(df_or_s, fun, args \\ [])

  def apply_dataframe(%DataFrame{} = df, fun, args) do
    case apply(Native, fun, [df.data | args]) do
      {:ok, new_df} when is_polars_df(new_df) -> update_dataframe(new_df, df)
      {:ok, new_series} when is_polars_series(new_series) -> create_series(new_series)
      {:ok, value} -> value
      {:error, error} -> raise "#{error}"
    end
  end

  def create_dataframe(polars_df) when is_polars_df(polars_df),
    do: %DataFrame{data: polars_df, groups: []}

  def update_dataframe(polars_df, %DataFrame{} = df)
      when is_polars_df(polars_df),
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
