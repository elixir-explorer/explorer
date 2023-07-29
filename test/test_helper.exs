defmodule Explorer.IOHelpers do
  # https://doc.rust-lang.org/std/primitive.f64.html#associatedconstant.EPSILON
  @f64_epsilon 2.2204460492503131e-16

  alias Explorer.DataFrame, as: DF
  alias Explorer.Series

  require ExUnit.Assertions

  @valid_dtypes Explorer.Shared.dtypes()

  def f64_epsilon, do: @f64_epsilon

  def assert_from_with_correct_type(type, value, parsed_value, reader_fun)
      when type in @valid_dtypes and is_function(reader_fun, 1) do
    df = List.wrap(value) |> Series.from_list() |> Series.cast(type) |> then(&DF.new(column: &1))

    %DF{} = df = reader_fun.(df)

    ExUnit.Assertions.assert(df[0][0] == parsed_value)
    ExUnit.Assertions.assert(df[0].dtype == type)
  end

  def tmp_filename(save_fun) when is_function(save_fun, 1) do
    hash = :crypto.strong_rand_bytes(3) |> Base.encode16(case: :lower)

    System.tmp_dir!()
    |> Path.join("tmp-df-#{hash}.tmp")
    |> tap(save_fun)
  end

  def tmp_file!(data) when is_binary(data) do
    tmp_filename(fn filename -> :ok = File.write!(filename, data) end)
  end

  # Defines functions like `tmp_parquet_file!(df)`.
  for format <- [:parquet, :ipc, :ipc_stream, :ndjson] do
    fun_name = :"tmp_#{format}_file!"
    to_name = :"to_#{format}"

    def unquote(fun_name)(df) do
      tmp_filename(fn filename -> :ok = apply(DF, unquote(to_name), [df, filename]) end)
    end
  end
end

ExUnit.start(exclude: :cloud_integration)
