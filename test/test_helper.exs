defmodule Explorer.IOHelpers do
  # https://doc.rust-lang.org/std/primitive.f64.html#associatedconstant.EPSILON
  @f64_epsilon 2.2204460492503131e-16

  alias Explorer.DataFrame, as: DF
  alias Explorer.Series

  require ExUnit.Assertions

  def f64_epsilon, do: @f64_epsilon

  def assert_from_with_correct_type(type, value, parsed_value, reader_fun)
      when is_atom(type) and is_function(reader_fun, 1) do
    df = List.wrap(value) |> Series.from_list() |> Series.cast(type) |> then(&DF.new(column: &1))

    # reader fun should return a DF
    df = reader_fun.(df)

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

  def tmp_parquet_file!(df) do
    tmp_filename(fn filename -> :ok = DF.to_parquet(df, filename) end)
  end

  def tmp_ipc_file!(df) do
    tmp_filename(fn filename -> :ok = DF.to_ipc(df, filename) end)
  end

  def tmp_ipc_stream_file!(df) do
    tmp_filename(fn filename -> :ok = DF.to_ipc_stream(df, filename) end)
  end
end

ExUnit.start()
