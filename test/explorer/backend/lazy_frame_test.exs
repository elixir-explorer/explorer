defmodule Explorer.Backend.LazyFrameTest do
  use ExUnit.Case, async: true

  alias Explorer.Backend
  alias Explorer.Backend.LazyFrame

  test "inspect/2 proxies to the original dataframe" do
    df = Explorer.DataFrame.new(a: [1, 2], b: [3.1, 4.5])
    ldf = LazyFrame.new(df)
    opaque_df = Backend.DataFrame.new(ldf, df.names, df.dtypes)

    assert inspect(opaque_df) ==
             """
             #Explorer.DataFrame<
               OpaqueLazyFrame[??? x 2]
               a integer [1, 2]
               b float [3.1, 4.5]
             >
             """
             |> String.trim_trailing()
  end
end
