defmodule Explorer.Backend.LazyFrameTest do
  use ExUnit.Case, async: true
  alias Explorer.Backend.LazyFrame

  test "inspect/2 prints the columns without data" do
    df = Explorer.DataFrame.new(a: [1, 2], b: [3.1, 4.5])
    ldf = LazyFrame.new(df)

    assert inspect(ldf) ==
             """
             #Explorer.DataFrame<
               LazyFrame[??? x 2]
               a integer
               b float
             >\
             """
  end
end
