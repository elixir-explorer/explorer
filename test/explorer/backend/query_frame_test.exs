defmodule Explorer.Backend.QueryFrameTest do
  use ExUnit.Case, async: true
  alias Explorer.Backend.QueryFrame

  test "inspect/2 prints the columns without data" do
    df = Explorer.DataFrame.new(a: [1, 2], b: [3.1, 4.5])
    ldf = QueryFrame.new(df)

    assert inspect(ldf) ==
             """
             #Explorer.DataFrame<
               QueryFrame[??? x 2]
               a s64
               b f64
             >\
             """
  end
end
