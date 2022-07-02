defmodule Explorer.PolarsBackend.ExpressionsTest do
  use ExUnit.Case, async: true

  alias Explorer.Backend.LazySeries
  alias Explorer.PolarsBackend.Expressions

  describe "to_expressions/1" do
    test "with basic int value" do
      lazy = %LazySeries{op: :equal, args: [%LazySeries{op: :column, args: ["col_a"]}, 5]}

      assert Expressions.to_expressions(lazy) == {:equal_int, {:column, "col_a"}, 5}
    end

    test "with basic float value" do
      lazy = %LazySeries{op: :equal, args: [%LazySeries{op: :column, args: ["col_a"]}, 1.4]}

      assert Expressions.to_expressions(lazy) == {:equal_float, {:column, "col_a"}, 1.4}
    end
  end
end
