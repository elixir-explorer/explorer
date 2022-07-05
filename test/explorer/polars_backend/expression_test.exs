defmodule Explorer.PolarsBackend.ExpressionTest do
  use ExUnit.Case, async: true

  alias Explorer.Backend.LazySeries
  alias Explorer.PolarsBackend.Expression

  describe "to_expr/1" do
    test "with basic int value" do
      lazy = %LazySeries{op: :equal, args: [%LazySeries{op: :column, args: ["col_a"]}, 5]}

      assert Expression.to_expr(lazy) == {:equal, [{:column, "col_a"}, 5]}
    end

    test "with basic float value" do
      lazy = %LazySeries{op: :equal, args: [%LazySeries{op: :column, args: ["col_a"]}, 1.4]}

      assert Expression.to_expr(lazy) == {:equal, [{:column, "col_a"}, 1.4]}
    end
  end
end
