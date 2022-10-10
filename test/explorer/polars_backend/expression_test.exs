defmodule Explorer.PolarsBackend.ExpressionTest do
  use ExUnit.Case, async: true

  alias Explorer.Backend.LazySeries
  alias Explorer.PolarsBackend.Expression

  describe "to_expr/1" do
    setup do
      df = Explorer.DataFrame.new(col_a: [1, 2, 3, 4, 5], col_b: [1.0, 2.4, 3.1, 1.4, 5.1])

      [df: df]
    end

    test "with basic int value", %{df: df} do
      lazy = %LazySeries{op: :eq, args: [%LazySeries{op: :column, args: ["col_a"]}, 5]}

      assert %Expression{} = expr = Expression.to_expr(lazy)

      assert Expression.describe_filter_plan(df, expr) == """
               FILTER [(col("col_a")) == (5i64)] FROM
                 DF ["col_a", "col_b"]; PROJECT */2 COLUMNS; SELECTION: "None"
             """
    end

    test "with basic float value" do
      lazy = %LazySeries{op: :eq, args: [%LazySeries{op: :column, args: ["col_b"]}, 1.4]}

      assert %Expression{} = Expression.to_expr(lazy)
    end

    test "with a string value" do
      lazy = %LazySeries{op: :eq, args: [%LazySeries{op: :column, args: ["col_b"]}, "foo"]}

      assert %Expression{} = Expression.to_expr(lazy)
    end

    test "with a bool value" do
      lazy = %LazySeries{op: :eq, args: [%LazySeries{op: :column, args: ["col_b"]}, true]}

      assert %Expression{} = Expression.to_expr(lazy)
    end

    test "with date value" do
      lazy = %LazySeries{
        op: :eq,
        args: [%LazySeries{op: :column, args: ["col_b"]}, ~D[2022-07-07]]
      }

      assert %Expression{} = Expression.to_expr(lazy)
    end

    test "with datetime value" do
      lazy = %LazySeries{
        op: :eq,
        args: [%LazySeries{op: :column, args: ["col_b"]}, ~N[2022-07-07 18:09:17.824019]]
      }

      assert %Expression{} = Expression.to_expr(lazy)
    end

    test "with series" do
      lazy = %LazySeries{
        op: :eq,
        args: [
          %LazySeries{op: :column, args: ["col_b"]},
          Explorer.Series.from_list([1, 2, 3]).data
        ]
      }

      assert %Expression{} = Expression.to_expr(lazy)
    end

    test "with another column", %{df: df} do
      lazy = %LazySeries{
        op: :eq,
        args: [
          %LazySeries{op: :column, args: ["col_a"]},
          %LazySeries{op: :column, args: ["col_b"]}
        ]
      }

      assert %Expression{} = expr = Expression.to_expr(lazy)

      assert Expression.describe_filter_plan(df, expr) == """
               FILTER [(col("col_a")) == (col("col_b"))] FROM
                 DF ["col_a", "col_b"]; PROJECT */2 COLUMNS; SELECTION: "None"
             """
    end
  end
end
