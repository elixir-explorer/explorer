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
      lazy = %LazySeries{op: :equal, args: [%LazySeries{op: :column, args: ["col_a"]}, 5]}

      assert %Expression{} = expr = Expression.to_expr(lazy)

      assert Expression.describe_filter_plan(df, expr) ==
               String.trim("""
               FILTER [(col("col_a")) == (5)] FROM
               DF ["col_a", "col_b"]; PROJECT */2 COLUMNS; SELECTION: "None"
               """)
    end

    test "with basic float value" do
      lazy = %LazySeries{op: :equal, args: [%LazySeries{op: :column, args: ["col_b"]}, 1.4]}

      assert %Expression{} = Expression.to_expr(lazy)
    end

    test "with a string value" do
      lazy = %LazySeries{op: :equal, args: [%LazySeries{op: :column, args: ["col_b"]}, "foo"]}

      assert %Expression{} = Expression.to_expr(lazy)
    end

    test "with a bool value" do
      lazy = %LazySeries{op: :equal, args: [%LazySeries{op: :column, args: ["col_b"]}, true]}

      assert %Expression{} = Expression.to_expr(lazy)
    end

    test "with date value" do
      lazy = %LazySeries{
        op: :equal,
        args: [%LazySeries{op: :column, args: ["col_b"]}, ~D[2022-07-07]]
      }

      assert %Expression{} = Expression.to_expr(lazy)
    end

    test "with datetime value" do
      lazy = %LazySeries{
        op: :equal,
        args: [%LazySeries{op: :column, args: ["col_b"]}, ~N[2022-07-07 18:09:17.824019]]
      }

      assert %Expression{} = Expression.to_expr(lazy)
    end

    test "with series" do
      lazy = %LazySeries{
        op: :equal,
        args: [
          %LazySeries{op: :column, args: ["col_b"]},
          Explorer.Series.from_list([1, 2, 3]).data
        ]
      }

      assert %Expression{} = Expression.to_expr(lazy)
    end

    test "with another column", %{df: df} do
      lazy = %LazySeries{
        op: :equal,
        args: [
          %LazySeries{op: :column, args: ["col_a"]},
          %LazySeries{op: :column, args: ["col_b"]}
        ]
      }

      assert %Expression{} = expr = Expression.to_expr(lazy)

      assert Expression.describe_filter_plan(df, expr) ==
               String.trim("""
               FILTER [(col("col_a")) == (col("col_b"))] FROM
               DF ["col_a", "col_b"]; PROJECT */2 COLUMNS; SELECTION: "None"
               """)
    end
  end

  describe "json" do
    test "can convert exprs to/from json" do
      lazy = %LazySeries{op: :column, args: ["a"]}
      expr1 = Expression.to_expr(lazy)
      json = Expression.to_json(expr1)
      expr2 = Expression.from_json(json)

      assert json == %{"Column" => "a"}
      assert %Expression{} = expr2
    end

    test "can perform an unsupported operation via json-derived exprs" do
      # Built in Python from:
      # `pl.col("list_col_unsorted").list.sort().meta.serialize()`
      list_col_sorted_expr_json = %{
        "Function" => %{
          "input" => [%{"Column" => "list_col_unsorted"}],
          "function" => %{
            "ListExpr" => %{
              "Sort" => %{
                "descending" => false,
                "nulls_last" => false,
                "multithreaded" => true,
                "maintain_order" => false
              }
            }
          },
          "options" => %{
            "collect_groups" => "ElementWise",
            "fmt_str" => "",
            "input_wildcard_expansion" => false,
            "returns_scalar" => false,
            "cast_to_supertypes" => false,
            "allow_rename" => false,
            "pass_name_to_apply" => false,
            "changes_length" => false,
            "check_lengths" => true,
            "allow_group_aware" => true
          }
        }
      }

      mutate_with_json = fn df, name, json ->
        expr =
          json
          |> Expression.from_json()
          |> Expression.alias_expr(name)

        ldf = Explorer.DataFrame.lazy(df)
        {:ok, lpdf_new} = Explorer.PolarsBackend.Native.lf_mutate_with(ldf.data, [expr])
        {:ok, pdf_new} = Explorer.PolarsBackend.Native.lf_collect(lpdf_new)
        Explorer.PolarsBackend.Shared.create_dataframe(pdf_new)
      end

      df = Explorer.DataFrame.new(%{list_col_unsorted: [[1, 5, 3], [1, 1, 2, 0]]})
      df_new = mutate_with_json.(df, "list_col_sorted", list_col_sorted_expr_json)

      series = Explorer.DataFrame.to_series(df_new)
      assert Explorer.Series.to_list(series["list_col_unsorted"]) == [[1, 5, 3], [1, 1, 2, 0]]
      assert Explorer.Series.to_list(series["list_col_sorted"]) == [[1, 3, 5], [0, 1, 1, 2]]
    end
  end
end
