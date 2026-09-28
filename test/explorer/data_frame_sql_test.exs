defmodule Explorer.DataFrameSQLTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame, as: DF

  describe "sql/2 with single DataFrame" do
    test "executes SQL query with table name" do
      df = DF.new(a: [1, 2, 3], b: ["x", "y", "y"])

      result = DF.sql(%{df: df}, "select ARRAY_AGG(a), b from df group by b order by b")

      assert result != nil
      result = DF.collect(result)
      assert DF.n_rows(result) == 2
      assert DF.names(result) == ["a", "b"]
    end

    test "executes SQL query with custom table name" do
      df = DF.new(a: [1, 2, 3])

      result = DF.sql(%{my_table: df}, "select a + 1 from my_table")

      assert result != nil
      result = DF.collect(result)
      assert DF.n_rows(result) == 3
      assert DF.names(result) == ["a"]
    end

    test "executes SQL query with WHERE clause" do
      df = DF.new(id: [1, 2, 3, 4, 5], value: [10, 20, 30, 40, 50])

      result = DF.sql(%{df: df}, "select id, value from df where id > 2")

      result = DF.collect(result)
      assert DF.n_rows(result) == 3
      assert DF.to_columns(result, atom_keys: true) == %{id: [3, 4, 5], value: [30, 40, 50]}
    end

    test "executes SQL query with ORDER BY clause" do
      df = DF.new(name: ["Alice", "Bob", "Charlie"], age: [30, 25, 35])

      result = DF.sql(%{df: df}, "select name, age from df order by age")

      result = DF.collect(result)
      assert DF.n_rows(result) == 3

      assert DF.to_columns(result, atom_keys: true) == %{
               name: ["Bob", "Alice", "Charlie"],
               age: [25, 30, 35]
             }
    end
  end

  describe "sql/2 with multiple DataFrames" do
    test "executes SQL query on single registered DataFrame" do
      df1 = DF.new(column_a: [1, 2, 3])

      result =
        DF.sql(%{t1: df1}, "select 2 * t.column_a as column_2a from t1 as t where t.column_a < 3")

      assert result != nil
      result = DF.collect(result)
      assert DF.n_rows(result) == 2
      assert DF.to_columns(result, atom_keys: true) == %{column_2a: [2, 4]}
    end

    test "executes SQL query with JOIN between two DataFrames" do
      df1 = DF.new(id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"])
      df2 = DF.new(id: [1, 2, 4], age: [25, 30, 35])

      result =
        DF.sql(
          %{users: df1, ages: df2},
          "SELECT users.name, ages.age FROM users JOIN ages ON users.id = ages.id"
        )

      assert result != nil
      result = DF.collect(result)
      assert DF.n_rows(result) == 2
      assert DF.names(result) == ["name", "age"]
      assert DF.to_columns(result, atom_keys: true) == %{name: ["Alice", "Bob"], age: [25, 30]}
    end

    test "executes SQL query with LEFT JOIN" do
      df1 = DF.new(id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"])
      df2 = DF.new(id: [1, 2], age: [25, 30])

      result =
        DF.sql(
          %{users: df1, ages: df2},
          "SELECT users.name, ages.age FROM users LEFT JOIN ages ON users.id = ages.id ORDER BY users.id"
        )

      result = DF.collect(result)
      assert DF.n_rows(result) == 3
    end

    test "executes SQL query with multiple table references" do
      df1 = DF.new(a: [1, 2, 3])
      df2 = DF.new(b: [1, 2, 3])
      df3 = DF.new(c: [1, 2, 3])

      result =
        DF.sql(
          %{t1: df1, t2: df2, t3: df3},
          "SELECT t1.a, t2.b, t3.c FROM t1 JOIN t2 ON t1.a = t2.b JOIN t3 ON t1.a = t3.c"
        )

      result = DF.collect(result)
      assert DF.n_rows(result) == 3
      assert DF.names(result) == ["a", "b", "c"]
    end

    test "executes SQL query with aggregation across tables" do
      df1 = DF.new(category: ["A", "A", "B", "B"], value: [10, 20, 30, 40])
      df2 = DF.new(category: ["A", "B"], multiplier: [2, 3])

      result =
        DF.sql(
          %{data: df1, factors: df2},
          "SELECT data.category, SUM(data.value * factors.multiplier) as total FROM data JOIN factors ON data.category = factors.category GROUP BY data.category ORDER BY data.category"
        )

      result = DF.collect(result)
      assert DF.n_rows(result) == 2
      assert DF.names(result) == ["category", "total"]
    end

    test "executes SQL query with string table names (not atoms)" do
      df1 = DF.new(id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"])
      df2 = DF.new(id: [1, 2, 4], age: [25, 30, 35])

      result =
        DF.sql(
          %{"users" => df1, "ages" => df2},
          "SELECT users.name FROM users JOIN ages ON users.id = ages.id"
        )

      result = DF.collect(result)
      assert DF.n_rows(result) == 2
      assert DF.names(result) == ["name"]
    end
  end

  describe "sql/2 without registered tables" do
    test "executes SQL query without any DataFrame registered" do
      result = DF.sql(%{}, "select 1 as column_a union all select 2 as column_a")

      assert result != nil
      result = DF.collect(result)
      assert DF.n_rows(result) == 2
      assert DF.to_columns(result, atom_keys: true) == %{column_a: [1, 2]}
    end

    test "executes SQL query with only literal values" do
      result = DF.sql(%{}, "select 1 + 2 as sum, 'hello' as greeting")

      result = DF.collect(result)
      assert DF.n_rows(result) == 1
      assert DF.to_columns(result, atom_keys: true) == %{sum: [3], greeting: ["hello"]}
    end
  end

  describe "error handling" do
    test "raises error for invalid SQL syntax" do
      df = DF.new(a: [1, 2, 3])

      assert_raise RuntimeError, fn ->
        DF.sql(%{t1: df}, "select from invalid syntax")
      end
    end

    test "raises error when referencing non-existent table" do
      df = DF.new(a: [1, 2, 3])

      assert_raise RuntimeError, fn ->
        DF.sql(%{t1: df}, "select * from nonexistent_table")
      end
    end

    test "raises error when referencing non-existent column" do
      df = DF.new(a: [1, 2, 3])

      assert_raise RuntimeError, fn ->
        DF.sql(%{t1: df}, "select nonexistent_column from t1")
      end
    end
  end

  describe "complex SQL operations" do
    test "executes SQL query with WHERE clause and comparison" do
      df = DF.new(id: [1, 2, 3, 4, 5], value: [10, 20, 30, 40, 50])

      result =
        DF.sql(
          %{data: df},
          "SELECT id, value FROM data WHERE value > 30"
        )

      result = DF.collect(result)
      assert DF.n_rows(result) == 2
      assert DF.to_columns(result, atom_keys: true) == %{id: [4, 5], value: [40, 50]}
    end

    test "executes SQL query with CASE expression" do
      df = DF.new(value: [10, 20, 30, 40, 50])

      result =
        DF.sql(
          %{data: df},
          "SELECT value, CASE WHEN value < 20 THEN 'low' WHEN value < 40 THEN 'medium' ELSE 'high' END as category FROM data"
        )

      result = DF.collect(result)
      assert DF.n_rows(result) == 5
      assert DF.names(result) == ["value", "category"]
    end

    test "executes SQL query with UNION" do
      df1 = DF.new(id: [1, 2], type: ["A", "A"])
      df2 = DF.new(id: [3, 4], type: ["B", "B"])

      result =
        DF.sql(
          %{a: df1, b: df2},
          "SELECT id, type FROM a UNION ALL SELECT id, type FROM b ORDER BY id"
        )

      result = DF.collect(result)
      assert DF.n_rows(result) == 4
    end
  end
end
