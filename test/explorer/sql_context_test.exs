defmodule Explorer.SQLContextTest do
  use ExUnit.Case, async: true

  require Explorer.DataFrame

  alias Explorer.DataFrame, as: DF
  alias Explorer.SQLContext

  describe "execute" do
    test "execute without any data frame registered" do
      case SQLContext.new()
           |> SQLContext.execute("select 1 as column_a union all select 2 as column_a") do
        {:ok, result} ->
          assert result != nil
          assert DF.compute(result) |> DF.to_columns(atom_keys: true) == %{column_a: [1, 2]}

        {:error, reason} ->
          flunk("SQL query execution failed with reason: #{inspect(reason)}")
      end
    end

    test "execute with registering single data frame" do
      df = DF.new(%{column_a: [1, 2, 3]})

      case SQLContext.new()
           |> SQLContext.register("t1", df)
           |> SQLContext.execute(
             "select 2 * t.column_a as column_2a from t1 as t where t.column_a < 3"
           ) do
        {:ok, result} ->
          assert result != nil
          assert DF.compute(result) |> DF.to_columns(atom_keys: true) == %{column_2a: [2, 4]}

        {:error, reason} ->
          flunk("SQL query execution failed with reason: #{inspect(reason)}")
      end
    end

    test "execute with registering multiple data frames" do
      df1 = DF.new(%{column_1a: [1, 2, 3]})

      df2 =
        DF.new(%{
          column_2a: [1, 2, 4],
          column_2b: ["a", "b", "c"]
        })

      case SQLContext.new()
           |> SQLContext.register("t1", df1)
           |> SQLContext.register("t2", df2)
           |> SQLContext.execute(
             "select t2.column_2b as col from t1 join t2 on t1.column_1a = t2.column_2a"
           ) do
        {:ok, result} ->
          assert result != nil
          assert DF.compute(result) |> DF.to_columns(atom_keys: true) == %{col: ["a", "b"]}

        {:error, reason} ->
          flunk("SQL query execution failed with reason: #{inspect(reason)}")
      end
    end

    test "get_tables get registered tables" do
      df = DF.new(%{col: [1]})

      tables =
        SQLContext.new()
        |> SQLContext.register("t1", df)
        |> SQLContext.register("t2", df)
        |> SQLContext.get_tables()

      assert tables == ["t1", "t2"]
    end

    test "unregister" do
      df = DF.new(%{col: [1]})

      tables =
        SQLContext.new()
        |> SQLContext.register("t1", df)
        |> SQLContext.register("t2", df)
        |> SQLContext.register("t3", df)
        |> SQLContext.unregister("t1")
        |> SQLContext.get_tables()

      assert tables == ["t2", "t3"]
    end
  end
end
