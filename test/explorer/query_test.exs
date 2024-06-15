defmodule Explorer.QueryTest do
  use ExUnit.Case, async: true

  alias Explorer.Series
  require Explorer.DataFrame, as: DF
  doctest Explorer.Query

  test "allows Kernel operations within pin" do
    assert DF.new(a: [1, 2, 3])
           |> DF.filter(a < ^if(true, do: 3, else: 1))
           |> DF.to_columns(atom_keys: true) == %{a: [1, 2]}
  end

  test "allows Kernel variables within pin" do
    two = 2
    three = 3

    assert DF.new(a: [1, 2, 3])
           |> DF.filter(a < ^(two + three))
           |> DF.to_columns(atom_keys: true) == %{a: [1, 2, 3]}
  end

  describe "conditionals" do
    test "support cond/1 macro" do
      df = DF.new(%{names: ["Alice", "Bob", "John"], grade: [10, 4, 6]})

      df =
        DF.mutate(df,
          simple_result:
            cond do
              grade > 9 -> "Exceptional"
              grade > 5 -> "Passed"
            end,
          result:
            cond do
              grade > 9 -> "Exceptional"
              grade > 5 -> "Passed"
              true -> cast(grade, :string)
            end
        )

      assert Series.to_list(df[:simple_result]) == ["Exceptional", nil, "Passed"]
      assert Series.to_list(df[:result]) == ["Exceptional", "4", "Passed"]
    end

    test "support pins in clauses" do
      df = DF.new(%{names: ["Alice", "Bob", "John"], grade: [10, 4, 6]})
      column = :grade
      passed = "Passed"
      last = true

      df =
        DF.mutate(df,
          result:
            cond do
              col(^column) > 9 -> "Exceptional"
              col(^column) > 5 -> ^passed
              ^last -> "Fallback"
            end
        )

      assert Series.to_list(df[:result]) == ["Exceptional", "Fallback", "Passed"]
    end

    test "support if/2 and unless/2 macros" do
      df = DF.new(%{names: ["Alice", "Bob", "John"], grade: [10, 4, 6]})

      df =
        DF.mutate(df,
          simple_result:
            if grade > 9 do
              "Exceptional"
            else
              "Passed"
            end,
          result:
            unless grade > 5 do
              "Failed"
            end
        )

      assert Series.to_list(df[:simple_result]) == ["Exceptional", "Passed", "Passed"]
      assert Series.to_list(df[:result]) == [nil, "Failed", nil]
    end
  end

  describe "comprehensions" do
    defp abc do
      DF.new(a: [1, 2, 3], b: [10.0, 20.0, 30.0], c: ~w(one two three))
    end

    test "adds new columns based on comprehensions" do
      assert abc()
             |> DF.mutate(
               for name <- [:a, :b] do
                 {name, 123}
               end
             )
             |> DF.to_columns(atom_keys: true) == %{
               a: [123, 123, 123],
               b: [123, 123, 123],
               c: ~w(one two three)
             }
    end

    test "adds new columns based on comprehensions with filters" do
      assert abc()
             |> DF.mutate(
               for name <- [:a, :b, :c], name == :b do
                 {name, 123}
               end
             )
             |> DF.to_columns(atom_keys: true) == %{
               a: [1, 2, 3],
               b: [123, 123, 123],
               c: ~w(one two three)
             }
    end

    test "uses across/0 to access dataframe columns" do
      assert abc()
             |> DF.mutate(
               for col <- across(), col.name == "b" do
                 {col.name, 123}
               end
             )
             |> DF.to_columns(atom_keys: true) == %{
               a: [1, 2, 3],
               b: [123, 123, 123],
               c: ~w(one two three)
             }

      assert abc()
             |> DF.mutate(
               for col <- across(), col.dtype in [{:s, 64}, {:f, 64}] do
                 {col.name, -col}
               end
             )
             |> DF.to_columns(atom_keys: true) == %{
               a: [-1, -2, -3],
               b: [-10.0, -20.0, -30.0],
               c: ~w(one two three)
             }

      assert abc()
             |> DF.mutate(
               for col <- across(), col.dtype == :string do
                 {:"#{col.name}_copy", col}
               end
             )
             |> DF.to_columns(atom_keys: true) == %{
               a: [1, 2, 3],
               b: [10.0, 20.0, 30.0],
               c: ~w(one two three),
               c_copy: ~w(one two three)
             }
    end

    test "uses across/1 to select some dataframe columns" do
      assert abc()
             |> DF.mutate(
               for col <- across([:a, :b]) do
                 {col.name, -col}
               end
             )
             |> DF.to_columns(atom_keys: true) == %{
               a: [-1, -2, -3],
               b: [-10.0, -20.0, -30.0],
               c: ~w(one two three)
             }

      assert abc()
             |> DF.mutate(
               for col <- across(~w(a b)) do
                 {col.name, -col}
               end
             )
             |> DF.to_columns(atom_keys: true) == %{
               a: [-1, -2, -3],
               b: [-10.0, -20.0, -30.0],
               c: ~w(one two three)
             }

      assert abc()
             |> DF.mutate(
               for col <- across(~r/a|b/) do
                 {col.name, -col}
               end
             )
             |> DF.to_columns(atom_keys: true) == %{
               a: [-1, -2, -3],
               b: [-10.0, -20.0, -30.0],
               c: ~w(one two three)
             }

      assert abc()
             |> DF.mutate(
               for col <- across(0..1) do
                 {col.name, -col}
               end
             )
             |> DF.to_columns(atom_keys: true) == %{
               a: [-1, -2, -3],
               b: [-10.0, -20.0, -30.0],
               c: ~w(one two three)
             }
    end

    test "uses across/1 to generate cross products" do
      assert abc()
             |> DF.mutate(
               for col_a <- across([:a, :b]), col_b <- across([:b, :a]) do
                 {:"#{col_a.name}_#{col_b.name}", col_a - 2 * col_b}
               end
             )
             |> DF.to_columns(atom_keys: true) == %{
               a: [1, 2, 3],
               a_a: [-1, -2, -3],
               a_b: [-19.0, -38.0, -57.0],
               b: [10.0, 20.0, 30.0],
               b_a: [8.0, 16.0, 24.0],
               b_b: [-10.0, -20.0, -30.0],
               c: ["one", "two", "three"]
             }
    end
  end

  describe "<>" do
    test "concatenates strings" do
      assert DF.new(a: [1, 2, 3])
             |> DF.mutate(a: "foo" <> "bar")
             |> DF.to_columns(atom_keys: true) == %{
               a: ~w(foobar foobar foobar)
             }
    end

    test "concatenates strings and series" do
      assert DF.new(a: ~w(bar baz bat))
             |> DF.mutate(a: "foo" <> a)
             |> DF.to_columns(atom_keys: true) == %{
               a: ~w(foobar foobaz foobat)
             }
    end
  end

  describe "and/2" do
    test "calculates and operation" do
      assert DF.new(grade: [10, 4, 6])
             |> DF.mutate(average: grade > 5 and grade < 8)
             |> DF.to_columns(atom_keys: true) == %{
               grade: [10, 4, 6],
               average: [false, false, true]
             }
    end

    test "supports scalar values in and/2" do
      assert DF.new(grade: [10, 4, 6])
             |> DF.mutate(good: true and grade > 7)
             |> DF.to_columns(atom_keys: true) == %{
               grade: [10, 4, 6],
               good: [true, false, false]
             }
    end
  end

  describe "or/2" do
    test "calculates or operation" do
      assert DF.new(grade: [10, 4, 6], exceptional: [true, true, false])
             |> DF.mutate(good: grade > 7 or exceptional)
             |> DF.to_columns(atom_keys: true) == %{
               grade: [10, 4, 6],
               exceptional: [true, true, false],
               good: [true, true, false]
             }
    end

    test "supports scalar values in or/2" do
      assert DF.new(grade: [10, 4, 6])
             |> DF.mutate(all: true or grade > 7)
             |> DF.to_columns(atom_keys: true) == %{
               grade: [10, 4, 6],
               all: [true, true, true]
             }
    end
  end

  describe "not/1" do
    test "calculates not operation" do
      assert DF.new(grade: [10, 4, 6])
             |> DF.mutate(not_good: not (grade > 7))
             |> DF.to_columns(atom_keys: true) == %{
               grade: [10, 4, 6],
               not_good: [false, true, true]
             }
    end

    test "supports scalar values in not/1" do
      assert DF.new(grade: [10, 4, 6])
             |> DF.mutate(always: not false)
             |> DF.to_columns(atom_keys: true) == %{
               grade: [10, 4, 6],
               always: [true, true, true]
             }
    end
  end

  describe "sql" do
    test "basic example" do
      assert DF.new(a: [1, 2, 3])
             |> DF.mutate(b: sql("max(a)"))
             |> DF.to_columns(atom_keys: true) == %{
               a: [1, 2, 3],
               b: [3, 3, 3]
             }
    end
  end

  test "raises on special forms" do
    assert_raise ArgumentError, "=/2 is not currently supported in Explorer.Query", fn ->
      Code.eval_quoted(
        quote do
          require DF
          DF.new(a: [1, 2, 3]) |> DF.filter(a = ^(two + three))
        end
      )
    end
  end
end
