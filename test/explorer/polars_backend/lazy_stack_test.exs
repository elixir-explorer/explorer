defmodule Explorer.PolarsBackend.LazyStackTest do
  use ExUnit.Case, async: true

  alias Explorer.PolarsBackend.LazyStack
  alias Explorer.DataFrame, as: DF

  describe "stack_by_groups/1" do
    setup do
      ldf = Explorer.Datasets.iris() |> DF.lazy()

      {:ok, [ldf: ldf]}
    end

    test "without any operation", %{ldf: ldf} do
      assert LazyStack.stack_by_groups(ldf) == []
    end

    test "one operation without groups", %{ldf: ldf} do
      ldf = ldf |> DF.head(990)
      assert LazyStack.stack_by_groups(ldf) == [{[], [{:head, [990]}]}]
    end

    test "two operations without groups", %{ldf: ldf} do
      ldf = ldf |> DF.head(990) |> DF.tail(500)

      assert LazyStack.stack_by_groups(ldf) == [{[], [{:head, [990]}, {:tail, [500]}]}]
    end

    test "two operations with groups", %{ldf: ldf} do
      ldf = ldf |> DF.group_by("species") |> DF.head(990) |> DF.tail(500)

      assert LazyStack.stack_by_groups(ldf) == [{["species"], [{:head, [990]}, {:tail, [500]}]}]
    end

    test "one operation with group and other without group", %{ldf: ldf} do
      ldf =
        ldf
        |> DF.group_by("species")
        |> DF.head(990)
        |> DF.ungroup("species")
        |> DF.tail(500)

      assert LazyStack.stack_by_groups(ldf) == [{["species"], [head: [990]]}, {[], [tail: [500]]}]
    end

    test "one operation without group and other two with group, and one without groups", %{
      ldf: ldf
    } do
      ldf =
        ldf
        |> DF.head(990)
        |> DF.group_by("species")
        |> DF.tail(500)
        |> DF.slice(0, 15)
        |> DF.ungroup()
        |> DF.head(200)

      assert LazyStack.stack_by_groups(ldf) == [
               {[], [head: [990]]},
               {["species"], [{:tail, [500]}, {:slice, [0, 15]}]},
               {[], [{:head, [200]}]}
             ]
    end
  end
end
