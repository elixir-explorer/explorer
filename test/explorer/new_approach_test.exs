defmodule Explorer.NewApproachTest do
  use ExUnit.Case, async: true

  require Explorer.Series, as: S
  require Explorer.DataFrame, as: DF
  require Explorer.Query, as: Query

  test "filter" do
    df0 = DF.new(a: [1, 2, 3], b: [true, true, false])

    # Filter inline
    df1 = DF.filter(df0, b == false)

    # Filter by building a lazy series with functions
    filter2 = S.col("b") |> S.equal(false)
    df2 = DF.filter_with(df0, filter2)

    # Filter by building a lazy series with a macro
    filter3 = Query.new(b == false)
    df3 = DF.filter_with(df0, filter3)

    for df <- [df1, df2, df3] do
      assert DF.to_columns(df, atom_keys: true) == %{a: [3], b: [false]}
    end
  end

  test "mutate" do
    df0 = DF.new(a: [1, 2, 3])

    # Mutate inline
    df1 = DF.mutate(df0, b: a + 3)

    # Mutate by building a lazy series with functions
    b = S.col("a") |> S.add(3)
    df2 = DF.mutate_with(df0, b: b)

    # Mutate by building a lazy series with a macro
    b = Query.new(a + 3)
    df3 = DF.mutate_with(df0, b: b)

    for df <- [df1, df2, df3] do
      assert DF.to_columns(df, atom_keys: true) == %{a: [1, 2, 3], b: [4, 5, 6]}
    end
  end

  test "sort" do
    df0 = DF.new(a: [1, 2, 3])

    # Sort inline
    df1 = DF.sort_by(df0, 1 - a)

    # Sort by building a lazy series with functions
    sorter2 = S.lit(1) |> S.subtract(S.col("a"))
    df2 = DF.sort_with(df0, sorter2)
    # Again but with a direction
    sorter3 = [desc: S.col("a")]
    df3 = DF.sort_with(df0, sorter3)

    # Sort by building a lazy series with a macro
    sorter4 = Query.new(1 - a)
    df4 = DF.sort_with(df0, sorter4)

    for df <- [df1, df2, df3, df4] do
      assert DF.to_columns(df, atom_keys: true) == %{a: [3, 2, 1]}
    end
  end
end
