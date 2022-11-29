defmodule Explorer.QueryTest do
  use ExUnit.Case, async: true

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
