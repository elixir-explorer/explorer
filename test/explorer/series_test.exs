defmodule Explorer.SeriesTest do
  use ExUnit.Case, async: true

  alias Explorer.Series

  doctest Explorer.Series

  describe "from_list/1" do
    test "with integers" do
      s = Series.from_list([1, 2, 3])

      assert Series.to_list(s) === [1, 2, 3]
    end

    test "with floats" do
      s = Series.from_list([1, 2.4, 3])
      assert Series.to_list(s) === [1.0, 2.4, 3.0]
    end

    test "mixing types" do
      assert_raise ArgumentError, fn ->
        s = Series.from_list([1, "foo", 3])
        Series.to_list(s)
      end
    end
  end

  test "fetch/2" do
    s = Series.from_list([1, 2, 3])
    assert s[0] === 1
    assert s[0..1] |> Series.to_list() === [1, 2]
    assert s[[0, 1]] |> Series.to_list() === [1, 2]
  end

  test "pop/2" do
    s = Series.from_list([1, 2, 3])
    assert {1, %Series{} = s1} = Access.pop(s, 0)
    assert Series.to_list(s1) == [2, 3]
    assert {%Series{} = s2, %Series{} = s3} = Access.pop(s, 0..1)
    assert Series.to_list(s2) == [1, 2]
    assert Series.to_list(s3) == [3]
    assert {%Series{} = s4, %Series{} = s5} = Access.pop(s, [0, 1])
    assert Series.to_list(s4) == [1, 2]
    assert Series.to_list(s5) == [3]
  end

  test "get_and_update/3" do
    s = Series.from_list([1, 2, 3])

    assert {2, %Series{} = s2} =
             Access.get_and_update(s, 1, fn current_value ->
               {current_value, current_value * 2}
             end)

    assert Series.to_list(s2) == [1, 4, 3]
  end

  describe "equal/2" do
    test "can compare boolean series" do
      s1 = Series.from_list([true, false, true])
      s2 = Series.from_list([false, true, true])
      assert s1 |> Series.equal(s2) |> Series.to_list() == [false, false, true]
    end
  end

  describe "memtype/1" do
    test "integer series" do
      s = Series.from_list([1, 2, 3])
      assert Series.memtype(s) == {:s, 64}
    end

    test "float series" do
      s = Series.from_list([1.2, 2.3, 3.4])
      assert Series.memtype(s) == {:f, 64}
    end

    test "boolean series" do
      s = Series.from_list([true, false, true])
      assert Series.memtype(s) == {:u, 8}
    end

    test "string series" do
      s = Series.from_list(["Bob", "Alice", "Joe"])
      assert Series.memtype(s) == :utf8
    end

    test "date series" do
      s = Series.from_list([~D[1999-12-31], ~D[1989-01-01]])
      assert Series.memtype(s) == {:s, 32}
    end

    test "datetime series" do
      s = Series.from_list([~N[2022-09-12 22:21:46.250899]])
      assert Series.memtype(s) == {:s, 64}
    end
  end
end
