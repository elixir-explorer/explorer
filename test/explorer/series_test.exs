defmodule Explorer.SeriesTest do
  use ExUnit.Case, async: true

  alias Explorer.Series

  doctest Explorer.Series

  test "from_list/1" do
    s = Series.from_list([1, 2, 3])

    assert Series.to_list(s) === [1, 2, 3]

    s = Series.from_list([1, 2.4, 3])
    assert Series.to_list(s) === [1.0, 2.4, 3.0]

    assert_raise ArgumentError, fn ->
      s = Series.from_list([1, "foo", 3])
      Series.to_list(s)
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
end
