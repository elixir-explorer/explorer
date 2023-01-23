defmodule Explorer.SeriesTest do
  use ExUnit.Case, async: true

  alias Explorer.Series

  doctest Explorer.Series

  test "defines doc metadata" do
    {:docs_v1, _, :elixir, "text/markdown", _docs, _metadata, entries} =
      Code.fetch_docs(Explorer.Series)

    for {{:function, name, arity}, _ann, _signature, docs, metadata} <- entries,
        is_map(docs) and map_size(docs) > 0,
        metadata[:type] not in [
          :shape,
          :introspection,
          :aggregation,
          :conversion,
          :window,
          :element_wise,
          :float_wise,
          :string_wise,
          :date_wise
        ] do
      flunk("invalid @doc type: #{inspect(metadata[:type])} for #{name}/#{arity}")
    end
  end

  describe "from_list/1" do
    test "with integers" do
      s = Series.from_list([1, 2, 3])

      assert Series.to_list(s) === [1, 2, 3]
      assert Series.dtype(s) == :integer
    end

    test "with floats" do
      s = Series.from_list([1, 2.4, 3])
      assert Series.to_list(s) === [1.0, 2.4, 3.0]
      assert Series.dtype(s) == :float
    end

    test "with binaries" do
      s = Series.from_list([<<228, 146, 51>>, <<22, 197, 116>>, <<42, 209, 236>>], dtype: :binary)
      assert Series.to_list(s) === [<<228, 146, 51>>, <<22, 197, 116>>, <<42, 209, 236>>]
      assert Series.dtype(s) == :binary
    end

    test "with strings" do
      s = Series.from_list(["a", "b", "c"])
      assert Series.to_list(s) === ["a", "b", "c"]
      assert Series.dtype(s) == :string
    end

    test "with binaries from strings" do
      s = Series.from_list(["a", "b", "c"], dtype: :binary)
      assert Series.to_list(s) === ["a", "b", "c"]
      assert Series.dtype(s) == :binary
    end

    test "mixing binaries and strings" do
      s = Series.from_list([<<228, 146, 51>>, "hello", <<42, 209, 236>>], dtype: :binary)
      assert Series.to_list(s) === [<<228, 146, 51>>, <<"hello">>, <<42, 209, 236>>]
      assert Series.dtype(s) == :binary
    end

    test "mixing types" do
      assert_raise ArgumentError, fn ->
        s = Series.from_list([1, "foo", 3])
        Series.to_list(s)
      end
    end

    test "with binaries without passing the dtype" do
      assert_raise ArgumentError, fn ->
        Series.from_list([<<228, 146, 51>>, <<22, 197, 116>>, <<42, 209, 236>>])
      end
    end

    test "with strings as categories" do
      s = Series.from_list(["a", "b", "c"], dtype: :category)
      assert Series.to_list(s) === ["a", "b", "c"]
      assert Series.dtype(s) == :category
    end
  end

  describe "fetch/2" do
    test "integer series" do
      s = Series.from_list([1, 2, 3, nil, 5])
      assert s[0] === 1
      assert s[0..1] |> Series.to_list() === [1, 2]
      assert s[[0, 1]] |> Series.to_list() === [1, 2]

      assert s[3] == nil
      assert s[-1] == 5
    end

    test "float series" do
      s = Series.from_list([1.2, 2.3, 3.4, nil, 5.6])
      assert s[0] === 1.2
      assert s[0..1] |> Series.to_list() === [1.2, 2.3]
      assert s[[0, 1]] |> Series.to_list() === [1.2, 2.3]

      assert s[3] == nil
      assert s[-1] == 5.6
    end

    test "string series" do
      s = Series.from_list(["a", "b", nil, "d"])
      assert s[0] === "a"
      assert s[2] == nil
      assert s[-1] == "d"
    end

    test "categorical series" do
      s = Series.from_list(["a", "b", nil, "d"], dtype: :category)
      assert s[0] === "a"
      assert s[2] == nil
      assert s[-1] == "d"
    end
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

  describe "fill_missing/2" do
    test "with literals" do
      s1 = Series.from_list([true, false, nil])
      assert Series.fill_missing(s1, true) |> Series.to_list() == [true, false, true]
      assert Series.fill_missing(s1, false) |> Series.to_list() == [true, false, false]
    end

    test "with integer" do
      s1 = Series.from_list([1, 2, nil, 4])
      assert Series.fill_missing(s1, 3) |> Series.to_list() == [1, 2, 3, 4]
    end

    test "with float" do
      s1 = Series.from_list([1.0, 2.0, nil, 4.0])
      assert Series.fill_missing(s1, 3.5) |> Series.to_list() == [1.0, 2.0, 3.5, 4.0]
    end

    test "with string" do
      s1 = Series.from_list(["1", "2", nil, "4"])
      assert Series.fill_missing(s1, "3") |> Series.to_list() == ["1", "2", "3", "4"]
    end

    test "with date" do
      s1 = Series.from_list([~D[2023-01-17], ~D[2023-01-18], nil, ~D[2023-01-09]])

      assert Series.fill_missing(s1, ~D[2023-01-19]) |> Series.to_list() == [
               ~D[2023-01-17],
               ~D[2023-01-18],
               ~D[2023-01-19],
               ~D[2023-01-09]
             ]
    end

    test "with datetime" do
      s1 =
        Series.from_list([
          ~N[2023-01-17 20:00:56.576456],
          ~N[2023-01-18 20:30:56.576456],
          nil,
          ~N[2023-01-09 21:00:56.576456]
        ])

      assert Series.fill_missing(s1, ~N[2023-01-19 20:00:56.576456]) |> Series.to_list() == [
               ~N[2023-01-17 20:00:56.576456],
               ~N[2023-01-18 20:30:56.576456],
               ~N[2023-01-19 20:00:56.576456],
               ~N[2023-01-09 21:00:56.576456]
             ]
    end

    test "with nan" do
      s1 = Series.from_list([1.0, 2.0, nil, 4.5])
      assert Series.fill_missing(s1, :nan) |> Series.to_list() == [1.0, 2.0, :nan, 4.5]
    end

    test "non-float series with nan" do
      s1 = Series.from_list([1, 2, nil, 4])

      assert_raise ArgumentError, fn ->
        Series.fill_missing(s1, :nan)
      end
    end

    test "with forward strategy" do
      s1 = Series.from_list([1, 2, nil, 4])
      assert Series.fill_missing(s1, :forward) |> Series.to_list() == [1, 2, 2, 4]
    end

    test "with backward strategy" do
      s1 = Series.from_list([1, 2, nil, 4])
      assert Series.fill_missing(s1, :backward) |> Series.to_list() == [1, 2, 4, 4]
    end

    test "with max strategy" do
      s1 = Series.from_list([1, 2, nil, 10, 5])
      assert Series.fill_missing(s1, :max) |> Series.to_list() == [1, 2, 10, 10, 5]
    end

    test "boolean series with max strategy" do
      s1 = Series.from_list([true, nil, false])
      assert Series.fill_missing(s1, :max) |> Series.to_list() == [true, true, false]
    end

    test "date series with max strategy" do
      s1 = Series.from_list([~D[2023-01-18], ~D[2023-01-17], nil, ~D[2023-01-09]])

      assert Series.fill_missing(s1, :max) |> Series.to_list() == [
               ~D[2023-01-18],
               ~D[2023-01-17],
               ~D[2023-01-18],
               ~D[2023-01-09]
             ]
    end

    test "datetime series with max strategy" do
      s1 =
        Series.from_list([
          ~N[2023-01-17 20:00:56.576456],
          ~N[2023-01-18 20:30:56.576456],
          nil,
          ~N[2023-01-09 21:00:56.576456],
          ~N[2023-01-18 23:35:56.576456]
        ])

      assert Series.fill_missing(s1, :max) |> Series.to_list() == [
               ~N[2023-01-17 20:00:56.576456],
               ~N[2023-01-18 20:30:56.576456],
               ~N[2023-01-18 23:35:56.576456],
               ~N[2023-01-09 21:00:56.576456],
               ~N[2023-01-18 23:35:56.576456]
             ]
    end

    test "with min strategy" do
      s1 = Series.from_list([1, 2, nil, 5])
      assert Series.fill_missing(s1, :min) |> Series.to_list() == [1, 2, 1, 5]
    end

    test "boolean series with min strategy" do
      s1 = Series.from_list([true, nil, false])
      assert Series.fill_missing(s1, :min) |> Series.to_list() == [true, false, false]
    end

    test "date series with min strategy" do
      s1 = Series.from_list([~D[2023-01-18], ~D[2023-01-17], nil, ~D[2023-01-09]])

      assert Series.fill_missing(s1, :min) |> Series.to_list() == [
               ~D[2023-01-18],
               ~D[2023-01-17],
               ~D[2023-01-09],
               ~D[2023-01-09]
             ]
    end

    test "datetime series with min strategy" do
      s1 =
        Series.from_list([
          ~N[2023-01-17 20:00:56.576456],
          ~N[2023-01-18 20:30:56.576456],
          nil,
          ~N[2023-01-09 21:00:56.576456],
          ~N[2023-01-18 23:35:56.576456]
        ])

      assert Series.fill_missing(s1, :min) |> Series.to_list() == [
               ~N[2023-01-17 20:00:56.576456],
               ~N[2023-01-18 20:30:56.576456],
               ~N[2023-01-09 21:00:56.576456],
               ~N[2023-01-09 21:00:56.576456],
               ~N[2023-01-18 23:35:56.576456]
             ]
    end

    test "with mean strategy" do
      s1 = Series.from_list([1, 3, nil, 5])
      assert Series.fill_missing(s1, :mean) |> Series.to_list() == [1, 3, 3, 5]
    end

    test "date series with mean strategy" do
      s1 = Series.from_list([~D[2023-01-18], ~D[2023-06-17], nil, ~D[2023-01-09]])

      assert Series.fill_missing(s1, :mean) |> Series.to_list() == [
               ~D[2023-01-18],
               ~D[2023-06-17],
               ~D[2023-03-06],
               ~D[2023-01-09]
             ]
    end

    test "datetime series with mean strategy" do
      s1 =
        Series.from_list([
          ~N[2023-01-18 20:30:56.576456],
          ~N[2023-06-17 20:00:56.576456],
          nil,
          ~N[2023-01-09 21:00:56.576456]
        ])

      assert Series.fill_missing(s1, :mean) |> Series.to_list() == [
               ~N[2023-01-18 20:30:56.576456],
               ~N[2023-06-17 20:00:56.576456],
               ~N[2023-03-06 20:30:56.576456],
               ~N[2023-01-09 21:00:56.576456]
             ]
    end

    test "boolean series with mean strategy" do
      s1 = Series.from_list([true, nil, false])

      assert_raise RuntimeError, fn ->
        Series.fill_missing(s1, :mean)
      end
    end
  end

  describe "equal/2" do
    test "compare boolean series" do
      s1 = Series.from_list([true, false, true])
      s2 = Series.from_list([false, true, true])
      assert s1 |> Series.equal(s2) |> Series.to_list() == [false, false, true]
    end

    test "compare integer series" do
      s1 = Series.from_list([1, 0, 2])
      s2 = Series.from_list([1, 0, 3])

      assert s1 |> Series.equal(s2) |> Series.to_list() == [true, true, false]
    end

    test "compare integer series with a scalar value on the right-hand side" do
      s1 = Series.from_list([1, 0, 2])
      assert s1 |> Series.equal(2) |> Series.to_list() == [false, false, true]

      s2 = Series.from_list(["foo", "bar", "baz"])
      assert s2 |> Series.equal("baz") |> Series.to_list() == [false, false, true]
    end

    test "compare integer series with a scalar value on the left-hand side" do
      s1 = Series.from_list([1, 0, 2])
      assert 2 |> Series.equal(s1) |> Series.to_list() == [false, false, true]

      s2 = Series.from_list(["foo", "bar", "baz"])
      assert "baz" |> Series.equal(s2) |> Series.to_list() == [false, false, true]
    end
  end

  describe "not_equal/2" do
    test "compare boolean series" do
      s1 = Series.from_list([true, false, true])
      s2 = Series.from_list([false, true, true])

      assert s1 |> Series.not_equal(s2) |> Series.to_list() == [true, true, false]
    end

    test "compare integer series" do
      s1 = Series.from_list([1, 0, 2])
      s2 = Series.from_list([1, 0, 3])

      assert s1 |> Series.not_equal(s2) |> Series.to_list() == [false, false, true]
    end

    test "compare integer series with a scalar value on the right-hand side" do
      s1 = Series.from_list([1, 0, 2])

      assert s1 |> Series.not_equal(2) |> Series.to_list() == [true, true, false]
    end

    test "compare integer series with a scalar value on the left-hand side" do
      s1 = Series.from_list([1, 0, 2])

      assert 2 |> Series.not_equal(s1) |> Series.to_list() == [true, true, false]
    end
  end

  describe "greater/2" do
    test "compare integer series" do
      s1 = Series.from_list([1, 0, 3])
      s2 = Series.from_list([1, 0, 2])

      assert s1 |> Series.greater(s2) |> Series.to_list() == [false, false, true]
    end

    test "compare integer series with a scalar value on the right-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert s1 |> Series.greater(2) |> Series.to_list() == [false, false, false, true]
    end

    test "compare integer series with a scalar value on the left-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert 2 |> Series.greater(s1) |> Series.to_list() == [true, true, false, false]
    end
  end

  describe "greater_equal/2" do
    test "compare integer series" do
      s1 = Series.from_list([1, 0, 2])
      s2 = Series.from_list([1, 0, 3])

      assert s1 |> Series.greater_equal(s2) |> Series.to_list() == [true, true, false]
    end

    test "compare integer series with a scalar value on the right-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert s1 |> Series.greater_equal(2) |> Series.to_list() == [false, false, true, true]
    end

    test "compare integer series with a scalar value on the left-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert 2 |> Series.greater_equal(s1) |> Series.to_list() == [true, true, true, false]
    end
  end

  describe "less/2" do
    test "compare integer series" do
      s1 = Series.from_list([1, 0, 2])
      s2 = Series.from_list([1, 0, 3])

      assert s1 |> Series.less(s2) |> Series.to_list() == [false, false, true]
    end

    test "compare integer series with a scalar value on the right-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert s1 |> Series.less(2) |> Series.to_list() == [true, true, false, false]
    end

    test "compare integer series with a scalar value on the left-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert 2 |> Series.less(s1) |> Series.to_list() == [false, false, false, true]
    end
  end

  describe "less_equal/2" do
    test "compare integer series" do
      s1 = Series.from_list([1, 0, 2])
      s2 = Series.from_list([1, 0, 3])

      assert s1 |> Series.less_equal(s2) |> Series.to_list() == [true, true, true]
    end

    test "compare integer series with a scalar value on the right-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert s1 |> Series.less_equal(2) |> Series.to_list() == [true, true, true, false]
    end

    test "compare integer series with a scalar value on the left-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert 2 |> Series.less_equal(s1) |> Series.to_list() == [false, false, true, true]
    end
  end

  describe "in/2" do
    test "with boolean series" do
      s1 = Series.from_list([true, false, true])
      s2 = Series.from_list([false, false, false, false])

      assert s1 |> Series.in(s2) |> Series.to_list() == [false, true, false]
    end

    test "with integer series" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([1, 0, 3])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "with float series" do
      s1 = Series.from_list([1.0, 2.0, 3.0])
      s2 = Series.from_list([1.0, 0.0, 3.0])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "with binary series" do
      s1 = Series.from_list([<<239, 191, 19>>, <<2>>, <<3>>], dtype: :binary)
      s2 = Series.from_list([<<239, 191, 19>>, <<0>>, <<3>>], dtype: :binary)

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "with string series" do
      s1 = Series.from_list(["1", "2", "3"])
      s2 = Series.from_list(["1", "0", "3"])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "with date series" do
      s1 = Series.from_list([~D[2023-01-17], ~D[2023-01-18], ~D[2023-01-09]])
      s2 = Series.from_list([~D[2023-01-17], ~D[2023-01-04], ~D[2023-01-09]])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "with datetime series" do
      s1 =
        Series.from_list([
          ~N[2023-01-17 20:00:56.576456Z],
          ~N[2023-01-18 20:30:56.576456Z],
          ~N[2023-01-09 21:00:56.576456Z]
        ])

      s2 =
        Series.from_list([
          ~N[2023-01-17 20:00:56.576456Z],
          ~N[2023-01-04 22:00:56.576456Z],
          ~N[2023-01-09 21:00:56.576456Z]
        ])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "with a list on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])
      l1 = [1, 0, 3]

      assert s1 |> Series.in(l1) |> Series.to_list() == [true, false, true]
    end

    test "with a smaller series on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([1, 3])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "with a bigger series on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([1, 3, 5, 10])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "compare boolean series with an integer series" do
      s1 = Series.from_list([true, false, true])
      s2 = Series.from_list([0, 1])

      assert_raise ArgumentError, fn ->
        Series.in(s1, s2)
      end
    end

    test "compare integer series with a float series" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([1.0, 0.5, 3.0])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "compare integer series with a string series" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list(["1", "0", "3"])

      assert_raise ArgumentError, fn ->
        Series.in(s1, s2)
      end
    end

    test "compare string series with a binary series" do
      s1 = Series.from_list(["1", "2", "3"])
      s2 = Series.from_list([<<1>>, <<0>>, <<"3">>], dtype: :binary)

      assert_raise ArgumentError, fn ->
        Series.in(s1, s2)
      end
    end

    test "compare date series with a datetime series" do
      s1 = Series.from_list([~D[2023-01-17], ~D[2023-01-18], ~D[2023-01-09]])

      s2 =
        Series.from_list([
          ~N[2023-01-17 20:00:56.576456Z],
          ~N[2023-01-04 22:00:56.576456Z],
          ~N[2023-01-09 21:00:56.576456Z]
        ])

      assert_raise ArgumentError, fn ->
        Series.in(s1, s2)
      end
    end
  end

  describe "iotype/1" do
    test "integer series" do
      s = Series.from_list([1, 2, 3])
      assert Series.iotype(s) == {:s, 64}
    end

    test "float series" do
      s = Series.from_list([1.2, 2.3, 3.4])
      assert Series.iotype(s) == {:f, 64}
    end

    test "boolean series" do
      s = Series.from_list([true, false, true])
      assert Series.iotype(s) == {:u, 8}
    end

    test "date series" do
      s = Series.from_list([~D[1999-12-31], ~D[1989-01-01]])
      assert Series.iotype(s) == {:s, 32}
    end

    test "datetime series" do
      s = Series.from_list([~N[2022-09-12 22:21:46.250899]])
      assert Series.iotype(s) == {:s, 64}
    end
  end

  describe "add/2" do
    test "adding two series together" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([4, 5, 6])

      s3 = Series.add(s1, s2)

      assert s3.dtype == :integer
      assert Series.to_list(s3) == [5, 7, 9]
    end

    test "adding a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.add(s1, -2)

      assert s2.dtype == :integer
      assert Series.to_list(s2) == [-1, 0, 1]
    end

    test "adding a series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.add(-2, s1)

      assert Series.to_list(s2) == [-1, 0, 1]
    end

    test "adding a series with a float scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.add(s1, 1.1)
      assert s2.dtype == :float

      assert Series.to_list(s2) == [2.1, 3.1, 4.1]
    end

    test "adding a series with a float scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.add(1.1, s1)
      assert s2.dtype == :float

      assert Series.to_list(s2) == [2.1, 3.1, 4.1]
    end
  end

  describe "subtract/2" do
    test "subtracting two series together" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([4, 5, 6])

      s3 = Series.subtract(s1, s2)

      assert s3.dtype == :integer
      assert Series.to_list(s3) == [-3, -3, -3]
    end

    test "subtracting a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.subtract(s1, -2)

      assert s2.dtype == :integer
      assert Series.to_list(s2) == [3, 4, 5]
    end

    test "subtracting a series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.subtract(-2, s1)

      assert Series.to_list(s2) == [-3, -4, -5]
    end

    test "subtracting a series with a float scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.subtract(s1, 1.5)
      assert s2.dtype == :float

      assert Series.to_list(s2) == [-0.5, 0.5, 1.5]
    end

    test "subtracting a series with a float scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.subtract(1.5, s1)
      assert s2.dtype == :float

      assert Series.to_list(s2) == [0.5, -0.5, -1.5]
    end
  end

  describe "multiply/2" do
    test "multiplying two series together" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([4, 5, 6])

      s3 = Series.multiply(s1, s2)

      assert s3.dtype == :integer
      assert Series.to_list(s3) == [4, 10, 18]
    end

    test "multiplying a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.multiply(s1, -2)

      assert s2.dtype == :integer
      assert Series.to_list(s2) == [-2, -4, -6]
    end

    test "multiplying a series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.multiply(-2, s1)

      assert s2.dtype == :integer
      assert Series.to_list(s2) == [-2, -4, -6]
    end

    test "multiplying a series with a float scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.multiply(s1, -2.5)

      assert s2.dtype == :float
      assert Series.to_list(s2) == [-2.5, -5.0, -7.5]
    end

    test "multiplying a series with a float scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.multiply(-2.5, s1)

      assert s2.dtype == :float
      assert Series.to_list(s2) == [-2.5, -5.0, -7.5]
    end
  end

  describe "divide/2" do
    test "dividing two series together" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([4, 5, 6])

      s3 = Series.divide(s2, s1)

      assert s3.dtype == :float
      assert Series.to_list(s3) == [4.0, 2.5, 2.0]
    end

    test "dividing a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.divide(s1, -2)

      assert s2.dtype == :float
      assert Series.to_list(s2) == [-0.5, -1, -1.5]
    end

    test "dividing a series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 5])

      s2 = Series.divide(-2, s1)

      assert s2.dtype == :float
      assert Series.to_list(s2) == [-2.0, -1.0, -0.4]
    end

    test "dividing a series with a float scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.divide(s1, -2.5)

      assert s2.dtype == :float
      assert Series.to_list(s2) == [-0.4, -0.8, -1.2]
    end

    test "dividing a series with a float scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.divide(-3.12, s1)

      assert s2.dtype == :float
      assert Series.to_list(s2) == [-3.12, -1.56, -1.04]
    end
  end

  describe "quotient/2" do
    test "quotient of two series" do
      s1 = Series.from_list([10, 11, 15])
      s2 = Series.from_list([2, 2, 2])

      s3 = Series.quotient(s1, s2)

      assert s3.dtype == :integer
      assert Series.to_list(s3) == [5, 5, 7]
    end

    test "quotient of a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([10, 11, 15])

      s2 = Series.quotient(s1, -2)

      assert s2.dtype == :integer
      assert Series.to_list(s2) == [-5, -5, -7]
    end

    test "quotient of a series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([10, 20, 25])

      s2 = Series.quotient(101, s1)

      assert s2.dtype == :integer
      assert Series.to_list(s2) == [10, 5, 4]
    end
  end

  describe "remainder/2" do
    test "remainder of two series" do
      s1 = Series.from_list([10, 11, 19])
      s2 = Series.from_list([2, 2, 2])

      s3 = Series.remainder(s1, s2)

      assert s3.dtype == :integer
      assert Series.to_list(s3) == [0, 1, 1]
    end

    test "remainder of a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([10, 11, 15])

      s2 = Series.remainder(s1, -2)

      assert s2.dtype == :integer
      assert Series.to_list(s2) == [0, 1, 1]
    end

    test "remainder of a series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([10, 20, 25])

      s2 = Series.remainder(101, s1)

      assert s2.dtype == :integer
      assert Series.to_list(s2) == [1, 1, 1]
    end
  end

  describe "pow/2" do
    test "pow of a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      result = Series.pow(s1, 2)

      assert result.dtype == :integer
      assert Series.to_list(result) == [1, 4, 9]
    end

    test "pow of a series with an float scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      result = Series.pow(s1, 2.0)

      assert result.dtype == :float
      assert Series.to_list(result) == [1.0, 4.0, 9.0]
    end

    test "pow of a series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      result = Series.pow(2, s1)

      assert result.dtype == :integer
      assert Series.to_list(result) == [2, 4, 8]
    end

    test "pow of a series with an float scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      result = Series.pow(2.0, s1)

      assert result.dtype == :float
      assert Series.to_list(result) == [2.0, 4.0, 8.0]
    end

    test "pow of a scalar value on the left-hand side to a series with a negative integer" do
      s1 = Series.from_list([1, -2, 3])

      assert_raise RuntimeError, "negative exponent with an integer base", fn ->
        Series.pow(2, s1)
      end
    end
  end

  describe "sample/2" do
    test "sample taking 10 elements" do
      s = 1..100 |> Enum.to_list() |> Series.from_list()
      result = Series.sample(s, 10, seed: 100)

      assert Series.size(result) == 10
      assert Series.to_list(result) == [72, 33, 15, 4, 16, 49, 23, 96, 45, 47]
    end

    test "sample taking 5% of elements" do
      s = 1..100 |> Enum.to_list() |> Series.from_list()

      result = Series.sample(s, 0.05, seed: 100)

      assert Series.size(result) == 5
      assert Series.to_list(result) == [68, 24, 6, 8, 36]
    end

    test "sample taking more than elements without replacement" do
      s = 1..10 |> Enum.to_list() |> Series.from_list()

      assert_raise ArgumentError,
                   "in order to sample more elements than are in the series (10), sampling `replacement` must be true",
                   fn ->
                     Series.sample(s, 15)
                   end
    end

    test "sample taking more than elements using fraction without replacement" do
      s = 1..10 |> Enum.to_list() |> Series.from_list()

      assert_raise ArgumentError,
                   "in order to sample more elements than are in the series (10), sampling `replacement` must be true",
                   fn ->
                     Series.sample(s, 1.2)
                   end
    end

    test "sample taking more than elements with replacement" do
      s = 1..10 |> Enum.to_list() |> Series.from_list()

      result = Series.sample(s, 15, replacement: true, seed: 100)

      assert Series.size(result) == 15
      assert Series.to_list(result) == [1, 8, 10, 1, 3, 10, 9, 1, 10, 10, 4, 5, 9, 7, 6]
    end

    test "sample taking more than elements with fraction and replacement" do
      s = 1..10 |> Enum.to_list() |> Series.from_list()

      result = Series.sample(s, 1.2, replacement: true, seed: 100)

      assert Series.size(result) == 12
      assert Series.to_list(result) == [1, 8, 10, 1, 3, 10, 9, 1, 10, 10, 4, 5]
    end
  end

  describe "select/3" do
    test "select elements of the same type" do
      predicate = [true, false, false, true, false] |> Series.from_list()
      on_true = 1..5 |> Enum.to_list() |> Series.from_list()
      on_false = 5..1//-1 |> Enum.to_list() |> Series.from_list()

      result = Series.select(predicate, on_true, on_false)

      assert Series.size(result) == 5
      assert Series.to_list(result) == [1, 4, 3, 4, 1]
    end

    test "select elements of compatible types" do
      predicate = [true, false, true] |> Series.from_list()
      on_true = [1.1, 1.2, 1.3] |> Series.from_list()
      on_false = [5, 3, 2] |> Series.from_list()

      result = Series.select(predicate, on_true, on_false)

      assert Series.size(result) == 3
      assert Series.to_list(result) == [1.1, 3, 1.3]
    end

    test "select errors mixing incompatible types" do
      predicate = [true, false, true] |> Series.from_list()
      on_true = [1.1, 1.2, 1.3] |> Series.from_list()
      on_false = ["foo", "bar", "baz"] |> Series.from_list()

      assert_raise ArgumentError, fn ->
        Series.select(predicate, on_true, on_false)
      end
    end

    test "select requires boolean predicate" do
      predicate = [1.1, 1.2, 1.3] |> Series.from_list()
      on_true = [1.1, 1.2, 1.3] |> Series.from_list()
      on_false = [5, 3, 2] |> Series.from_list()

      assert_raise ArgumentError, fn ->
        Series.select(predicate, on_true, on_false)
      end
    end
  end

  describe "sort/2" do
    test "sort a series in ascending order" do
      s1 = Series.from_list([3, 1, nil, 2])

      result = Series.sort(s1)

      assert Series.to_list(result) == [1, 2, 3, nil]
    end

    test "sort a series in descending order" do
      s1 = Series.from_list([3, 1, nil, 2])

      result = Series.sort(s1, direction: :desc)

      assert Series.to_list(result) == [nil, 3, 2, 1]
    end

    test "sort a series in descending order, but with nils last" do
      s1 = Series.from_list([3, 1, nil, 2])

      result = Series.sort(s1, direction: :desc, nils: :last)

      assert Series.to_list(result) == [3, 2, 1, nil]
    end

    test "sort a series in ascending order, but nils first" do
      s1 = Series.from_list([3, 1, nil, 2])

      result = Series.sort(s1, nils: :first)

      assert Series.to_list(result) == [nil, 1, 2, 3]
    end
  end

  describe "argsort/2" do
    test "indices of sorting a series in ascending order" do
      s1 = Series.from_list([3, 1, nil, 2])

      result = Series.argsort(s1)

      assert Series.to_list(result) == [1, 3, 0, 2]
    end

    # There is a bug which is not considering "nils first" for descending argsort
    @tag :skip
    test "indices of sorting a series in descending order" do
      s1 = Series.from_list([9, 4, nil, 5])

      result = Series.argsort(s1, direction: :desc, nils: :first)

      assert Series.to_list(result) == [2, 0, 3, 1]
    end

    test "sort a series in descending order, but with nils last" do
      s1 = Series.from_list([9, 4, nil, 5])

      result = Series.argsort(s1, direction: :desc, nils: :last)

      assert Series.to_list(result) == [0, 3, 1, 2]
    end

    test "sort a series in ascending order, but nils first" do
      s1 = Series.from_list([9, 4, nil, 5])

      result = Series.argsort(s1, nils: :first)

      assert Series.to_list(result) == [2, 1, 3, 0]
    end
  end

  describe "at/2" do
    test "fetch an item from a given position in the series - integer" do
      s1 = Series.from_list([9, 4, nil, 5])

      assert Series.at(s1, 1) == 4
      assert Series.at(s1, 3) == 5
    end

    test "fetch an item from a given position in the series - float" do
      s1 = Series.from_list([9.1, 4.2, 3.6, 5.9])

      assert Series.at(s1, 1) == 4.2
      assert Series.at(s1, 2) == 3.6
    end

    test "fetch an item from a given position in the series - string" do
      s1 = Series.from_list(["a", "b", "c", "d"])

      assert Series.at(s1, 0) == "a"
      assert Series.at(s1, 3) == "d"
    end

    test "fetch an item from a given position in the series - binary" do
      s1 =
        Series.from_list([<<114, 231, 242>>, <<181, 43, 48>>, <<89, 155, 216>>], dtype: :binary)

      assert Series.at(s1, 1) == <<181, 43, 48>>
      assert Series.at(s1, 2) == <<89, 155, 216>>
    end
  end

  test "not/1 invert a boolean series" do
    s1 = Series.from_list([true, false, false, nil, true])
    result = Series.not(s1)

    assert Series.to_list(result) == [false, true, true, nil, false]
  end

  test "nil_count/1" do
    s1 = Explorer.Series.from_list(["a", nil, "c", nil, nil])
    s2 = Explorer.Series.from_list([1, nil, 3, nil, nil, 6, 7, nil])
    s3 = Explorer.Series.from_list(["a", "b", "c"])

    assert Series.nil_count(s1) == 3
    assert Series.nil_count(s2) == 4
    assert Series.nil_count(s3) == 0
  end

  test "categories/1" do
    s = Series.from_list(["a", "b", "c", nil, "a"], dtype: :category)
    categories = Series.categories(s)
    assert Series.to_list(categories) === ["a", "b", "c"]
    assert Series.dtype(categories) == :string
  end

  describe "categorise/2" do
    test "takes int series and categorise with categorical series" do
      categories = Series.from_list(["a", "b", "c"], dtype: :category)
      indexes = Series.from_list([0, 2, 1, 0, 2])
      categorized = Series.categorise(indexes, categories)

      assert Series.to_list(categorized) == ["a", "c", "b", "a", "c"]
      assert Series.dtype(categorized) == :category
    end

    test "takes int series and categorise with string series" do
      categories = Series.from_list(["a", "b", "c"], dtype: :string)
      indexes = Series.from_list([0, 2, 1, 0, 2])
      categorized = Series.categorise(indexes, categories)

      assert Series.to_list(categorized) == ["a", "c", "b", "a", "c"]
      assert Series.dtype(categorized) == :category
    end

    test "takes int series and categorise with string list and nils" do
      indexes = Series.from_list([0, 2, 1, 0, 2, 7, 1, 9, nil])
      categorized = Series.categorise(indexes, ["a", "b", "c"])

      assert Series.to_list(categorized) == ["a", "c", "b", "a", "c", nil, "b", nil, nil]
      assert Series.dtype(categorized) == :category
    end
  end
end
