defmodule Explorer.SeriesTest do
  use ExUnit.Case, async: true

  # Note that for the `{:list, _}` and `{:struct, _}` dtypes, we have a separated file for the tests.

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
          :datetime_wise,
          :list_wise,
          :struct_wise
        ] do
      flunk("invalid @doc type: #{inspect(metadata[:type])} for #{name}/#{arity}")
    end
  end

  describe "from_list/1" do
    test "with nils" do
      s = Series.from_list([nil, nil, nil])

      assert Series.to_list(s) === [nil, nil, nil]
      assert Series.dtype(s) == :null
    end

    test "with non nils and dtype :null" do
      s = Series.from_list([1, 2, 3], dtype: :null)

      assert Series.to_list(s) === [nil, nil, nil]
      assert Series.dtype(s) == :null
    end

    test "with integers" do
      s = Series.from_list([1, 2, 3])

      assert Series.to_list(s) === [1, 2, 3]
      assert Series.dtype(s) == {:s, 64}
    end

    test "with unsigned integers" do
      s = Series.from_list([1, 2, 3], dtype: {:u, 64})

      assert Series.to_list(s) === [1, 2, 3]
      assert Series.dtype(s) == {:u, 64}
    end

    test "with floats" do
      s = Series.from_list([1, 2.4, 3])
      assert Series.to_list(s) === [1.0, 2.4, 3.0]
      assert Series.dtype(s) == {:f, 64}
    end

    test "float 32 overflow" do
      assert_raise ArgumentError,
                   "argument error",
                   fn ->
                     Series.from_list([1_055_028_234_663_852_885_981_170_418_348_451_692_544.0],
                       dtype: {:f, 32}
                     )
                   end
    end

    test "with nan" do
      s = Series.from_list([:nan, :nan, :nan])
      assert Series.to_list(s) === [:nan, :nan, :nan]
      assert Series.dtype(s) == {:f, 64}
    end

    test "with infinity" do
      s = Series.from_list([:infinity, :infinity, :infinity])
      assert Series.to_list(s) === [:infinity, :infinity, :infinity]
      assert Series.dtype(s) == {:f, 64}
    end

    test "with negative infinity" do
      s = Series.from_list([:neg_infinity, :neg_infinity, :neg_infinity])
      assert Series.to_list(s) === [:neg_infinity, :neg_infinity, :neg_infinity]
      assert Series.dtype(s) == {:f, 64}
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

    test "with time" do
      time = ~T[02:05:03.654321]
      s = Series.from_list([time])
      assert Series.to_list(s) === [time]
      assert Series.dtype(s) == :time
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

    test "mixing floats and integers" do
      s = Series.from_list([1, 2.4, 3])
      assert Series.to_list(s) === [1.0, 2.4, 3.0]
      assert Series.dtype(s) == {:f, 64}
    end

    test "mixing integers and nan" do
      s = Series.from_list([1, :nan, 3])
      assert Series.to_list(s) === [1.0, :nan, 3.0]
      assert Series.dtype(s) == {:f, 64}
    end

    test "mixing integers and infinity" do
      s = Series.from_list([1, :infinity, 3])
      assert Series.to_list(s) === [1.0, :infinity, 3.0]
      assert Series.dtype(s) == {:f, 64}
    end

    test "mixing integers and negative infinity" do
      s = Series.from_list([1, :neg_infinity, 3])
      assert Series.to_list(s) === [1.0, :neg_infinity, 3.0]
      assert Series.dtype(s) == {:f, 64}
    end

    test "mixing floats and nan" do
      s = Series.from_list([3.0, :nan, 0.5])
      assert Series.to_list(s) === [3.0, :nan, 0.5]
      assert Series.dtype(s) == {:f, 64}
    end

    test "mixing floats and infinity" do
      s = Series.from_list([1.0, :infinity, 3.0])
      assert Series.to_list(s) === [1.0, :infinity, 3.0]
      assert Series.dtype(s) == {:f, 64}
    end

    test "mixing floats and negative infinity" do
      s = Series.from_list([1.0, :neg_infinity, 3.0])
      assert Series.to_list(s) === [1.0, :neg_infinity, 3.0]
      assert Series.dtype(s) == {:f, 64}
    end

    test "mixing floats, integers, nan, infinity and negative infinity" do
      s = Series.from_list([1, :nan, 2.0, :infinity, :neg_infinity])
      assert Series.to_list(s) === [1.0, :nan, 2.0, :infinity, :neg_infinity]
      assert Series.dtype(s) == {:f, 64}
    end

    test "floats as {:f, 32}" do
      s = Series.from_list([1.0, 2.5, :nan, 3.5, :infinity], dtype: {:f, 32})
      assert s[0] == 1.0
      assert Series.to_list(s) === [1.0, 2.5, :nan, 3.5, :infinity]
      assert Series.dtype(s) == {:f, 32}
    end

    test "integers as {:f, 64}" do
      # Shortcuts and the "real" dtype
      for dtype <- [:float, :f64, {:f, 64}] do
        s = Series.from_list([1, 2, 3, 4], dtype: dtype)
        assert s[0] === 1.0
        assert Series.to_list(s) === [1.0, 2.0, 3.0, 4.0]
        assert Series.dtype(s) === {:f, 64}
      end
    end

    test "integers as {:f, 32}" do
      # Shortcut and the "real" dtype
      for dtype <- [:f32, {:f, 32}] do
        s = Series.from_list([1, 2, 3, 4], dtype: dtype)
        assert s[0] === 1.0
        assert Series.to_list(s) === [1.0, 2.0, 3.0, 4.0]
        assert Series.dtype(s) === {:f, 32}
      end
    end

    test "mixing integers with an invalid atom" do
      assert_raise ArgumentError, "unsupported datatype: :error", fn ->
        Series.from_list([1, 2, :error, 4])
      end
    end

    test "mixing floats with an invalid atom" do
      assert_raise ArgumentError, "unsupported datatype: :error", fn ->
        Series.from_list([1.0, 2.0, :error, 4.0])
      end
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

    test "with nils series as string series" do
      s = Series.from_list([nil, nil, nil], dtype: :string)

      assert Series.to_list(s) === [nil, nil, nil]
      assert Series.dtype(s) == :string
    end

    test "with dates" do
      dates = [~D[1643-01-04], ~D[-0030-08-12], ~D[1994-05-01]]
      assert Series.from_list(dates, dtype: :date) |> Series.to_list() == dates

      today_in_days = Date.utc_today() |> Date.to_gregorian_days()

      dates =
        for _i <- 0..50 do
          days = :rand.uniform(today_in_days)
          Date.from_gregorian_days(days)
        end

      assert Series.from_list(dates, dtype: :date) |> Series.to_list() == dates

      dates = Enum.intersperse(dates, nil)
      assert Series.from_list(dates, dtype: :date) |> Series.to_list() == dates
    end

    test "with naive datetimes" do
      dates = [
        ~N[2022-04-13 15:44:31.560227],
        ~N[1022-01-04 21:18:31.224123],
        ~N[1988-11-23 06:36:16.158432],
        ~N[2353-03-07 00:39:35.702789]
      ]

      assert Series.from_list(dates, dtype: {:datetime, :microsecond}) |> Series.to_list() ==
               dates

      today_in_days = Date.utc_today() |> Date.to_gregorian_days()
      day_in_seconds = 86_400

      dates =
        for _i <- 0..50 do
          days = :rand.uniform(today_in_days)
          seconds = days * day_in_seconds
          microseconds = {:rand.uniform(999_999), 6}

          seconds
          |> NaiveDateTime.from_gregorian_seconds(microseconds)
          |> NaiveDateTime.add(:rand.uniform(24) * 60 * 60, :second)
          |> NaiveDateTime.add(:rand.uniform(60) * 60, :second)
          |> NaiveDateTime.add(:rand.uniform(60), :second)
        end

      assert Series.from_list(dates, dtype: {:datetime, :microsecond}) |> Series.to_list() ==
               dates
    end

    test "with 32-bit integers" do
      for dtype <- [:s32, {:s, 32}] do
        s = Series.from_list([-1, 0, 1, 2, 3, nil], dtype: dtype)

        assert s[0] === -1
        assert Series.to_list(s) === [-1, 0, 1, 2, 3, nil]
        assert Series.dtype(s) == {:s, 32}
      end
    end

    test "with 16-bit integers" do
      for dtype <- [:s16, {:s, 16}] do
        s = Series.from_list([-1, 0, 1, 2, 3, nil], dtype: dtype)

        assert s[0] === -1
        assert Series.to_list(s) === [-1, 0, 1, 2, 3, nil]
        assert Series.dtype(s) == {:s, 16}
      end
    end

    test "with 8-bit integers" do
      for dtype <- [:s8, {:s, 8}] do
        s = Series.from_list([-1, 0, 1, 2, 3, nil], dtype: dtype)

        assert s[0] === -1
        assert Series.to_list(s) === [-1, 0, 1, 2, 3, nil]
        assert Series.dtype(s) == {:s, 8}
      end
    end

    test "with 64-bit unsigned integers" do
      for dtype <- [:u64, {:u, 64}] do
        s = Series.from_list([0, 1, 2, 3, 2 ** 64 - 1, nil], dtype: dtype)

        assert s[3] === 3
        assert Series.to_list(s) === [0, 1, 2, 3, 2 ** 64 - 1, nil]
        assert Series.dtype(s) == {:u, 64}
      end
    end

    test "with 32-bit unsigned integers" do
      for dtype <- [:u32, {:u, 32}] do
        s = Series.from_list([0, 1, 2, 3, 2 ** 32 - 1, nil], dtype: dtype)

        assert s[3] === 3
        assert Series.to_list(s) === [0, 1, 2, 3, 2 ** 32 - 1, nil]
        assert Series.dtype(s) == {:u, 32}
      end
    end

    test "with 16-bit unsigned integers" do
      for dtype <- [:u16, {:u, 16}] do
        s = Series.from_list([0, 1, 2, 3, 2 ** 16 - 1, nil], dtype: dtype)

        assert s[1] === 1
        assert Series.to_list(s) === [0, 1, 2, 3, 2 ** 16 - 1, nil]
        assert Series.dtype(s) == {:u, 16}
      end
    end

    test "with 8-bit unsigned integers" do
      for dtype <- [:u8, {:u, 8}] do
        s = Series.from_list([0, 1, 2, 3, 2 ** 8 - 1, nil], dtype: dtype)

        assert s[1] === 1
        assert Series.to_list(s) === [0, 1, 2, 3, 2 ** 8 - 1, nil]
        assert Series.dtype(s) == {:u, 8}
      end
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

    test "with binary" do
      s1 = Series.from_list([<<1>>, <<2>>, nil, <<4>>], dtype: :binary)

      assert Series.fill_missing(s1, <<239, 191, 19>>) |> Series.to_list() == [
               <<1>>,
               <<2>>,
               <<239, 191, 19>>,
               <<4>>
             ]
    end

    test "mixing binary series with string" do
      s1 = Series.from_list([<<239, 191, 19>>, <<2>>, nil, <<4>>], dtype: :binary)

      assert Series.fill_missing(s1, "3") |> Series.to_list() == [
               <<239, 191, 19>>,
               <<2>>,
               "3",
               <<4>>
             ]
    end

    test "with string" do
      s1 = Series.from_list(["1", "2", nil, "4"])
      assert Series.fill_missing(s1, "3") |> Series.to_list() == ["1", "2", "3", "4"]
    end

    test "mixing string series with a binary that is a valid string" do
      s1 = Series.from_list(["1", "2", nil, "4"])
      assert Series.fill_missing(s1, <<3>>) |> Series.to_list() == ["1", "2", <<3>>, "4"]
    end

    test "mixing string series with a binary that is an invalid string" do
      s1 = Series.from_list(["1", "2", nil, "4"])

      assert_raise RuntimeError, "Generic Error: cannot cast to string", fn ->
        Series.fill_missing(s1, <<239, 191, 19>>)
      end
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

      assert_raise RuntimeError,
                   "Polars Error: invalid operation: `mean` operation not supported for dtype `Boolean`",
                   fn -> Series.fill_missing(s1, :mean) end
    end

    test "with nan" do
      s1 = Series.from_list([1.0, 2.0, nil, 4.5])
      assert Series.fill_missing(s1, :nan) |> Series.to_list() == [1.0, 2.0, :nan, 4.5]
    end

    test "non-float series with nan" do
      s1 = Series.from_list([1, 2, nil, 4])

      assert_raise ArgumentError,
                   "fill_missing with :nan values require a float series, got {:s, 64}",
                   fn -> Series.fill_missing(s1, :nan) end
    end

    test "with infinity" do
      s1 = Series.from_list([1.0, 2.0, nil, 4.5])
      assert Series.fill_missing(s1, :infinity) |> Series.to_list() == [1.0, 2.0, :infinity, 4.5]
    end

    test "non-float series with infinity" do
      s1 = Series.from_list([1, 2, nil, 4])

      assert_raise ArgumentError,
                   "fill_missing with :infinity values require a float series, got {:s, 64}",
                   fn -> Series.fill_missing(s1, :infinity) end
    end

    test "with neg_infinity" do
      s1 = Series.from_list([1.0, 2.0, nil, 4.5])

      assert Series.fill_missing(s1, :neg_infinity) |> Series.to_list() ==
               [1.0, 2.0, :neg_infinity, 4.5]
    end

    test "non-float series with neg_infinity" do
      s1 = Series.from_list([1, 2, nil, 4])

      assert_raise ArgumentError,
                   "fill_missing with :neg_infinity values require a float series, got {:s, 64}",
                   fn -> Series.fill_missing(s1, :neg_infinity) end
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

    test "compare float series" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])
      s2 = Series.from_list([1.0, 3.0, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.equal(s2) |> Series.to_list() == [true, false, true, true, true]
    end

    test "compare time series" do
      s1 = Series.from_list([~T[00:00:00.000000], ~T[12:00:00.000000], ~T[23:59:59.999999]])
      s2 = Series.from_list([~T[00:00:00.000000], ~T[12:30:00.000000], ~T[23:59:59.999999]])

      assert s1 |> Series.equal(s2) |> Series.to_list() == [true, false, true]
    end

    test "compare integer series with a scalar value on the right-hand side" do
      s1 = Series.from_list([1, 0, 2])
      assert s1 |> Series.equal(2) |> Series.to_list() == [false, false, true]

      s2 = Series.from_list(["foo", "bar", "baz"])
      assert s2 |> Series.equal("baz") |> Series.to_list() == [false, false, true]
    end

    test "compare float series with a float value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])
      assert s1 |> Series.equal(2.5) |> Series.to_list() == [false, true, false, false, false]
    end

    test "compare float series with a nan value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])
      assert s1 |> Series.equal(:nan) |> Series.to_list() == [false, false, true, false, false]
    end

    test "compare float series with an infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.equal(:infinity) |> Series.to_list() ==
               [false, false, false, true, false]
    end

    test "compare float series with an negative infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.equal(:neg_infinity) |> Series.to_list() ==
               [false, false, false, false, true]
    end

    test "compare integer series with a scalar value on the left-hand side" do
      s1 = Series.from_list([1, 0, 2])
      assert 2 |> Series.equal(s1) |> Series.to_list() == [false, false, true]

      s2 = Series.from_list(["foo", "bar", "baz"])
      assert "baz" |> Series.equal(s2) |> Series.to_list() == [false, false, true]
    end

    test "compare float series with a float value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])
      assert 2.5 |> Series.equal(s1) |> Series.to_list() == [false, true, false, false, false]
    end

    test "compare float series with a nan value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])
      assert :nan |> Series.equal(s1) |> Series.to_list() == [false, false, true, false, false]
    end

    test "compare float series with an infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      assert :infinity |> Series.equal(s1) |> Series.to_list() ==
               [false, false, false, true, false]
    end

    test "compare float series with an negative infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      assert :neg_infinity |> Series.equal(s1) |> Series.to_list() ==
               [false, false, false, false, true]
    end

    test "compare categories with integers" do
      s = Series.from_list(["a", "b", "c", nil, "a"], dtype: :category)

      assert Series.equal(Series.from_list(["a"]), s) |> Series.to_list() ==
               [true, false, false, nil, true]

      assert Series.equal(s, "a") |> Series.to_list() == [true, false, false, nil, true]
    end

    test "performs broadcasting" do
      s1 = Series.from_list([-1, 0, 1])
      s2 = Series.from_list([0])
      assert s1 |> Series.equal(s2) |> Series.to_list() == [false, true, false]
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

    test "compare float series" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])
      s2 = Series.from_list([1.0, 3.0, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.not_equal(s2) |> Series.to_list() == [false, true, false, false, false]
    end

    test "compare time series" do
      s1 = Series.from_list([~T[00:00:00.000000], ~T[12:00:00.000000], ~T[23:59:59.999999]])
      s2 = Series.from_list([~T[00:00:00.000000], ~T[12:30:00.000000], ~T[23:59:59.999999]])

      assert s1 |> Series.not_equal(s2) |> Series.to_list() == [false, true, false]
    end

    test "compare integer series with a scalar value on the right-hand side" do
      s1 = Series.from_list([1, 0, 2])

      assert s1 |> Series.not_equal(2) |> Series.to_list() == [true, true, false]
    end

    test "compare float series with a float value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])
      assert s1 |> Series.not_equal(2.5) |> Series.to_list() == [true, false, true, true, true]
    end

    test "compare float series with a nan value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])
      assert s1 |> Series.not_equal(:nan) |> Series.to_list() == [true, true, false, true, true]
    end

    test "compare float series with an infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.not_equal(:infinity) |> Series.to_list() ==
               [true, true, true, false, true]
    end

    test "compare float series with an negative infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.not_equal(:neg_infinity) |> Series.to_list() ==
               [true, true, true, true, false]
    end

    test "compare integer series with a scalar value on the left-hand side" do
      s1 = Series.from_list([1, 0, 2])

      assert 2 |> Series.not_equal(s1) |> Series.to_list() == [true, true, false]
    end

    test "compare float series with a float value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])
      assert 2.5 |> Series.not_equal(s1) |> Series.to_list() == [true, false, true, true, true]
    end

    test "compare float series with a nan value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])
      assert :nan |> Series.not_equal(s1) |> Series.to_list() == [true, true, false, true, true]
    end

    test "compare float series with an infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      assert :infinity |> Series.not_equal(s1) |> Series.to_list() ==
               [true, true, true, false, true]
    end

    test "compare float series with an negative infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      assert :neg_infinity |> Series.not_equal(s1) |> Series.to_list() ==
               [true, true, true, true, false]
    end
  end

  describe "greater/2" do
    test "compare integer series" do
      s1 = Series.from_list([1, 0, 3])
      s2 = Series.from_list([1, 0, 2])

      assert s1 |> Series.greater(s2) |> Series.to_list() == [false, false, true]
    end

    test "compare float series" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity, :infinity])
      s2 = Series.from_list([1.0, 2.0, :nan, :infinity, :neg_infinity, :neg_infinity])

      assert s1 |> Series.greater(s2) |> Series.to_list() ==
               [false, true, false, false, false, true]
    end

    test "compare time series" do
      s1 = Series.from_list([~T[00:00:00.000000], ~T[12:00:00.000000], ~T[23:59:59.999999]])
      s2 = Series.from_list([~T[00:00:00.000000], ~T[12:30:00.000000], ~T[23:50:59.999999]])

      assert s1 |> Series.greater(s2) |> Series.to_list() == [false, false, true]
    end

    test "compare integer series with a scalar value on the right-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert s1 |> Series.greater(2) |> Series.to_list() == [false, false, false, true]
    end

    test "compare float series with a float value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])
      assert s1 |> Series.greater(2.0) |> Series.to_list() == [false, true, true, true, false]
    end

    test "compare float series with a nan value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])
      assert s1 |> Series.greater(:nan) |> Series.to_list() == [false, false, false, false, false]
    end

    test "compare float series with an infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.greater(:infinity) |> Series.to_list() ==
               [false, false, true, false, false]
    end

    test "compare float series with an negative infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.greater(:neg_infinity) |> Series.to_list() ==
               [true, true, true, true, false]
    end

    test "compare integer series with a scalar value on the left-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert 2 |> Series.greater(s1) |> Series.to_list() == [true, true, false, false]
    end

    test "compare float series with a float value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])
      assert 2.5 |> Series.greater(s1) |> Series.to_list() == [true, false, false, false, true]
    end

    test "compare float series with a nan value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])
      assert :nan |> Series.greater(s1) |> Series.to_list() == [true, true, false, true, true]
    end

    test "compare float series with an infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert :infinity |> Series.greater(s1) |> Series.to_list() ==
               [true, true, false, false, true]
    end

    test "compare float series with an negative infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert :neg_infinity |> Series.greater(s1) |> Series.to_list() ==
               [false, false, false, false, false]
    end

    test "compares series of different sizes" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([3, 2, 1, 4])

      assert_raise ArgumentError,
                   "series must either have the same size or one of them must have size of 1, got: 3 and 4",
                   fn -> Series.equal(s1, s2) end
    end
  end

  describe "greater_equal/2" do
    test "compare integer series" do
      s1 = Series.from_list([1, 0, 2])
      s2 = Series.from_list([1, 0, 3])

      assert s1 |> Series.greater_equal(s2) |> Series.to_list() == [true, true, false]
    end

    test "compare float series" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity, :infinity])
      s2 = Series.from_list([1.0, 2.0, :nan, :infinity, :neg_infinity, :neg_infinity])

      assert s1 |> Series.greater_equal(s2) |> Series.to_list() ==
               [true, true, true, true, true, true]
    end

    test "compare time series" do
      s1 = Series.from_list([~T[00:00:00.000000], ~T[12:00:00.000000], ~T[23:59:59.999999]])
      s2 = Series.from_list([~T[00:00:00.000000], ~T[12:30:00.000000], ~T[23:50:59.999999]])

      assert s1 |> Series.greater_equal(s2) |> Series.to_list() == [true, false, true]
    end

    test "compare integer series with a scalar value on the right-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert s1 |> Series.greater_equal(2) |> Series.to_list() == [false, false, true, true]
    end

    test "compare float series with a float value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.greater_equal(2.0) |> Series.to_list() ==
               [false, true, true, true, false]
    end

    test "compare float series with a nan value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.greater_equal(:nan) |> Series.to_list() ==
               [false, false, true, false, false]
    end

    test "compare float series with an infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.greater_equal(:infinity) |> Series.to_list() ==
               [false, false, true, true, false]
    end

    test "compare float series with an negative infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.greater_equal(:neg_infinity) |> Series.to_list() ==
               [true, true, true, true, true]
    end

    test "compare integer series with a scalar value on the left-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert 2 |> Series.greater_equal(s1) |> Series.to_list() == [true, true, true, false]
    end

    test "compare float series with a float value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert 2.5 |> Series.greater_equal(s1) |> Series.to_list() ==
               [true, false, false, false, true]
    end

    test "compare float series with a nan value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert :nan |> Series.greater_equal(s1) |> Series.to_list() ==
               [true, true, true, true, true]
    end

    test "compare float series with an infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert :infinity |> Series.greater_equal(s1) |> Series.to_list() ==
               [true, true, false, true, true]
    end

    test "compare float series with an negative infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert :neg_infinity |> Series.greater_equal(s1) |> Series.to_list() ==
               [false, false, false, false, true]
    end
  end

  describe "less/2" do
    test "compare integer series" do
      s1 = Series.from_list([1, 0, 2])
      s2 = Series.from_list([1, 0, 3])

      assert s1 |> Series.less(s2) |> Series.to_list() == [false, false, true]
    end

    test "compare float series" do
      s1 = Series.from_list([1.0, 2.0, :nan, :infinity, :neg_infinity, :neg_infinity])
      s2 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity, :infinity])

      assert s1 |> Series.less(s2) |> Series.to_list() ==
               [false, true, false, false, false, true]
    end

    test "compare time series" do
      s1 = Series.from_list([~T[00:00:00.000000], ~T[12:00:00.000000], ~T[23:59:59.999999]])
      s2 = Series.from_list([~T[00:00:00.000000], ~T[12:30:00.000000], ~T[23:50:59.999999]])

      assert s1 |> Series.less(s2) |> Series.to_list() == [false, true, false]
    end

    test "compare integer series with a scalar value on the right-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert s1 |> Series.less(2) |> Series.to_list() == [true, true, false, false]
    end

    test "compare float series with a float value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.less(2.0) |> Series.to_list() ==
               [true, false, false, false, true]
    end

    test "compare float series with a nan value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.less(:nan) |> Series.to_list() ==
               [true, true, false, true, true]
    end

    test "compare float series with an infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.less(:infinity) |> Series.to_list() ==
               [true, true, false, false, true]
    end

    test "compare float series with an negative infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.less(:neg_infinity) |> Series.to_list() ==
               [false, false, false, false, false]
    end

    test "compare integer series with a scalar value on the left-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert 2 |> Series.less(s1) |> Series.to_list() == [false, false, false, true]
    end

    test "compare float series with a float value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert 2.5 |> Series.less(s1) |> Series.to_list() ==
               [false, true, true, true, false]
    end

    test "compare float series with a nan value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert :nan |> Series.less(s1) |> Series.to_list() ==
               [false, false, false, false, false]
    end

    test "compare float series with an infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert :infinity |> Series.less(s1) |> Series.to_list() ==
               [false, false, true, false, false]
    end

    test "compare float series with an negative infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert :neg_infinity |> Series.less(s1) |> Series.to_list() ==
               [true, true, true, true, false]
    end

    test "raises on value mismatch" do
      assert_raise ArgumentError,
                   "cannot invoke Explorer.Series.less/2 with mismatched dtypes: {:f, 64} and nil",
                   fn -> Series.less(Series.from_list([12.3]), nil) end

      assert_raise ArgumentError,
                   ~r"HINT: we have noticed that one of the values is the atom Foo",
                   fn -> Series.less(Series.from_list([123]), Foo) end
    end
  end

  describe "less_equal/2" do
    test "compare integer series" do
      s1 = Series.from_list([1, 0, 2])
      s2 = Series.from_list([1, 0, 3])

      assert s1 |> Series.less_equal(s2) |> Series.to_list() == [true, true, true]
    end

    test "compare float series" do
      s1 = Series.from_list([1.0, 2.0, :nan, :infinity, :neg_infinity, :neg_infinity])
      s2 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity, :infinity])

      assert s1 |> Series.less_equal(s2) |> Series.to_list() ==
               [true, true, true, true, true, true]
    end

    test "compare time series" do
      s1 = Series.from_list([~T[00:00:00.000000], ~T[12:00:00.000000], ~T[23:59:59.999999]])
      s2 = Series.from_list([~T[00:00:00.000000], ~T[12:30:00.000000], ~T[23:50:59.999999]])

      assert s1 |> Series.less_equal(s2) |> Series.to_list() == [true, true, false]
    end

    test "compare integer series with a scalar value on the right-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert s1 |> Series.less_equal(2) |> Series.to_list() == [true, true, true, false]
    end

    test "compare float series with a float value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.less_equal(2.0) |> Series.to_list() ==
               [true, false, false, false, true]
    end

    test "compare float series with a nan value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.less_equal(:nan) |> Series.to_list() ==
               [true, true, true, true, true]
    end

    test "compare float series with an infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.less_equal(:infinity) |> Series.to_list() ==
               [true, true, false, true, true]
    end

    test "compare float series with an negative infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.less_equal(:neg_infinity) |> Series.to_list() ==
               [false, false, false, false, true]
    end

    test "compare integer series with a scalar value on the left-hand side" do
      s1 = Series.from_list([1, 0, 2, 3])

      assert 2 |> Series.less_equal(s1) |> Series.to_list() == [false, false, true, true]
    end

    test "compare float series with a float value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert 2.5 |> Series.less_equal(s1) |> Series.to_list() ==
               [false, true, true, true, false]
    end

    test "compare float series with a nan value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert :nan |> Series.less_equal(s1) |> Series.to_list() ==
               [false, false, true, false, false]
    end

    test "compare float series with an infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert :infinity |> Series.less_equal(s1) |> Series.to_list() ==
               [false, false, true, true, false]
    end

    test "compare float series with an negative infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert :neg_infinity |> Series.less_equal(s1) |> Series.to_list() ==
               [true, true, true, true, true]
    end
  end

  describe "in/2" do
    test "with boolean series" do
      s1 = Series.from_list([true, false, true])
      s2 = Series.from_list([false, false, false, false])

      assert s1 |> Series.in(s2) |> Series.to_list() == [false, true, false]
    end

    test "with signed integer series" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([1, 0, 3])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "with signed integer series and nil on the left-hand side" do
      s1 = Series.from_list([1, 2, 3, nil])
      s2 = Series.from_list([1, 0, 3])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true, nil]
    end

    test "with signed integer series and nil on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([1, 0, 3, nil])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "with signed integer series and nil on both sides" do
      s1 = Series.from_list([1, 2, 3, nil])
      s2 = Series.from_list([1, 0, 3, nil])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true, nil]
    end

    test "with unsigned integer series" do
      s1 = Series.from_list([1, 2, 3], dtype: :u16)
      s2 = Series.from_list([1, 0, 3], dtype: :u32)

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "with unsigned and signed integer series" do
      s1 = Series.from_list([1, 2, 3], dtype: :s16)
      s2 = Series.from_list([1, 0, 3], dtype: :u32)

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true]
    end

    test "with float series" do
      s1 = Series.from_list([1.0, 2.0, :nan, :infinity, :neg_infinity])
      s2 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      assert s1 |> Series.in(s2) |> Series.to_list() == [true, false, true, true, true]
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

    test "with time series" do
      s1 =
        Explorer.Series.from_list([~T[00:00:00.000000], ~T[12:00:00.000000], ~T[23:59:59.999999]])

      s2 =
        Explorer.Series.from_list([~T[00:00:00.000000], ~T[12:30:00.000000], ~T[23:59:59.999999]])

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

    test "compare categories with strings" do
      s = Series.from_list(["a", "b", "c", "a"], dtype: :category)

      assert Series.in(s, ["a", "b"]) |> Series.to_list() ==
               [true, true, false, true]
    end

    test "compare categories with strings when left-hand side contains nil" do
      s = Series.from_list(["a", "b", "c", "a", nil], dtype: :category)

      assert Series.in(s, ["a", "b"]) |> Series.to_list() ==
               [true, true, false, true, nil]
    end

    test "compare categories with strings when right-hand side contains nil" do
      s = Series.from_list(["a", "b", "c", "a"], dtype: :category)

      assert Series.in(s, ["a", "b", nil]) |> Series.to_list() ==
               [true, true, false, true]
    end

    test "compare categories with strings when both sides contains nil" do
      s = Series.from_list(["a", "b", "c", "a", nil], dtype: :category)

      assert Series.in(s, ["a", "b", nil]) |> Series.to_list() ==
               [true, true, false, true, nil]
    end

    test "compare categories with strings when left-hand side contains nil and right-hand contains unknown element" do
      s = Series.from_list(["a", "b", "c", "a", nil], dtype: :category)

      assert Series.in(s, ["a", "z"]) |> Series.to_list() ==
               [true, false, false, true, nil]
    end

    test "compare categories with strings when left-hand side contains nil and right-hand contains unknown element and nil" do
      s = Series.from_list(["a", "b", "c", "a", nil], dtype: :category)

      assert Series.in(s, ["a", "z", nil]) |> Series.to_list() ==
               [true, false, false, true, nil]
    end

    test "compare categories with categories that are incompatible" do
      s = Series.from_list(["a", "b", "c", "a"], dtype: :category)
      s1 = Series.from_list(["a", "b", "z"]) |> Series.categorise(s)

      assert Series.in(s, s1) |> Series.to_list() ==
               [true, true, false, true]
    end

    test "compare categories with categories and nil on left-hand side" do
      s = Series.from_list(["a", nil, "c", "a"], dtype: :category)
      s1 = Series.from_list(["a", "c"]) |> Series.categorise(s)

      assert Series.in(s, s1) |> Series.to_list() ==
               [true, nil, true, true]
    end

    test "compare categories with categories and nil on right-hand side" do
      s = Series.from_list(["a", "b", "c", "a"], dtype: :category)
      s1 = Series.from_list(["a", "b", nil]) |> Series.categorise(s)

      assert Series.in(s, s1) |> Series.to_list() ==
               [true, true, false, true]
    end

    test "compare categories with categories and nil on both sides" do
      s = Series.from_list(["a", "b", "c", "a", nil], dtype: :category)
      s1 = Series.from_list(["a", "b", nil]) |> Series.categorise(s)

      assert Series.in(s, s1) |> Series.to_list() ==
               [true, true, false, true, nil]
    end

    test "compare categories with categories that are equivalent" do
      s = Series.from_list(["a", "b", "c", "a", nil], dtype: :category)
      s1 = Series.from_list(["a", "b", "c", nil]) |> Series.categorise(s)

      assert Series.in(s, s1) |> Series.to_list() ==
               [true, true, true, true, nil]
    end

    test "compare categories with categories that are different" do
      s = Series.from_list(["a", "b", "c", "a", nil], dtype: :category)
      s1 = Series.from_list(["z"], dtype: :category)

      assert_raise RuntimeError,
                   ~s/Generic Error: cannot compare categories from different sources. See Explorer.Series.categorise\/2/,
                   fn ->
                     Series.in(s, s1)
                   end
    end
  end

  describe "iotype/1" do
    test "integer series" do
      s = Series.from_list([1, 2, 3])
      assert Series.iotype(s) == {:s, 64}
    end

    test "float series" do
      s = Series.from_list([1.2, 2.3, 3.4, :nan, :infinity, :neg_infinity])
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

    test "time series" do
      s = Series.from_list([~T[00:00:00.000000], ~T[23:59:59.999999]])
      assert Series.iotype(s) == {:s, 64}
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

      assert s3.dtype == {:s, 64}
      assert Series.to_list(s3) == [5, 7, 9]
    end

    test "adding two float series together" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity, :infinity])
      s2 = Series.from_list([4.0, 4.5, :nan, :infinity, :neg_infinity, :neg_infinity])

      s3 = Series.add(s1, s2)

      assert s3.dtype == {:f, 64}
      assert Series.to_list(s3) == [5.0, 7.0, :nan, :infinity, :neg_infinity, :nan]
    end

    test "adding a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.add(s1, -2)

      assert s2.dtype == {:s, 64}
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
      assert s2.dtype == {:f, 64}

      assert Series.to_list(s2) == [2.1, 3.1, 4.1]
    end

    test "adding a series with a nan value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.add(s1, :nan)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:nan, :nan, :nan, :nan, :nan]
    end

    test "adding a series with a infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.add(s1, :infinity)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:infinity, :infinity, :nan, :infinity, :nan]
    end

    test "adding a series with a negative infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.add(s1, :neg_infinity)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:neg_infinity, :neg_infinity, :nan, :nan, :neg_infinity]
    end

    test "adding a series with a float scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.add(1.1, s1)
      assert s2.dtype == {:f, 64}

      assert Series.to_list(s2) == [2.1, 3.1, 4.1]
    end

    test "adding a series with a nan value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.add(:nan, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:nan, :nan, :nan, :nan, :nan]
    end

    test "adding a series with a infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.add(:infinity, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:infinity, :infinity, :nan, :infinity, :nan]
    end

    test "adding a series with a negative infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.add(:neg_infinity, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:neg_infinity, :neg_infinity, :nan, :nan, :neg_infinity]
    end

    test "adding two numbers" do
      assert_raise ArgumentError,
                   "add/2 expects a series as one of its arguments, instead got two scalars: 1 and 2",
                   fn ->
                     Series.add(1, 2)
                   end
    end

    test "adding two unsigned integer series of the same dtype together" do
      s1 = Series.from_list([1, 2, 3], dtype: :u32)
      s2 = Series.from_list([4, 5, 6], dtype: :u32)

      s3 = Series.add(s1, s2)

      assert s3.dtype == {:u, 32}
      assert Series.to_list(s3) == [5, 7, 9]
    end

    test "adding two unsigned integer series of different dtype together" do
      s1 = Series.from_list([1, 2, 3], dtype: :u16)
      s2 = Series.from_list([4, 5, 6], dtype: :u32)

      s3 = Series.add(s1, s2)

      assert s3.dtype == {:u, 32}
      assert Series.to_list(s3) == [5, 7, 9]
    end

    test "adding signed and unsigned integer series together" do
      s1 = Series.from_list([1, 2, 3], dtype: :s16)
      s2 = Series.from_list([4, 5, 6], dtype: :u32)

      s3 = Series.add(s1, s2)

      assert s3.dtype == {:s, 64}
      assert Series.to_list(s3) == [5, 7, 9]
    end
  end

  describe "subtract/2" do
    test "subtracting two series together" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([4, 5, 6])

      s3 = Series.subtract(s1, s2)

      assert s3.dtype == {:s, 64}
      assert Series.to_list(s3) == [-3, -3, -3]
    end

    test "subtracting two float series together" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity, :infinity, :neg_infinity])
      s2 = Series.from_list([4.0, 4.5, :nan, :infinity, :neg_infinity, :neg_infinity, :infinity])

      s3 = Series.subtract(s1, s2)

      assert s3.dtype == {:f, 64}
      assert Series.to_list(s3) == [-3.0, -2.0, :nan, :nan, :nan, :infinity, :neg_infinity]
    end

    test "subtracting a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.subtract(s1, -2)

      assert s2.dtype == {:s, 64}
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
      assert s2.dtype == {:f, 64}

      assert Series.to_list(s2) == [-0.5, 0.5, 1.5]
    end

    test "subtracting a series with a nan value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.subtract(s1, :nan)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:nan, :nan, :nan, :nan, :nan]
    end

    test "subtracting a series with a infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.subtract(s1, :infinity)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:neg_infinity, :neg_infinity, :nan, :nan, :neg_infinity]
    end

    test "subtracting a series with a negative infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.subtract(s1, :neg_infinity)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:infinity, :infinity, :nan, :infinity, :nan]
    end

    test "subtracting a series with a float scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.subtract(1.5, s1)
      assert s2.dtype == {:f, 64}

      assert Series.to_list(s2) == [0.5, -0.5, -1.5]
    end

    test "subtracting a series with a nan value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.subtract(:nan, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:nan, :nan, :nan, :nan, :nan]
    end

    test "subtracting a series with a infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.subtract(:infinity, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:infinity, :infinity, :nan, :nan, :infinity]
    end

    test "subtracting a series with a negative infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.subtract(:neg_infinity, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:neg_infinity, :neg_infinity, :nan, :neg_infinity, :nan]
    end

    test "subtracting two unsigned integer series of the same dtype together" do
      s1 = Series.from_list([1, 2, 3], dtype: :s32)
      s2 = Series.from_list([4, 5, 6], dtype: :u32)

      s3 = Series.subtract(s1, s2)

      assert s3.dtype == {:s, 64}
      assert Series.to_list(s3) == [-3, -3, -3]
    end

    test "subtracting two unsigned integer series of different dtype together" do
      s1 = Series.from_list([4, 5, nil, 6], dtype: :u32)
      s2 = Series.from_list([1, 2, nil, 3], dtype: :u16)

      s3 = Series.subtract(s1, s2)

      assert s3.dtype == {:u, 32}
      assert Series.to_list(s3) == [3, 3, nil, 3]
    end

    test "subtracting signed and unsigned integer series together" do
      s1 = Series.from_list([1, 2, 3], dtype: :s16)
      s2 = Series.from_list([4, 5, 6], dtype: :u32)

      s3 = Series.subtract(s1, s2)

      assert s3.dtype == {:s, 64}
      assert Series.to_list(s3) == [-3, -3, -3]
    end

    test "subtracting unsigned integer and float series" do
      s1 = Series.from_list([1, 2, 3], dtype: :u32)
      s2 = Series.from_list([4.2, 5.2, 6.5])

      s3 = Series.subtract(s1, s2)

      assert s3.dtype == {:f, 64}
      assert Series.to_list(s3) == [-3.2, -3.2, -3.5]
    end
  end

  describe "multiply/2" do
    test "multiplying two signed integer series together" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([4, 5, 6])

      s3 = Series.multiply(s1, s2)

      assert s3.dtype == {:s, 64}
      assert Series.to_list(s3) == [4, 10, 18]
    end

    test "multiplying two unsigned integer series together" do
      s1 = Series.from_list([1, 2, 3], dtype: :u16)
      s2 = Series.from_list([4, 5, 6], dtype: :u32)

      s3 = Series.multiply(s1, s2)

      assert s3.dtype == {:u, 32}
      assert Series.to_list(s3) == [4, 10, 18]
    end

    test "multiplying signed and unsigned integer series together" do
      s1 = Series.from_list([1, 2, 3], dtype: :s16)
      s2 = Series.from_list([4, 5, 6], dtype: :u32)

      s3 = Series.multiply(s1, s2)

      assert s3.dtype == {:s, 64}
      assert Series.to_list(s3) == [4, 10, 18]
    end

    test "multiplying two float series together" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity, :infinity])
      s2 = Series.from_list([4.0, 4.5, :nan, :infinity, :neg_infinity, :neg_infinity])

      s3 = Series.multiply(s1, s2)

      assert s3.dtype == {:f, 64}
      assert Series.to_list(s3) == [4.0, 11.25, :nan, :infinity, :infinity, :neg_infinity]
    end

    test "multiplying a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.multiply(s1, -2)

      assert s2.dtype == {:s, 64}
      assert Series.to_list(s2) == [-2, -4, -6]
    end

    test "multiplying a series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.multiply(-2, s1)

      assert s2.dtype == {:s, 64}
      assert Series.to_list(s2) == [-2, -4, -6]
    end

    test "multiplying a series with a float scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.multiply(s1, -2.5)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [-2.5, -5.0, -7.5]
    end

    test "multiplying a series with a nan value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.multiply(s1, :nan)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:nan, :nan, :nan, :nan, :nan]
    end

    test "multiplying a series with a infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.multiply(s1, :infinity)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:infinity, :infinity, :nan, :infinity, :neg_infinity]
    end

    test "multiplying a series with a negative infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.multiply(s1, :neg_infinity)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:neg_infinity, :neg_infinity, :nan, :neg_infinity, :infinity]
    end

    test "multiplying a series with a float scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.multiply(-2.5, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [-2.5, -5.0, -7.5]
    end

    test "multiplying a series with a nan value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.multiply(:nan, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:nan, :nan, :nan, :nan, :nan]
    end

    test "multiplying a series with a infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.multiply(:infinity, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:infinity, :infinity, :nan, :infinity, :neg_infinity]
    end

    test "multiplying a series with a negative infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.multiply(:neg_infinity, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:neg_infinity, :neg_infinity, :nan, :neg_infinity, :infinity]
    end
  end

  describe "divide/2" do
    test "dividing two series together" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([4, 5, 6])

      s3 = Series.divide(s2, s1)

      assert s3.dtype == {:f, 64}
      assert Series.to_list(s3) == [4.0, 2.5, 2.0]
    end

    test "dividing two float series together" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity, :infinity])
      s2 = Series.from_list([4.0, 4.5, :nan, :infinity, :neg_infinity, :neg_infinity])

      s3 = Series.divide(s1, s2)

      assert s3.dtype == {:f, 64}
      assert Series.to_list(s3) == [0.25, 0.5555555555555556, :nan, :nan, :nan, :nan]
    end

    test "dividing a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.divide(s1, -2)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [-0.5, -1, -1.5]
    end

    test "dividing a series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 5])

      s2 = Series.divide(-2, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [-2.0, -1.0, -0.4]
    end

    test "dividing a series with a float scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.divide(s1, -2.5)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [-0.4, -0.8, -1.2]
    end

    test "dividing a series with a nan value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.divide(s1, :nan)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:nan, :nan, :nan, :nan, :nan]
    end

    test "dividing a series with a infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.divide(s1, :infinity)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [0.0, 0.0, :nan, :nan, :nan]
    end

    test "dividing a series with a negative infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.divide(s1, :neg_infinity)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [-0.0, -0.0, :nan, :nan, :nan]
    end

    test "dividing a series with a float scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.divide(-3.12, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [-3.12, -1.56, -1.04]
    end

    test "dividing a series with a nan value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.divide(:nan, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:nan, :nan, :nan, :nan, :nan]
    end

    test "dividing a series with a infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.divide(:infinity, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:infinity, :infinity, :nan, :nan, :nan]
    end

    test "dividing a series with a negative infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.divide(:neg_infinity, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:neg_infinity, :neg_infinity, :nan, :nan, :nan]
    end

    test "dividing two unsigned integer series together" do
      s1 = Series.from_list([1, 2, 3], dtype: :u16)
      s2 = Series.from_list([4, 5, 6], dtype: :u32)

      s3 = Series.divide(s2, s1)

      assert s3.dtype == {:f, 64}
      assert Series.to_list(s3) == [4.0, 2.5, 2.0]
    end

    test "dividing unsigned integer by signed integer series together" do
      s1 = Series.from_list([1, 2, 3], dtype: :u16)
      s2 = Series.from_list([4, 5, 6], dtype: :s8)

      s3 = Series.divide(s2, s1)

      assert s3.dtype == {:f, 64}
      assert Series.to_list(s3) == [4.0, 2.5, 2.0]
    end

    test "dividing signed integer by unsigned integer series together" do
      s1 = Series.from_list([1, 2, 3], dtype: :s16)
      s2 = Series.from_list([4, 5, 6], dtype: :u8)

      s3 = Series.divide(s2, s1)

      assert s3.dtype == {:f, 64}
      assert Series.to_list(s3) == [4.0, 2.5, 2.0]
    end
  end

  describe "quotient/2" do
    test "quotient of two signed series" do
      s1 = Series.from_list([10, 11, 15])
      s2 = Series.from_list([2, 2, 2])

      s3 = Series.quotient(s1, s2)

      assert s3.dtype == {:s, 64}
      assert Series.to_list(s3) == [5, 5, 7]
    end

    test "quotient of two unsigned series" do
      s1 = Series.from_list([10, 11, 15], dtype: :u32)
      s2 = Series.from_list([2, 2, 2], dtype: :u8)

      s3 = Series.quotient(s1, s2)

      assert s3.dtype == {:u, 32}
      assert Series.to_list(s3) == [5, 5, 7]
    end

    test "quotient of signed by unsigned series" do
      s1 = Series.from_list([10, 11, 15], dtype: :s32)
      s2 = Series.from_list([2, 2, 2], dtype: :u8)

      s3 = Series.quotient(s1, s2)

      assert s3.dtype == {:s, 32}
      assert Series.to_list(s3) == [5, 5, 7]
    end

    test "quotient of a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([10, 11, 15])

      s2 = Series.quotient(s1, -2)

      assert s2.dtype == {:s, 64}
      assert Series.to_list(s2) == [-5, -5, -7]
    end

    test "quotient of a series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([10, 20, 25])

      s2 = Series.quotient(101, s1)

      assert s2.dtype == {:s, 64}
      assert Series.to_list(s2) == [10, 5, 4]
    end
  end

  describe "remainder/2" do
    test "remainder of two signed integer series" do
      s1 = Series.from_list([10, 11, 19])
      s2 = Series.from_list([2, 2, 2])

      s3 = Series.remainder(s1, s2)

      assert s3.dtype == {:s, 64}
      assert Series.to_list(s3) == [0, 1, 1]
    end

    test "remainder of two unsigned integer series" do
      s1 = Series.from_list([10, 11, 19], dtype: :u16)
      s2 = Series.from_list([2, 2, 2], dtype: :u32)

      s3 = Series.remainder(s1, s2)

      assert s3.dtype == {:u, 32}
      assert Series.to_list(s3) == [0, 1, 1]
    end

    test "remainder of signed and unsigned integer series" do
      s1 = Series.from_list([10, 11, 19], dtype: :s16)
      s2 = Series.from_list([2, 2, 2], dtype: :u8)

      s3 = Series.remainder(s1, s2)

      assert s3.dtype == {:s, 16}
      assert Series.to_list(s3) == [0, 1, 1]
    end

    test "remainder of a series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([10, 11, 15])

      s2 = Series.remainder(s1, -2)

      assert s2.dtype == {:s, 64}
      assert Series.to_list(s2) == [0, 1, 1]
    end

    test "remainder of a series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([10, 20, 25])

      s2 = Series.remainder(101, s1)

      assert s2.dtype == {:s, 64}
      assert Series.to_list(s2) == [1, 1, 1]
    end
  end

  describe "pow/2" do
    test "pow(uint, uint) == uint" do
      for u_base <- [8, 16, 32, 64], u_power <- [8, 16, 32, 64] do
        base = Series.from_list([1, 2, 3], dtype: {:u, u_base})
        power = Series.from_list([3, 2, 1], dtype: {:u, u_power})

        result = Series.pow(base, power)

        assert result.dtype == {:u, max(u_base, u_power)}
        assert Series.to_list(result) == [1, 4, 3]
      end
    end

    test "pow(uint, sint) == float64" do
      for u_base <- [8, 16, 32, 64], s_power <- [8, 16, 32, 64] do
        base = Series.from_list([1, 2, 3], dtype: {:u, u_base})
        power = Series.from_list([3, 2, 1], dtype: {:s, s_power})

        result = Series.pow(base, power)

        assert result.dtype == {:f, 64}
        assert Series.to_list(result) == [1, 4, 3]
      end
    end

    test "pow(sint, uint) == sint" do
      for s_base <- [8, 16, 32, 64], u_power <- [8, 16, 32, 64] do
        base = Series.from_list([1, 2, 3], dtype: {:s, s_base})
        power = Series.from_list([3, 2, 1], dtype: {:u, u_power})

        result = Series.pow(base, power)

        # Unsigned integers have twice the precision as signed integers.
        assert result.dtype == {:s, min(64, max(s_base, 2 * u_power))}
        assert Series.to_list(result) == [1, 4, 3]
      end
    end

    test "pow(sint, sint) == float64" do
      for s_base <- [8, 16, 32, 64], s_power <- [8, 16, 32, 64] do
        base = Series.from_list([1, 2, 3], dtype: {:s, s_base})
        power = Series.from_list([3, 2, 1], dtype: {:s, s_power})

        result = Series.pow(base, power)

        assert result.dtype == {:f, 64}
        assert Series.to_list(result) === [1.0, 4.0, 3.0]
      end
    end

    test "pow(float, uint_or_sint) = float" do
      for f_base <- [32, 64], d_power <- [:s, :u], n_power <- [8, 16, 32, 64] do
        base = Series.from_list([1, 2, 3], dtype: {:f, f_base})
        power = Series.from_list([3, 2, 1], dtype: {d_power, n_power})

        result = Series.pow(base, power)

        assert result.dtype == {:f, f_base}
        assert Series.to_list(result) === [1.0, 4.0, 3.0]
      end
    end

    test "pow(uint_or_sint, float) = float" do
      for d_base <- [:s, :u], n_base <- [8, 16, 32, 64], f_power <- [32, 64] do
        base = Series.from_list([1, 2, 3], dtype: {d_base, n_base})
        power = Series.from_list([3, 2, 1], dtype: {:f, f_power})

        result = Series.pow(base, power)

        assert result.dtype == {:f, f_power}
        assert Series.to_list(result) === [1.0, 4.0, 3.0]
      end
    end

    test "pow(float, float) = float" do
      for f_base <- [32, 64], f_power <- [32, 64] do
        base = Series.from_list([1, 2, 3], dtype: {:f, f_base})
        power = Series.from_list([3, 2, 1], dtype: {:f, f_power})

        result = Series.pow(base, power)

        assert result.dtype == {:f, max(f_base, f_power)}
        assert Series.to_list(result) === [1.0, 4.0, 3.0]
      end
    end

    test "pow of an integer series with an integer series that contains negative integer" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([1, -2, 3])

      result = Series.pow(s1, s2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) === [1.0, 0.25, 27.0]
    end

    test "pow of an integer series with a float series" do
      s1 = Series.from_list([1, 2, 3, 4, 5])
      s2 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(s1, s2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1.0, 11.313708498984761, :nan, :infinity, 0.0]
    end

    test "pow of an integer series with a float series that contains negative float" do
      s1 = Series.from_list([1, 2, 3, 4, 5])
      s2 = Series.from_list([1.0, -3.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(s1, s2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1.0, 0.08838834764831845, :nan, :infinity, 0.0]
    end

    test "pow of an integer series with an integer series that contains nil" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([3, nil, 1])

      result = Series.pow(s1, s2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1, nil, 3]
    end

    test "pow of an integer series that contains nil with an integer series" do
      s1 = Series.from_list([1, nil, 3])
      s2 = Series.from_list([3, 2, 1])

      result = Series.pow(s1, s2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1, nil, 3]
    end

    test "pow of an integer series that contains nil with an integer series also with nil" do
      s1 = Series.from_list([1, nil, 3])
      s2 = Series.from_list([3, nil, 1])

      result = Series.pow(s1, s2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1, nil, 3]
    end

    test "pow of an integer series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      result = Series.pow(s1, 2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1, 4, 9]
    end

    test "pow of an integer series with a negative integer scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      result = Series.pow(s1, -2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) === [1.0, 1 / 4, 1 / 9]
    end

    test "pow of an integer series with a float scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      result = Series.pow(s1, 2.0)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1.0, 4.0, 9.0]
    end

    test "pow of an integer series with a negative float scalar value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      result = Series.pow(s1, -2.0)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1.0, 0.25, 0.1111111111111111]
    end

    test "pow of an integer series with a nan value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.pow(s1, :nan)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [1, :nan, :nan]
    end

    test "pow of an integer series with an infinity value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.pow(s1, :infinity)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [1.0, :infinity, :infinity]
    end

    test "pow of an integer series with a negative infinity value on the right-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.pow(s1, :neg_infinity)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [1.0, 0.0, 0.0]
    end

    test "pow of an integer series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      result = Series.pow(2, s1)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) === [2.0, 4.0, 8.0]
    end

    test "pow of an integer series that contains negative integer with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([1, -2, 3])

      result = Series.pow(2, s1)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) === [2.0, 0.25, 8.0]
    end

    test "pow of an integer series with a negative integer scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      result = Series.pow(-2, s1)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [-2, 4, -8]
    end

    test "pow of an integer series with a float scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      result = Series.pow(2.0, s1)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [2.0, 4.0, 8.0]
    end

    test "pow of an integer series with a negative float scalar value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      result = Series.pow(-2.0, s1)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [-2.0, 4.0, -8.0]
    end

    test "pow of an integer series with a nan value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.pow(:nan, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:nan, :nan, :nan]
    end

    test "pow of an integer series with an infinity value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.pow(:infinity, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:infinity, :infinity, :infinity]
    end

    test "pow of an integer series with a negative infinity value on the left-hand side" do
      s1 = Series.from_list([1, 2, 3])

      s2 = Series.pow(:neg_infinity, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:neg_infinity, :infinity, :neg_infinity]
    end

    test "pow of a series with a series and different sizes" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([3, 2, 1, 4])

      assert_raise ArgumentError,
                   "series must either have the same size or one of them must have size of 1, got: 3 and 4",
                   fn -> Series.pow(s1, s2) end

      s1 = Series.from_list([1, 2, 3, 4])
      s2 = Series.from_list([3, 2, 1])

      assert_raise ArgumentError,
                   "series must either have the same size or one of them must have size of 1, got: 4 and 3",
                   fn -> Series.pow(s1, s2) end
    end

    test "pow of a float series with a float series" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])
      s2 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(s1, s2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1.0, 24.705294220065465, :nan, :infinity, 0.0]
    end

    test "pow of a float series with a float series that contains negative float" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])
      s2 = Series.from_list([1.0, -3.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(s1, s2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1.0, 0.040477154050155256, :nan, :infinity, 0.0]
    end

    test "pow of a float series with an integer series" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])
      s2 = Series.from_list([1, 2, 3, 4, 5])

      result = Series.pow(s1, s2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1.0, 12.25, :nan, :infinity, :neg_infinity]
    end

    test "pow of a float series with an integer series that contains negative integer" do
      s1 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity])
      s2 = Series.from_list([1, -2, 3, 4, 5])

      result = Series.pow(s1, s2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1.0, 0.08163265306122448, :nan, :infinity, :neg_infinity]
    end

    test "pow of a float series with a float series that contains nil" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity, 4.5])
      s2 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity, nil])

      result = Series.pow(s1, s2)

      assert Series.to_list(result) == [1.0, 24.705294220065465, :nan, :infinity, 0.0, nil]
    end

    test "pow of a float series that contains nil with a float series" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity, nil])
      s2 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity, 4.5])

      result = Series.pow(s1, s2)

      assert Series.to_list(result) == [1.0, 24.705294220065465, :nan, :infinity, 0.0, nil]
    end

    test "pow of a float series that contains nil with a float series also with nil" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity, nil])
      s2 = Series.from_list([1.0, 3.5, :nan, :infinity, :neg_infinity, nil])

      result = Series.pow(s1, s2)

      assert Series.to_list(result) == [1.0, 24.705294220065465, :nan, :infinity, 0.0, nil]
    end

    test "pow of a float series with an integer scalar value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(s1, 2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1.0, 6.25, :nan, :infinity, :infinity]
    end

    test "pow of a float series with a negative integer scalar value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(s1, -2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1.0, 0.16, :nan, 0.0, 0.0]
    end

    test "pow of a float series with a float scalar value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(s1, 2.0)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1.0, 6.25, :nan, :infinity, :infinity]
    end

    test "pow of a float series with a negative float scalar value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(s1, -2.0)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [1.0, 0.16, :nan, 0.0, 0.0]
    end

    test "pow of a float series with a nan value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.pow(s1, :nan)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [1, :nan, :nan, :nan, :nan]
    end

    test "pow of a float series with an infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.pow(s1, :infinity)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [1.0, :infinity, :nan, :infinity, :infinity]
    end

    test "pow of a float series with a negative infinity value on the right-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.pow(s1, :neg_infinity)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [1.0, 0.0, :nan, 0.0, 0.0]
    end

    test "pow of a float series with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(2, s1)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [2.0, 5.656854249492381, :nan, :infinity, 0.0]
    end

    test "pow of a float series that contains negative float with an integer scalar value on the left-hand side" do
      s1 = Series.from_list([1.0, -2.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(2, s1)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [2.0, 0.1767766952966369, :nan, :infinity, 0.0]
    end

    test "pow of a float series with a negative integer scalar value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(-2, s1)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [-2.0, :nan, :nan, :infinity, 0.0]
    end

    test "pow of a float series with a float scalar value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(2.0, s1)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [2.0, 5.656854249492381, :nan, :infinity, 0.0]
    end

    test "pow of a float series with a negative float scalar value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      result = Series.pow(-2.0, s1)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [-2.0, :nan, :nan, :infinity, 0.0]
    end

    test "pow of a float series with a nan value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.pow(:nan, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:nan, :nan, :nan, :nan, :nan]
    end

    test "pow of a float series with an infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.pow(:infinity, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:infinity, :infinity, :nan, :infinity, 0.0]
    end

    test "pow of a float series with a negative infinity value on the left-hand side" do
      s1 = Series.from_list([1.0, 2.5, :nan, :infinity, :neg_infinity])

      s2 = Series.pow(:neg_infinity, s1)

      assert s2.dtype == {:f, 64}
      assert Series.to_list(s2) == [:neg_infinity, :infinity, :nan, :infinity, 0.0]
    end

    test "pow of a float series with a float series and different sizes" do
      s1 = Series.from_list([1.5, 2.3, 3.7])
      s2 = Series.from_list([3.2, 2.5, 1.7, 4.4])

      assert_raise ArgumentError,
                   "series must either have the same size or one of them must have size of 1, got: 3 and 4",
                   fn -> Series.pow(s1, s2) end

      s1 = Series.from_list([1.5, 2.3, 3.7, 5.9])
      s2 = Series.from_list([3.2, 2.5, 1.7])

      assert_raise ArgumentError,
                   "series must either have the same size or one of them must have size of 1, got: 4 and 3",
                   fn -> Series.pow(s1, s2) end
    end
  end

  describe "log/1" do
    test "calculates the natural logarithm" do
      args = Series.from_list([1, 2, 3, nil, 4])

      result = Series.log(args)

      assert result.dtype == {:f, 64}

      assert Series.to_list(result) == [
               0.0,
               0.6931471805599453,
               1.0986122886681098,
               nil,
               1.3862943611198906
             ]
    end
  end

  describe "log/2" do
    test "log of an integer argument series with an integer base" do
      args = Series.from_list([1, 8, 16, nil, 32])

      result = Series.log(args, 2)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [0.0, 3.0, 4.0, nil, 5.0]
    end

    test "log of an integer argument series with float base" do
      args = Series.from_list([8, 16, 32])

      result = Series.log(args, 2.0)

      assert result.dtype == {:f, 64}
      assert Series.to_list(result) == [3.0, 4.0, 5.0]
    end

    test "log to the base of 0" do
      args = Series.from_list([1, 8, 16, nil, 32])

      assert_raise ArgumentError, "base must be a positive number", fn ->
        Series.log(args, 0)
      end
    end

    test "log to the base of 1" do
      args = Series.from_list([1, 8, 16, nil, 32])

      assert_raise ArgumentError, "base cannot be equal to 1", fn ->
        Series.log(args, 1)
      end
    end
  end

  describe "exp/1" do
    test "calculates the exponential of all elements in the series" do
      s = Series.from_list([1.0, 2.5])

      series = Series.exp(s)

      assert Series.to_list(series) == [2.718281828459045, 12.182493960703473]
    end
  end

  describe "abs/1" do
    test "calculates the absolute value of all elements in the series (float)" do
      s = Series.from_list([1.0, -2.0, 3.0])

      series = Series.abs(s)

      assert Series.to_list(series) == [1.0, 2.0, 3.0]
    end

    test "calculates the absolute value of all elements in the series (integer)" do
      s = Series.from_list([1, -2, 3])

      series = Series.abs(s)

      assert Series.to_list(series) == [1, 2, 3]
    end

    test "calculates the absolute value of all elements in the series with NaN" do
      s = Series.from_list([1, -2, :nan])

      series = Series.abs(s)

      assert Series.to_list(series) == [1, 2, :nan]
    end

    test "calculates the absolute value of all elements in the series with Infinity" do
      s = Series.from_list([1, -2, :infinity])

      series = Series.abs(s)

      assert Series.to_list(series) == [1, 2, :infinity]
    end
  end

  describe "sin/1" do
    test "calculates the sine of all elements in the series" do
      pi = :math.pi()
      s = Explorer.Series.from_list([0, pi / 2, pi, 2 * pi])

      series = Series.sin(s)

      assert Series.to_list(series) == [0.0, 1.0, 1.2246467991473532e-16, -2.4492935982947064e-16]
    end
  end

  describe "cos/1" do
    test "calculates the cosine of all elements in the series" do
      pi = :math.pi()
      s = Explorer.Series.from_list([0, pi / 2, pi, 2 * pi])

      series = Series.cos(s)

      assert Series.to_list(series) == [1.0, 6.123233995736766e-17, -1.0, 1.0]
    end
  end

  describe "tan/1" do
    test "calculates the tangent of all elements in the series" do
      pi = :math.pi()
      s = Explorer.Series.from_list([0, pi / 2, pi, 2 * pi])

      series = Series.tan(s)

      assert Series.to_list(series) == [
               0.0,
               1.633123935319537e16,
               -1.2246467991473532e-16,
               -2.4492935982947064e-16
             ]
    end
  end

  describe "asin/1" do
    test "calculates the arcsine of all elements in the series" do
      s = Explorer.Series.from_list([0.0, 1.0])

      series = Series.asin(s)

      assert Series.to_list(series) == [0.0, 1.5707963267948966]
    end
  end

  describe "acos/1" do
    test "calculates the arccosine of all elements in the series" do
      s = Explorer.Series.from_list([0.0, 1.0])

      series = Series.acos(s)

      assert Series.to_list(series) == [1.5707963267948966, 0.0]
    end
  end

  describe "atan/1" do
    test "calculates the arctangent of all elements in the series" do
      s = Explorer.Series.from_list([0.0, 1.0])

      series = Series.atan(s)

      assert Series.to_list(series) == [0.0, 0.7853981633974483]
    end
  end

  describe "format/1" do
    test "with two string series" do
      s1 = Series.from_list(["a", "b"])
      s2 = Series.from_list(["c", "d"])

      assert Series.format([s1, s2]) |> Series.to_list() == ["ac", "bd"]
    end

    test "with two strings with nulls" do
      assert Series.format(["a", nil, "b"]) |> Series.to_list() == ["ab"]
    end

    test "with two strings" do
      assert Series.format(["a", "b"]) |> Series.to_list() == ["ab"]
    end

    test "with a string series and a string value" do
      s1 = Series.from_list(["a", "b"])

      assert Series.format([s1, "c"]) |> Series.to_list() == ["ac", "bc"]
    end

    test "with a string value and a string series" do
      s1 = Series.from_list(["a", "b"])

      assert Series.format(["c", s1]) |> Series.to_list() == ["ca", "cb"]
    end

    test "with many string series with separator" do
      s1 = Series.from_list(["a", "b"])
      s2 = Series.from_list(["c", "d"])
      s3 = Series.from_list(["e", "f"])
      s4 = Series.from_list(["g", "h"])

      assert Series.format([s1, " / ", s2, " - ", s3, " / ", s4]) |> Series.to_list() ==
               ["a / c - e / g", "b / d - f / h"]
    end

    test "with two binary series" do
      s1 = Series.from_list([<<1>>, <<2>>], dtype: :binary)
      s2 = Series.from_list([<<3>>, <<4>>], dtype: :binary)

      assert Series.format([s1, s2]) |> Series.to_list() == ["\x01\x03", "\x02\x04"]
    end

    test "with two binaries" do
      assert Series.format([<<1>>, <<2>>]) |> Series.to_list() == ["\x01\x02"]
    end

    test "with a binary series and a binary value" do
      s1 = Series.from_list([<<1>>, <<2>>], dtype: :binary)

      assert Series.format([s1, <<3>>]) |> Series.to_list() == ["\x01\x03", "\x02\x03"]
    end

    test "with a binary value and a binary series" do
      s1 = Series.from_list([<<1>>, <<2>>], dtype: :binary)

      assert Series.format([<<3>>, s1]) |> Series.to_list() == ["\x03\x01", "\x03\x02"]
    end

    test "with many binary series with separator" do
      s1 = Series.from_list([<<1>>, <<2>>], dtype: :binary)
      s2 = Series.from_list([<<3>>, <<4>>], dtype: :binary)
      s3 = Series.from_list([<<5>>, <<6>>], dtype: :binary)
      s4 = Series.from_list([<<7>>, <<8>>], dtype: :binary)

      assert Series.format([s1, " / ", s2, " - ", s3, " / ", s4]) |> Series.to_list() ==
               ["\x01 / \x03 - \x05 / \a", "\x02 / \x04 - \x06 / \b"]
    end

    test "with two binary series but with one binary which is an invalid string" do
      s1 = Series.from_list([<<1>>, <<239, 191, 19>>], dtype: :binary)
      s2 = Series.from_list([<<3>>, <<4>>], dtype: :binary)

      assert_raise RuntimeError,
                   "Polars Error: invalid utf8",
                   fn -> Series.format([s1, s2]) end
    end

    test "with two integer series" do
      s1 = Series.from_list([1, 2])
      s2 = Series.from_list([3, 4])

      assert Series.format([s1, s2]) |> Series.to_list() == ["13", "24"]
    end

    test "with many integer series with separator" do
      s1 = Series.from_list([1, 2])
      s2 = Series.from_list([3, 4])
      s3 = Series.from_list([5, 6])
      s4 = Series.from_list([7, 8])

      assert Series.format([s1, " / ", s2, " - ", s3, " / ", s4]) |> Series.to_list() ==
               ["1 / 3 - 5 / 7", "2 / 4 - 6 / 8"]
    end

    test "with two float series" do
      s1 = Series.from_list([1.2, 2.6])
      s2 = Series.from_list([3.1, 4.9])

      assert Series.format([s1, s2]) |> Series.to_list() == ["1.23.1", "2.64.9"]
    end

    test "with many float series with separator" do
      s1 = Series.from_list([1.5, 2.7])
      s2 = Series.from_list([:nan, :infinity])
      s3 = Series.from_list([:neg_infinity, 3.2])
      s4 = Series.from_list([4.5, 5.3])

      assert Series.format([s1, " / ", s2, " - ", s3, " / ", s4]) |> Series.to_list() ==
               ["1.5 / NaN - -inf / 4.5", "2.7 / inf - 3.2 / 5.3"]
    end

    test "with two boolean series" do
      s1 = Series.from_list([true, false])
      s2 = Series.from_list([true, false])

      assert Series.format([s1, s2]) |> Series.to_list() == ["truetrue", "falsefalse"]
    end

    test "with many boolean series with separator" do
      s1 = Series.from_list([true, false])
      s2 = Series.from_list([true, false])
      s3 = Series.from_list([true, false])
      s4 = Series.from_list([true, false])

      assert Series.format([s1, " / ", s2, " - ", s3, " / ", s4]) |> Series.to_list() ==
               ["true / true - true / true", "false / false - false / false"]
    end

    test "with two date series" do
      s1 = Series.from_list([~D[2023-01-01], ~D[2023-01-02]])
      s2 = Series.from_list([~D[2023-01-03], ~D[2023-01-04]])

      assert Series.format([s1, s2]) |> Series.to_list() ==
               ["2023-01-012023-01-03", "2023-01-022023-01-04"]
    end

    test "with many date series with separator" do
      s1 = Series.from_list([~D[2023-01-01], ~D[2023-01-02]])
      s2 = Series.from_list([~D[2023-01-03], ~D[2023-01-04]])
      s3 = Series.from_list([~D[2023-01-05], ~D[2023-01-06]])
      s4 = Series.from_list([~D[2023-01-07], ~D[2023-01-08]])

      assert Series.format([s1, " / ", s2, " - ", s3, " / ", s4]) |> Series.to_list() == [
               "2023-01-01 / 2023-01-03 - 2023-01-05 / 2023-01-07",
               "2023-01-02 / 2023-01-04 - 2023-01-06 / 2023-01-08"
             ]
    end

    test "with two time series" do
      # Notice that Polars drops the microseconds part when converting
      # a Time series to String series.
      # See: https://github.com/pola-rs/polars/pull/8351
      s1 = Series.from_list([~T[01:00:00.000543], ~T[02:00:00.000000]])
      s2 = Series.from_list([~T[03:00:00.000000], ~T[04:00:00.000201]])

      assert Series.format([s1, " <=> ", s2]) |> Series.to_list() ==
               ["01:00:00 <=> 03:00:00", "02:00:00 <=> 04:00:00"]
    end

    test "with two datetime series" do
      s1 = Series.from_list([~N[2023-01-01 01:00:00.000000], ~N[2023-01-02 02:00:00.000000]])
      s2 = Series.from_list([~N[2023-01-03 03:00:00.000000], ~N[2023-01-04 04:00:00.000000]])

      assert Series.format([s1, s2]) |> Series.to_list() == [
               "2023-01-01 01:00:00.0000002023-01-03 03:00:00.000000",
               "2023-01-02 02:00:00.0000002023-01-04 04:00:00.000000"
             ]
    end

    test "with many datetime series with separator" do
      s1 = Series.from_list([~N[2023-01-01 01:00:00.000000], ~N[2023-01-02 02:00:00.000000]])
      s2 = Series.from_list([~N[2023-01-03 03:00:00.000000], ~N[2023-01-04 04:00:00.000000]])
      s3 = Series.from_list([~N[2023-01-05 01:00:00.000000], ~N[2023-01-06 02:00:00.000000]])
      s4 = Series.from_list([~N[2023-01-07 03:00:00.000000], ~N[2023-01-08 04:00:00.000000]])

      assert Series.format([s1, " / ", s2, " - ", s3, " / ", s4]) |> Series.to_list() == [
               "2023-01-01 01:00:00.000000 / 2023-01-03 03:00:00.000000 - 2023-01-05 01:00:00.000000 / 2023-01-07 03:00:00.000000",
               "2023-01-02 02:00:00.000000 / 2023-01-04 04:00:00.000000 - 2023-01-06 02:00:00.000000 / 2023-01-08 04:00:00.000000"
             ]
    end

    test "mixing types" do
      s1 = Series.from_list(["a", "b"])
      s2 = Series.from_list([1, 2])
      s3 = Series.from_list([1.5, :infinity])
      s4 = Series.from_list([true, false])
      s5 = Series.from_list([~D[2023-01-01], ~D[2023-01-02]])

      assert Series.format([s1, " / ", s2, " - ", s3, " / ", s4, " : ", s5]) |> Series.to_list() ==
               ["a / 1 - 1.5 / true : 2023-01-01", "b / 2 - inf / false : 2023-01-02"]
    end

    test "with series that have nil value" do
      s1 = Series.from_list(["a", "b", "c", "d"])
      s2 = Series.from_list(["e", "f", "g", "h"])
      s3 = Series.from_list(["i", "j", nil, "l"])
      s4 = Series.from_list(["m", "n", "o", "p"])

      assert Series.format([s1, " / ", s2, " - ", s3, " / ", s4]) |> Series.to_list() ==
               ["a / e - i / m", "b / f - j / n", "c / g -  / o", "d / h - l / p"]
    end
  end

  describe "filter/2" do
    test "basic example" do
      require Explorer.Series

      s = Series.from_list([1, 2, 3, 4])
      filtered = Series.filter(s, _ > 2)
      assert Series.to_list(filtered) == [3, 4]
    end

    test "aggregation" do
      require Explorer.Series

      s = Series.from_list([1, 2, 3, 4])
      filtered = Series.filter(s, _ == count(_))
      assert Series.to_list(filtered) == [4]
    end

    test "mismatched columns" do
      require Explorer.Series

      s = Series.from_list([1, 2, 3, 4])

      message =
        "could not find column name \"n\". The available columns are: [\"_\"].\nIf you are attempting to interpolate a value, use ^n.\n"

      assert_raise ArgumentError, message, fn ->
        Series.filter(s, n > 2)
      end
    end
  end

  describe "filter_with/2" do
    test "basic example" do
      s = Series.from_list([1, 2, 3, 4])
      filtered = Series.filter_with(s, &Series.greater(&1, 2))
      assert Series.to_list(filtered) == [3, 4]
    end

    test "raise an error if the function is not returning a lazy series" do
      s = Series.from_list([1, 2, 3, 4])

      message =
        "expecting the function to return a single or a list of boolean LazySeries, but instead it contains:\ntrue"

      assert_raise ArgumentError, message, fn ->
        Series.filter_with(s, &(&1 > 2))
      end
    end
  end

  describe "map/2" do
    test "basic example" do
      require Explorer.Series

      s = Series.from_list([1, 2, 3, 4])
      mapped = Series.map(s, _ * 2)
      assert Series.to_list(mapped) == [2, 4, 6, 8]
    end

    test "aggregation" do
      require Explorer.Series

      s = Series.from_list([1, 2, 3, 4])
      mapped = Series.map(s, _ - min(_))
      assert Series.to_list(mapped) == [0, 1, 2, 3]
    end

    test "mismatched columns" do
      require Explorer.Series

      s = Series.from_list([1, 2, 3, 4])

      message =
        "could not find column name \"n\". The available columns are: [\"_\"].\nIf you are attempting to interpolate a value, use ^n.\n"

      assert_raise ArgumentError, message, fn ->
        Series.map(s, n * 2)
      end
    end
  end

  describe "map_with/2" do
    test "basic example" do
      s = Series.from_list([1, 2, 3, 4])
      mapped = Series.map_with(s, &Series.multiply(&1, 2))
      assert Series.to_list(mapped) == [2, 4, 6, 8]
    end

    test "aggregation" do
      s = Series.from_list([1, 2, 3, 4])
      mapped = Series.map_with(s, &Series.subtract(&1, Series.min(&1)))
      assert Series.to_list(mapped) == [0, 1, 2, 3]
    end
  end

  describe "sort_by/2" do
    test "ascending order (default)" do
      require Explorer.Series

      s1 = Series.from_list([1, 2, 3])
      result = Series.sort_by(s1, remainder(_, 3))
      assert Series.to_list(result) == [3, 1, 2]
    end

    test "descending order" do
      require Explorer.Series

      s1 = Series.from_list([1, 2, 3])
      result = Series.sort_by(s1, remainder(_, 3), direction: :desc)
      assert Series.to_list(result) == [2, 1, 3]
    end

    test "nils last (default)" do
      require Explorer.Series

      s1 = Series.from_list([1, nil, 2, 3])
      result = Series.sort_by(s1, remainder(_, 3))
      assert Series.to_list(result) == [3, 1, 2, nil]
    end

    test "nils first" do
      require Explorer.Series

      s1 = Series.from_list([1, nil, 2, 3])
      result = Series.sort_by(s1, remainder(_, 3), nils: :first)
      assert Series.to_list(result) == [nil, 3, 1, 2]
    end
  end

  describe "sort_with/2" do
    test "ascending order (default)" do
      s1 = Series.from_list([1, 2, 3])
      result = Series.sort_with(s1, &Series.remainder(&1, 3))
      assert Series.to_list(result) == [3, 1, 2]
    end

    test "descending order" do
      s1 = Series.from_list([1, 2, 3])
      result = Series.sort_with(s1, &Series.remainder(&1, 3), direction: :desc)
      assert Series.to_list(result) == [2, 1, 3]
    end

    test "nils last (default)" do
      s1 = Series.from_list([1, nil, 2, 3])
      result = Series.sort_with(s1, &Series.remainder(&1, 3))
      assert Series.to_list(result) == [3, 1, 2, nil]
    end

    test "nils first" do
      s1 = Series.from_list([1, nil, 2, 3])
      result = Series.sort_with(s1, &Series.remainder(&1, 3), nils: :first)
      assert Series.to_list(result) == [nil, 3, 1, 2]
    end
  end

  describe "sample/2" do
    test "sample taking 10 elements" do
      s = 1..100 |> Enum.to_list() |> Series.from_list()
      result = Series.sample(s, 10, seed: 100)

      assert Series.size(result) == 10
      assert Series.to_list(result) == [57, 9, 54, 62, 50, 77, 35, 88, 1, 69]
    end

    test "sample taking 5% of elements" do
      s = 1..100 |> Enum.to_list() |> Series.from_list()

      result = Series.sample(s, 0.05, seed: 100)

      assert Series.size(result) == 5
      assert Series.to_list(result) == [9, 56, 79, 28, 54]
    end

    test "sample taking more than elements without replace" do
      s = 1..10 |> Enum.to_list() |> Series.from_list()

      assert_raise ArgumentError,
                   "in order to sample more elements than are in the series (10), sampling `replace` must be true",
                   fn ->
                     Series.sample(s, 15)
                   end
    end

    test "sample taking more than elements using fraction without replace" do
      s = 1..10 |> Enum.to_list() |> Series.from_list()

      assert_raise ArgumentError,
                   "in order to sample more elements than are in the series (10), sampling `replace` must be true",
                   fn ->
                     Series.sample(s, 1.2)
                   end
    end

    test "sample taking more than elements with replace" do
      s = 1..10 |> Enum.to_list() |> Series.from_list()

      result = Series.sample(s, 15, replace: true, seed: 100)

      assert Series.size(result) == 15
      assert Series.to_list(result) == [7, 1, 6, 7, 6, 8, 3, 6, 4, 9, 1, 7, 1, 1, 9]
    end

    test "sample taking more than elements with fraction and replace" do
      s = 1..10 |> Enum.to_list() |> Series.from_list()

      result = Series.sample(s, 1.2, replace: true, seed: 100)

      assert Series.size(result) == 12
      assert Series.to_list(result) == [7, 1, 6, 7, 6, 8, 3, 6, 4, 9, 1, 7]
    end

    test "sample with the exact amount of elements, but shuffle off" do
      s = 0..9 |> Enum.to_list() |> Series.from_list()

      result = Series.sample(s, 1.0, seed: 100, shuffle: false)

      assert Series.size(result) == 10
      assert Series.to_list(result) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    end

    test "sample with the exact amount of elements, but shuffle on" do
      s = 0..9 |> Enum.to_list() |> Series.from_list()

      result = Series.sample(s, 1.0, seed: 100, shuffle: true)

      assert Series.size(result) == 10
      assert Series.to_list(result) == [7, 9, 2, 0, 4, 1, 3, 8, 5, 6]
    end
  end

  describe "shuffle/2" do
    test "change the order of the elements randomly" do
      s = 0..9 |> Enum.to_list() |> Series.from_list()

      result = Series.shuffle(s, seed: 100)

      assert Series.size(result) == 10
      assert Series.to_list(result) == [7, 9, 2, 0, 4, 1, 3, 8, 5, 6]
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

    test "select broadcasts on predicate" do
      true_predicate = [true] |> Series.from_list()
      false_predicate = [false] |> Series.from_list()
      on_true = [1.1, 1.2, 1.3] |> Series.from_list()
      on_false = [5, 3, 2] |> Series.from_list()

      assert Series.select(true_predicate, on_true, on_false) |> Series.to_list() ==
               [1.1, 1.2, 1.3]

      assert Series.select(false_predicate, on_true, on_false) |> Series.to_list() == [5, 3, 2]
    end

    test "select errors if on_true or on_false is not same size as predicate" do
      predicate = Series.from_list([true, false, true, false])
      on_true = Series.from_list([1, 2, 3, 4])
      on_false = Series.from_list([5, 4, 3, 2, 1])

      assert_raise ArgumentError, fn ->
        Series.select(predicate, on_true, on_false)
      end
    end

    test "select allows if on_true or on_false is not same size as predicate, but one of them is of size 1" do
      predicate = Series.from_list([true, false, true, false])
      on_true = Series.from_list([1, 2, 3, 4])
      on_false = Series.from_list([0])

      assert Series.select(predicate, on_true, on_false) |> Series.to_list() == [1, 0, 3, 0]
    end

    test "select allows if on_true or on_false is not same size as predicate, but both of them are of size 1" do
      predicate = Series.from_list([true, false, true, false])
      on_true = Series.from_list([1])
      on_false = Series.from_list([0])

      assert Series.select(predicate, on_true, on_false) |> Series.to_list() == [1, 0, 1, 0]
    end

    test "select allows if on_true or on_false is a series or a scalar" do
      s = Series.from_list([1, 2, 3])
      s1 = Series.select(Series.less_equal(s, 2), -1, 1)
      assert Series.to_list(s1) == [-1, -1, 1]

      s2 = Series.select(Series.less_equal(s, 2), s, :infinity)
      assert Series.to_list(s2) == [1, 2, :infinity]

      s3 = Series.select(Series.less_equal(s, 2), -1, s)
      assert Series.to_list(s3) == [-1, -1, 3]

      s3 = Series.select(Series.less_equal(s, 2), %{"a" => true}, %{"a" => false})
      assert Series.to_list(s3) == [%{"a" => true}, %{"a" => true}, %{"a" => false}]
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

    test "sort a float series in ascending order" do
      s1 = Series.from_list([3.0, 1.0, :nan, nil, :infinity, :neg_infinity, 2.0])

      result = Series.sort(s1)

      assert Series.to_list(result) == [:neg_infinity, 1.0, 2.0, 3.0, :infinity, :nan, nil]
    end

    test "sort a float series in descending order" do
      s1 = Series.from_list([3.0, 1.0, :nan, nil, :infinity, :neg_infinity, 2.0])

      result = Series.sort(s1, direction: :desc)

      assert Series.to_list(result) == [nil, :nan, :infinity, 3.0, 2.0, 1.0, :neg_infinity]
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

    test "sort a float series in descending order, but with nils last" do
      s1 = Series.from_list([3.0, 1.0, :nan, nil, :infinity, :neg_infinity, 2.0])

      result = Series.sort(s1, direction: :desc, nils: :last)

      assert Series.to_list(result) == [:nan, :infinity, 3.0, 2.0, 1.0, :neg_infinity, nil]
    end

    test "sort a float series in ascending order, but nils first" do
      s1 = Series.from_list([3.0, 1.0, :nan, nil, :infinity, :neg_infinity, 2.0])

      result = Series.sort(s1, nils: :first)

      assert Series.to_list(result) == [nil, :neg_infinity, 1.0, 2.0, 3.0, :infinity, :nan]
    end
  end

  describe "argsort/2" do
    test "indices of sorting a series in ascending order" do
      s1 = Series.from_list([3, 1, nil, 2])

      result = Series.argsort(s1)
      assert Series.dtype(result) == {:u, 32}

      assert Series.to_list(result) == [1, 3, 0, 2]
    end

    test "indices of sorting a series in descending order" do
      s1 = Series.from_list([9, 4, nil, 5])

      result = Series.argsort(s1, direction: :desc, nils: :first)

      assert Series.to_list(result) == [2, 0, 3, 1]
    end

    test "indices of sorting a float series in ascending order" do
      s1 = Series.from_list([3.0, 1.0, :nan, nil, :infinity, :neg_infinity, 2.0])

      result = Series.argsort(s1)

      assert Series.to_list(result) == [5, 1, 6, 0, 4, 2, 3]
    end

    test "indices of sorting a float series in descending order" do
      s1 = Series.from_list([3.0, 1.0, :nan, nil, :infinity, :neg_infinity, 2.0])

      result = Series.argsort(s1, direction: :desc)

      assert Series.to_list(result) == [3, 2, 4, 0, 6, 1, 5]
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

    test "sort a float series in descending order, but with nils last" do
      s1 = Series.from_list([3.0, 1.0, :nan, nil, :infinity, :neg_infinity, 2.0])

      result = Series.argsort(s1, direction: :desc, nils: :last)

      assert Series.to_list(result) == [2, 4, 0, 6, 1, 5, 3]
    end

    test "sort a float series in ascending order, but nils first" do
      s1 = Series.from_list([3.0, 1.0, :nan, nil, :infinity, :neg_infinity, 2.0])

      result = Series.argsort(s1, nils: :first)

      assert Series.to_list(result) == [3, 5, 1, 6, 0, 4, 2]
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

  test "and/2 calculates element-wise and of two boolean series" do
    s1 = Series.from_list([true, false, false, nil, false])
    s2 = Series.from_list([true, true, false, true, true])
    result = Series.and(s1, s2)

    assert Series.to_list(result) == [true, false, false, nil, false]
  end

  test "or/2 calculates element-wise or of two boolean series" do
    s1 = Series.from_list([true, false, false, nil, false])
    s2 = Series.from_list([true, true, false, true, true])
    result = Series.or(s1, s2)

    assert Series.to_list(result) == [true, true, false, true, true]
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
    assert Series.to_list(categories) == ["a", "b", "c"]
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

    test "takes string series and categorise with categorical series" do
      categories = Series.from_list(["a", "b", "c"], dtype: :category)

      indexes = Series.from_list(["c", "b", "a", "a", "c"])
      categorized = Series.categorise(indexes, categories)

      assert Series.to_list(categorized) == ["c", "b", "a", "a", "c"]
      assert Series.dtype(categorized) == :category
    end

    test "takes string series containing nils and categorise with categorical series" do
      categories = Series.from_list(["a", "b", "c"], dtype: :category)

      indexes = Series.from_list(["c", "b", nil, "a", "a", "c"])
      categorized = Series.categorise(indexes, categories)

      assert Series.to_list(categorized) == ["c", "b", nil, "a", "a", "c"]
      assert Series.dtype(categorized) == :category
    end

    test "takes string series containing more elements and categorise with categorical series" do
      categories = Series.from_list(["a", "b", "c"], dtype: :category)

      indexes = Series.from_list(["z", "c", "b", "a", "a", "c", "w"])
      categorized = Series.categorise(indexes, categories)

      assert Series.to_list(categorized) == [nil, "c", "b", "a", "a", "c", nil]
      assert Series.dtype(categorized) == :category
    end

    test "takes string series and categorise with another string series" do
      categories = Series.from_list(["a", "b", "c"])

      indexes = Series.from_list(["c", "b", "a", "a", "c"])
      categorized = Series.categorise(indexes, categories)

      assert Series.to_list(categorized) == ["c", "b", "a", "a", "c"]
      assert Series.dtype(categorized) == :category
    end

    test "takes unsigned int series and categorise with categorical series" do
      categories = Series.from_list(["a", "b", "c"], dtype: :category)
      indexes = Series.from_list([0, 2, 1, 0, 2], dtype: :u32)
      categorized = Series.categorise(indexes, categories)

      assert Series.to_list(categorized) == ["a", "c", "b", "a", "c"]
      assert Series.dtype(categorized) == :category
    end

    test "raise for string list with nils" do
      categories = ["a", "b", "c", nil]
      indexes = Series.from_list([0, 2, 1, 0, 2], dtype: :u32)

      assert_raise ArgumentError,
                   ~r"categories as strings cannot have nil values",
                   fn -> Series.categorise(indexes, categories) end
    end

    test "raise for string list with duplicated" do
      categories = ["a", "b", "c", "c"]
      indexes = Series.from_list([0, 2, 1, 0, 2], dtype: :u32)

      assert_raise ArgumentError,
                   ~r"categories as strings cannot have duplicated values",
                   fn -> Series.categorise(indexes, categories) end
    end

    test "raise for string series with nils" do
      categories = Series.from_list(["a", "b", "c", nil], dtype: :string)
      indexes = Series.from_list([0, 2, 1, 0, 2], dtype: :u32)

      assert_raise ArgumentError,
                   ~r"categories as strings cannot have nil values",
                   fn -> Series.categorise(indexes, categories) end
    end

    test "raise for string series with duplicated" do
      categories = Series.from_list(["a", "b", "c", "c"], dtype: :string)
      indexes = Series.from_list([0, 2, 1, 0, 2], dtype: :u32)

      assert_raise ArgumentError,
                   ~r"categories as strings cannot have duplicated values",
                   fn -> Series.categorise(indexes, categories) end
    end
  end

  describe "cast/2" do
    test "integer series to null" do
      s = Series.from_list([1, 2, 3])
      s1 = Series.cast(s, :null)
      assert Series.to_list(s1) == [nil, nil, nil]
      assert Series.dtype(s1) == :null
    end

    test "string series to null" do
      s = Series.from_list(["a", "b", "c"])
      s1 = Series.cast(s, :null)
      assert Series.to_list(s1) == [nil, nil, nil]
      assert Series.dtype(s1) == :null
    end

    test "integer series to string" do
      s = Series.from_list([1, 2, 3])
      s1 = Series.cast(s, :string)

      assert Series.to_list(s1) == ["1", "2", "3"]
      assert Series.dtype(s1) == :string
    end

    test "integer series to float" do
      s = Series.from_list([1, 2, 3])
      s1 = Series.cast(s, {:f, 64})

      assert Series.to_list(s1) == [1.0, 2.0, 3.0]
      assert Series.dtype(s1) == {:f, 64}
    end

    test "integer series to date" do
      s = Series.from_list([1, 2, 3])
      s1 = Series.cast(s, :date)

      assert Series.to_list(s1) == [~D[1970-01-02], ~D[1970-01-03], ~D[1970-01-04]]
      assert Series.dtype(s1) == :date
    end

    test "integer series to time" do
      s = Series.from_list([1, 2, 3]) |> Series.multiply(1_000)
      s1 = Series.cast(s, :time)

      assert Series.to_list(s1) == [
               ~T[00:00:00.000001],
               ~T[00:00:00.000002],
               ~T[00:00:00.000003]
             ]

      assert Series.dtype(s1) == :time

      s2 = Series.from_list([86399 * 1_000 * 1_000 * 1_000])
      s3 = Series.cast(s2, :time)

      assert Series.to_list(s3) == [~T[23:59:59.000000]]
      assert Series.dtype(s3) == :time
    end

    test "integer series to datetime" do
      s = Series.from_list([1, 2, 3])
      s1 = Series.cast(s, {:datetime, :microsecond})

      assert Series.to_list(s1) == [
               ~N[1970-01-01 00:00:00.000001],
               ~N[1970-01-01 00:00:00.000002],
               ~N[1970-01-01 00:00:00.000003]
             ]

      assert Series.dtype(s1) == {:datetime, :microsecond}

      s2 = Series.from_list([1_649_883_642 * 1_000 * 1_000])
      s3 = Series.cast(s2, {:datetime, :microsecond})

      assert Series.to_list(s3) == [~N[2022-04-13 21:00:42.000000]]
      assert Series.dtype(s3) == {:datetime, :microsecond}
    end

    test "string series to category" do
      s = Series.from_list(["apple", "banana", "apple", "lemon"])
      s1 = Series.cast(s, :category)

      assert Series.to_list(s1) == ["apple", "banana", "apple", "lemon"]
      assert Series.dtype(s1) == :category
    end

    test "string series to datetime" do
      s = Series.from_list(["2023-08-29 17:39:43", "2023-08-29 17:20:09"])
      ms = Series.cast(s, {:datetime, :millisecond})
      us = Series.cast(s, {:datetime, :microsecond})
      ns = Series.cast(s, {:datetime, :nanosecond})

      assert Series.dtype(ms) == {:datetime, :millisecond}
      assert Series.dtype(us) == {:datetime, :microsecond}
      assert Series.dtype(ns) == {:datetime, :nanosecond}

      expected = [~N[2023-08-29 17:39:43.000000], ~N[2023-08-29 17:20:09.000000]]
      assert Series.to_list(ms) == expected
      assert Series.to_list(us) == expected
      assert Series.to_list(ns) == expected
    end

    test "no-op with the same dtype" do
      s = Series.from_list([1, 2, 3])
      s1 = Series.cast(s, :integer)

      assert s == s1
    end

    test "error when casting with unknown dtype" do
      assert_raise ArgumentError,
                   ~r"Explorer.Series.cast/2 not implemented for dtype :money",
                   fn -> Series.from_list([1, 2, 3]) |> Series.cast(:money) end
    end
  end

  describe "concat/1" do
    test "concat null" do
      sn = Series.from_list([nil, nil, nil])
      sn1 = Series.from_list([nil, nil])
      sr = Series.concat([sn, sn1])
      assert sn.dtype == :null
      assert sn1.dtype == :null
      assert Series.size(sr) == 5
      assert Series.to_list(sr) == [nil, nil, nil, nil, nil]
      assert Series.dtype(sr) == :null
    end

    test "concat series of multiple signed and unsigned" do
      sn = Series.from_list([nil])
      u8 = Series.from_list([8], dtype: {:s, 8})
      s8 = Series.from_list([9], dtype: {:u, 8})
      s16 = Series.from_list([16], dtype: {:s, 16})
      sr = Series.concat([sn, u8, s8, s16])
      assert sr.dtype == {:s, 16}
      assert Series.size(sr) == 4
      assert Series.to_list(sr) == [nil, 8, 9, 16]
    end

    test "concat series of multiple signed and unsigned 64" do
      sn = Series.from_list([nil])
      u8 = Series.from_list([8], dtype: {:s, 8})
      u64 = Series.from_list([1], dtype: {:u, 64})
      s16 = Series.from_list([16], dtype: {:s, 16})
      sr = Series.concat([sn, u8, u64, s16])
      assert sr.dtype == {:s, 64}
      assert Series.size(sr) == 4
      assert Series.to_list(sr) == [nil, 8, 1, 16]
    end

    test "concat null with {:s, 8}" do
      sn = Series.from_list([nil, nil, nil])
      s1 = Series.from_list([4, 5, 6], dtype: {:s, 8})

      sr = Series.concat([sn, s1])
      assert sn.dtype == :null
      assert s1.dtype == {:s, 8}
      assert Series.size(sr) == 6
      assert Series.to_list(sr) == [nil, nil, nil, 4, 5, 6]
      assert Series.dtype(sr) == {:s, 8}
    end

    test "concat integer series" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([4, 5, 6])

      s3 = Series.concat([s1, s2])

      assert Series.size(s3) == 6
      assert Series.to_list(s3) == [1, 2, 3, 4, 5, 6]
      assert Series.dtype(s3) == {:s, 64}
    end

    test "concat float series" do
      s1 = Series.from_list([1.0, 2.1, 3.2])
      s2 = Series.from_list([4.3, 5.4, 6.5])

      s3 = Series.concat([s1, s2])

      assert Series.size(s3) == 6
      assert Series.to_list(s3) == [1.0, 2.1, 3.2, 4.3, 5.4, 6.5]
      assert Series.dtype(s3) == {:f, 64}
    end

    test "concat integer and float series" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list([4.3, 5.4, 6.5])

      s3 = Series.concat([s1, s2])

      assert Series.size(s3) == 6
      assert Series.to_list(s3) == [1.0, 2.0, 3.0, 4.3, 5.4, 6.5]
      assert Series.dtype(s3) == {:f, 64}
    end

    test "concat incompatible dtypes" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list(["a", "b", "c"])

      error_message =
        "cannot concatenate series with mismatched dtypes: [{:s, 64}, :string]. " <>
          "First cast the series to the desired dtype."

      assert_raise ArgumentError, error_message, fn ->
        Series.concat([s1, s2])
      end
    end
  end

  describe "slice/2" do
    test "from a list of indices" do
      s = Series.from_list(["a", "b", "c"])
      s1 = Series.slice(s, [0, 2])
      assert Series.to_list(s1) == ["a", "c"]
    end

    test "from a range" do
      s = Series.from_list(["a", "b", "c"])
      s1 = Series.slice(s, 1..2)

      assert Series.to_list(s1) == ["b", "c"]
    end

    test "from a range with negative numbers" do
      s = Series.from_list(["a", "b", "c"])
      s1 = Series.slice(s, -2..-1//1)

      assert Series.to_list(s1) == ["b", "c"]
    end

    test "from a range that is out of bounds" do
      s = Series.from_list(["a", "b", "c"])
      s1 = Series.slice(s, 3..2//1)

      assert Series.to_list(s1) == []
    end

    test "from a series of indices" do
      s = Series.from_list(["a", "b", "c"])

      for dtype <- [:u8, :s16, :s64] do
        s1 = Series.slice(s, Series.from_list([0, 2], dtype: dtype))

        assert Series.dtype(s1) == s.dtype
        assert Series.to_list(s1) == ["a", "c"]
      end
    end

    test "from a series of strings" do
      s = Series.from_list(["a", "b", "c"])

      assert_raise ArgumentError,
                   "Explorer.Series.slice/2 not implemented for dtype :string. " <>
                     "Valid dtypes are {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}",
                   fn -> Series.slice(s, Series.from_list(["0", "2"])) end
    end

    test "from a series of indices with a negative number" do
      s = Series.from_list(["a", "b", "c"])

      assert_raise RuntimeError,
                   "Generic Error: slice/2 expects a series of positive integers",
                   fn ->
                     Series.slice(s, Series.from_list([0, 2, -1]))
                   end
    end

    test "from a series of indices out-of-bounds" do
      s = Series.from_list(["a", "b", "c"])

      assert_raise RuntimeError,
                   "Generic Error: slice/2 cannot select from indices that are out-of-bounds",
                   fn ->
                     Series.slice(s, Series.from_list([0, 2, 20]))
                   end
    end
  end

  describe "to_enum/1" do
    test "returns an enumerable" do
      enum1 =
        [1, 2, 3, 4]
        |> Series.from_list()
        |> Series.to_enum()

      enum2 =
        ["a", "b", "c"]
        |> Series.from_list()
        |> Series.to_enum()

      assert Enum.zip(enum1, enum2) == [{1, "a"}, {2, "b"}, {3, "c"}]

      assert Enum.reduce(enum1, 0, &+/2) == 10
      assert Enum.reduce(enum2, "", &<>/2) == "cba"

      assert Enum.count(enum1) == 4
      assert Enum.count(enum2) == 3

      assert Enum.slice(enum1, 1..2) == [2, 3]
      assert Enum.slice(enum2, 1..2) == ["b", "c"]
    end
  end

  describe "ewm_mean/2" do
    test "returns calculated ewm values with default options used for calculation" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_mean(s1)

      assert Series.to_list(s2) == [
               1.0,
               1.6666666666666667,
               2.4285714285714284,
               3.2666666666666666,
               4.161290322580645,
               5.095238095238095,
               6.05511811023622,
               7.031372549019608,
               8.017612524461839,
               9.009775171065494
             ]
    end

    test "returns calculated ewma with differernt smoothing factor if different alpha is passed" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_mean(s1, alpha: 0.8)

      assert Series.to_list(s2) == [
               1.0,
               1.8333333333333335,
               2.7741935483870965,
               3.7564102564102564,
               4.7516005121638925,
               5.750384024577572,
               6.750089601146894,
               7.750020480052428,
               8.75000460800236,
               9.750001024000106
             ]
    end

    test "returns calculated ewma with nils for index less than min period size, if min_periods is set" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_mean(s1, min_periods: 5)

      assert Series.to_list(s2) == [
               nil,
               nil,
               nil,
               nil,
               4.161290322580645,
               5.095238095238095,
               6.05511811023622,
               7.031372549019608,
               8.017612524461839,
               9.009775171065494
             ]
    end

    test "ignores nil by default and calculates ewma" do
      s1 = Series.from_list([1, nil, 2, nil, 3, 4, 5, 6, 7, 8])
      s2 = Series.ewm_mean(s1, ignore_nils: true)

      assert Series.to_list(s2) == [
               1.0,
               1.0,
               1.6666666666666667,
               1.6666666666666667,
               2.4285714285714284,
               3.2666666666666666,
               4.161290322580645,
               5.095238095238095,
               6.05511811023622,
               7.031372549019608
             ]
    end

    test "does not ignore nil if set ignore_nils option to false and calculates ewma" do
      s1 = Series.from_list([1, nil, 2, nil, 3, 4, 5, 6, 7, 8])
      s2 = Series.ewm_mean(s1, ignore_nils: false)

      assert Series.to_list(s2) == [
               1.0,
               1.0,
               1.8,
               1.8,
               2.7142857142857144,
               3.490566037735849,
               4.316239316239316,
               5.1959183673469385,
               6.1177644710578845,
               7.069101678183613
             ]
    end

    test "returns calculated ewma without adjustment if adjust option is set to false" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_mean(s1, adjust: false)

      assert Series.to_list(s2) == [
               1.0,
               1.5,
               2.25,
               3.125,
               4.0625,
               5.03125,
               6.015625,
               7.0078125,
               8.00390625,
               9.001953125
             ]
    end
  end

  describe "ewm_standard_deviation/2" do
    test "returns calculated ewm std values with default options used for calculation" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_standard_deviation(s1)

      assert Series.to_list(s2) == [
               0.0,
               0.7071067811865476,
               0.9636241116594314,
               1.1771636613972951,
               1.3452425132127066,
               1.4709162008918397,
               1.5607315639222439,
               1.6224598916602895,
               1.6634845490537977,
               1.689976601128564
             ]
    end

    test "returns calculated ewm std with different smoothing factor if different alpha is passed" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_standard_deviation(s1, alpha: 0.8)

      assert Series.to_list(s2) == [
               0.0,
               0.7071067811865476,
               0.8613567692141088,
               0.930593876392466,
               0.9563763729664396,
               0.9647929424175131,
               0.9672984330369606,
               0.9679969383076764,
               0.9681825776281606,
               0.9682301709724406
             ]
    end

    test "returns calculated ewm std with nils for index less than min period size, if min_periods is set" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_standard_deviation(s1, min_periods: 5)

      assert Series.to_list(s2) == [
               nil,
               nil,
               nil,
               nil,
               1.3452425132127066,
               1.4709162008918397,
               1.5607315639222439,
               1.6224598916602895,
               1.6634845490537977,
               1.689976601128564
             ]
    end

    test "ignores nil by default and calculates ewm std" do
      s1 = Series.from_list([1, nil, 2, nil, 3, 4, 5, 6, 7, 8])
      s2 = Series.ewm_standard_deviation(s1, ignore_nils: true)

      assert Series.to_list(s2) == [
               0.0,
               0.0,
               0.7071067811865476,
               0.7071067811865476,
               0.9636241116594314,
               1.1771636613972951,
               1.3452425132127066,
               1.4709162008918397,
               1.5607315639222439,
               1.6224598916602895
             ]
    end

    test "does not ignore nil if set ignore_nils option to false and calculates ewm std" do
      s1 = Series.from_list([1, nil, 2, nil, 3, 4, 5, 6, 7, 8])
      s2 = Series.ewm_standard_deviation(s1, ignore_nils: false)

      assert Series.to_list(s2) == [
               0.0,
               0.0,
               0.7071067811865476,
               0.7071067811865476,
               0.8864052604279183,
               0.9772545497599153,
               1.1470897308102692,
               1.3067888637766594,
               1.4363395171897309,
               1.5336045526865307
             ]
    end

    test "returns calculated ewm std without adjustment if adjust option is set to false" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_standard_deviation(s1, adjust: false)

      assert Series.to_list(s2) == [
               0.0,
               0.7071067811865476,
               1.0488088481701516,
               1.300183137283433,
               1.46929354773366,
               1.5764952405261994,
               1.641829587869702,
               1.6805652557493016,
               1.7030595977801866,
               1.7159083446458816
             ]
    end

    test "returns calculated ewm std with bias if bias option is set to true" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_standard_deviation(s1, bias: true)

      assert Series.to_list(s2) == [
               0.0,
               0.4714045207910317,
               0.7284313590846835,
               0.9285592184789413,
               1.0805247886738212,
               1.191428190780648,
               1.2693050154594225,
               1.3221328870469677,
               1.3568998042691014,
               1.3791855333404945
             ]
    end
  end

  describe "ewm_variance/2" do
    test "returns calculated ewm var values with default options used for calculation" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_variance(s1)

      assert Series.to_list(s2) == [
               0.0,
               0.5,
               0.9285714285714284,
               1.385714285714286,
               1.8096774193548393,
               2.163594470046083,
               2.435883014623173,
               2.632376100046318,
               2.7671808449407167,
               2.8560209123620535
             ]
    end

    test "returns calculated ewm var with different smoothing factor if different alpha is passed" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_variance(s1, alpha: 0.8)

      assert Series.to_list(s2) == [
               0.0,
               0.5,
               0.7419354838709674,
               0.8660049627791564,
               0.9146557667684424,
               0.9308254217386428,
               0.9356662585557595,
               0.9370180725730355,
               0.9373775036227093,
               0.9374696639813216
             ]
    end

    test "returns calculated ewm var with nils for index less than min period size, if min_periods is set" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_variance(s1, min_periods: 5)

      assert Series.to_list(s2) == [
               nil,
               nil,
               nil,
               nil,
               1.8096774193548393,
               2.163594470046083,
               2.435883014623173,
               2.632376100046318,
               2.7671808449407167,
               2.8560209123620535
             ]
    end

    test "ignores nil by default and calculates ewm var" do
      s1 = Series.from_list([1, nil, 2, nil, 3, 4, 5, 6, 7, 8])
      s2 = Series.ewm_variance(s1, ignore_nils: true)

      assert Series.to_list(s2) == [
               0.0,
               0.0,
               0.5,
               0.5,
               0.9285714285714284,
               1.385714285714286,
               1.8096774193548393,
               2.163594470046083,
               2.435883014623173,
               2.632376100046318
             ]
    end

    test "does not ignore nil if set ignore_nils option to false and calculates ewm var" do
      s1 = Series.from_list([1, nil, 2, nil, 3, 4, 5, 6, 7, 8])
      s2 = Series.ewm_variance(s1, ignore_nils: false)

      assert Series.to_list(s2) == [
               0.0,
               0.0,
               0.5,
               0.5,
               0.7857142857142857,
               0.9550264550264549,
               1.315814850530376,
               1.7076971344906926,
               2.0630712086408294,
               2.3519429240208543
             ]
    end

    test "returns calculated ewm var without adjustment if adjust option is set to false" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_variance(s1, adjust: false)

      assert Series.to_list(s2) == [
               0.0,
               0.5,
               1.1,
               1.6904761904761905,
               2.1588235294117646,
               2.4853372434017595,
               2.695604395604396,
               2.824299578831716,
               2.9004119935912107,
               2.9443414472253693
             ]
    end

    test "returns calculated ewm var with bias if bias option is set to true" do
      s1 = 1..10 |> Enum.to_list() |> Series.from_list()
      s2 = Series.ewm_variance(s1, bias: true)

      assert Series.to_list(s2) == [
               0.0,
               0.2222222222222222,
               0.5306122448979591,
               0.8622222222222223,
               1.167533818938606,
               1.4195011337868484,
               1.6111352222704451,
               1.7480353710111498,
               1.8411770788255257,
               1.9021527353757046
             ]
    end
  end

  describe "mean/1" do
    test "returns the mean of an integer series" do
      s = Series.from_list([1, 2, nil, 3])
      assert Series.mean(s) == 2.0
    end

    test "returns the mean of a float series" do
      s = Series.from_list([1.2, 2.4, nil, 3.9])
      assert Series.mean(s) == 2.5
    end

    test "returns the mean of a float series with an infinity number" do
      s = Series.from_list([1.2, 2.4, nil, 3.9, :infinity])
      assert Series.mean(s) == :infinity
    end

    test "returns the mean of a float series with an infinity number and a nan" do
      s = Series.from_list([1.2, 2.4, nil, 3.9, :infinity, :nan])
      assert Series.mean(s) == :nan
    end

    test "mean of unsigned integer series" do
      for dtype <- [:u8, :u16, :u32, :u64] do
        s = Series.from_list([1, 2, nil, 3], dtype: dtype)
        assert Series.mean(s) == 2.0
      end
    end
  end

  describe "median/1" do
    test "returns the median of an integer series" do
      s = Series.from_list([1, 2, nil, 3])
      assert Series.median(s) == 2.0
    end

    test "returns the median of a float series" do
      s = Series.from_list([1.2, 2.4, nil, 3.9])
      assert Series.median(s) == 2.4
    end

    test "returns the median of a float series with an infinity number" do
      s = Series.from_list([1.2, 2.4, nil, 3.9, :infinity])
      assert Series.median(s) == 3.15
    end

    test "returns the median of a float series with an infinity number and nan" do
      s = Series.from_list([1.2, 2.4, nil, 3.9, :infinity, :nan])
      assert Series.median(s) == 3.9
    end

    test "median of unsigned integer series" do
      for dtype <- [:u8, :u16, :u32, :u64] do
        s = Series.from_list([1, 2, nil, 3], dtype: dtype)
        assert Series.median(s) == 2.0
      end
    end
  end

  describe "mode/1" do
    test "returns the mode of an integer series" do
      s = Series.from_list([1, 2, 2, 3])
      mode = Series.mode(s)
      s2 = Series.from_list([2])
      assert Series.equal(mode, s2)
    end

    test "returns the mode of an integer series when it is multiple values" do
      s = Series.from_list([1, 2, 2, 3, 3])
      mode = s |> Series.mode() |> Series.sort()
      s2 = Series.from_list([2, 3])
      assert Series.equal(mode, s2)
    end

    test "returns the mode of a float series" do
      s = Series.from_list([1.0, 2.0, 2.0, 3.0])
      mode = Series.mode(s)
      s2 = Series.from_list([2.0])
      assert Series.equal(mode, s2)
    end

    test "returns the mode of a string series" do
      s = Series.from_list(["a", "b", "b", "c"])
      mode = Series.mode(s)
      s2 = Series.from_list(["b"])
      assert Series.equal(mode, s2)
    end

    test "returns the mode of a date series" do
      s =
        Series.from_list([
          ~D[2022-01-01],
          ~D[2022-01-02],
          ~D[2022-01-02],
          ~D[2022-01-03]
        ])

      mode = Series.mode(s)
      s2 = Series.from_list([~D[2022-01-02]])
      assert Series.equal(mode, s2)
    end

    test "returns the mode of a datetime series" do
      s =
        Series.from_list([
          ~N[2022-01-01 00:00:00],
          ~N[2022-01-01 00:01:00],
          ~N[2022-01-01 00:01:00]
        ])

      mode = Series.mode(s)
      s2 = Series.from_list([~N[2022-01-01 00:01:00]])
      assert Series.equal(mode, s2)
    end

    test "returns the mode of a boolean series" do
      s = Series.from_list([true, false, false, true])
      mode = Series.mode(s)
      s2 = Series.from_list([false])
      assert Series.equal(mode, s2)
    end

    test "returns the mode of a category series" do
      s = Series.from_list(["EUA", "Brazil", "Brazil", "Poland"], dtype: :category)
      mode = Series.mode(s)
      assert Series.to_list(mode) == ["Brazil"]
    end
  end

  describe "sum/1" do
    test "sum of integers" do
      s = Series.from_list([1, 2, nil, 3])
      assert Series.sum(s) === 6
    end

    test "sum of floats" do
      s = Series.from_list([1.0, 2.0, nil, 3.0])
      assert Series.sum(s) === 6.0
    end

    test "sum of floats with nan" do
      s = Series.from_list([1.0, 2.0, nil, :nan, 3.0])
      assert Series.sum(s) == :nan
    end

    test "sum of floats with infinity" do
      s = Series.from_list([1.0, 2.0, nil, :infinity, 3.0])
      assert Series.sum(s) == :infinity
    end

    test "sum of floats with infinity and nan" do
      s = Series.from_list([1.0, :nan, 2.0, nil, :infinity, 3.0])
      assert Series.sum(s) == :nan
    end

    test "sum of boolean values" do
      s = Series.from_list([true, false, true])
      assert Series.sum(s) === 2
    end

    test "sum of unsigned integers" do
      for dtype <- [:u8, :u16, :u32, :u64] do
        s = Series.from_list([1, 2, 3, 4], dtype: dtype)
        assert Series.sum(s) === 10
      end
    end
  end

  describe "product/1" do
    test "product of integers" do
      s = Series.from_list([1, 2, 3])
      assert Series.product(s) === 6
    end

    test "product of integers with nil" do
      s = Series.from_list([1, 2, nil, 3])
      assert Series.product(s) === 6
    end

    test "product of a series with a single value" do
      s = Series.from_list([5])
      assert Series.product(s) === 5
    end

    test "product of floats" do
      s = Series.from_list([1.0, 2.0, 3.0])
      assert Series.product(s) === 6.0
    end

    test "product of a series with negative integers" do
      s = Series.from_list([-2, 4, -3])
      assert Series.product(s) === 24
    end

    test "product of an empty series" do
      s = Series.from_list([], dtype: :integer)
      assert Series.product(s) === 1

      s = Series.from_list([], dtype: {:f, 64})
      assert Series.product(s) === 1.0
    end

    test "product of a series with zero" do
      s = Series.from_list([1, 0, 2])
      assert Series.product(s) === 0
    end

    test "product of a series with NaN" do
      s = Series.from_list([1.0, :nan, 2.0])
      assert Series.product(s) === :nan
    end

    test "product of a series with infinity" do
      s = Series.from_list([2.0, :infinity, 3.0])
      assert Series.product(s) === :infinity
    end

    test "product of unsigned integers" do
      for dtype <- [:u8, :u16, :u32, :u64] do
        s = Series.from_list([1, 2, 3], dtype: dtype)
        assert Series.product(s) === 6
      end
    end
  end

  describe "cumulative_product/1" do
    test "cumulative product of integers" do
      s = Series.from_list([1, 2, 3])
      p = Series.cumulative_product(s)
      assert Series.to_list(p) === [1, 2, 6]
    end

    test "cumulative product of integers with nil" do
      s = Series.from_list([1, 2, nil, 3])
      p = Series.cumulative_product(s)
      assert Series.to_list(p) === [1, 2, nil, 6]
    end

    test "cumulative product of a series with a single value" do
      s = Series.from_list([1])
      p = Series.cumulative_product(s)
      assert Series.to_list(p) === [1]
    end

    test "cumulative product of floats" do
      s = Series.from_list([1.0, 2.0, 3.0])
      p = Series.cumulative_product(s)
      assert Series.to_list(p) === [1.0, 2.0, 6.0]
    end

    test "cumulative product of a series with negative integers" do
      s = Series.from_list([-2, 4, -3])
      p = Series.cumulative_product(s)
      assert Series.to_list(p) === [-2, -8, 24]
    end

    test "cumulative product of an empty series" do
      s = Series.from_list([], dtype: :float)
      p = Series.cumulative_product(s)
      assert Series.to_list(p) === []
    end

    test "cumulative product of a series with zero" do
      s = Series.from_list([1, 2, 0, 3])
      p = Series.cumulative_product(s)
      assert Series.to_list(p) === [1, 2, 0, 0]
    end

    test "cumulative product of a series with NaN" do
      s = Series.from_list([1, 2, :nan, 3])
      p = Series.cumulative_product(s)
      assert Series.to_list(p) === [1.0, 2.0, :nan, :nan]
    end

    test "cumulative product of a series with infinity" do
      s = Series.from_list([1, 2, :infinity, 3])
      p = Series.cumulative_product(s)
      assert Series.to_list(p) === [1.0, 2.0, :infinity, :infinity]
    end
  end

  describe "min/1" do
    test "min of a signed integer series" do
      s = Series.from_list([-3, 1, 2, nil, -2, -42, 3])
      assert Series.min(s) === -42
    end

    test "min of an unsigned integer series" do
      s = Series.from_list([10, 15, 2, nil, 2, 42, 3], dtype: :u32)
      assert Series.min(s) === 2
    end

    test "min of a float series" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, 3.9])
      assert Series.min(s) === -12.6
    end

    test "min of a float series with a nan" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :nan, 3.9])
      assert Series.min(s) === -12.6
    end

    test "min of a float series with infinity positive" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :infinity, 3.9])
      assert Series.min(s) === -12.6
    end

    test "min of a float series with infinity negative" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :neg_infinity, 3.9])
      assert Series.min(s) === :neg_infinity
    end
  end

  describe "max/1" do
    test "max of a signed integer series" do
      s = Series.from_list([-3, 1, 2, nil, -2, -42, 3])
      assert Series.max(s) === 3
    end

    test "min of an unsigned integer series" do
      s = Series.from_list([10, 15, 2, nil, 2, 42, 3], dtype: :u32)
      assert Series.max(s) === 42
    end

    test "max of a float series" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, 3.9])
      assert Series.max(s) === 3.9
    end

    test "max of a float series with a nan" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :nan, 3.9])
      assert Series.max(s) === 3.9
    end

    test "max of a float series with infinity positive" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :infinity, 3.9])
      assert Series.max(s) === :infinity
    end

    test "max of a float series with infinity negative" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :neg_infinity, 3.9])
      assert Series.max(s) === 3.9
    end
  end

  describe "rank/2" do
    test "rank of a series of integers" do
      s = Series.from_list([1, 2, 0, 3])
      r = Series.rank(s)
      assert Series.to_list(r) === [2.0, 3.0, 1.0, 4.0]
    end

    test "rank of a series of floats" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, 3.9])
      r = Series.rank(s)
      assert Series.to_list(r) === [2.0, 4.0, 5.0, nil, 3.0, 1.0, 6.0]
    end

    test "rank of a series of dates" do
      s =
        Series.from_list([
          ~N[2022-07-07 17:44:13.020548],
          ~N[2022-07-07 17:43:08.473561],
          ~N[2022-07-07 17:45:00.116337]
        ])

      r = Series.rank(s)
      assert Series.to_list(r) === [2.0, 1.0, 3.0]
    end

    test "rank of a series of strings" do
      s = Series.from_list(~w[I love elixir])

      r = Series.rank(s)
      assert Series.to_list(r) === [1.0, 3.0, 2.0]
    end

    test "rank of a series of floats (method: ordinal)" do
      s = Series.from_list([3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1])
      r = Series.rank(s, method: :ordinal)
      assert Series.to_list(r) === [8, 2, 5, 3, 9, 10, 6, 7, 1, 4]
    end

    test "rank of a series of floats (method: min)" do
      s = Series.from_list([3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1])
      r = Series.rank(s, method: :min)
      assert Series.to_list(r) === [8, 2, 5, 3, 9, 10, 6, 6, 1, 3]
    end

    test "rank of a series of floats (method: max)" do
      s = Series.from_list([3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1])
      r = Series.rank(s, method: :max)
      assert Series.to_list(r) === [8, 2, 5, 4, 9, 10, 7, 7, 1, 4]
    end

    test "rank of a series of floats (method: dense)" do
      s = Series.from_list([3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1])
      r = Series.rank(s, method: :dense)
      assert Series.to_list(r) === [6, 2, 4, 3, 7, 8, 5, 5, 1, 3]
    end

    test "rank of a series of floats (method: random)" do
      s = Series.from_list([3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1])
      r = Series.rank(s, method: :random, seed: 4242)
      assert Series.to_list(r) === [8, 2, 5, 4, 9, 10, 7, 6, 1, 3]
    end

    test "rank of a float series with a nan" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :nan, 3.9])
      r = Series.rank(s)
      assert Series.to_list(r) === [2.0, 4.0, 5.0, nil, 3.0, 1.0, 7.0, 6.0]
    end

    test "rank of a float series with infinity positive" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :infinity, 3.9])
      r = Series.rank(s)
      assert Series.to_list(r) === [2.0, 4.0, 5.0, nil, 3.0, 1.0, 7.0, 6.0]
    end

    test "rank of a float series with infinity negative" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :neg_infinity, 3.9])
      r = Series.rank(s)
      assert Series.to_list(r) === [3.0, 5.0, 6.0, nil, 4.0, 2.0, 1.0, 7.0]
    end

    test "invalid rank method" do
      s = Series.from_list([3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1])

      assert_raise ArgumentError, ~s(unsupported rank method :not_a_method), fn ->
        Series.rank(s, method: :not_a_method, seed: 4242)
      end
    end
  end

  describe "skew/2" do
    test "returns the skew of an integer series" do
      s = Series.from_list([1, 2, 3, nil, 1])
      assert Series.skew(s) - 0.8545630383279711 < 1.0e-4
    end

    test "returns the skew of a float series" do
      s = Series.from_list([1.0, 2.0, 3.0, nil, 1.0])
      assert Series.skew(s, bias: true) - 0.49338220021815865 < 1.0e-4
    end

    test "returns the skew of an integer series (bias true)" do
      s = Series.from_list([1, 2, 3, 4, 5, 23])
      assert Series.skew(s, bias: true) - 1.6727687946848508 < 1.0e-4
    end

    test "returns the skew of an integer series (bias false)" do
      s = Series.from_list([1, 2, 3, 4, 5, 23])
      assert Series.skew(s, bias: false) - 2.2905330058490514 < 1.0e-4
    end

    test "skew of unsigned integer series" do
      for dtype <- [:u8, :u16, :u32, :u64] do
        s = Series.from_list([1, 2, 3, nil, 1], dtype: dtype)
        assert Series.skew(s) - 0.8545630383279711 < 1.0e-4
      end
    end
  end

  describe "clip/3" do
    test "with integers" do
      s1 = Series.from_list([-50, 5, nil, 50])
      clipped1 = Series.clip(s1, 1, 10)
      assert Series.to_list(clipped1) == [1, 5, nil, 10]
      assert clipped1.dtype == {:s, 64}
    end

    test "with unsigned integers" do
      s1 = Series.from_list([1, 5, nil, 50], dtype: :u16)
      clipped1 = Series.clip(s1, 3, 10)
      assert Series.to_list(clipped1) == [3, 5, nil, 10]
      assert clipped1.dtype == {:u, 16}
    end

    test "with regular floats" do
      s2 = Series.from_list([-50, 5, nil, 50])
      clipped2 = Series.clip(s2, 1.5, 10.5)
      assert Series.to_list(clipped2) == [1.5, 5.0, nil, 10.5]
      assert clipped2.dtype == {:f, 64}
    end

    test "with special floats" do
      s3 = Series.from_list([:neg_infinity, :nan, nil, :infinity])
      clipped3 = Series.clip(s3, 1.5, 10.5)
      assert Series.to_list(clipped3) == [1.5, :nan, nil, 10.5]
      assert clipped3.dtype == {:f, 64}
    end

    test "errors" do
      assert_raise ArgumentError,
                   ~r"expects both the min and max bounds to be numbers",
                   fn -> Series.clip(Series.from_list([1]), 1, "a") end

      assert_raise ArgumentError,
                   ~r"expects the max bound to be greater than the min bound",
                   fn -> Series.clip(Series.from_list([1]), 1, -1) end

      assert_raise ArgumentError,
                   ~r"expects both the min and max bounds to be numbers",
                   fn -> Series.clip(Series.from_list([1]), "a", 1) end

      assert_raise ArgumentError,
                   "Explorer.Series.clip/3 not implemented for dtype :string. " <>
                     "Valid dtypes are {:f, 32}, {:f, 64}, {:s, 8}, {:s, 16}, {:s, 32}, {:s, 64}, {:u, 8}, {:u, 16}, {:u, 32} and {:u, 64}",
                   fn -> Series.clip(Series.from_list(["a"]), 1, 10) end
    end
  end

  describe "correlation/2 and covariance/2" do
    test "correlation and covariance of different dtypes and edge cases" do
      for {values1, values2, exp_cov, exp_corr} <- [
            [
              [1, 8, 3],
              [4, 5, 2],
              3.0,
              0.5447047794019223
            ],
            [
              [1, 8, 3, nil],
              [4, 5, 2, nil],
              3.0,
              0.5447047794019223
            ],
            [
              [1, 8, 3, :nan],
              [4, 5, 2, :nan],
              3.0,
              0.5447047794019223
            ]
          ] do
        s1 = Series.from_list(values1)
        s2 = Series.from_list(values2)
        assert abs(Series.correlation(s1, s2) - exp_cov) < 1.0e-4
        assert abs(Series.covariance(s1, s2) - exp_corr) < 1.0e-4
      end
    end

    test "explicit pearson and spearman rank methods for correlation" do
      s1 = Series.from_list([1, 8, 3])
      s2 = Series.from_list([4, 5, 2])
      assert abs(Series.correlation(s1, s2, method: :spearman) - 0.5) < 1.0e-4
      assert abs(Series.correlation(s1, s2, method: :pearson) - 0.5447047794019223) < 1.0e-4

      assert_raise ArgumentError, ~s(unsupported correlation method :not_a_method), fn ->
        Series.correlation(s1, s2, method: :not_a_method)
      end
    end

    test "impossible correlation and covariance" do
      s1 = Series.from_list([], dtype: {:f, 64})
      s2 = Series.from_list([], dtype: {:f, 64})
      assert Series.correlation(s1, s2) == :nan
      assert Series.covariance(s1, s2) == -0.0

      s1 = Series.from_list([1.0])
      s2 = Series.from_list([2.0])
      assert Series.correlation(s1, s2) == :nan
      assert Series.covariance(s1, s2) == :nan

      s1 = Series.from_list([1.0, 2.0])
      s2 = Series.from_list([2.0, 3.0, 4.0])

      assert_raise ArgumentError,
                   ~r/series must either have the same size/,
                   fn -> Series.correlation(s1, s2) end

      assert_raise ArgumentError,
                   ~r/series must either have the same size/,
                   fn -> Series.covariance(s1, s2) end

      s1 = Series.from_list([1.0, 2.0])
      s2 = Series.from_list(["a", "b"])

      assert_raise ArgumentError,
                   "cannot invoke Explorer.Series.correlation/4 with mismatched dtypes: {:f, 64} and :string",
                   fn -> Series.correlation(s1, s2) end

      assert_raise ArgumentError,
                   "cannot invoke Explorer.Series.covariance/3 with mismatched dtypes: {:f, 64} and :string",
                   fn -> Series.covariance(s1, s2) end
    end
  end

  describe "variance/1" do
    test "variance of an integer series" do
      s = Series.from_list([1, 2, nil, 3])
      assert Series.variance(s) === 1.0
    end

    test "variance of a float series" do
      s = Series.from_list([1.0, 2.0, nil, 3.0])
      assert Series.variance(s) === 1.0
    end

    test "variance of a float series with a nan" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :nan, 3.9])
      assert Series.variance(s) == :nan
    end

    test "variance of a float series with infinity positive" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :infinity, 3.9])
      assert Series.variance(s) === :nan
    end

    test "variance of a float series with infinity negative" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :neg_infinity, 3.9])
      assert Series.variance(s) === :nan
    end

    test "variance of unsigned integers" do
      for dtype <- [:u8, :u16, :u32, :u64] do
        s = Series.from_list([1, 2, nil, 3], dtype: dtype)
        assert Series.variance(s) === 1.0
      end
    end
  end

  describe "standard_deviation/1" do
    test "standard deviation of an integer series" do
      s = Series.from_list([1, 2, nil, 3])
      assert Series.standard_deviation(s) === 1.0
    end

    test "standard deviation of a float series" do
      s = Series.from_list([1.0, 2.0, nil, 3.0])
      assert Series.standard_deviation(s) === 1.0
    end

    test "standard deviation of a float series with a nan" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :nan, 3.9])
      assert Series.standard_deviation(s) == :nan
    end

    test "standard deviation of a float series with infinity positive" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :infinity, 3.9])
      assert Series.standard_deviation(s) === :nan
    end

    test "standard deviation of a float series with infinity negative" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :neg_infinity, 3.9])
      assert Series.standard_deviation(s) === :nan
    end

    test "standard deviation of unsigned integer series" do
      for dtype <- [:u8, :u16, :u32, :u64] do
        s = Series.from_list([1, 2, 3, nil], dtype: dtype)
        assert Series.standard_deviation(s) === 1.0
      end
    end
  end

  describe "window_standard_deviation/2" do
    test "window standard deviation of an integer series" do
      s = Series.from_list([1, 2, nil, 3])
      ws = Series.window_standard_deviation(s, 2)
      assert Series.to_list(ws) === [0.0, 0.7071067811865476, 0.0, 0.0]
    end

    test "window standard deviation of a float series" do
      s = Series.from_list([1.0, 2.0, nil, 3.0])
      ws = Series.window_standard_deviation(s, 2)
      assert Series.to_list(ws) === [0.0, 0.7071067811865476, 0.0, 0.0]
    end

    test "window standard deviation of a float series with a nan" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :nan, 3.9])
      ws = Series.window_standard_deviation(s, 2)

      assert Series.to_list(ws) === [
               0.0,
               3.0405591591021546,
               0.7778174593052014,
               0.0,
               0.0,
               7.212489168102784,
               :nan,
               :nan
             ]
    end

    test "window standard deviation of a float series with infinity positive" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :infinity, 3.9])
      ws = Series.window_standard_deviation(s, 2)

      assert Series.to_list(ws) === [
               0.0,
               3.0405591591021546,
               0.7778174593052014,
               0.0,
               0.0,
               7.212489168102784,
               :nan,
               :nan
             ]
    end

    test "window standard deviation of a float series with infinity negative" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :neg_infinity, 3.9])
      ws = Series.window_standard_deviation(s, 2)

      assert Series.to_list(ws) === [
               0.0,
               3.0405591591021546,
               0.7778174593052014,
               0.0,
               0.0,
               7.212489168102784,
               :nan,
               :nan
             ]
    end
  end

  describe "quantile/1" do
    test "quantile of an integer series" do
      s = Series.from_list([1, 2, nil, 3])
      assert Series.quantile(s, 0.2) === 1
    end

    test "quantile of a float series" do
      s = Series.from_list([1.0, 2.0, nil, 3.0])
      assert Series.quantile(s, 0.2) === 1.0
    end

    test "quantile of a float series with a nan" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :nan, 3.9])

      assert Series.quantile(s, 0.2) == -3.1
      assert Series.quantile(s, 0.92) == :nan
    end

    test "quantile of a float series with infinity positive" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :infinity, 3.9])

      assert Series.quantile(s, 0.2) == -3.1
      assert Series.quantile(s, 0.92) == :infinity
    end

    test "quantile of a float series with infinity negative" do
      s = Series.from_list([-3.1, 1.2, 2.3, nil, -2.4, -12.6, :neg_infinity, 3.9])

      assert Series.quantile(s, 0.2) == -12.6
      assert Series.quantile(s, 0.08) == :neg_infinity
    end
  end

  describe "argmax/1 and argmin/1" do
    test "argmax and argmin for different dtypes" do
      for {list, exp_argmax, exp_argmin, exp_argmin_filled} <- [
            {[1, 2, 3, nil], 2, 0, 0},
            {[1.3, nil, 5.4, 2.6], 2, 0, 0},
            {[nil, ~D[2023-01-01], ~D[2022-01-01], ~D[2021-01-01]], 1, 3, 3},
            {[~N[2023-01-01 00:00:00], ~N[2022-01-01 00:00:00], ~N[2021-01-01 00:00:00], nil], 0,
             2, 2},
            {[~N[2023-01-01 10:00:00], ~N[2022-01-01 01:00:00], ~N[2021-01-01 00:10:00], nil], 0,
             2, 2},
            {[1.0, :infinity, :neg_infinity, nil], 1, 2, 2}
          ] do
        series = Series.from_list(list)

        assert Series.argmax(series) == exp_argmax
        assert Series.argmin(series) == exp_argmin
        assert Series.argmin(Series.fill_missing(series, :max)) == exp_argmin_filled
      end
    end
  end

  describe "replace/3" do
    test "replaces all occurences of pattern in string by replacement string" do
      series = Series.from_list(["1,200", "1,234,567", "asdf", nil])

      assert Series.replace(series, ",", "") |> Series.to_list() ==
               ["1200", "1234567", "asdf", nil]

      assert Series.replace(series, "[,]", "") |> Series.to_list() ==
               ["1,200", "1,234,567", "asdf", nil]
    end

    test "doesn't work with non string series" do
      series = Series.from_list([1200, 1_234_567, nil])

      assert_raise ArgumentError,
                   "Explorer.Series.replace/3 not implemented for dtype {:s, 64}. Valid dtype is :string",
                   fn -> Series.replace(series, ",", "") end
    end

    test "raises error if pattern is not string" do
      series = Series.from_list(["1,200", "1,234,567", "asdf", nil])

      assert_raise ArgumentError,
                   "pattern and replacement in replace/3 need to be a string",
                   fn -> Series.replace(series, 2, "") end
    end

    test "raises error if replacement is not string" do
      series = Series.from_list(["1,200", "1,234,567", "asdf", nil])

      assert_raise ArgumentError,
                   "pattern and replacement in replace/3 need to be a string",
                   fn -> Series.replace(series, ",", nil) end
    end
  end

  describe "strip, strip, lstrip, rstrip" do
    test "strip/1" do
      series = Series.from_list(["  123   ", "       2   ", "    20$    "])

      assert Series.strip(series) |> Series.to_list() == ["123", "2", "20$"]
    end

    test "strip/2" do
      series = Series.from_list(["£1£23", "2£", "£20£"])

      assert Series.strip(series, "£") |> Series.to_list() == ["1£23", "2", "20"]
    end

    test "lstrip/1" do
      series = Series.from_list(["  123   ", "       2   ", "    20$    "])

      assert Series.lstrip(series) |> Series.to_list() == ["123   ", "2   ", "20$    "]
    end

    test "lstrip/2" do
      series = Series.from_list(["£1£23", "2£", "£20£"])

      assert Series.lstrip(series, "£") |> Series.to_list() == ["1£23", "2£", "20£"]
    end

    test "rstrip/1" do
      series = Series.from_list(["  123   ", "  2   ", "    20$    "])

      assert Series.rstrip(series) |> Series.to_list() == ["  123", "  2", "    20$"]
    end

    test "rstrip/2" do
      series = Series.from_list(["£1£23", "2£", "£20£"])

      assert Series.rstrip(series, "£") |> Series.to_list() == ["£1£23", "2", "£20"]
    end
  end

  describe "string_slicing" do
    test "string_slice/2 positive offset" do
      series = Series.from_list(["earth", "mars", "neptune"])

      assert Series.substring(series, 2) |> Series.to_list() == ["rth", "rs", "ptune"]
      assert Series.substring(series, 20) |> Series.to_list() == ["", "", ""]
    end

    test "string_slice/2 negative offset" do
      series = Series.from_list(["earth", "mars", "neptune"])

      assert Series.substring(series, -3) |> Series.to_list() == ["rth", "ars", "une"]
      assert Series.substring(series, -9) |> Series.to_list() == ["earth", "mars", "neptune"]
    end

    test "string_slice/3 positive offset" do
      series = Series.from_list(["earth", "mars", "neptune"])

      assert Series.substring(series, 2, 3) |> Series.to_list() == ["rth", "rs", "ptu"]
      assert Series.substring(series, 12, 13) |> Series.to_list() == ["", "", ""]
    end

    test "string_slice/3 negative offset" do
      series = Series.from_list(["earth", "mars", "neptune"])

      assert Series.substring(series, -4, 4) |> Series.to_list() == ["arth", "mars", "tune"]
      assert Series.substring(series, -20, 4) |> Series.to_list() == ["eart", "mars", "nept"]
    end
  end

  describe "split" do
    test "split/2 exclusive" do
      series = Series.from_list(["1", "1|2"])

      assert series |> Series.split("|") |> Series.to_list() == [["1"], ["1", "2"]]
    end
  end

  describe "split_into" do
    test "split_into/3 exclusive" do
      series = Series.from_list(["Smith, John", "Jones, Jane"])
      split_series = series |> Series.split_into(", ", ["Last Name", "First Name"])

      assert Series.to_list(split_series) == [
               %{"First Name" => "John", "Last Name" => "Smith"},
               %{"First Name" => "Jane", "Last Name" => "Jones"}
             ]
    end
  end

  describe "strptime/2 and strftime/2" do
    test "parse datetime from string" do
      series = Series.from_list(["2023-01-05 12:34:56", "XYZ", nil])

      assert Series.strptime(series, "%Y-%m-%d %H:%M:%S") |> Series.to_list() ==
               [~N[2023-01-05 12:34:56.000000], nil, nil]
    end

    test "convert datetime to string" do
      series = Series.from_list([~N[2023-01-05 12:34:56], nil])

      assert Series.strftime(series, "%Y-%m-%d %H:%M:%S") |> Series.to_list() ==
               ["2023-01-05 12:34:56", nil]
    end

    test "ensure compatibility with chrono's format" do
      for {dt, dt_str, format_string} <- [
            {~N[2001-07-08 00:00:00.000000], "07/08/01", "%D"},
            {~N[2000-11-03 00:00:00.000000], "11/03/00 % \t \n", "%D %% %t %n"},
            {~N[1987-06-05 00:35:00.026000], "1987-06-05 00:35:00.026", "%F %X%.3f"},
            {~N[1999-03-01 00:00:00.000000], "1999/3/1", "%Y/%-m/%-d"}
          ] do
        series = Series.from_list([dt_str])
        assert Series.strptime(series, format_string) |> Series.to_list() == [dt]
        series = Series.from_list([dt])
        assert Series.strftime(series, format_string) |> Series.to_list() == [dt_str]
      end
    end
  end

  describe "categorisation functions" do
    test "cut/6 with no nils" do
      series = -30..30//5 |> Enum.map(&(&1 / 10)) |> Enum.to_list() |> Series.from_list()
      df = Series.cut(series, [-1, 1])
      freqs = Series.frequencies(df[:category])
      assert Series.to_list(freqs[:values]) == ["(-inf, -1]", "(-1, 1]", "(1, inf]"]
      assert Series.to_list(freqs[:counts]) == [5, 4, 4]
    end

    test "cut/6 with nils" do
      series = Series.from_list([1, 2, 3, nil, nil])
      df = Series.cut(series, [2])
      assert [_, _, _, nil, nil] = Series.to_list(df[:category])
    end

    test "cut/6 options" do
      series = Series.from_list([1, 2, 3])

      assert_raise ArgumentError,
                   "lengths don't match: labels count must equal bins count",
                   fn -> Series.cut(series, [2], labels: ["x"]) end

      df =
        Series.cut(series, [2],
          labels: ["x", "y"],
          break_point_label: "bp",
          category_label: "cat"
        )

      assert Explorer.DataFrame.names(df) == ["values", "bp", "cat"]
    end

    test "qcut/6" do
      series = Enum.to_list(-5..3) |> Series.from_list()
      df = Series.qcut(series, [0.0, 0.25, 0.75])
      freqs = Series.frequencies(df[:category])

      assert Series.to_list(freqs[:values]) == [
               "(-3, 1]",
               "(-5, -3]",
               "(1, inf]",
               "(-inf, -5]"
             ]

      assert Series.to_list(freqs[:counts]) == [4, 2, 2, 1]
    end
  end

  describe "join/2" do
    test "join/2" do
      series = Series.from_list([["1"], ["1", "2"]])

      assert series |> Series.join("|") |> Series.to_list() == ["1", "1|2"]
    end

    test "with nulls" do
      series = Series.from_list([["1"], ["1", nil, "2"]])

      assert series |> Series.join("|") |> Series.to_list() == ["1", "1|2"]
    end
  end

  describe "lengths/1" do
    test "calculates the length of each list in a series" do
      series = Series.from_list([[1], [1, 2, 3], [1, 2]])

      assert series |> Series.lengths() |> Series.to_list() == [1, 3, 2]
    end
  end

  describe "member?/2" do
    test "checks if any of the element lists contain the given value" do
      series = Series.from_list([[1], [1, 2, 3], [1, 2]])

      assert series |> Series.member?(1) |> Series.to_list() == [true, true, true]
      assert series |> Series.member?(2) |> Series.to_list() == [false, true, true]
      assert series |> Series.member?(3) |> Series.to_list() == [false, true, false]
    end

    test "works with floats" do
      series = Series.from_list([[1.0], [1.0, 2.0]])

      assert series |> Series.member?(2.0) |> Series.to_list() == [false, true]
      assert series |> Series.member?(2) |> Series.to_list() == [false, true]
    end

    test "works with booleans" do
      series = Series.from_list([[true], [true, false]])

      assert series |> Series.member?(false) |> Series.to_list() == [false, true]
    end

    test "works with strings" do
      series = Series.from_list([["a"], ["a", "b"]])

      assert series |> Series.member?("b") |> Series.to_list() == [false, true]
    end

    test "works with dates" do
      series = Series.from_list([[~D[2021-01-01]], [~D[2021-01-01], ~D[2021-01-02]]])

      assert series |> Series.member?(~D[2021-01-02]) |> Series.to_list() == [false, true]
    end

    test "works with times" do
      series = Series.from_list([[~T[00:00:00]], [~T[00:00:00], ~T[00:00:01]]])

      assert series |> Series.member?(~T[00:00:01]) |> Series.to_list() == [false, true]
    end

    test "works with datetimes" do
      series =
        Series.from_list([
          [~N[2021-01-01 00:00:00]],
          [~N[2021-01-01 00:00:00], ~N[2021-01-01 00:00:01]]
        ])

      assert series |> Series.member?(~N[2021-01-01 00:00:01]) |> Series.to_list() == [
               false,
               true
             ]
    end

    test "works with durations" do
      series = Series.from_list([[1], [1, 2]], dtype: {:list, {:duration, :millisecond}})
      duration = %Explorer.Duration{value: 2000, precision: :microsecond}

      assert series |> Series.member?(duration) |> Series.to_list() == [false, true]
    end
  end

  describe "to_iovec/1" do
    test "64-bit signed integer" do
      series = Series.from_list([-1, 0, 1], dtype: :s64)

      assert Series.to_iovec(series) == [
               <<-1::signed-64-native, 0::signed-64-native, 1::signed-64-native>>
             ]
    end

    test "32-bit signed integer" do
      series = Series.from_list([-25, 0, 12], dtype: :s32)

      assert Series.to_iovec(series) == [
               <<-25::signed-32-native, 0::signed-32-native, 12::signed-32-native>>
             ]
    end

    test "16-bit signed integer" do
      series = Series.from_list([-73, 0, 19], dtype: :s16)

      assert Series.to_iovec(series) == [
               <<-73::signed-16-native, 0::signed-16-native, 19::signed-16-native>>
             ]
    end

    test "8-bit signed integer" do
      series = Series.from_list([-3, 0, 63], dtype: :s8)

      assert Series.to_iovec(series) == [
               <<-3::signed-8-native, 0::signed-8-native, 63::signed-8-native>>
             ]
    end

    test "64-bit unsigned integer" do
      series = Series.from_list([1_249_123, 0, 1], dtype: :u64)

      assert Series.to_iovec(series) == [
               <<1_249_123::unsigned-64-native, 0::unsigned-64-native, 1::unsigned-64-native>>
             ]
    end

    test "32-bit unsigned integer" do
      series = Series.from_list([25, 0, 12], dtype: :u32)

      assert Series.to_iovec(series) == [
               <<25::unsigned-32-native, 0::unsigned-32-native, 12::unsigned-32-native>>
             ]
    end

    test "16-bit unsigned integer" do
      series = Series.from_list([73, 0, 19], dtype: :u16)

      assert Series.to_iovec(series) == [
               <<73::unsigned-16-native, 0::unsigned-16-native, 19::unsigned-16-native>>
             ]
    end

    test "8-bit unsigned integer" do
      series = Series.from_list([3, 0, 63], dtype: :u8)

      assert Series.to_iovec(series) == [
               <<3::unsigned-8-native, 0::unsigned-8-native, 63::unsigned-8-native>>
             ]
    end

    test "float 64" do
      series = Series.from_list([1.0, 2.0, 3.0])

      assert Series.to_iovec(series) == [
               <<1.0::float-64-native, 2.0::float-64-native, 3.0::float-64-native>>
             ]
    end

    test "float 32" do
      series = Series.from_list([1.0, 2.0, 3.0], dtype: {:f, 32})

      assert Series.to_iovec(series) == [
               <<1.0::float-32-native, 2.0::float-32-native, 3.0::float-32-native>>
             ]
    end

    test "boolean" do
      series = Series.from_list([true, false, true])
      assert Series.to_iovec(series) == [<<1, 0, 1>>]
    end

    test "date" do
      series = Series.from_list([~D[0001-01-01], ~D[1970-01-01], ~D[1986-10-13]])

      assert Series.to_iovec(series) == [
               <<-719_162::signed-32-native, 0::signed-32-native, 6129::signed-32-native>>
             ]
    end

    test "time" do
      series = Series.from_list([~T[00:00:00.000000], ~T[23:59:59.999999]])

      assert Series.to_iovec(series) == [
               <<0::signed-64-native, 86_399_999_999_000::signed-64-native>>
             ]
    end

    test "datetime" do
      series =
        Series.from_list([
          ~N[0001-01-01 00:00:00],
          ~N[1970-01-01 00:00:00],
          ~N[1986-10-13 01:23:45.987654]
        ])

      assert Series.to_iovec(series) ==
               [
                 <<-62_135_596_800_000_000::signed-64-native, 0::signed-64-native,
                   529_550_625_987_654::signed-64-native>>
               ]
    end

    test "category" do
      series = Series.from_list(["a", "b", "c", "b"], dtype: :category)

      assert Series.to_iovec(series) ==
               [
                 <<0::unsigned-32-native, 1::unsigned-32-native, 2::unsigned-32-native,
                   1::unsigned-32-native>>
               ]
    end

    test "string" do
      series = Explorer.Series.from_list(["a", "b", "c", "b"])

      assert_raise ArgumentError, "cannot convert series of dtype :string into iovec", fn ->
        Series.to_iovec(series)
      end
    end

    test "binary" do
      series = Explorer.Series.from_list(["a", "b", "c", "b"], dtype: :binary)

      assert_raise ArgumentError, "cannot convert series of dtype :binary into iovec", fn ->
        Series.to_iovec(series)
      end
    end

    test "list" do
      series = Series.from_list([[-1], [0, 1]])

      assert_raise ArgumentError,
                   "cannot convert series of dtype {:list, {:s, 64}} into iovec",
                   fn -> Series.to_iovec(series) end
    end

    test "struct" do
      series = Series.from_list([%{a: 1}, %{a: 2}])

      assert_raise ArgumentError,
                   ~S'cannot convert series of dtype {:struct, [{"a", {:s, 64}}]} into iovec',
                   fn -> Series.to_iovec(series) end
    end
  end

  describe "from_binary/2" do
    test "64-bit signed integer" do
      series =
        Series.from_binary(
          <<-1::signed-64-native, 0::signed-64-native, 1::signed-64-native>>,
          :integer
        )

      assert series.dtype == {:s, 64}
      assert Series.to_list(series) == [-1, 0, 1]
    end

    test "32-bit signed integer" do
      series =
        Series.from_binary(
          <<-1::signed-32-native, 0::signed-32-native, 1::signed-32-native>>,
          :s32
        )

      assert series.dtype == {:s, 32}
      assert Series.to_list(series) == [-1, 0, 1]
    end

    test "16-bit signed integer" do
      series =
        Series.from_binary(
          <<-14::signed-16-native, 0::signed-16-native, 12::signed-16-native>>,
          :s16
        )

      assert series.dtype == {:s, 16}
      assert Series.to_list(series) == [-14, 0, 12]
    end

    test "8-bit signed integer" do
      series =
        Series.from_binary(
          <<-2::signed-8-native, 0::signed-8-native, 3::signed-8-native>>,
          :s8
        )

      assert series.dtype == {:s, 8}
      assert Series.to_list(series) == [-2, 0, 3]
    end

    test "64-bit unsigned integer" do
      series =
        Series.from_binary(
          <<3::unsigned-64-native, 0::unsigned-64-native, 1::unsigned-64-native>>,
          :u64
        )

      assert series.dtype == {:u, 64}
      assert Series.to_list(series) == [3, 0, 1]
    end

    test "32-bit unsigned integer" do
      series =
        Series.from_binary(
          <<1_234_567::unsigned-32-native, 0::unsigned-32-native, 1::unsigned-32-native>>,
          :u32
        )

      assert series.dtype == {:u, 32}
      assert Series.to_list(series) == [1_234_567, 0, 1]
    end

    test "16-bit unsigned integer" do
      series =
        Series.from_binary(
          <<14::unsigned-16-native, 0::unsigned-16-native, 12::unsigned-16-native>>,
          :u16
        )

      assert series.dtype == {:u, 16}
      assert Series.to_list(series) == [14, 0, 12]
    end

    test "8-bit unsigned integer" do
      series =
        Series.from_binary(
          <<255::unsigned-8-native, 0::unsigned-8-native, 3::unsigned-8-native>>,
          :u8
        )

      assert series.dtype == {:u, 8}
      assert Series.to_list(series) == [255, 0, 3]
    end

    test "float 64" do
      series =
        Series.from_binary(
          <<1.0::float-64-native, 2.0::float-64-native, 3.0::float-64-native>>,
          {:f, 64}
        )

      assert series.dtype == {:f, 64}
      assert Series.to_list(series) == [1.0, 2.0, 3.0]
    end

    test "float 32" do
      series =
        Series.from_binary(
          <<1.0::float-32-native, 2.0::float-32-native, 3.0::float-32-native>>,
          {:f, 32}
        )

      assert series.dtype == {:f, 32}
      assert Series.to_list(series) == [1.0, 2.0, 3.0]
    end

    test "boolean" do
      series = Series.from_binary(<<1, 0, 1>>, :boolean)
      assert series.dtype == :boolean
      assert Series.to_list(series) == [true, false, true]
    end

    test "date" do
      series =
        Series.from_binary(
          <<-719_162::signed-32-native, 0::signed-32-native, 6129::signed-32-native>>,
          :date
        )

      assert series.dtype == :date
      assert Series.to_list(series) == [~D[0001-01-01], ~D[1970-01-01], ~D[1986-10-13]]
    end

    test "time" do
      series =
        Series.from_binary(<<0::signed-64-native, 86_399_999_999_000::signed-64-native>>, :time)

      assert series.dtype == :time

      assert Series.to_list(series) ==
               [~T[00:00:00.000000], ~T[23:59:59.999999]]
    end

    @tag :skip
    test "datetime" do
      series =
        Series.from_binary(
          <<-62_135_596_800_000_000::signed-64-native, 0::signed-64-native,
            529_550_625_987_654::signed-64-native>>,
          {:datetime, :microsecond}
        )

      # There is a precision problem here. Investigate.
      assert Series.to_list(series) == [
               ~N[0001-01-01 00:00:00],
               ~N[1970-01-01 00:00:00],
               ~N[1986-10-13 01:23:45.987654]
             ]
    end
  end

  describe "frequencies/1" do
    test "integer" do
      s = Series.from_list([1, 2, 3, 1, 3, 4, 1, 5, 6, 1, 2])

      df = Series.frequencies(s)

      assert Series.dtype(df[:values]) == {:s, 64}
      assert Series.dtype(df[:counts]) == {:u, 32}

      assert Explorer.DataFrame.to_columns(df, atom_keys: true) == %{
               values: [1, 2, 3, 4, 5, 6],
               counts: [4, 2, 2, 1, 1, 1]
             }
    end

    test "string" do
      s = Series.from_list(["a", "a", "b", "c", "c", "c"])

      df = Series.frequencies(s)

      assert Series.dtype(df[:values]) == :string
      assert Series.dtype(df[:counts]) == {:u, 32}

      assert Explorer.DataFrame.to_columns(df, atom_keys: true) == %{
               values: ["c", "a", "b"],
               counts: [3, 2, 1]
             }
    end

    test "list of integer" do
      s = Series.from_list([[1, 2], [3, 1, 3], [4, 1], [5, 6], [1, 2], [4, 1]])

      df = Series.frequencies(s)

      assert Series.dtype(df[:values]) == {:list, {:s, 64}}
      assert Series.dtype(df[:counts]) == {:u, 32}

      assert Explorer.DataFrame.to_columns(df, atom_keys: true) == %{
               values: [[1, 2], [4, 1], [3, 1, 3], [5, 6]],
               counts: [2, 2, 1, 1]
             }
    end

    test "list of list of string" do
      s = Series.from_list([["a"], ["a", "b"], ["c"], ["c"], ["c"]])

      assert_raise ArgumentError,
                   "frequencies/1 only works with series of lists of numeric types, but list[string] was given",
                   fn ->
                     Series.frequencies(s)
                   end
    end
  end

  describe "peaks/1" do
    test "max with signed integers" do
      s = Series.from_list([1, 2, 4, 1, 4])
      peaks = Series.peaks(s)

      assert Series.dtype(peaks) == :boolean
      assert Series.to_list(peaks) == [false, false, true, false, true]
    end

    test "max with unsigned integers" do
      s = Series.from_list([1, 2, 4, 1, 4], dtype: :u32)
      peaks = Series.peaks(s)

      assert Series.dtype(peaks) == :boolean
      assert Series.to_list(peaks) == [false, false, true, false, true]
    end

    test "max with floats" do
      s = Series.from_list([1.2, 2.3, 4.0, 4.1, 4.0])
      peaks = Series.peaks(s)

      assert Series.dtype(peaks) == :boolean
      assert Series.to_list(peaks) == [false, false, false, true, false]
    end

    test "min with signed integers" do
      s = Series.from_list([5, 1, 2, 4, 1, 4])
      peaks = Series.peaks(s, :min)

      assert Series.dtype(peaks) == :boolean
      assert Series.to_list(peaks) == [false, true, false, false, true, false]
    end

    test "min with unsigned integers" do
      s = Series.from_list([5, 1, 2, 4, 1, 4], dtype: :u32)
      peaks = Series.peaks(s, :min)

      assert Series.dtype(peaks) == :boolean
      assert Series.to_list(peaks) == [false, true, false, false, true, false]
    end

    test "min with floats" do
      s = Series.from_list([1.2, 0.3, 4.0, 2.5, 4.0])
      peaks = Series.peaks(s, :min)

      assert Series.dtype(peaks) == :boolean
      assert Series.to_list(peaks) == [false, true, false, true, false]
    end
  end

  describe "json_decode/2" do
    test "raises for invalid json" do
      assert_raise RuntimeError,
                   "Polars Error: error deserializing JSON: json parsing error: 'InternalError(TapeError) at character 1 ('a')'",
                   fn ->
                     Series.from_list(["a"]) |> Series.json_decode(:string)
                   end
    end

    test "extracts primitive from json and nil for mismatch" do
      s = Series.from_list(["1", "\"a\""])
      sj = Series.json_decode(s, {:s, 64})
      assert sj.dtype == {:s, 64}
      assert Series.to_list(sj) == [1, nil]
    end

    test "extracts struct from json with dtype" do
      s = Series.from_list(["{\"n\": 1}"])
      sj = Series.json_decode(s, {:struct, %{"n" => {:f, 64}}})
      assert sj.dtype == {:struct, [{"n", {:f, 64}}]}
      assert Series.to_list(sj) == [%{"n" => 1.0}]
    end
  end
end
