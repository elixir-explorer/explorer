defmodule Explorer.Series.StructTest do
  use ExUnit.Case, async: true

  alias Explorer.Series

  describe "from_list/2" do
    test "allows struct values" do
      s = Series.from_list([%{a: 1}, %{a: 3}, %{a: 5}])

      assert s.dtype == {:struct, %{"a" => :integer}}

      assert Series.to_list(s) == [%{"a" => 1}, %{"a" => 3}, %{"a" => 5}]
    end

    test "allows structs with nil values" do
      s =
        Series.from_list([
          %{a: nil, b: 2},
          %{a: 3, b: nil},
          %{a: 5, b: 6}
        ])

      assert s.dtype == {:struct, %{"a" => :integer, "b" => :integer}}

      assert Series.to_list(s) == [
               %{"a" => nil, "b" => 2},
               %{"a" => 3, "b" => nil},
               %{"a" => 5, "b" => 6}
             ]
    end

    test "allows nested structs" do
      s =
        Series.from_list([
          %{a: %{b: 1}},
          %{a: %{b: 2}},
          %{a: %{b: 3}}
        ])

      assert s.dtype == {:struct, %{"a" => {:struct, %{"b" => :integer}}}}

      assert Series.to_list(s) == [
               %{"a" => %{"b" => 1}},
               %{"a" => %{"b" => 2}},
               %{"a" => %{"b" => 3}}
             ]
    end

    test "allows structs structs with special float values" do
      series = Series.from_list([%{a: :nan, b: :infinity, c: :neg_infinity}])

      assert series.dtype == {:struct, %{"a" => {:f, 64}, "b" => {:f, 64}, "c" => {:f, 64}}}
      assert series[0] == %{"a" => :nan, "b" => :infinity, "c" => :neg_infinity}
      assert Series.to_list(series) == [%{"a" => :nan, "b" => :infinity, "c" => :neg_infinity}]
    end

    test "allows structs mixing integers and floats" do
      series = Series.from_list([%{a: 1, b: 2.4}, %{a: 1.5, b: 2}])

      assert series.dtype == {:struct, %{"a" => {:f, 64}, "b" => {:f, 64}}}
      assert Series.to_list(series) == [%{"a" => 1.0, "b" => 2.4}, %{"a" => 1.5, "b" => 2.0}]
    end

    test "errors when structs have mismatched types" do
      assert_raise ArgumentError,
                   "the value %{a: \"a\"} does not match the inferred series dtype {:struct, %{\"a\" => :integer}}",
                   fn -> Series.from_list([%{a: 1}, %{a: "a"}]) end

      assert_raise ArgumentError,
                   "the value %{b: 1} does not match the inferred series dtype {:struct, %{\"a\" => :integer}}",
                   fn -> Series.from_list([%{a: 1}, %{b: 1}]) end
    end
  end

  describe "cast/2" do
    test "struct with integers to struct with floats" do
      s = Series.from_list([%{a: 1}, %{a: 2}])
      s1 = Series.cast(s, {:struct, %{"a" => {:f, 64}}})

      assert Series.to_list(s1) == [%{"a" => 1.0}, %{"a" => 2.0}]
      assert Series.dtype(s1) == {:struct, %{"a" => {:f, 64}}}
    end

    test "nested structs with integers to nested structs with floats" do
      s = Series.from_list([%{a: %{b: 1}}, %{a: %{b: 2}}])
      s1 = Series.cast(s, {:struct, %{"a" => {:struct, %{"b" => {:f, 64}}}}})

      assert Series.to_list(s1) == [%{"a" => %{"b" => 1.0}}, %{"a" => %{"b" => 2.0}}]
      assert Series.dtype(s1) == {:struct, %{"a" => {:struct, %{"b" => {:f, 64}}}}}
    end

    test "structs with integers to structs with datetimes" do
      s =
        Series.from_list([
          %{a: 1},
          %{a: 2},
          %{a: 3},
          %{a: 1_649_883_642 * 1_000 * 1_000}
        ])

      s1 = Series.cast(s, {:struct, %{"a" => {:datetime, :microsecond}}})

      assert Series.to_list(s1) == [
               %{"a" => ~N[1970-01-01 00:00:00.000001]},
               %{"a" => ~N[1970-01-01 00:00:00.000002]},
               %{"a" => ~N[1970-01-01 00:00:00.000003]},
               %{"a" => ~N[2022-04-13 21:00:42.000000]}
             ]

      assert Series.dtype(s1) == {:struct, %{"a" => {:datetime, :microsecond}}}
    end
  end

  describe "inspect/1" do
    test "struct with integer values" do
      s = Series.from_list([%{a: 1}, %{a: 2}])

      assert inspect(s) ==
               """
               #Explorer.Series<
                 Polars[2]
                 struct[1] [%{"a" => 1}, %{"a" => 2}]
               >\
               """
    end
  end

  test "struct with nested values" do
    s = Series.from_list([%{a: %{b: 1}, c: [2]}, %{a: %{b: 2}, c: [4]}])

    assert inspect(s) ==
             """
             #Explorer.Series<
               Polars[2]
               struct[2] [%{"a" => %{"b" => 1}, "c" => [2]}, %{"a" => %{"b" => 2}, "c" => [4]}]
             >\
             """
  end
end
