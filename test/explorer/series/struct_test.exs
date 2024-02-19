defmodule Explorer.Series.StructTest do
  use ExUnit.Case, async: true

  alias Explorer.Series

  describe "from_list/2" do
    test "allows struct of all nil value" do
      s =
        Series.from_list([
          %{a: nil, b: nil},
          %{a: 3, b: nil},
          %{a: 5, b: nil}
        ])

      assert s.dtype == {:struct, [{"a", {:s, 64}}, {"b", :null}]}

      assert Series.to_list(s) == [
               %{"a" => nil, "b" => nil},
               %{"a" => 3, "b" => nil},
               %{"a" => 5, "b" => nil}
             ]
    end

    test "allow nils" do
      s = Series.from_list([nil, %{"a" => 1, "b" => 2}, nil])
      assert s.dtype == {:struct, [{"a", {:s, 64}}, {"b", {:s, 64}}]}

      assert Series.to_list(s) == [
               %{"a" => nil, "b" => nil},
               %{"a" => 1, "b" => 2},
               %{"a" => nil, "b" => nil}
             ]
    end

    test "allows struct values" do
      s = Series.from_list([%{a: 1}, %{a: 3}, %{a: 5}])

      assert s.dtype == {:struct, [{"a", {:s, 64}}]}

      assert Series.to_list(s) == [%{"a" => 1}, %{"a" => 3}, %{"a" => 5}]
    end

    test "allows structs with nil values" do
      s =
        Series.from_list([
          %{a: nil, b: 2},
          %{a: 3, b: nil},
          %{a: 5, b: 6}
        ])

      assert s.dtype == {:struct, [{"a", {:s, 64}}, {"b", {:s, 64}}]}

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

      assert s.dtype == {:struct, [{"a", {:struct, [{"b", {:s, 64}}]}}]}

      assert Series.to_list(s) == [
               %{"a" => %{"b" => 1}},
               %{"a" => %{"b" => 2}},
               %{"a" => %{"b" => 3}}
             ]
    end

    test "allows structs structs with special float values" do
      series = Series.from_list([%{a: :nan, b: :infinity, c: :neg_infinity}])

      assert series.dtype == {:struct, [{"a", {:f, 64}}, {"b", {:f, 64}}, {"c", {:f, 64}}]}
      assert series[0] == %{"a" => :nan, "b" => :infinity, "c" => :neg_infinity}
      assert Series.to_list(series) == [%{"a" => :nan, "b" => :infinity, "c" => :neg_infinity}]
    end

    test "allows structs mixing integers and floats" do
      series = Series.from_list([%{a: 1, b: 2.4}, %{a: 1.5, b: 2}])

      assert series.dtype == {:struct, [{"a", {:f, 64}}, {"b", {:f, 64}}]}
      assert Series.to_list(series) == [%{"a" => 1.0, "b" => 2.4}, %{"a" => 1.5, "b" => 2.0}]
    end

    test "allows nested lists with structs" do
      series = Series.from_list([[%{a: 1}, %{a: 2}], [%{a: 3}]])

      assert series.dtype == {:list, {:struct, [{"a", {:s, 64}}]}}
      assert Series.to_list(series) == [[%{"a" => 1}, %{"a" => 2}], [%{"a" => 3}]]
    end

    test "allows custom dtype for struct values" do
      s = Series.from_list([%{a: 1}, %{a: 3}, %{a: 5}], dtype: {:struct, a: :u8})
      assert s.dtype == {:struct, [{"a", {:u, 8}}]}

      assert Series.to_list(s) == [%{"a" => 1}, %{"a" => 3}, %{"a" => 5}]

      s1 = Series.from_list([%{a: 1}, %{a: 3}, %{a: 5}], dtype: {:struct, %{"a" => :s16}})
      assert s1.dtype == {:struct, [{"a", {:s, 16}}]}

      assert Series.to_list(s1) == [%{"a" => 1}, %{"a" => 3}, %{"a" => 5}]
    end

    test "preserves manually provided dtype order" do
      series =
        Series.from_list(
          [%{"a" => "a", "b" => "b"}, %{"b" => "b", "a" => "a"}],
          dtype: {:struct, [{"b", :string}, {"a", :string}]}
        )

      assert series.dtype == {:struct, [{"b", :string}, {"a", :string}]}

      series1 =
        Series.from_list(
          [%{"a" => "a", "b" => "b"}, %{"b" => "b", "a" => "a"}],
          dtype: {:struct, [{"a", :string}, {"b", :string}]}
        )

      assert series1.dtype == {:struct, [{"a", :string}, {"b", :string}]}
    end

    test "errors when structs have mismatched types" do
      assert_raise ArgumentError,
                   "the value \"a\" does not match the inferred dtype {:s, 64}",
                   fn -> Series.from_list([%{a: 1}, %{a: "a"}]) end

      assert_raise ArgumentError,
                   "the value %{b: 1} does not match the inferred dtype {:struct, [{\"a\", {:s, 64}}]}",
                   fn -> Series.from_list([%{a: 1}, %{b: 1}]) end

      assert_raise ArgumentError,
                   "the value \"a\" does not match the inferred dtype {:s, 64}",
                   fn -> Series.from_list([[%{a: 1}], [%{a: "a"}]]) end
    end
  end

  describe "cast/2" do
    test "struct with integers to struct with floats" do
      s = Series.from_list([%{a: 1}, %{a: 2}])
      s1 = Series.cast(s, {:struct, [{"a", {:f, 64}}]})

      assert Series.to_list(s1) == [%{"a" => 1.0}, %{"a" => 2.0}]
      assert Series.dtype(s1) == {:struct, [{"a", {:f, 64}}]}
    end

    test "nested structs with integers to nested structs with floats" do
      s = Series.from_list([%{a: %{b: 1}}, %{a: %{b: 2}}])
      s1 = Series.cast(s, {:struct, [{"a", {:struct, [{"b", {:f, 64}}]}}]})

      assert Series.to_list(s1) == [%{"a" => %{"b" => 1.0}}, %{"a" => %{"b" => 2.0}}]
      assert Series.dtype(s1) == {:struct, [{"a", {:struct, [{"b", {:f, 64}}]}}]}
    end

    test "structs with integers to structs with datetimes" do
      s =
        Series.from_list([
          %{a: 1},
          %{a: 2},
          %{a: 3},
          %{a: 1_649_883_642 * 1_000 * 1_000}
        ])

      s1 = Series.cast(s, {:struct, [{"a", {:datetime, :microsecond}}]})

      assert Series.to_list(s1) == [
               %{"a" => ~N[1970-01-01 00:00:00.000001]},
               %{"a" => ~N[1970-01-01 00:00:00.000002]},
               %{"a" => ~N[1970-01-01 00:00:00.000003]},
               %{"a" => ~N[2022-04-13 21:00:42.000000]}
             ]

      assert Series.dtype(s1) == {:struct, [{"a", {:datetime, :microsecond}}]}
    end

    test "can cast dtype order" do
      series =
        Series.from_list([%{"a" => "a", "b" => "b"}, %{"b" => "b", "a" => "a"}],
          dtype: {:struct, [{"a", :string}, {"b", :string}]}
        )

      casted = Series.cast(series, {:struct, [{"b", :string}, {"a", :string}]})
      assert casted.dtype == {:struct, [{"b", :string}, {"a", :string}]}
    end

    test "errors when casting to invalid nested types" do
      s = Series.from_list([%{a: 1}, %{a: 2}])

      assert_raise ArgumentError,
                   ~r"Explorer.Series.cast/2 not implemented for dtype {:struct, %{\"a\" => :invalid_type}}",
                   fn -> Series.cast(s, {:struct, %{"a" => :invalid_type}}) end
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

  describe "field/2" do
    test "extract field using string key" do
      s = Series.from_list([%{a: 1}, %{a: 2}])
      a = Series.field(s, "a")
      assert s.dtype == {:struct, [{"a", {:s, 64}}]}
      assert a.dtype == {:s, 64}
    end

    test "extract field using atom key" do
      s = Series.from_list([%{a: 1}, %{a: 2}])
      a = Series.field(s, :a)
      assert s.dtype == {:struct, [{"a", {:s, 64}}]}
      assert a.dtype == {:s, 64}
    end

    test "raise error - invalid field" do
      s = Series.from_list([%{a: 1}, %{a: 2}])

      assert_raise ArgumentError, "field \"m\" not found in fields [\"a\"]", fn ->
        Series.field(s, "m")
      end
    end
  end
end
