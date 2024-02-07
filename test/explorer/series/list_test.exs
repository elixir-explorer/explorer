defmodule Explorer.Series.ListTest do
  use ExUnit.Case, async: true

  alias Explorer.Series

  describe "from_list/2" do
    test "list of list of nulls" do
      series = Series.from_list([[nil, nil], [nil]])
      assert series.dtype == {:list, :null}
      assert series[0] == [nil, nil]
      assert Series.to_list(series) == [[nil, nil], [nil]]
    end

    test "list of lists of one integer" do
      series = Series.from_list([[1]])

      assert series.dtype == {:list, {:s, 64}}
      assert series[0] == [1]
      assert Series.to_list(series) == [[1]]
    end

    test "list of lists of integers" do
      series = Series.from_list([[-1], [1], [2], [3, nil, 4], nil, []])

      assert series.dtype == {:list, {:s, 64}}
      assert series[0] == [-1]
      assert Series.to_list(series) == [[-1], [1], [2], [3, nil, 4], nil, []]
    end

    test "list of lists of integers recursively" do
      series = Series.from_list([[[1]]])

      assert series.dtype == {:list, {:list, {:s, 64}}}
      assert series[0] == [[1]]
      assert Series.to_list(series) == [[[1]]]
    end

    test "list of lists of floats recursively" do
      series = Series.from_list([[[1.52]]])

      assert series.dtype == {:list, {:list, {:f, 64}}}
      assert series[0] == [[1.52]]
      assert Series.to_list(series) == [[[1.52]]]
    end

    test "list of lists of floats" do
      series = Series.from_list([[1.2], [2.3], [3.4, nil, 4.5], []])

      assert series.dtype == {:list, {:f, 64}}
      assert series[0] == [1.2]
      assert Series.to_list(series) == [[1.2], [2.3], [3.4, nil, 4.5], []]
    end

    test "list of lists of nans" do
      s = Series.from_list([[:nan], [:nan, :nan]])
      assert Series.dtype(s) == {:list, {:f, 64}}

      assert Series.to_list(s) === [[:nan], [:nan, :nan]]
    end

    test "list of lists of deep nans" do
      s = Series.from_list([[[:nan], [:nan, :nan]]])
      assert Series.dtype(s) == {:list, {:list, {:f, 64}}}

      assert Series.to_list(s) === [[[:nan], [:nan, :nan]]]
    end

    test "list of lists of infinity" do
      s = Series.from_list([[:infinity, :neg_infinity], [:infinity]])
      assert Series.dtype(s) == {:list, {:f, 64}}

      assert Series.to_list(s) === [[:infinity, :neg_infinity], [:infinity]]
    end

    test "list of lists mixing floats and integers" do
      s = Series.from_list([[1, 2.4], [3]])
      assert Series.dtype(s) == {:list, {:f, 64}}

      assert Series.to_list(s) === [[1.0, 2.4], [3.0]]
    end

    test "list of lists of deep integers and numerics" do
      s = Series.from_list([[[-1], [7.2, 6]]])
      assert Series.dtype(s) == {:list, {:list, {:f, 64}}}

      assert Series.to_list(s) === [[[-1.0], [7.2, 6.0]]]
    end

    test "list of lists of deep integers and floats" do
      s =
        Series.from_list([
          [[-1], [7.2, 6.6]],
          [[-2.2], [8.3, 7.7]],
          [[-3], [9.4, 8.9]]
        ])

      assert Series.dtype(s) == {:list, {:list, {:f, 64}}}

      assert Series.to_list(s) === [
               [[-1.0], [7.2, 6.6]],
               [[-2.2], [8.3, 7.7]],
               [[-3.0], [9.4, 8.9]]
             ]
    end

    test "list of lists of strings" do
      series = Series.from_list([["a"], ["b"], ["c", nil, "d"], []])

      assert series.dtype == {:list, :string}
      assert series[0] == ["a"]
      assert Series.to_list(series) == [["a"], ["b"], ["c", nil, "d"], []]
    end

    test "list of lists of booleans" do
      series = Series.from_list([[true], [false], [true, nil, false], []])

      assert series.dtype == {:list, :boolean}
      assert series[0] == [true]
      assert Series.to_list(series) == [[true], [false], [true, nil, false], []]
    end

    test "list of lists of lists of lists of integers" do
      series = Series.from_list([[[[[1, 2]]]]])

      assert series.dtype == {:list, {:list, {:list, {:list, {:s, 64}}}}}
      assert series[0] == [[[[1, 2]]]]
      assert Series.to_list(series) == [[[[[1, 2]]]]]
    end

    test "list of lists of one date" do
      series = Series.from_list([[~D[2023-11-10]]])

      assert series.dtype == {:list, :date}
      assert series[0] == [~D[2023-11-10]]
      assert Series.to_list(series) == [[~D[2023-11-10]]]
    end

    test "list of lists of one datetime" do
      series = Series.from_list([[~N[2023-11-10 00:19:30]]])

      assert series.dtype == {:list, {:datetime, :microsecond}}
      assert series[0] == [~N[2023-11-10 00:19:30.000000]]
      assert Series.to_list(series) == [[~N[2023-11-10 00:19:30.000000]]]
    end

    test "list of lists of one category" do
      series = Series.from_list([["a"]], dtype: {:list, :category})

      assert series.dtype == {:list, :category}
      assert series[0] == ["a"]
      assert Series.to_list(series) == [["a"]]
    end

    test "list of lists of one binary" do
      series = Series.from_list([[<<118, 225, 252, 151>>]], dtype: {:list, :binary})

      assert series.dtype == {:list, :binary}
      assert series[0] == [<<118, 225, 252, 151>>]
      assert Series.to_list(series) == [[<<118, 225, 252, 151>>]]
    end

    test "mixing list of lists of strings and numbers" do
      assert_raise ArgumentError,
                   "the value \"a\" does not match the inferred dtype {:s, 64}",
                   fn -> Series.from_list([[1], ["a"]]) end

      assert_raise ArgumentError,
                   "the value \"z\" does not match the inferred dtype {:s, 64}",
                   fn -> Series.from_list([[[[[1, 2], ["z", "b"]]]]]) end
    end

    test "list of structs" do
      series =
        Series.from_list([[%{"a" => 42}], []], dtype: {:list, {:struct, %{"a" => :integer}}})

      assert Series.dtype(series) == {:list, {:struct, %{"a" => {:s, 64}}}}
      assert Series.to_list(series) == [[%{"a" => 42}], []]
    end

    test "list of structs with first empty" do
      series =
        Series.from_list([[], [%{"a" => 42}], []], dtype: {:list, {:struct, %{"a" => :integer}}})

      assert Series.dtype(series) == {:list, {:struct, %{"a" => {:s, 64}}}}
      assert Series.to_list(series) == [[], [%{"a" => 42}], []]
    end
  end

  describe "cast/2" do
    test "list of integers series to list of floats" do
      s = Series.from_list([[1]])

      s1 = Series.cast(s, {:list, {:f, 64}})

      assert s1.dtype == {:list, {:f, 64}}
      assert s1[0] === [1.0]
      assert Series.to_list(s1) == [[1.0]]
    end

    test "list of floats series to list of integers" do
      s = Series.from_list([[1.0]])

      s1 = Series.cast(s, {:list, {:s, 64}})

      assert s1.dtype == {:list, {:s, 64}}
      assert s1[0] === [1]
      assert Series.to_list(s1) === [[1]]
    end

    test "deeper list of integers series to list of floats" do
      s = Series.from_list([[[1, 2]], [[3, 4]]])

      s1 = Series.cast(s, {:list, {:list, {:f, 64}}})

      assert s1.dtype == {:list, {:list, {:f, 64}}}
      assert s1[0] === [[1.0, 2.0]]
      assert Series.to_list(s1) === [[[1.0, 2.0]], [[3.0, 4.0]]]
    end

    test "list of integer series to list of datetime" do
      s = Series.from_list([[1, 2, 3], [1_649_883_642 * 1_000 * 1_000]])
      s1 = Series.cast(s, {:list, {:datetime, :microsecond}})

      assert Series.to_list(s1) == [
               [
                 ~N[1970-01-01 00:00:00.000001],
                 ~N[1970-01-01 00:00:00.000002],
                 ~N[1970-01-01 00:00:00.000003]
               ],
               [~N[2022-04-13 21:00:42.000000]]
             ]

      assert Series.dtype(s1) == {:list, {:datetime, :microsecond}}
    end

    test "deeper list of integers series to list of invalid dtype" do
      s = Series.from_list([[[1, 2]], [[3, 4]]])

      error_regex =
        ~r[Explorer.Series.cast/2 not implemented for dtype {:list, {:list, :invalid_dtype}}]

      assert_raise ArgumentError, error_regex, fn ->
        Series.cast(s, {:list, {:list, :invalid_dtype}})
      end
    end
  end

  describe "inspect/1" do
    test "list of integers series" do
      s = Series.from_list([[1, 2], [3, nil, 4], [5, 6]])

      assert inspect(s) ==
               """
               #Explorer.Series<
                 Polars[3]
                 list[s64] [[1, 2], [3, nil, 4], [5, 6]]
               >\
               """
    end

    test "list of floats series" do
      s =
        Series.from_list([
          [1.3, 2.4],
          [3.5, 4.6],
          [5.7, 6.8],
          [nil, :nan],
          [:infinity, :neg_infinity]
        ])

      assert inspect(s) ==
               """
               #Explorer.Series<
                 Polars[5]
                 list[f64] [[1.3, 2.4], [3.5, 4.6], [5.7, 6.8], [nil, NaN], [Inf, -Inf]]
               >\
               """
    end

    test "deeper list of integers series" do
      s = Series.from_list([[[1, 2]], [[3, 4]]])

      assert inspect(s) ==
               """
               #Explorer.Series<
                 Polars[2]
                 list[list[s64]] [[[1, 2]], [[3, 4]]]
               >\
               """
    end

    test "list of lists of datetime" do
      s = Series.from_list([[~N[2023-11-10 00:19:30]]])

      assert inspect(s) ==
               """
               #Explorer.Series<
                 Polars[1]
                 list[datetime[μs]] [[2023-11-10 00:19:30.000000]]
               >\
               """
    end
  end
end
