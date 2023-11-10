defmodule Explorer.Series.ListTest do
  use ExUnit.Case, async: true

  alias Explorer.Series

  describe "from_list/2" do
    test "list of lists of one integer" do
      series = Series.from_list([[1]])

      assert series.dtype == {:list, :integer}
      assert series[0] == [1]
      assert Series.to_list(series) == [[1]]
    end

    test "list of lists of integers" do
      series = Series.from_list([[-1], [1], [2], [3, nil, 4], nil, []])

      assert series.dtype == {:list, :integer}
      assert series[0] == [-1]
      assert Series.to_list(series) == [[-1], [1], [2], [3, nil, 4], nil, []]
    end

    test "list of lists of integers recursively" do
      series = Series.from_list([[[1]]])

      assert series.dtype == {:list, {:list, :integer}}
      assert series[0] == [[1]]
      assert Series.to_list(series) == [[[1]]]
    end

    test "list of lists of floats recursively" do
      series = Series.from_list([[[1.52]]])

      assert series.dtype == {:list, {:list, :float}}
      assert series[0] == [[1.52]]
      assert Series.to_list(series) == [[[1.52]]]
    end

    test "list of lists of floats" do
      series = Series.from_list([[1.2], [2.3], [3.4, nil, 4.5], []])

      assert series.dtype == {:list, :float}
      assert series[0] == [1.2]
      assert Series.to_list(series) == [[1.2], [2.3], [3.4, nil, 4.5], []]
    end

    test "list of lists of nans" do
      s = Series.from_list([[:nan], [:nan, :nan]])
      assert Series.dtype(s) == {:list, :float}

      assert Series.to_list(s) === [[:nan], [:nan, :nan]]
    end

    test "list of lists of deep nans" do
      s = Series.from_list([[[:nan], [:nan, :nan]]])
      assert Series.dtype(s) == {:list, {:list, :float}}

      assert Series.to_list(s) === [[[:nan], [:nan, :nan]]]
    end

    test "list of lists of infinity" do
      s = Series.from_list([[:infinity, :neg_infinity], [:infinity]])
      assert Series.dtype(s) == {:list, :float}

      assert Series.to_list(s) === [[:infinity, :neg_infinity], [:infinity]]
    end

    test "list of lists mixing floats and integers" do
      s = Series.from_list([[1, 2.4], [3]])
      assert Series.dtype(s) == {:list, :float}

      assert Series.to_list(s) === [[1.0, 2.4], [3.0]]
    end

    test "list of lists of deep integers and numerics" do
      s = Series.from_list([[[-1], [7.2, 6]]])
      assert Series.dtype(s) == {:list, {:list, :float}}

      assert Series.to_list(s) === [[[-1.0], [7.2, 6.0]]]
    end

    test "list of lists of deep integers and floats" do
      s =
        Series.from_list([
          [[-1], [7.2, 6.6]],
          [[-2.2], [8.3, 7.7]],
          [[-3], [9.4, 8.9]]
        ])

      assert Series.dtype(s) == {:list, {:list, :float}}

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
      series =
        Series.from_list([[true], [false], [true, nil, false], []])

      assert series.dtype == {:list, :boolean}
      assert series[0] == [true]
      assert Series.to_list(series) == [[true], [false], [true, nil, false], []]
    end

    test "list of lists of lists of lists of integers" do
      series = Series.from_list([[[[[1, 2]]]]])

      assert series.dtype == {:list, {:list, {:list, {:list, :integer}}}}
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
                   "the value \"a\" does not match the inferred series dtype :integer",
                   fn -> Series.from_list([[1], ["a"]]) end

      assert_raise ArgumentError,
                   "the value \"z\" does not match the inferred series dtype :integer",
                   fn -> Series.from_list([[[[[1, 2], ["z", "b"]]]]]) end
    end
  end
end
