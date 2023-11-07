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

    test "list of lists of infinity" do
      s = Series.from_list([[:infinity, :neg_infinity], [:infinity]])
      assert Series.dtype(s) == {:list, :float}

      assert Series.to_list(s) === [[:infinity, :neg_infinity], [:infinity]]
    end

    @tag skip: true
    test "list of lists mixing floats and integers" do
      s = Series.from_list([[1, 2.4], [3]])
      assert Series.dtype(s) == {:list, :float}

      assert Series.to_list(s) === [[1.0, 2.4], [3.0]]
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
  end
end
