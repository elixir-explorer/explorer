defmodule Explorer.Series.ListTest do
  use ExUnit.Case, async: true

  alias Explorer.Series

  describe "from_list/2" do
    test "with a list of lists of integers" do
      one_item = Series.from_list([[1], [2], [3, nil, 4], []], dtype: {:list, :integer})

      assert one_item.dtype == {:list, :integer}
      assert one_item[0] == [1]
      assert Series.to_list(one_item) == [[1], [2], [3, nil, 4], []]
    end

    test "with a list of lists of floats" do
      one_item = Series.from_list([[1.2], [2.3], [3.4, nil, 4.5], []], dtype: {:list, :float})

      assert one_item.dtype == {:list, :float}
      assert one_item[0] == [1.2]
      assert Series.to_list(one_item) == [[1.2], [2.3], [3.4, nil, 4.5], []]
    end

    test "with a list of lists of strings" do
      one_item = Series.from_list([["a"], ["b"], ["c", nil, "d"], []], dtype: {:list, :string})

      assert one_item.dtype == {:list, :string}
      assert one_item[0] == ["a"]
      assert Series.to_list(one_item) == [["a"], ["b"], ["c", nil, "d"], []]
    end

    test "with a list of lists of booleans" do
      one_item =
        Series.from_list([[true], [false], [true, nil, false], []], dtype: {:list, :boolean})

      assert one_item.dtype == {:list, :boolean}
      assert one_item[0] == [true]
      assert Series.to_list(one_item) == [[true], [false], [true, nil, false], []]
    end
  end
end
