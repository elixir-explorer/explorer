defmodule Explorer.Series.ListTest do
  use ExUnit.Case, async: true

  alias Explorer.Series

  describe "from_list/2" do
    test "with a list of lists of integers" do
      one_item = Series.from_list([[1]], dtype: {:list, :integer})

      assert one_item.dtype == {:list, :integer}
      assert one_item[0] == [1]
      assert Series.to_list(one_item) == [[1]]
    end
  end
end
