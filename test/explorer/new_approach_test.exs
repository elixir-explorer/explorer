defmodule Explorer.NewApproachTest do
  use ExUnit.Case, async: true

  # Tests for most IO operations are in the data_frame folder
  # Tests for summarise, group, ungroup are available in grouped_test.exs

  # Doctests assume the module has been required
  require Explorer.DataFrame
  # doctest Explorer.DataFrame

  # import ExUnit.CaptureIO
  alias Explorer.DataFrame, as: DF
  # alias Explorer.Datasets
  # alias Explorer.Series

  describe "filter_ls/2 (experimental)" do
    test "filter by a boolean value" do
      df = DF.new(a: [1, 2, 3], b: [true, true, false])

      df1 = DF.filter_ls(df, b == false)
      assert DF.to_columns(df1, atom_keys: true) == %{a: [3], b: [false]}
    end
  end
end
