defmodule Explorer.Series.InferredDtypePropertyTest do
  @moduledoc """
  Property tests for checking the inferred dtype logic when the dtype isn't
  specified in `Explorer.Series.from_list/1`.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Explorer.Series

  @moduletag timeout: :infinity

  property "inferred dtype should always be a sub-dtype" do
    check all(
            dtype <- Explorer.Generator.dtype(scalar: constant({:s, 64})),
            list_of_dtype <- Explorer.Generator.column(dtype, as: :list),
            max_run_time: 60_000,
            max_runs: 10_000
          ) do
      assert list_of_dtype |> Series.from_list() |> Series.dtype() |> sub_dtype_of?(dtype)
    end
  end

  # The idea behind a "sub" dtype is that in the dtype tree, you can replace
  # any subtree with `:null` and it's still valid. This is to deal with empty
  # lists where we can't reasonably infer the dtype of a list with no elements.
  defp sub_dtype_of?(x, x), do: true
  defp sub_dtype_of?(:null, _), do: true
  defp sub_dtype_of?({:list, sub_dtype}, {:list, dtype}), do: sub_dtype_of?(sub_dtype, dtype)

  defp sub_dtype_of?({:struct, sub_dtype_keyword}, {:struct, dtype_keyword})
       when is_list(sub_dtype_keyword) and is_list(dtype_keyword) do
    if length(sub_dtype_keyword) != length(dtype_keyword) do
      false
    else
      # Note: the need to sort here indicates we may want to normalize the result
      # of `Series.dtype/1`.
      Enum.sort(sub_dtype_keyword)
      |> Enum.zip(Enum.sort(dtype_keyword))
      |> Enum.all?(fn {{sub_key, sub_value}, {key, value}} ->
        sub_key == key and sub_dtype_of?(sub_value, value)
      end)
    end
  end

  defp sub_dtype_of?(_sub_dtype, _dtype), do: false
end
