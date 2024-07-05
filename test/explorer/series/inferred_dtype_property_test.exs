defmodule Explorer.Series.InferredDtypePropertyTest do
  @moduledoc """
  Property tests for checking the inferred dtype logic when the dtype isn't
  specified in `Explorer.Series.from_list/1`.

  ## Notes

    * A maximum of 3 used quite a bit. This is intentional. Usually issues stem
      from empty lists, not really long lists. By keeping lists small, we can
      iterate much quicker through the input space.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  import StreamData

  alias Explorer.Series

  @moduletag timeout: :infinity

  property "inferred dtype should always be a sub-dtype" do
    check all(
            dtype <- dtype_generator(),
            series <- series_of_dtype_generator(dtype),
            max_run_time: 60_000,
            max_runs: 10_000
          ) do
      assert series |> Series.dtype() |> sub_dtype_of?(dtype)
    end
  end

  defp dtype_generator do
    scalar_dtype_generator = constant({:s, 64})

    # We don't need complicated keys: single letter strings should suffice.
    key_generator = string(?a..?z, min_length: 1, max_length: 1)

    dtype_generator =
      tree(scalar_dtype_generator, fn generator ->
        # Building the keyword list from a map ensures unique keys.
        keyword_generator =
          map(nonempty(map_of(key_generator, generator, max_length: 3)), &Enum.to_list/1)

        one_of([
          tuple({constant(:list), generator}),
          tuple({constant(:struct), keyword_generator})
        ])
      end)

    dtype_generator
  end

  defp series_of_dtype_generator(dtype) do
    series_value_generator = build_series_value_generator(dtype)

    bind(list_of(series_value_generator, max_length: 3), fn series_values ->
      constant(Explorer.Series.from_list(series_values))
    end)
  end

  defp build_series_value_generator({:s, 64}),
    do: integer()

  defp build_series_value_generator({:list, dtype}),
    do: list_of(build_series_value_generator(dtype), max_length: 3)

  defp build_series_value_generator({:struct, keyword_of_dtypes}) do
    keyword_of_dtypes
    |> Map.new(fn {key, dtype} -> {key, build_series_value_generator(dtype)} end)
    |> fixed_map()
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
