defmodule Explorer.PolarsBackend.DecimalUnstableTest do
  # This module tracks some oddities we've found with decimal dtypes. The Polars
  # docs warn that decimals are unstable:
  #
  #   > This functionality is considered unstable. It is a work-in-progress
  #   > feature and may not always work as expected. It may be changed at any
  #   > point without it being considered a breaking change.
  #
  # https://docs.pola.rs/api/python/stable/reference/api/polars.datatypes.Decimal.html
  #
  # If the tests in the module start breaking, it probably means Polars has
  # changed its decimal implementation. Be prepared to change the tests: they
  # function as canaries rather than imposing expected behavior.

  use ExUnit.Case, async: true

  alias Explorer.PolarsBackend

  require Explorer.DataFrame, as: DF

  setup do
    %{df: DF.new(a: [Decimal.new("1.0"), Decimal.new("2.0")])}
  end

  test "mean returns decimal instead of float", %{df: df} do
    assert_raise(
      RuntimeError,
      """
      DataFrame mismatch.

      expected:

          names: ["a"]
          dtypes: %{"a" => {:f, 64}}

      got:

          names: ["a"]
          dtypes: %{"a" => {:decimal, 38, 1}}
      """,
      fn -> DF.summarise(df, a: mean(a)) end
    )
  end

  test "unchecked mean returns nil", %{df: df} do
    # Here we recreate the internals of `DF.summarise(df, a: mean(a))` to avoid
    # the invalid dtype expectation. What we should get is a decimal that
    # represents the mean, but what we do get is `nil`.
    qf = Explorer.Query.new(df)
    ldf = PolarsBackend.DataFrame.lazy(df)
    lazy_mean_a = Explorer.Series.mean(qf["a"])

    expr =
      lazy_mean_a.data
      |> PolarsBackend.Expression.to_expr()
      |> PolarsBackend.Expression.alias_expr("a")

    df =
      with {:ok, pdf1} <- PolarsBackend.Native.lf_summarise_with(ldf.data, [], [expr]),
           {:ok, pdf2} <- PolarsBackend.Native.lf_compute(pdf1),
           do: PolarsBackend.Shared.create_dataframe!(pdf2)

    assert Explorer.Series.to_list(df["a"]) == [nil]
  end
end
