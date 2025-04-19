defmodule Example do
  alias Explorer.Query
  alias Explorer.Series
  require Explorer.DataFrame, as: DataFrame

  def main(df, filter, missing_rows) do
    query = Query.new(df)

    prob =
      Query.if(query[filter.metric] == 0,
        do: query["counts"] |> Series.add(missing_rows) |> Series.divide(10000),
        else: query["counts"] |> Series.divide(10000)
      )

    cumsum = Series.cumulative_sum(prob)

    DataFrame.mutate_with(df,
      idx: Series.row_index(query["counts"]),
      prob: prob,
      cumsum: cumsum,
      distances: cumsum |> Series.subtract(0.5) |> Series.abs()
    )
  end
end

filter = %{metric: "my_metric"}
missing_rows = 5
df = Explorer.DataFrame.new(my_metric: [0, 1, 2], counts: [100, 200, 300])
Example.main(df, filter, missing_rows)
