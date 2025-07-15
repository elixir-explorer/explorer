defmodule Explorer.DataFrame.GroupedTest do
  use ExUnit.Case, async: true

  require Explorer.DataFrame, as: DF
  alias Explorer.Datasets
  alias Explorer.Series

  setup do
    df = Datasets.fossil_fuels()
    {:ok, df: df}
  end

  describe "group_by/2" do
    test "groups a dataframe by one column", %{df: df} do
      assert df.groups.columns == []
      df1 = DF.group_by(df, "country")

      assert df1.groups.columns == ["country"]
      assert DF.groups(df1) == ["country"]
    end

    test "groups a dataframe by two columns", %{df: df} do
      df1 = DF.group_by(df, ["country", "year"])

      assert df1.groups.columns == ["country", "year"]
      assert DF.groups(df1) == ["country", "year"]
    end

    test "adds a group for an already grouped dataframe", %{df: df} do
      df1 = DF.group_by(df, ["country"])
      df2 = DF.group_by(df1, "year")

      assert df2.groups.columns == ["country", "year"]
      assert DF.groups(df2) == ["country", "year"]
    end

    test "raise error for unknown columns", %{df: df} do
      assert_raise ArgumentError, ~r"could not find column name \"something_else\"", fn ->
        DF.group_by(df, "something_else")
      end
    end
  end

  describe "ungroup/2" do
    test "removes one group", %{df: df} do
      df1 = DF.group_by(df, "country")
      df2 = DF.ungroup(df1, "country")

      assert df2.groups.columns == []
      assert DF.groups(df2) == []
    end

    test "remove one group for a dataframe that is grouped by two groups", %{df: df} do
      df1 = DF.group_by(df, ["country", "year"])
      df2 = DF.ungroup(df1, "country")

      assert df2.groups.columns == ["year"]
      assert DF.groups(df2) == ["year"]
    end

    test "remove two groups of a dataframe", %{df: df} do
      df1 = DF.group_by(df, ["country", "year"])
      df2 = DF.ungroup(df1, ["year", "country"])

      assert df2.groups.columns == []
      assert DF.groups(df2) == []
    end

    test "ungroup by range", %{df: df} do
      df1 = DF.group_by(df, ["country", "year"])
      df2 = DF.ungroup(df1, 1..1)

      # Note: the range selected the column at index 1 of `df.names` to ungroup,
      # not `df.groups`.
      assert Enum.take(df.names, 2) == ["year", "country"]
      assert df2.groups.columns == ["year"]
      assert DF.groups(df2) == ["year"]
    end

    test "raise error for unknown groups", %{df: df} do
      df1 = DF.group_by(df, ["country", "year"])

      assert_raise ArgumentError, ~r"could not find column name \"something_else\"", fn ->
        DF.ungroup(df1, ["something_else"])
      end
    end
  end

  describe "summarise/2" do
    test "with one group and one column with aggregation", %{df: df} do
      df1 = df |> DF.group_by("year", stable: true) |> DF.summarise(total: count(total))

      assert DF.names(df1) == ["year", "total"]

      assert DF.dtypes(df1) == %{
               "year" => {:s, 64},
               "total" => {:u, 32}
             }

      assert DF.groups(df1) == []

      assert DF.to_columns(df1, atom_keys: true) == %{
               year: [2010, 2011, 2012, 2013, 2014],
               total: [217, 217, 220, 220, 220]
             }
    end

    test "with two groups and one column with aggregations", %{df: df} do
      df1 =
        df
        |> DF.head(5)
        |> DF.group_by(["country", "year"], stable: true)
        |> DF.summarise(total_max: max(total), total_min: min(total))

      assert DF.names(df1) == ["country", "year", "total_max", "total_min"]

      assert DF.dtypes(df1) == %{
               "year" => {:s, 64},
               "total_min" => {:s, 64},
               "total_max" => {:s, 64},
               "country" => :string
             }

      assert DF.groups(df1) == []

      assert DF.to_columns(df1, atom_keys: true) == %{
               year: [2010, 2010, 2010, 2010, 2010],
               country: ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA"],
               total_max: [2308, 1254, 32500, 141, 7924],
               total_min: [2308, 1254, 32500, 141, 7924]
             }
    end

    test "pivot_wider and then summarise with a rename" do
      df =
        DF.new(
          names: ["cou", "adv", "spo", "cou", "adv", "spo"],
          val: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
          team: ["A", "A", "A", "B", "B", "B"]
        )

      df2 =
        df
        |> DF.pivot_wider("names", "val")
        |> DF.group_by("team", stable: true)
        |> DF.summarise(%{"adv" => max(adv), "cou" => max(cou), "spo" => max(spo)})

      assert DF.names(df2) == ["team", "adv", "cou", "spo"]

      assert DF.dtypes(df2) == %{
               "team" => :string,
               "adv" => {:f, 64},
               "cou" => {:f, 64},
               "spo" => {:f, 64}
             }

      assert Series.to_list(df2["cou"]) == [1.0, 4.0]
      assert Series.to_list(df2["adv"]) == [2.0, 5.0]
      assert Series.to_list(df2["spo"]) == [3.0, 6.0]
    end

    test "with two groups and two columns with aggregations", %{df: df} do
      equal_filters =
        for country <- ["BRAZIL", "AUSTRALIA", "POLAND"], do: Series.equal(df["country"], country)

      masks = Enum.reduce(equal_filters, fn filter, acc -> Series.or(acc, filter) end)

      df1 =
        df
        |> DF.mask(masks)
        |> DF.group_by(["country", "year"], stable: true)
        |> DF.summarise(
          total_max: max(total),
          total_min: min(total),
          cement_median: median(cement)
        )
        |> DF.sort_by(country)

      assert DF.to_columns(df1, atom_keys: true) == %{
               country: [
                 "AUSTRALIA",
                 "AUSTRALIA",
                 "AUSTRALIA",
                 "AUSTRALIA",
                 "AUSTRALIA",
                 "BRAZIL",
                 "BRAZIL",
                 "BRAZIL",
                 "BRAZIL",
                 "BRAZIL",
                 "POLAND",
                 "POLAND",
                 "POLAND",
                 "POLAND",
                 "POLAND"
               ],
               year: [
                 2010,
                 2011,
                 2012,
                 2013,
                 2014,
                 2010,
                 2011,
                 2012,
                 2013,
                 2014,
                 2010,
                 2011,
                 2012,
                 2013,
                 2014
               ],
               total_min: [
                 106_589,
                 106_850,
                 105_843,
                 101_518,
                 98517,
                 114_468,
                 119_829,
                 128_178,
                 137_354,
                 144_480,
                 86246,
                 86446,
                 81792,
                 82432,
                 77922
               ],
               total_max: [
                 106_589,
                 106_850,
                 105_843,
                 101_518,
                 98517,
                 114_468,
                 119_829,
                 128_178,
                 137_354,
                 144_480,
                 86246,
                 86446,
                 81792,
                 82432,
                 77922
               ],
               cement_median: [
                 1129.0,
                 1170.0,
                 1156.0,
                 1142.0,
                 1224.0,
                 8040.0,
                 8717.0,
                 9428.0,
                 9517.0,
                 9691.0,
                 2111.0,
                 2523.0,
                 2165.0,
                 1977.0,
                 2089.0
               ]
             }
    end

    test "pull from summarised DF", %{df: df} do
      series =
        df
        |> DF.group_by("country")
        |> DF.summarise(total_count: count(total))
        |> DF.pull("total_count")

      assert Series.min(series) == 2
    end

    test "using mode" do
      df =
        Datasets.iris()
        |> DF.group_by(:species, stable: true)
        |> DF.summarise(petal_width_mode: mode(petal_width))

      assert DF.dtypes(df) == %{
               "petal_width_mode" => {:list, {:f, 64}},
               "species" => :string
             }

      assert DF.to_columns(df) == %{
               "petal_width_mode" => [[0.2], [1.3], [1.8]],
               "species" => ["Iris-setosa", "Iris-versicolor", "Iris-virginica"]
             }
    end
  end

  describe "summarise_with/2" do
    test "with one group and one column with aggregations", %{df: df} do
      df1 =
        df
        |> DF.group_by("year", stable: true)
        |> DF.summarise_with(fn ldf ->
          total = ldf["total"]

          [total_min: Series.min(total), total_max: Series.max(total)]
        end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               year: [2010, 2011, 2012, 2013, 2014],
               total_min: [1, 2, 2, 2, 3],
               total_max: [2_393_248, 2_654_360, 2_734_817, 2_797_384, 2_806_634]
             }
    end

    test "with one group and two columns with aggregations", %{df: df} do
      df1 =
        df
        |> DF.group_by("year", stable: true)
        |> DF.summarise_with(fn ldf ->
          total = ldf["total"]
          liquid_fuel = ldf["liquid_fuel"]

          [
            total_min: Series.min(total),
            total_max: Series.max(total),
            median_liquid_fuel: Series.median(liquid_fuel)
          ]
        end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               year: [2010, 2011, 2012, 2013, 2014],
               total_min: [1, 2, 2, 2, 3],
               total_max: [2_393_248, 2_654_360, 2_734_817, 2_797_384, 2_806_634],
               median_liquid_fuel: [1193.0, 1236.0, 1199.0, 1260.0, 1255.0]
             }
    end

    test "with one group and aggregations with addition and subtraction", %{df: df} do
      df1 =
        df
        |> DF.group_by("year", stable: true)
        |> DF.summarise_with(fn ldf ->
          total = ldf["total"]
          liquid_fuel = ldf["liquid_fuel"]

          [
            total_min: Series.min(Series.add(total, 4)),
            total_max: Series.max(Series.subtract(total, liquid_fuel))
          ]
        end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               year: [2010, 2011, 2012, 2013, 2014],
               total_min: [5, 6, 6, 6, 7],
               total_max: [2_095_057, 2_347_630, 2_413_662, 2_460_424, 2_461_909]
             }
    end

    test "with two groups and one column with aggregations", %{df: df} do
      df1 =
        df
        |> DF.head(5)
        |> DF.group_by(["country", "year"], stable: true)
        |> DF.summarise_with(fn ldf ->
          total = ldf["total"]

          [total_min: Series.min(total), total_max: Series.max(total)]
        end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               year: [2010, 2010, 2010, 2010, 2010],
               country: ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA"],
               total_max: [2308, 1254, 32500, 141, 7924],
               total_min: [2308, 1254, 32500, 141, 7924]
             }
    end

    test "with one group and some aggregations", %{df: df} do
      df1 =
        df
        |> DF.group_by(["year"], stable: true)
        |> DF.summarise_with(fn ldf ->
          [
            count: Series.count(ldf["country"]),
            standard_deviation: Series.standard_deviation(ldf["gas_fuel"]),
            variance: Series.variance(ldf["total"])
          ]
        end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               year: [2010, 2011, 2012, 2013, 2014],
               count: [217, 217, 220, 220, 220],
               standard_deviation: [
                 30422.959346722248,
                 31276.44754166337,
                 31861.60786849842,
                 32568.516005143898,
                 32855.06720653753
               ],
               variance: [
                 38_857_563_094.67129,
                 44_768_861_168.99865,
                 45_897_543_842.42208,
                 47_520_301_869.29951,
                 48_253_624_259.85474
               ]
             }
    end

    test "with one group but no aggregation", %{df: df} do
      message = """
      expecting summarise with an aggregation operation or plain column, but none of which were found in: #Explorer.Series<
        LazySeries[???]
        s64 (column("solid_fuel") + 50)
      >\
      """

      assert_raise RuntimeError, message, fn ->
        df
        |> DF.group_by(["year"])
        |> DF.summarise_with(fn ldf ->
          [add: Series.add(ldf["solid_fuel"], 50)]
        end)
      end
    end

    test "with one group, one aggregation with a window function inside", %{df: df} do
      df1 =
        df
        |> DF.group_by(["year"], stable: true)
        |> DF.summarise_with(fn ldf ->
          [
            count: Series.count(ldf["country"]),
            max_of_win_solid_fuel_mean: Series.max(Series.window_mean(ldf["solid_fuel"], 2))
          ]
        end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               year: [2010, 2011, 2012, 2013, 2014],
               count: [217, 217, 220, 220, 220],
               max_of_win_solid_fuel_mean: [
                 898_651.5,
                 1_000_359.5,
                 1_021_872.5,
                 1_026_043.5,
                 1_016_740.5
               ]
             }
    end

    test "with one group and one window function with one aggregation inside", %{df: df} do
      message =
        "it's not possible to have an aggregation operation inside :window_mean, " <>
          "which is a window function"

      assert_raise RuntimeError, message, fn ->
        df
        |> DF.group_by(["year"])
        |> DF.summarise_with(fn ldf ->
          [
            count: Series.count(ldf["country"]),
            max_of_win_solid_fuel_mean: Series.window_mean(Series.max(ldf["solid_fuel"]), 2)
          ]
        end)
      end
    end
  end

  describe "sort_by/2" do
    test "sorts by group", %{df: df} do
      df = DF.sort_by(df, total)
      grouped_df = df |> DF.group_by("country") |> DF.sort_by(total)

      assert df["total"][0] == Series.min(df["total"])

      assert grouped_df
             |> DF.ungroup()
             |> DF.filter_with(&Series.equal(&1["country"], "HONDURAS"))
             |> DF.pull("total")
             |> Series.first() == 2175
    end

    test "sorts by group keeping nils last" do
      with_nil =
        DF.new(%{
          id: [1, 1, 1, 2, 2, 2, 3, 3, 3],
          data: [1, 0.5, nil, 0.7, 1, 0.9, 0.2, 0.2, 0.3]
        })

      grouped = DF.group_by(with_nil, :id, stable: true)

      assert grouped |> DF.sort_by(data, nils: :last) |> DF.to_columns(atom_keys: true) == %{
               data: [0.5, 1.0, nil, 0.7, 0.9, 1.0, 0.2, 0.2, 0.3],
               id: [1, 1, 1, 2, 2, 2, 3, 3, 3]
             }
    end
  end

  describe "sort_with/2" do
    test "sorts by group", %{df: df} do
      grouped_df =
        df
        |> DF.group_by("country")
        |> DF.sort_with(fn ldf -> [asc: ldf["total"]] end)

      assert grouped_df
             |> DF.ungroup()
             |> DF.filter_with(&Series.equal(&1["country"], "HONDURAS"))
             |> DF.pull("total")
             |> Series.first() == 2175
    end
  end

  describe "mutate/2" do
    test "adds a new column when there is a group" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"], c: [1, 1, 2])

      df1 = DF.group_by(df, :c)
      df2 = DF.mutate(df1, d: -7.1)

      assert DF.to_columns(df2, atom_keys: true) == %{
               a: [1, 2, 3],
               b: ["a", "b", "c"],
               c: [1, 1, 2],
               d: [-7.1, -7.1, -7.1]
             }

      assert df2.names == ["a", "b", "c", "d"]
      assert df2.dtypes == %{"a" => {:s, 64}, "b" => :string, "c" => {:s, 64}, "d" => {:f, 64}}
      assert df2.groups.columns == ["c"]
    end

    test "adds a new column with aggregation when there is a group" do
      df = DF.new(a: [1, 2, 3, 4], b: ["a", "b", "c", "d"], c: [1, 1, 2, 2])

      df1 = DF.group_by(df, :c)
      df2 = DF.mutate(df1, d: mean(a))

      assert DF.to_columns(df2, atom_keys: true) == %{
               a: [1, 2, 3, 4],
               b: ["a", "b", "c", "d"],
               c: [1, 1, 2, 2],
               d: [1.5, 1.5, 3.5, 3.5]
             }

      assert df2.names == ["a", "b", "c", "d"]
      assert df2.dtypes == %{"a" => {:s, 64}, "b" => :string, "c" => {:s, 64}, "d" => {:f, 64}}
      assert df2.groups.columns == ["c"]
    end
  end

  describe "mutate_with/2" do
    test "adds new columns when there is a group" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"], c: [1, 1, 2])

      df1 = DF.group_by(df, :c)

      df2 =
        DF.mutate_with(df1, fn ldf ->
          [d: Series.add(ldf["a"], -7.1), e: Series.count(ldf["c"])]
        end)

      assert DF.to_columns(df2, atom_keys: true) == %{
               a: [1, 2, 3],
               b: ["a", "b", "c"],
               c: [1, 1, 2],
               d: [-6.1, -5.1, -4.1],
               e: [2, 2, 1]
             }

      assert df2.names == ["a", "b", "c", "d", "e"]

      assert df2.dtypes == %{
               "a" => {:s, 64},
               "b" => :string,
               "c" => {:s, 64},
               "d" => {:f, 64},
               "e" => {:u, 32}
             }

      assert df2.groups.columns == ["c"]
    end

    test "adds new columns with window functions" do
      df = DF.new(a: Enum.to_list(1..10), z: [1, 1, 1, 1, 1, 2, 2, 2, 2, 2])
      df1 = DF.group_by(df, :z)

      df2 =
        DF.mutate_with(df1, fn ldf ->
          a = ldf["a"]

          [
            b: Series.window_max(a, 2, weights: [1.0, 2.0]),
            c: Series.window_mean(a, 2, weights: [0.25, 0.75]),
            d: Series.window_median(a, 2, weights: [0.25, 0.75]),
            e: Series.window_min(a, 2, weights: [1.0, 2.0]),
            f: Series.window_sum(a, 2, weights: [1.0, 2.0]),
            g: Series.window_standard_deviation(a, 2),
            p: Series.cumulative_max(a),
            q: Series.cumulative_min(a),
            r: Series.cumulative_sum(a),
            s: Series.cumulative_max(a, reverse: true),
            t: Series.cumulative_product(a)
          ]
        end)

      assert DF.to_columns(df2, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
               b: [1.0, 4.0, 6.0, 8.0, 10.0, 6, 14.0, 16.0, 18.0, 20.0],
               c: [0.25, 1.75, 2.75, 3.75, 4.75, 1.5, 6.75, 7.75, 8.75, 9.75],
               d: [1.5, 1.5, 2.5, 3.5, 4.5, 6.5, 6.5, 7.5, 8.5, 9.5],
               e: [1.0, 1.0, 2.0, 3.0, 4.0, 6.0, 6.0, 7.0, 8.0, 9.0],
               f: [1.0, 5.0, 8.0, 11.0, 14.0, 6.0, 20.0, 23.0, 26.0, 29.0],
               g: [
                 nil,
                 0.7071067811865476,
                 0.7071067811865476,
                 0.7071067811865476,
                 0.7071067811865476,
                 nil,
                 0.7071067811865476,
                 0.7071067811865476,
                 0.7071067811865476,
                 0.7071067811865476
               ],
               p: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
               q: [1, 1, 1, 1, 1, 6, 6, 6, 6, 6],
               r: [1, 3, 6, 10, 15, 6, 13, 21, 30, 40],
               s: [5, 5, 5, 5, 5, 10, 10, 10, 10, 10],
               t: [1, 2, 6, 24, 120, 6, 42, 336, 3024, 30240],
               z: [1, 1, 1, 1, 1, 2, 2, 2, 2, 2]
             }
    end
  end

  describe "distinct/2" do
    # Distinct does not behave differently when used in a DF with groups.
    # The only difference is that the groups are kept.
    test "with one group", %{df: df} do
      df1 = DF.group_by(df, "year")

      df2 = DF.distinct(df1, [:country])
      assert DF.names(df2) == ["year", "country"]
      assert DF.groups(df2) == ["year"]
      assert DF.shape(df2) == {222, 2}
    end

    test "with one group and distinct as the same", %{df: df} do
      df1 = DF.group_by(df, "country")
      df2 = DF.distinct(df1, [:country])

      assert DF.names(df2) == ["country"]
      assert DF.groups(df2) == ["country"]
      assert DF.shape(df2) == {222, 1}
    end

    test "multiple groups and different distinct", %{df: df} do
      df1 = DF.group_by(df, ["country", "year"])

      rows_count = df1[:bunker_fuels] |> Series.n_distinct()

      df2 = DF.distinct(df1, [:bunker_fuels])
      assert DF.names(df2) == ["country", "year", "bunker_fuels"]
      assert DF.groups(df2) == ["country", "year"]
      assert DF.shape(df2) == {rows_count, 3}
    end

    test "with groups and keeping all", %{df: df} do
      df1 = DF.group_by(df, "year")

      df2 = DF.distinct(df1, [:country], keep_all: true)

      assert DF.names(df2) == [
               "year",
               "country",
               "total",
               "solid_fuel",
               "liquid_fuel",
               "gas_fuel",
               "cement",
               "gas_flaring",
               "per_capita",
               "bunker_fuels"
             ]

      assert DF.groups(df2) == ["year"]

      assert DF.shape(df2) == {222, 10}
    end

    test "with distinct by two columns", %{df: df} do
      df1 = DF.group_by(df, "year")

      df2 = DF.distinct(df1, [:year, :country])

      assert DF.groups(df2) == ["year"]
      assert DF.names(df2) == ["year", "country"]

      assert DF.shape(df2) == {1094, 2}
    end
  end

  describe "filter_with/2" do
    test "filter with an aggregation and a group" do
      df = DF.new(col1: ["a", "a", "b", "b"], col2: [1, 2, 3, 4])
      grouped = DF.group_by(df, "col1")

      df1 =
        DF.filter_with(grouped, fn df -> Series.greater(df["col2"], Series.mean(df["col2"])) end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               col1: ["a", "b"],
               col2: [2, 4]
             }

      assert DF.groups(df1) == ["col1"]
    end
  end

  describe "select/2" do
    test "trying to select only a column while having another group does not have effect" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])
      grouped = DF.group_by(df, "b")
      df1 = DF.select(grouped, ["a"])

      assert DF.names(df1) == ["a", "b"]
      assert DF.groups(df1) == ["b"]
    end
  end

  describe "discard/2" do
    test "trying to discard a group does not have effect" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])
      grouped = DF.group_by(df, "b")
      df1 = DF.discard(grouped, ["b"])

      assert DF.names(df1) == ["a", "b"]
      assert DF.groups(df1) == ["b"]
    end
  end

  describe "head/2" do
    test "selects the first 5 rows of each group by default", %{df: df} do
      df1 = DF.group_by(df, ["year"])
      df2 = DF.head(df1)
      # 2010..2014
      assert DF.shape(df2) == {25, 10}
    end

    test "selects the first 2 rows", %{df: df} do
      df1 = DF.group_by(df, ["year"])
      df2 = DF.head(df1, 2)
      assert DF.shape(df2) == {10, 10}
    end
  end

  describe "tail/2" do
    test "selects the last 5 rows of each group by default", %{df: df} do
      df1 = DF.group_by(df, ["year"])
      df2 = DF.tail(df1)
      # 2010..2014
      assert DF.shape(df2) == {25, 10}
    end

    test "selects the last 2 rows", %{df: df} do
      df1 = DF.group_by(df, ["year"])
      df2 = DF.tail(df1, 2)
      assert DF.shape(df2) == {10, 10}
    end
  end

  describe "shape/1" do
    test "does not consider groups when counting rows", %{df: df} do
      df1 = DF.group_by(df, ["year"])

      assert DF.shape(df1) == {1094, 10}
      assert DF.shape(df) == DF.shape(df1)
    end
  end

  describe "size/1, count/1, and count_nil/1" do
    test "work with `nil` and `:nan` correctly" do
      df =
        DF.new(
          a: [1, 2, 3],
          b: [1, nil, 3],
          c: [1, :nan, 3],
          group: [1, 1, 2]
        )
        |> DF.group_by(:group, stable: true)

      assert df
             |> DF.summarise(
               a_count: count(a),
               a_nil_count: nil_count(a),
               a_size: size(a),
               b_nil_count: nil_count(b),
               b_count: count(b),
               b_size: size(b),
               c_count: count(c),
               c_nil_count: nil_count(c),
               c_size: size(c)
             )
             |> DF.to_columns(atom_keys: true) == %{
               a_count: [2, 1],
               a_nil_count: [0, 0],
               a_size: [2, 1],
               b_count: [1, 1],
               b_nil_count: [1, 0],
               b_size: [2, 1],
               c_count: [2, 1],
               c_nil_count: [0, 0],
               c_size: [2, 1],
               group: [1, 2]
             }
    end
  end

  describe "n_columns/1" do
    test "groups don't affect counting of columns", %{df: df} do
      df1 = DF.group_by(df, ["year"])

      assert DF.n_columns(df1) == 10
      assert DF.n_columns(df) == DF.n_columns(df1)
    end
  end

  describe "n_rows/1" do
    test "does not consider groups when counting rows", %{df: df} do
      df1 = DF.group_by(df, ["year"])

      assert DF.n_rows(df1) == 1094
      assert DF.n_rows(df) == DF.n_rows(df1)
    end
  end

  describe "pull/2" do
    test "does not consider groups when counting rows", %{df: df} do
      df1 = DF.group_by(df, ["year"])

      assert Series.to_list(DF.pull(df1, "country")) == Series.to_list(DF.pull(df, "country"))
    end
  end

  describe "rename/2" do
    test "renames groups as well" do
      df = DF.new(a: ["a", "b", "a"], b: [1, 3, 1])
      df1 = DF.group_by(df, "b")
      df2 = DF.rename(df1, b: :my_group)

      assert DF.names(df2) == ["a", "my_group"]
      assert DF.groups(df2) == ["my_group"]
    end
  end

  describe "rename_with/2" do
    test "renames groups as well" do
      df = DF.new(a: ["a", "b", "a"], b: [1, 3, 1])
      df1 = DF.group_by(df, "b")
      df2 = DF.rename_with(df1, &String.upcase/1)

      assert DF.names(df2) == ["A", "B"]
      assert DF.groups(df2) == ["B"]
    end
  end

  describe "sample/3" do
    test "sample 2 from each group" do
      df = Datasets.iris()
      grouped = DF.group_by(df, "species")
      grouped1 = DF.sample(grouped, 2, seed: 100)

      assert DF.n_rows(grouped1) == 6
    end

    test "sample 0.1 from each group" do
      df = Datasets.iris()
      grouped = DF.group_by(df, "species")
      grouped1 = DF.sample(grouped, 0.1, seed: 100)

      assert DF.n_rows(grouped1) == 15
    end

    test "sample more than the size from each group without replacement" do
      df = Datasets.iris()
      grouped = DF.group_by(df, "species")

      assert_raise RuntimeError, ~r/cannot take a larger sample than the total population/, fn ->
        DF.sample(grouped, 60, seed: 100)
      end

      assert_raise RuntimeError, ~r/cannot take a larger sample than the total population/, fn ->
        DF.sample(grouped, 1.2, seed: 100)
      end
    end

    test "sample more than the size from each group with replacement" do
      df = Datasets.iris()
      grouped = DF.group_by(df, "species")
      grouped1 = DF.sample(grouped, 60, seed: 100, replace: true)

      assert DF.n_rows(grouped1) == 180
    end

    test "sample more than 100% from each group with replacement" do
      df = Datasets.iris()
      grouped = DF.group_by(df, "species")
      grouped1 = DF.sample(grouped, 1.2, seed: 100, replace: true)

      assert DF.n_rows(grouped1) == 180
    end
  end

  describe "slice/2" do
    test "take two by indices of each group" do
      df = Datasets.iris()
      grouped = DF.group_by(df, "species", stable: true)
      grouped1 = DF.slice(grouped, [0, 2])

      assert DF.n_rows(grouped1) == 6

      assert Series.to_list(grouped1["species"]) == [
               "Iris-setosa",
               "Iris-setosa",
               "Iris-versicolor",
               "Iris-versicolor",
               "Iris-virginica",
               "Iris-virginica"
             ]

      assert DF.groups(grouped1) == ["species"]
    end

    test "take two by series of indices of each group" do
      df = Datasets.iris()
      grouped = DF.group_by(df, "species", stable: true)
      grouped1 = DF.slice(grouped, Series.from_list([0, 2]))

      assert DF.n_rows(grouped1) == 6

      assert Series.to_list(grouped1["species"]) == [
               "Iris-setosa",
               "Iris-setosa",
               "Iris-versicolor",
               "Iris-versicolor",
               "Iris-virginica",
               "Iris-virginica"
             ]

      assert DF.groups(grouped1) == ["species"]
    end

    test "take the range of zero to two per group" do
      df = Datasets.iris()
      grouped = DF.group_by(df, "species")
      grouped1 = DF.slice(grouped, 0..2)

      assert DF.n_rows(grouped1) == 9

      assert Series.to_list(grouped1["species"]) == [
               "Iris-setosa",
               "Iris-setosa",
               "Iris-setosa",
               "Iris-versicolor",
               "Iris-versicolor",
               "Iris-versicolor",
               "Iris-virginica",
               "Iris-virginica",
               "Iris-virginica"
             ]

      assert DF.groups(grouped1) == ["species"]
    end
  end

  describe "slice/3" do
    test "take first two of each group" do
      df = Datasets.iris()
      grouped = DF.group_by(df, "species", stable: true)
      grouped1 = DF.slice(grouped, 0, 2)

      assert DF.n_rows(grouped1) == 6

      assert Series.to_list(grouped1["species"]) == [
               "Iris-setosa",
               "Iris-setosa",
               "Iris-versicolor",
               "Iris-versicolor",
               "Iris-virginica",
               "Iris-virginica"
             ]

      assert DF.groups(grouped1) == ["species"]
    end

    test "take two of each group starting with negative index" do
      df = Datasets.iris()
      grouped = DF.group_by(df, "species", stable: true)
      grouped1 = DF.slice(grouped, -6, 2)

      assert DF.n_rows(grouped1) == 6

      assert Series.to_list(grouped1["species"]) == [
               "Iris-setosa",
               "Iris-setosa",
               "Iris-versicolor",
               "Iris-versicolor",
               "Iris-virginica",
               "Iris-virginica"
             ]

      assert DF.groups(grouped1) == ["species"]
    end
  end

  describe "to_csv/2" do
    @tag :tmp_dir
    test "does not consider groups when saving file", %{df: df, tmp_dir: tmp_dir} do
      ungrouped_file_path = Path.join(tmp_dir, "ungrouped-tmp.csv")
      :ok = DF.to_csv(df, ungrouped_file_path)

      grouped_df = DF.group_by(df, "year")
      grouped_file_path = Path.join(tmp_dir, "grouped-tmp.csv")

      assert :ok = DF.to_csv(grouped_df, grouped_file_path)

      # Files with the same content
      assert File.read!(ungrouped_file_path) == File.read!(grouped_file_path)
    end
  end

  describe "to_ipc/2" do
    @tag :tmp_dir
    test "does not consider groups when saving file", %{df: df, tmp_dir: tmp_dir} do
      ungrouped_file_path = Path.join(tmp_dir, "ungrouped-tmp.ipc")
      :ok = DF.to_ipc(df, ungrouped_file_path)

      grouped_df = DF.group_by(df, "year")
      grouped_file_path = Path.join(tmp_dir, "grouped-tmp.ipc")

      assert :ok = DF.to_ipc(grouped_df, grouped_file_path)

      # Files with the same content
      assert File.read!(ungrouped_file_path) == File.read!(grouped_file_path)
    end
  end

  describe "to_ndjson/2" do
    @tag :tmp_dir
    test "does not consider groups when saving file", %{df: df, tmp_dir: tmp_dir} do
      ungrouped_file_path = Path.join(tmp_dir, "ungrouped-tmp.ndjson")
      :ok = DF.to_ndjson(df, ungrouped_file_path)

      grouped_df = DF.group_by(df, "year")
      grouped_file_path = Path.join(tmp_dir, "grouped-tmp.ndjson")

      assert :ok = DF.to_ndjson(grouped_df, grouped_file_path)

      # Files with the same content
      assert File.read!(ungrouped_file_path) == File.read!(grouped_file_path)
    end
  end

  describe "to_parquet/2" do
    @tag :tmp_dir
    test "does not consider groups when saving file", %{df: df, tmp_dir: tmp_dir} do
      ungrouped_file_path = Path.join(tmp_dir, "ungrouped-tmp.parquet")
      :ok = DF.to_parquet(df, ungrouped_file_path)

      grouped_df = DF.group_by(df, "year")
      grouped_file_path = Path.join(tmp_dir, "grouped-tmp.parquet")

      assert :ok = DF.to_parquet(grouped_df, grouped_file_path)

      # Files with the same content
      assert File.read!(ungrouped_file_path) == File.read!(grouped_file_path)
    end
  end

  describe "dump_csv/2" do
    test "does not consider groups when dumping DF", %{df: df} do
      dumped_csv = DF.dump_csv!(df)

      grouped_df = DF.group_by(df, "year")

      assert DF.dump_csv!(grouped_df) == dumped_csv
    end
  end

  describe "pivot_longer/3" do
    test "keep the groups if they are not in the list of pivoting" do
      df = Datasets.iris()
      grouped = DF.group_by(df, "species")
      pivoted = DF.pivot_longer(grouped, ["sepal_length"])

      assert DF.groups(pivoted) == ["species"]
    end

    test "remove groups that are in the list of pivoting" do
      df = Datasets.iris()
      grouped = DF.group_by(df, "species")
      pivoted = DF.pivot_longer(grouped, ["species"])

      assert DF.groups(pivoted) == []
    end
  end

  describe "pivot_wider/4" do
    test "keep the groups if they are not in the list of pivoting" do
      df =
        DF.new(
          weekday: [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday"
          ],
          team: ["A", "B", "C", "A", "B", "C", "A", "B", "C", "A"],
          hour: [10, 9, 10, 10, 11, 15, 14, 16, 14, 16]
        )

      grouped = DF.group_by(df, "team")
      pivoted = DF.pivot_wider(grouped, "weekday", "hour")

      assert DF.groups(pivoted) == ["team"]
    end

    test "remove groups that are in the list of pivoting" do
      df =
        DF.new(
          weekday: [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday"
          ],
          team: ["A", "B", "C", "A", "B", "C", "A", "B", "C", "A"],
          hour: [10, 9, 10, 10, 11, 15, 14, 16, 14, 16]
        )

      grouped = DF.group_by(df, "weekday")
      pivoted = DF.pivot_wider(grouped, "weekday", "hour")

      assert DF.groups(pivoted) == []
    end

    test "remove groups that are not in the list of id_columns" do
      df =
        DF.new(
          weekday: [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday"
          ],
          team: ["A", "B", "C", "A", "B", "C", "A", "B", "C", "A"],
          other: [1, 2, 1, 2, 1, 2, 1, 2, 1, 2],
          hour: [10, 9, 10, 10, 11, 15, 14, 16, 14, 16]
        )

      grouped = DF.group_by(df, ["team", "other"])
      pivoted = DF.pivot_wider(grouped, "weekday", "hour", id_columns: ["team"])

      assert DF.names(pivoted) == ["team", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
      assert DF.groups(pivoted) == ["team"]
    end
  end

  describe "dummies/2" do
    test "drops the groups" do
      df = DF.new(col_x: ["a", "b", "a", "c"], col_y: ["b", "a", "b", "d"])
      grouped = DF.group_by(df, "col_x")
      dummies = DF.dummies(grouped, "col_x")

      assert DF.groups(dummies) == []
    end
  end

  describe "join/4" do
    test "inner join keep groups from left" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(a: [1, 2, 2], c: ["d", "e", "f"])

      grouped_left = DF.group_by(left, "b")
      grouped_right = DF.group_by(right, "c")

      joined = DF.join(grouped_left, grouped_right)

      assert DF.groups(joined) == ["b"]
    end

    test "left join keep groups from left" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(a: [1, 2, 2], c: ["d", "e", "f"])

      grouped_left = DF.group_by(left, "b")
      grouped_right = DF.group_by(right, "c")

      joined = DF.join(grouped_left, grouped_right, how: :left)

      assert DF.groups(joined) == ["b"]
    end

    test "right join keep groups from right" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(a: [1, 2, 4], c: ["d", "e", "f"])

      grouped_left = DF.group_by(left, "b")
      grouped_right = DF.group_by(right, "c")

      joined = DF.join(grouped_left, grouped_right, how: :right)

      assert DF.groups(joined) == ["c"]
    end

    test "outer join keep groups from the left" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(a: [1, 2, 4], c: ["d", "e", "f"])

      grouped_left = DF.group_by(left, "b")
      grouped_right = DF.group_by(right, "c")

      joined = DF.join(grouped_left, grouped_right, how: :outer)

      assert DF.groups(joined) == ["b"]
    end

    test "cross join keep groups from the left" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(a: [1, 2, 4], c: ["d", "e", "f"])

      grouped_left = DF.group_by(left, "b")
      grouped_right = DF.group_by(right, "c")

      joined = DF.join(grouped_left, grouped_right, how: :cross)

      assert DF.groups(joined) == ["b"]
    end
  end

  describe "concat_rows/1" do
    test "keep groups from the first dataframe" do
      first = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      second = DF.new(a: [4, 5, 6], b: ["d", "e", "f"])

      grouped_first = DF.group_by(first, :a)
      grouped_second = DF.group_by(second, :b)

      stacked = DF.concat_rows([grouped_first, grouped_second])

      assert DF.groups(stacked) == ["a"]
      assert DF.dtypes(stacked) == %{"a" => {:s, 64}, "b" => :string}
      assert DF.n_rows(stacked) == 6
    end

    test "keep groups even with cast of one column" do
      first = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      second = DF.new(a: [4.1, 5.2, 6.5], b: ["d", "e", "f"])

      grouped_first = DF.group_by(first, :a)
      grouped_second = DF.group_by(second, :b)

      stacked = DF.concat_rows([grouped_first, grouped_second])

      assert DF.groups(stacked) == ["a"]
      assert DF.dtypes(stacked) == %{"a" => {:f, 64}, "b" => :string}
      assert DF.n_rows(stacked) == 6
    end
  end

  describe "concat_columns/1" do
    test "keep groups from the first dataframe" do
      first = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      second = DF.new(c: [4, 5, 6], d: [0.3, 0.2, 0.1])

      grouped_first = DF.group_by(first, :a)
      grouped_second = DF.group_by(second, :d)

      stacked = DF.concat_columns([grouped_first, grouped_second])

      assert DF.groups(stacked) == ["a"]

      assert DF.dtypes(stacked) == %{
               "a" => {:s, 64},
               "b" => :string,
               "c" => {:s, 64},
               "d" => {:f, 64}
             }

      assert DF.n_rows(stacked) == 3
      assert DF.n_columns(stacked) == 4
    end

    test "keep groups even with duplication of one column" do
      first = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      second = DF.new(a: [4.1, 5.2, 6.5], d: [10, 9, 8])

      grouped_first = DF.group_by(first, :a)
      grouped_second = DF.group_by(second, :d)

      stacked = DF.concat_columns([grouped_first, grouped_second])

      assert DF.groups(stacked) == ["a"]

      assert DF.dtypes(stacked) == %{
               "a" => {:s, 64},
               "b" => :string,
               "a_1" => {:f, 64},
               "d" => {:s, 64}
             }

      assert DF.n_rows(stacked) == 3
      assert DF.n_columns(stacked) == 4
    end
  end

  test "lazy/1", %{df: df} do
    grouped = DF.group_by(df, ["country", "year"])
    assert ["country", "year"] = DF.lazy(grouped).groups.columns
  end

  describe "put/3" do
    test "adds a new column to a dataframe" do
      df = DF.new(a: [1, 2, 3], b: [4, 5, 6])
      grouped = DF.group_by(df, "a")

      df1 = DF.put(grouped, :c, Series.from_list(~w(a b c)))

      assert DF.names(df1) == ["a", "b", "c"]
      assert DF.dtypes(df1) == %{"a" => {:s, 64}, "b" => {:s, 64}, "c" => :string}
      assert DF.groups(df1) == ["a"]

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3],
               b: [4, 5, 6],
               c: ["a", "b", "c"]
             }
    end

    test "replaces a column in the dataframe" do
      df = DF.new(a: [1, 2, 3], b: [4, 5, 6])
      grouped = DF.group_by(df, "a")

      df1 = DF.put(grouped, :b, Series.from_list([10, 10, 10]))

      assert DF.names(df1) == ["a", "b"]
      assert DF.dtypes(df1) == %{"a" => {:s, 64}, "b" => {:s, 64}}
      assert DF.groups(df1) == ["a"]

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3],
               b: [10, 10, 10]
             }
    end
  end
end
