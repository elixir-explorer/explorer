defmodule Explorer.DataFrameTest do
  use ExUnit.Case, async: true

  # Tests for most IO operations are in the data_frame folder
  # Tests for summarise, group, ungroup are available in grouped_test.exs

  # Doctests assume the module has been required
  require Explorer.DataFrame
  doctest Explorer.DataFrame

  import ExUnit.CaptureIO
  alias Explorer.DataFrame, as: DF
  alias Explorer.Datasets
  alias Explorer.Series

  setup do
    {:ok, df: Datasets.fossil_fuels()}
  end

  test "defines doc metadata" do
    {:docs_v1, _, :elixir, "text/markdown", _docs, _metadata, entries} =
      Code.fetch_docs(Explorer.DataFrame)

    for {{:function, name, arity}, _ann, _signature, docs, metadata} <- entries,
        is_map(docs) and map_size(docs) > 0,
        metadata[:type] not in [:single, :multi, :introspection, :io, :rows, :conversion] do
      flunk("invalid @doc type: #{inspect(metadata[:type])} for #{name}/#{arity}")
    end
  end

  describe "from_query/3" do
    alias Adbc.{Database, Connection}

    setup do
      db = start_supervised!({Database, driver: :sqlite})
      conn = start_supervised!({Connection, database: db})
      [conn: conn]
    end

    test "queries database", %{conn: conn} do
      {:ok, %DF{} = df} =
        Explorer.DataFrame.from_query(conn, "SELECT 123 as num, 'abc' as str", [])

      assert DF.to_columns(df, atom_keys: true) == %{
               num: [123],
               str: ["abc"]
             }
    end

    test "returns error", %{conn: conn} do
      assert {:error, %Adbc.Error{} = error} =
               Explorer.DataFrame.from_query(conn, "INVALID SQL", [])

      assert Exception.message(error) =~ "syntax error"
    end

    test "queries database!", %{conn: conn} do
      assert Explorer.DataFrame.from_query!(conn, "SELECT 123 as num, 'abc' as str", [])
             |> DF.to_columns(atom_keys: true) == %{
               num: [123],
               str: ["abc"]
             }
    end

    test "returns error!", %{conn: conn} do
      assert_raise Adbc.Error, ~r/syntax error/, fn ->
        Explorer.DataFrame.from_query!(conn, "INVALID SQL", [])
      end
    end
  end

  describe "new/1" do
    test "from series" do
      df = DF.new(%{floats: Series.from_list([1.0, 2.0]), integers: Series.from_list([1, 2])})

      assert DF.to_columns(df, atom_keys: true) == %{floats: [1.0, 2.0], integers: [1, 2]}
      assert DF.dtypes(df) == %{"floats" => {:f, 64}, "integers" => {:s, 64}}
    end

    test "from columnar data" do
      df = DF.new(%{floats: [1.0, 2.0], integers: [1, nil]})

      assert DF.to_columns(df, atom_keys: true) == %{floats: [1.0, 2.0], integers: [1, nil]}
      assert DF.dtypes(df) == %{"floats" => {:f, 64}, "integers" => {:s, 64}}
    end

    test "from columnar data with binaries" do
      df =
        DF.new(%{floats: [1.0, 2.0], integers: [1, nil], binaries: [<<239, 191, 19>>, nil]},
          dtypes: [{:binaries, :binary}]
        )

      assert DF.to_columns(df, atom_keys: true) == %{
               floats: [1.0, 2.0],
               integers: [1, nil],
               binaries: [<<239, 191, 19>>, nil]
             }

      assert DF.dtypes(df) == %{
               "floats" => {:f, 64},
               "integers" => {:s, 64},
               "binaries" => :binary
             }
    end

    test "from rows" do
      rows = [
        %{
          lower: 17,
          middle: 17.5,
          outliers: <<131, 107, 0, 8, 12, 12, 23, 24, 25, 25, 26, 27>>,
          upper: 19,
          ymax: 22,
          ymin: 14
        },
        %{
          lower: 12,
          middle: 15.5,
          outliers: <<131, 107, 0, 8, 9, 5, 23, 24, 25, 25, 26, 27>>,
          upper: 20.3,
          ymax: 22,
          ymin: 10
        },
        %{
          lower: 22.2,
          middle: 26.5,
          outliers: <<131, 107, 0, 2, 11, 13, 38, 72, 36, 88, 55, 90>>,
          upper: 31.7,
          ymax: 35,
          ymin: 16
        }
      ]

      df = DF.new(rows, dtypes: [{:outliers, :binary}, {:ymax, {:f, 64}}, {:ymin, {:f, 64}}])

      assert DF.to_columns(df, atom_keys: true) == %{
               lower: [17.0, 12.0, 22.2],
               middle: [17.5, 15.5, 26.5],
               outliers: [
                 <<131, 107, 0, 8, 12, 12, 23, 24, 25, 25, 26, 27>>,
                 <<131, 107, 0, 8, 9, 5, 23, 24, 25, 25, 26, 27>>,
                 <<131, 107, 0, 2, 11, 13, 38, 72, 36, 88, 55, 90>>
               ],
               upper: [19.0, 20.3, 31.7],
               ymax: [22.0, 22.0, 35.0],
               ymin: [14.0, 10.0, 16.0]
             }

      assert DF.dtypes(df) == %{
               "lower" => {:f, 64},
               "middle" => {:f, 64},
               "outliers" => :binary,
               "upper" => {:f, 64},
               "ymax" => {:f, 64},
               "ymin" => {:f, 64}
             }
    end

    test "with integers" do
      df = DF.new(%{integers: [1, 2, 3]})

      assert DF.to_columns(df, atom_keys: true) == %{integers: [1, 2, 3]}
      assert DF.dtypes(df) == %{"integers" => {:s, 64}}
    end

    test "with floats" do
      df = DF.new(%{floats: [1, 2.4, 3]})

      assert DF.to_columns(df, atom_keys: true) == %{floats: [1, 2.4, 3]}
      assert DF.dtypes(df) == %{"floats" => {:f, 64}}
    end

    test "with binaries" do
      df =
        DF.new(%{binaries: [<<239, 191, 19>>, <<228, 146, 51>>]}, dtypes: [{:binaries, :binary}])

      assert DF.to_columns(df, atom_keys: true) == %{
               binaries: [<<239, 191, 19>>, <<228, 146, 51>>]
             }

      assert DF.dtypes(df) == %{"binaries" => :binary}
    end

    test "with strings" do
      df = DF.new(%{strings: ["a", "b", "c"]})

      assert DF.to_columns(df, atom_keys: true) == %{strings: ["a", "b", "c"]}
      assert DF.dtypes(df) == %{"strings" => :string}
    end

    test "with binaries from strings" do
      df =
        DF.new(%{binaries_from_strings: ["a", "b", "c"]},
          dtypes: [{:binaries_from_strings, :binary}]
        )

      assert DF.to_columns(df, atom_keys: true) == %{binaries_from_strings: ["a", "b", "c"]}
      assert DF.dtypes(df) == %{"binaries_from_strings" => :binary}
    end

    test "mixing binaries and strings" do
      df =
        DF.new(%{strings_and_binaries: [<<228, 146, 51>>, "hello", <<42, 209, 236>>]},
          dtypes: [{:strings_and_binaries, :binary}]
        )

      assert DF.to_columns(df, atom_keys: true) == %{
               strings_and_binaries: [<<228, 146, 51>>, "hello", <<42, 209, 236>>]
             }

      assert DF.dtypes(df) == %{"strings_and_binaries" => :binary}
    end

    test "mixing types" do
      assert_raise ArgumentError, fn ->
        DF.new(%{mixing_types: [1, "foo", 3]})
      end
    end

    test "with binaries without passing the dtypes" do
      assert_raise ArgumentError, fn ->
        DF.new(%{binaries: [<<228, 146, 51>>, <<22, 197, 116>>, <<42, 209, 236>>]})
      end
    end

    test "with series of nils and dtype string" do
      df =
        DF.new(%{strings: [nil, nil, nil]},
          dtypes: [{:strings, :string}]
        )

      assert DF.to_columns(df, atom_keys: true) == %{
               strings: [nil, nil, nil]
             }

      assert DF.dtypes(df) == %{"strings" => :string}
    end

    test "with series of nils and dtype integer" do
      df =
        DF.new(%{integers: [nil, nil, nil]},
          dtypes: [{:integers, :integer}]
        )

      assert DF.to_columns(df, atom_keys: true) == %{
               integers: [nil, nil, nil]
             }

      assert DF.dtypes(df) == %{"integers" => {:s, 64}}
    end

    test "with series of integers and dtype string" do
      df =
        DF.new(%{strings: [1, 2, 3]},
          dtypes: [{:strings, :string}]
        )

      assert DF.to_columns(df, atom_keys: true) == %{
               strings: ["1", "2", "3"]
             }

      assert DF.dtypes(df) == %{"strings" => :string}
    end
  end

  describe "mask/2" do
    test "raises with mask of invalid size", %{df: df} do
      assert_raise ArgumentError,
                   "size of the mask (3) must match number of rows in the dataframe (1094)",
                   fn -> DF.mask(df, [true, false, true]) end
    end
  end

  describe "filter_with/2" do
    test "filter columns with equal comparison" do
      df = DF.new(a: [1, 2, 3, 2], b: [5.3, 2.4, 1.0, 2.0])

      df1 = DF.filter_with(df, fn ldf -> Series.equal(ldf["a"], 2) end)
      assert DF.to_columns(df1, atom_keys: true) == %{a: [2, 2], b: [2.4, 2.0]}

      df2 = DF.filter_with(df, fn ldf -> Series.equal(1.0, ldf["b"]) end)
      assert DF.to_columns(df2, atom_keys: true) == %{a: [3], b: [1.0]}

      df3 =
        DF.filter_with(df, fn ldf -> Series.equal(ldf["a"], Series.from_list([1, 0, 3, 0])) end)

      assert DF.to_columns(df3, atom_keys: true) == %{a: [1, 3], b: [5.3, 1.0]}

      df4 =
        DF.filter_with(df, fn ldf -> Series.equal(Series.from_list([0, 0, 1.0, 0]), ldf["b"]) end)

      assert DF.to_columns(df4, atom_keys: true) == %{a: [3], b: [1.0]}

      df5 = DF.filter_with(df, fn ldf -> Series.equal(ldf["a"], ldf["b"]) end)
      assert DF.to_columns(df5, atom_keys: true) == %{a: [2], b: [2.0]}
    end

    test "raise an error if the last operation is an arithmetic operation" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9, 8, 7, 6, 5, 4, 3])

      message =
        "expecting the function to return a boolean LazySeries, but instead it returned a LazySeries of type {:f, 64}"

      assert_raise ArgumentError, message, fn ->
        DF.filter_with(df, fn ldf ->
          a = ldf["a"]

          Series.pow(a, 3)
        end)
      end
    end

    test "raise an error if the last operation is an aggregation operation" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9, 8, 7, 6, 5, 4, 3])

      message =
        "expecting the function to return a boolean LazySeries, " <>
          "but instead it returned a LazySeries of type {:s, 64}"

      assert_raise ArgumentError, message, fn ->
        DF.filter_with(df, fn ldf ->
          Series.sum(ldf["a"])
        end)
      end
    end

    test "raise an error if the function is not returning a lazy series" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9, 8, 7, 6, 5, 4, 3])

      message =
        "expecting the function to return a single or a list of boolean LazySeries, but instead it contains:\n:foo"

      assert_raise ArgumentError, message, fn ->
        DF.filter_with(df, fn _ldf -> :foo end)
      end
    end
  end

  describe "filter/2" do
    test "filter columns with equal comparison" do
      df = DF.new(a: [1, 2, 3, 2], b: [5.3, 2.4, 1.0, 2.0])

      df1 = DF.filter(df, a == 2)
      assert DF.to_columns(df1, atom_keys: true) == %{a: [2, 2], b: [2.4, 2.0]}

      df2 = DF.filter(df, 1.0 == b)
      assert DF.to_columns(df2, atom_keys: true) == %{a: [3], b: [1.0]}

      df3 = DF.filter(df, a == Series.from_list([1, 0, 3, 0]))

      assert DF.to_columns(df3, atom_keys: true) == %{a: [1, 3], b: [5.3, 1.0]}

      df4 = DF.filter(df, Series.from_list([0, 0, 1.0, 0]) == b)

      assert DF.to_columns(df4, atom_keys: true) == %{a: [3], b: [1.0]}

      df5 = DF.filter(df, a == b)
      assert DF.to_columns(df5, atom_keys: true) == %{a: [2], b: [2.0]}
    end

    test "filter columns with other comparison ops" do
      df = DF.new(a: [1, 2, 3, 2], b: [5.3, 2.4, 1.0, 2.0])

      df1 = DF.filter(df, a != 2)
      assert DF.to_columns(df1, atom_keys: true) == %{a: [1, 3], b: [5.3, 1.0]}

      df2 = DF.filter(df, a > 2)
      assert DF.to_columns(df2, atom_keys: true) == %{a: [3], b: [1.0]}

      df3 = DF.filter(df, a >= 2)
      assert DF.to_columns(df3, atom_keys: true) == %{a: [2, 3, 2], b: [2.4, 1.0, 2.0]}

      df4 = DF.filter(df, a < 2)
      assert DF.to_columns(df4, atom_keys: true) == %{a: [1], b: [5.3]}

      df5 = DF.filter(df, a <= 2)
      assert DF.to_columns(df5, atom_keys: true) == %{a: [1, 2, 2], b: [5.3, 2.4, 2.0]}
    end

    test "filter by a string value" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      df1 = DF.filter(df, b == "b")
      assert DF.to_columns(df1, atom_keys: true) == %{a: [2], b: ["b"]}
    end

    test "filter by a boolean value" do
      df = DF.new(a: [1, 2, 3], b: [true, true, false])

      df1 = DF.filter(df, b == false)
      assert DF.to_columns(df1, atom_keys: true) == %{a: [3], b: [false]}
    end

    test "filter by a given date" do
      df = DF.new(a: [1, 2, 3], b: [~D[2022-07-07], ~D[2022-07-08], ~D[2022-07-09]])

      df1 = DF.filter(df, b == ~D[2022-07-07])
      assert DF.to_columns(df1, atom_keys: true) == %{a: [1], b: [~D[2022-07-07]]}
    end

    test "filter by a given datetime" do
      df =
        DF.new(
          a: [1, 2, 3],
          b: [
            ~N[2022-07-07 17:43:08.473561],
            ~N[2022-07-07 17:44:13.020548],
            ~N[2022-07-07 17:45:00.116337]
          ]
        )

      df1 = DF.filter(df, b > ~N[2022-07-07 17:44:13.020548])

      assert DF.to_columns(df1, atom_keys: true) == %{a: [3], b: [~N[2022-07-07 17:45:00.116337]]}
    end

    test "filter with a complex boolean filter" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9, 8, 7, 6, 5, 4, 3])

      df1 = DF.filter(df, a > 5 or (a <= 2 and b != 9))

      assert DF.to_columns(df1, atom_keys: true) == %{a: [2, 6], b: [8, 4]}
    end

    test "filter with a complex series" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9, 8, 7, 6, 5, 4, 3])

      df2 =
        DF.filter(
          df,
          from_list([true, false, false, false, false, false, false]) or
            (a > 4 and from_list([true, false, false, false, false, true, true]))
        )

      assert DF.to_columns(df2, atom_keys: true) == %{a: [1, 6, 5], b: [9, 4, 3]}
    end

    test "filter for nil values" do
      df = DF.new(a: [1, 2, 3, nil, 5, nil, 5], b: [9, 8, 7, 6, 5, 4, 3])

      df1 = DF.filter(df, is_nil(a))

      assert DF.to_columns(df1, atom_keys: true) == %{a: [nil, nil], b: [6, 4]}
    end

    test "filter for not nil values" do
      df = DF.new(a: [1, 2, 3, nil, 5, nil, 5], b: [9, 8, 7, 6, 5, 4, 3])

      df1 = DF.filter(df, is_not_nil(a))

      assert DF.to_columns(df1, atom_keys: true) == %{a: [1, 2, 3, 5, 5], b: [9, 8, 7, 5, 3]}
    end

    test "filter with in" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9, 8, 7, 6, 5, 4, 3])

      df2 = DF.filter(df, a in b)

      assert DF.to_columns(df2, atom_keys: true) == %{a: [3, 4, 5, 6, 5], b: [7, 6, 5, 4, 3]}
    end

    test "filter with add operation" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9, 8, 7, 6, 5, 4, 3])

      df1 = DF.filter(df, a > b + 1)

      assert DF.to_columns(df1, atom_keys: true) == %{a: [6, 5], b: [4, 3]}
    end

    test "filter with subtract operation" do
      df = DF.new(a: [1.1, 2.2, 3.3, 4.4, 5.5, 6.5, 5.8], b: [9, 8, 7, 6, 5, 4, 3])

      df1 = DF.filter(df, a > b - a)

      assert DF.to_columns(df1, atom_keys: true) == %{a: [4.4, 5.5, 6.5, 5.8], b: [6, 5, 4, 3]}
    end

    test "filter with divide operation" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9, 8, 7, 6, 5, 4, 3])

      df1 = DF.filter(df, a > b / 3)

      assert DF.to_columns(df1, atom_keys: true) == %{a: [3, 4, 5, 6, 5], b: [7, 6, 5, 4, 3]}
    end

    test "filter with pow operation" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9.2, 8.0, 7.1, 6.0, 5.0, 4.0, 3.2])

      df1 = DF.filter(df, b == a ** 3)

      assert DF.to_columns(df1, atom_keys: true) == %{a: [2], b: [8.0]}
    end

    test "filter with count operation" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9.2, 8.0, 7.1, 6.0, 5.0, 4.0, 3.2])

      df1 = DF.filter(df, b > count(a))

      assert DF.to_columns(df1, atom_keys: true) == %{a: [1, 2, 3], b: [9.2, 8.0, 7.1]}
    end

    test "filter with max operation" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9.2, 8.0, 7.1, 6.0, 5.0, 4.0, 3.2])

      df1 = DF.filter(df, b > max(a))

      assert DF.to_columns(df1, atom_keys: true) == %{a: [1, 2, 3], b: [9.2, 8.0, 7.1]}
    end

    test "filter with last operation" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9.2, 8.0, 7.1, 6.0, 5.0, 4.0, 3.2])

      df1 = DF.filter(df, b > last(a))

      assert DF.to_columns(df1, atom_keys: true) == %{a: [1, 2, 3, 4], b: [9.2, 8.0, 7.1, 6.0]}
    end

    test "filter with coalesce operation" do
      df = DF.new(a: [1, nil, 3, nil], b: [nil, 2, nil, 4])

      df1 = DF.filter(df, greater(coalesce(a, b), 3))

      assert DF.to_columns(df1, atom_keys: true) == %{a: [nil], b: [4]}

      df2 = DF.filter(df, is_nil(coalesce(a, b)))

      assert DF.to_columns(df2, atom_keys: true) == %{a: [], b: []}
    end

    test "filter with window operation" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9.2, 8.0, 7.1, 6.0, 5.0, 4.0, 3.2])

      df1 = DF.filter(df, a > window_mean(a, 3))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [2, 3, 4, 5, 6],
               b: [8.0, 7.1, 6.0, 5.0, 4.0]
             }
    end

    test "filter with all_equal equals true" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6], b: [1, 2, 3, 4, 5, 6])

      df1 = DF.filter(df, all_equal(a, b))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5, 6],
               b: [1, 2, 3, 4, 5, 6]
             }
    end

    test "filter with all_equal equals false" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6], b: [1, 2, 3, 4, 5, 7])

      df1 = DF.filter(df, all_equal(a, b))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [],
               b: []
             }
    end

    test "filter with an aggregation and without a group" do
      df = DF.new(col1: ["a", "a", "b", "b"], col2: [1, 2, 3, 4])

      df1 = DF.filter(df, col2 > mean(col2))

      assert DF.to_columns(df1, atom_keys: true) == %{
               col1: ["b", "b"],
               col2: [3, 4]
             }

      assert DF.groups(df1) == []
    end

    test "filter with a list of predicates forming an AND" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9, 8, 7, 6, 5, 4, 3])

      df1 = DF.filter(df, [a > 4, b < 4])

      assert DF.to_columns(df1, atom_keys: true) == %{a: [5], b: [3]}
    end

    test "filter with a list of predicates, but one is not boolean" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9, 8, 7, 6, 5, 4, 3])

      error_message =
        "expecting the function to return a single or a list of boolean LazySeries, but instead it contains:\n" <>
          "#Explorer.Series<\n  LazySeries[???]\n  s64 (column(\"a\") + column(\"b\"))\n>"

      assert_raise ArgumentError, error_message, fn ->
        DF.filter(df, [a > 4, b < 4, a + b])
      end
    end

    test "filter a binary column with a binary value" do
      df =
        DF.new([a: [1, 2, 3, 4, 5], key: [<<1>>, <<2>>, <<1>>, <<1>>, <<2>>]],
          dtypes: [key: :binary]
        )

      df1 = DF.filter(df, key == <<2>>)

      assert DF.to_columns(df1, atom_keys: true) == %{a: [2, 5], key: [<<2>>, <<2>>]}
    end

    test "filter a binary column with a binary value from a string" do
      df =
        DF.new([a: [1, 2, 3, 4, 5], key: [<<1>>, <<2>>, <<1>>, <<1>>, <<2>>]],
          dtypes: [key: :binary]
        )

      df1 = DF.filter(df, key == "foo")

      assert DF.to_columns(df1, atom_keys: true) == %{a: [], key: []}
    end

    test "filter with a multiplication with the scalar on the left" do
      df = DF.new(a: [1, 2, 3, 4, 5, 6, 5], b: [9, 8, 7, 6, 5, 4, 3])

      df1 = DF.filter(df, b > 2 * a)

      assert DF.to_columns(df1, atom_keys: true) == %{a: [1, 2, 3], b: [9, 8, 7]}
    end
  end

  describe "mutate_with/2" do
    test "adds new columns" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      df1 =
        DF.mutate_with(df, fn ldf ->
          [c: Series.add(ldf["a"], 5), d: Series.add(2, ldf["a"])]
        end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3],
               b: ["a", "b", "c"],
               c: [6, 7, 8],
               d: [3, 4, 5]
             }

      assert df1.names == ["a", "b", "c", "d"]
      assert df1.dtypes == %{"a" => {:s, 64}, "b" => :string, "c" => {:s, 64}, "d" => {:s, 64}}
    end

    test "changes a column" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      df1 =
        DF.mutate_with(df, fn ldf ->
          [a: Series.cast(ldf["a"], {:f, 64})]
        end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1.0, 2.0, 3.0],
               b: ["a", "b", "c"]
             }

      assert df1.names == ["a", "b"]
      assert df1.dtypes == %{"a" => {:f, 64}, "b" => :string}
    end

    test "extracts a field from struct to new column" do
      df = DF.new([%{a: %{n: 1}}, %{a: %{n: 1}}])
      df2 = DF.mutate(df, n: field(a, "n"))
      assert df.dtypes == %{"a" => {:struct, [{"n", {:s, 64}}]}}
      assert df2.dtypes == %{"a" => {:struct, [{"n", {:s, 64}}]}, "n" => {:s, 64}}
    end

    test "throws error when a field is not found in struct" do
      df = DF.new([%{a: %{n: 1}}, %{a: %{n: 1}}])

      assert_raise ArgumentError, "field \"m\" not found in fields [\"n\"]", fn ->
        DF.mutate(df, n: field(a, "m"))
      end
    end
  end

  describe "mutate/2" do
    test "with nil" do
      df = DF.new(strs: ["a", "b", "c"], nums: [1, 2, 3])
      df1 = DF.mutate(df, c: nil)
      assert df1.names == ["strs", "nums", "c"]
      assert df1.dtypes == %{"strs" => :string, "nums" => {:s, 64}, "c" => :null}

      assert DF.to_columns(df1, atom_keys: true) == %{
               strs: ["a", "b", "c"],
               nums: [1, 2, 3],
               c: [nil, nil, nil]
             }
    end

    test "with map " do
      df = DF.new(%{a: [1, nil, 3], b: ["a", "b", nil]})
      df1 = DF.mutate(df, c: %{a: a, b: b, lit: 1, null: is_nil(a)})
      assert df1.names == ["a", "b", "c"]

      assert df1.dtypes == %{
               "a" => {:s, 64},
               "b" => :string,
               "c" =>
                 {:struct,
                  [{"a", {:s, 64}}, {"b", :string}, {"lit", {:s, 64}}, {"null", :boolean}]}
             }

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, nil, 3],
               b: ["a", "b", nil],
               c: [
                 %{"a" => 1, "b" => "a", "lit" => 1, "null" => false},
                 %{"a" => nil, "b" => "b", "lit" => 1, "null" => true},
                 %{"a" => 3, "b" => nil, "lit" => 1, "null" => false}
               ]
             }
    end

    test "with map nested" do
      df = DF.new(%{a: [1, nil, 3], b: ["a", "b", nil]})
      df1 = DF.mutate(df, c: %{s: %{a: a, b: b}})
      assert df1.names == ["a", "b", "c"]

      assert df1.dtypes == %{
               "a" => {:s, 64},
               "b" => :string,
               "c" => {:struct, [{"s", {:struct, [{"a", {:s, 64}}, {"b", :string}]}}]}
             }

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, nil, 3],
               b: ["a", "b", nil],
               c: [
                 %{"s" => %{"a" => 1, "b" => "a"}},
                 %{"s" => %{"a" => nil, "b" => "b"}},
                 %{"s" => %{"a" => 3, "b" => nil}}
               ]
             }
    end

    test "add eager series to dataframe" do
      df = DF.new([%{a: 1}, %{a: 2}, %{a: 3}])
      s = Series.from_list(["x", "y", "z"])

      df1 = DF.mutate(df, s: ^s, a1: a + 1)
      assert df1.names == ["a", "s", "a1"]

      assert df1.dtypes == %{
               "a" => {:s, 64},
               "s" => :string,
               "a1" => {:s, 64}
             }

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3],
               a1: [2, 3, 4],
               s: ["x", "y", "z"]
             }
    end

    test "json_path_match/2 - extract value from a valid json string using json path" do
      df = DF.new([%{a: ~s({"n": 1})}, %{a: ~s({"m": 1})}])
      df1 = DF.mutate(df, m: json_path_match(a, "$.m"), n: json_path_match(a, "$.n"))
      assert df1.names == ["a", "m", "n"]

      assert df1.dtypes == %{
               "a" => :string,
               "n" => :string,
               "m" => :string
             }

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: ["{\"n\": 1}", "{\"m\": 1}"],
               n: ["1", nil],
               m: [nil, "1"]
             }
    end

    test "json_path_match/2 - extracts nil from invalidjson using json path" do
      df = DF.new([%{a: ~s({"n": 1)}, %{a: ~s({"m": 1})}])
      df1 = DF.mutate(df, m: json_path_match(a, "$.m"), n: json_path_match(a, "$.n"))
      assert df1.names == ["a", "m", "n"]

      assert df1.dtypes == %{
               "a" => :string,
               "n" => :string,
               "m" => :string
             }

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: ["{\"n\": 1", "{\"m\": 1}"],
               m: [nil, "1"],
               n: [nil, nil]
             }
    end

    test "json_path_match/2 - raises for invalid json path" do
      df = DF.new([%{a: ~s({"n": 1})}, %{a: ~s({"m": 1})}])

      assert_raise RuntimeError,
                   "Polars Error: ComputeError(ErrString(\"error compiling JSONpath expression path error: \\nEof\\n\"))",
                   fn ->
                     DF.mutate(df, n: json_path_match(a, "$."))
                   end
    end

    test "adds new columns" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      df1 =
        DF.mutate(df,
          c: a + 5,
          d: 2 + a,
          e: 42,
          f: 842.1,
          g: "Elixir",
          h: true,
          i: ~D[2023-01-01],
          j: ~N[2023-01-01 12:34:56]
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3],
               b: ["a", "b", "c"],
               c: [6, 7, 8],
               d: [3, 4, 5],
               e: [42, 42, 42],
               f: [842.1, 842.1, 842.1],
               g: ["Elixir", "Elixir", "Elixir"],
               h: [true, true, true],
               i: [~D[2023-01-01], ~D[2023-01-01], ~D[2023-01-01]],
               j: [
                 ~N[2023-01-01 12:34:56.000000],
                 ~N[2023-01-01 12:34:56.000000],
                 ~N[2023-01-01 12:34:56.000000]
               ]
             }

      assert df1.names == ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]

      assert df1.dtypes == %{
               "a" => {:s, 64},
               "b" => :string,
               "c" => {:s, 64},
               "d" => {:s, 64},
               "e" => {:s, 64},
               "f" => {:f, 64},
               "g" => :string,
               "h" => :boolean,
               "i" => :date,
               "j" => {:datetime, :nanosecond}
             }
    end

    test "changes a column" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      df1 = DF.mutate(df, a: cast(a, {:f, 64}))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1.0, 2.0, 3.0],
               b: ["a", "b", "c"]
             }

      assert df1.names == ["a", "b"]
      assert df1.dtypes == %{"a" => {:f, 64}, "b" => :string}
    end

    test "adds new columns ordering, sorting and negating" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"], c: [1.3, 5.4, 2.6])

      df1 =
        DF.mutate(df,
          c: reverse(a),
          d: argsort(a, direction: :desc),
          e: sort(b, direction: :desc),
          f: distinct(a),
          g: unordered_distinct(a),
          h: -a,
          i: rank(a, method: :ordinal),
          j: rank(c)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3],
               b: ["a", "b", "c"],
               c: [3, 2, 1],
               d: [2, 1, 0],
               e: ["c", "b", "a"],
               f: [1, 2, 3],
               g: [1, 2, 3],
               h: [-1, -2, -3],
               i: [1, 2, 3],
               j: [1.0, 3.0, 2.0]
             }

      assert df1.names == ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]

      assert df1.dtypes == %{
               "a" => {:s, 64},
               "b" => :string,
               "c" => {:s, 64},
               "d" => {:u, 32},
               "e" => :string,
               "f" => {:s, 64},
               "g" => {:s, 64},
               "h" => {:s, 64},
               "i" => {:u, 32},
               "j" => {:f, 64}
             }
    end

    test "adds some columns with arithmetic operations on (lazy series, number)" do
      df = DF.new(a: [1, 2, 4])

      df1 =
        DF.mutate(df,
          calc1: add(a, 2),
          calc2: subtract(a, 2),
          calc3: multiply(a, 2),
          calc4: divide(a, 2),
          calc5: pow(a, 2),
          calc6: quotient(a, 2),
          calc7: remainder(a, 2),
          calc8: divide(a * 0.0, 0.0),
          calc9: divide(a, 0.0),
          calc10: is_nan(divide(a * 0.0, 0.0))
        )

      assert DF.to_columns(df1, atom_keys: true) === %{
               a: [1, 2, 4],
               calc1: [3, 4, 6],
               calc2: [-1, 0, 2],
               calc3: [2, 4, 8],
               calc4: [0.5, 1.0, 2.0],
               calc5: [1.0, 4.0, 16.0],
               calc6: [0, 1, 2],
               calc7: [1, 0, 0],
               calc8: [:nan, :nan, :nan],
               calc9: [:infinity, :infinity, :infinity],
               calc10: [true, true, true]
             }

      assert DF.dtypes(df1) == %{
               "a" => {:s, 64},
               "calc1" => {:s, 64},
               "calc2" => {:s, 64},
               "calc3" => {:s, 64},
               "calc4" => {:f, 64},
               "calc5" => {:f, 64},
               "calc6" => {:s, 64},
               "calc7" => {:s, 64},
               "calc8" => {:f, 64},
               "calc9" => {:f, 64},
               "calc10" => :boolean
             }
    end

    test "adds some columns with arithmetic operations on (number, lazy series)" do
      df = DF.new(a: [1, 2, 4])

      df1 =
        DF.mutate(df,
          calc1: add(2, a),
          calc2: subtract(2, a),
          calc3: multiply(2, a),
          calc4: divide(2, a),
          calc5: pow(2, a),
          calc5_1: pow(2.0, a),
          calc6: quotient(2, a),
          calc7: remainder(2, a)
        )

      assert DF.to_columns(df1, atom_keys: true) === %{
               a: [1, 2, 4],
               calc1: [3, 4, 6],
               calc2: [1, 0, -2],
               calc3: [2, 4, 8],
               calc4: [2.0, 1.0, 0.5],
               calc5: [2.0, 4.0, 16.0],
               calc5_1: [2.0, 4.0, 16.0],
               calc6: [2, 1, 0],
               calc7: [0, 0, 2]
             }

      assert DF.dtypes(df1) == %{
               "a" => {:s, 64},
               "calc1" => {:s, 64},
               "calc2" => {:s, 64},
               "calc3" => {:s, 64},
               "calc4" => {:f, 64},
               "calc5" => {:f, 64},
               "calc5_1" => {:f, 64},
               "calc6" => {:s, 64},
               "calc7" => {:s, 64}
             }
    end

    test "adds some columns with arithmetic operations on (lazy series, series)" do
      df = DF.new(a: [1, 2, 4])
      series = Explorer.Series.from_list([2, 1, 2])

      df1 =
        DF.mutate(df,
          calc1: add(a, ^series),
          calc2: subtract(a, ^series),
          calc3: multiply(a, ^series),
          calc4: divide(a, ^series),
          calc5: pow(a, ^series),
          calc6: quotient(a, ^series),
          calc7: remainder(a, ^series)
        )

      assert DF.to_columns(df1, atom_keys: true) === %{
               a: [1, 2, 4],
               calc1: [3, 3, 6],
               calc2: [-1, 1, 2],
               calc3: [2, 2, 8],
               calc4: [0.5, 2.0, 2.0],
               calc5: [1.0, 2.0, 16.0],
               calc6: [0, 2, 2],
               calc7: [1, 0, 0]
             }

      assert DF.dtypes(df1) == %{
               "a" => {:s, 64},
               "calc1" => {:s, 64},
               "calc2" => {:s, 64},
               "calc3" => {:s, 64},
               "calc4" => {:f, 64},
               "calc5" => {:f, 64},
               "calc6" => {:s, 64},
               "calc7" => {:s, 64}
             }
    end

    test "adds some columns with arithmetic operations on (series, lazy series)" do
      df = DF.new(a: [2, 1, 2])
      series = Explorer.Series.from_list([1, 2, 4])

      df1 =
        DF.mutate(df,
          calc1: add(^series, a),
          calc2: subtract(^series, a),
          calc3: multiply(^series, a),
          calc4: divide(^series, a),
          calc5: pow(^series, a),
          calc6: quotient(^series, a),
          calc7: remainder(^series, a)
        )

      assert DF.to_columns(df1, atom_keys: true) === %{
               a: [2, 1, 2],
               calc1: [3, 3, 6],
               calc2: [-1, 1, 2],
               calc3: [2, 2, 8],
               calc4: [0.5, 2.0, 2.0],
               calc5: [1.0, 2.0, 16.0],
               calc6: [0, 2, 2],
               calc7: [1, 0, 0]
             }

      assert DF.dtypes(df1) == %{
               "a" => {:s, 64},
               "calc1" => {:s, 64},
               "calc2" => {:s, 64},
               "calc3" => {:s, 64},
               "calc4" => {:f, 64},
               "calc5" => {:f, 64},
               "calc6" => {:s, 64},
               "calc7" => {:s, 64}
             }
    end

    test "adds some columns with arithmetic operations on (lazy series, lazy series)" do
      df = DF.new(a: [1, 2, 3], b: [20, 40, 60], c: [10, 0, 8], d: [3, 2, 1])

      df1 =
        DF.mutate(df,
          calc1: add(a, b),
          calc2: subtract(b, a),
          calc3: multiply(a, d),
          calc4: divide(b, c),
          calc5: pow(a, d),
          calc6: quotient(b, c),
          calc7: remainder(b, c)
        )

      assert DF.to_columns(df1, atom_keys: true) === %{
               a: [1, 2, 3],
               b: [20, 40, 60],
               c: [10, 0, 8],
               d: [3, 2, 1],
               calc1: [21, 42, 63],
               calc2: [19, 38, 57],
               calc3: [3, 4, 3],
               calc4: [2.0, :infinity, 7.5],
               calc5: [1.0, 4.0, 3.0],
               calc6: [2, nil, 7],
               calc7: [0, nil, 4]
             }

      assert DF.dtypes(df1) == %{
               "a" => {:s, 64},
               "b" => {:s, 64},
               "c" => {:s, 64},
               "d" => {:s, 64},
               "calc1" => {:s, 64},
               "calc2" => {:s, 64},
               "calc3" => {:s, 64},
               "calc4" => {:f, 64},
               "calc5" => {:f, 64},
               "calc6" => {:s, 64},
               "calc7" => {:s, 64}
             }
    end

    test "adds a new column with some aggregations without groups" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      df1 =
        DF.mutate(df,
          c: Series.first(a),
          d: Series.last(a),
          e: Series.count(a),
          f: Series.median(a),
          g: Series.sum(a),
          h: Series.min(a) |> Series.add(a),
          i: Series.quantile(a, 0.2),
          j: Series.skew(a)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3],
               b: ["a", "b", "c"],
               c: [1, 1, 1],
               d: [3, 3, 3],
               e: [3, 3, 3],
               f: [2.0, 2.0, 2.0],
               g: [6, 6, 6],
               h: [2, 3, 4],
               i: [1, 1, 1],
               j: [0, 0, 0]
             }

      assert df1.names == ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]

      assert df1.dtypes == %{
               "a" => {:s, 64},
               "b" => :string,
               "c" => {:s, 64},
               "d" => {:s, 64},
               "e" => {:u, 32},
               "f" => {:f, 64},
               "g" => {:s, 64},
               "h" => {:s, 64},
               "i" => {:f, 64},
               "j" => {:f, 64}
             }
    end

    test "valid skew dtype" do
      df = DF.new(a: [1, 2, 3, 1, 2, 23, 30])

      df1 = DF.mutate(df, c: skew(a))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 1, 2, 23, 30],
               c: [
                 1.0273558639184455,
                 1.0273558639184455,
                 1.0273558639184455,
                 1.0273558639184455,
                 1.0273558639184455,
                 1.0273558639184455,
                 1.0273558639184455
               ]
             }

      assert df1.dtypes == %{
               "a" => {:s, 64},
               "c" => {:f, 64}
             }
    end

    test "correlation and covariance" do
      df = DF.new(a: [1, 8, 3], b: [4, 5, 2])
      df1 = DF.mutate(df, c: covariance(a, b), d: correlation(a, b))

      assert df1 |> DF.head(1) |> DF.to_columns(atom_keys: true) ==
               %{a: [1], b: [4], c: [3.0], d: [0.5447047794019219]}

      df2 =
        DF.new(a: [1, 8, 3, nil], b: [4, 5, 2, nil])
        |> DF.mutate(c: covariance(a, b), d: correlation(a, b))

      assert df2 |> DF.head(1) |> DF.to_columns(atom_keys: true) ==
               %{a: [1], b: [4], c: [3.0], d: [0.5447047794019219]}

      df3 = DF.new(a: [1], b: [4]) |> DF.mutate(c: covariance(a, a), d: correlation(a, a))

      assert df3 |> DF.head(1) |> DF.to_columns(atom_keys: true) ==
               %{a: [1], b: [4], c: [:nan], d: [:nan]}
    end

    test "clip/3" do
      df = DF.new(a: [-50, 5, nil, 50])

      df1 = df |> DF.mutate(b: clip(a, 1, 10)) |> DF.select(:b)
      assert DF.to_columns(df1, atom_keys: true) == %{b: [1, 5, nil, 10]}
      assert DF.dtypes(df1) == %{"b" => {:s, 64}}

      df2 = df |> DF.mutate(b: clip(a, 1.5, 10.5)) |> DF.select(:b)
      assert DF.to_columns(df2, atom_keys: true) == %{b: [1.5, 5.0, nil, 10.5]}
      assert DF.dtypes(df2) == %{"b" => {:f, 64}}

      assert_raise ArgumentError,
                   ~r"expects both the min and max bounds to be numbers",
                   fn -> df |> DF.mutate(b: clip(a, nil, nil)) end
    end

    test "adds some columns with select functions" do
      a = Series.from_list([true, false, true])
      b = Series.from_list([3, 4, 2])
      c = Series.from_list([6, 2, 1])
      df = DF.new(a: a, b: b, c: c)

      df1 = DF.mutate(df, select1: select(a, b, c))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [true, false, true],
               b: [3, 4, 2],
               c: [6, 2, 1],
               select1: [3, 2, 2]
             }
    end

    test "adds some columns with select broadcast functions" do
      df = DF.new(b: [3, 4, 2], c: [6, 2, 1])

      df1 =
        DF.mutate(df,
          select1: select(from_list([true]), b, c),
          select2: select(from_list([false]), b, c)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               b: [3, 4, 2],
               c: [6, 2, 1],
               select1: [3, 4, 2],
               select2: [6, 2, 1]
             }
    end

    test "adds some columns with select functions and scalar values" do
      a = Series.from_list([true, false, true])
      b = Series.from_list([3, 4, 2])
      c = Series.from_list([6, 2, 1])
      df = DF.new(a: a, b: b, c: c)

      df1 = DF.mutate(df, select1: select(a, "passed", "failed"), select2: select(b > c, 50, 0))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [true, false, true],
               b: [3, 4, 2],
               c: [6, 2, 1],
               select1: ["passed", "failed", "passed"],
               select2: [0, 50, 50]
             }
    end

    test "adds some columns with slice and dice functions" do
      a = Series.from_list([1, nil, 3])
      b = Series.from_list([20.0, 40.0, 60.0])
      df = DF.new(a: a, b: b)

      df1 =
        DF.mutate(df,
          concat1: concat(^a, b) |> slice(1, 3),
          concat2: concat(a, b) |> slice(1, 3),
          concat3: concat(a, b) |> slice(1, 3),
          coalesce1: coalesce(^a, b),
          coalesce2: coalesce(a, b),
          coalesce3: coalesce(a, b)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, nil, 3],
               b: [20.0, 40.0, 60.0],
               concat1: [nil, 3, 20.0],
               concat2: [nil, 3, 20.0],
               concat3: [nil, 3, 20.0],
               coalesce1: [1.0, 40.0, 3.0],
               coalesce2: [1.0, 40.0, 3.0],
               coalesce3: [1.0, 40.0, 3.0]
             }

      assert DF.dtypes(df1) == %{
               "a" => {:s, 64},
               "b" => {:f, 64},
               "concat1" => {:f, 64},
               "concat2" => {:f, 64},
               "concat3" => {:f, 64},
               "coalesce1" => {:f, 64},
               "coalesce2" => {:f, 64},
               "coalesce3" => {:f, 64}
             }
    end

    test "adds some columns with window functions" do
      df = DF.new(a: Enum.to_list(1..10))

      df1 =
        DF.mutate(df,
          b: window_max(a, 2, weights: [1.0, 2.0]),
          c: window_mean(a, 2, weights: [0.25, 0.75]),
          d: window_median(a, 2, weights: [0.25, 0.75]),
          e: window_min(a, 2, weights: [1.0, 2.0]),
          f: window_sum(a, 2, weights: [1.0, 2.0]),
          g: window_standard_deviation(a, 2),
          h: ewm_mean(a),
          p: cumulative_max(a),
          q: cumulative_min(a),
          r: cumulative_sum(a),
          s: cumulative_max(a, reverse: true),
          t: cumulative_product(a),
          z: abs(a)
        )

      assert df1.dtypes == %{
               "a" => {:s, 64},
               "b" => {:f, 64},
               "c" => {:f, 64},
               "d" => {:f, 64},
               "e" => {:f, 64},
               "f" => {:f, 64},
               "g" => {:f, 64},
               "h" => {:f, 64},
               "p" => {:s, 64},
               "q" => {:s, 64},
               "r" => {:s, 64},
               "s" => {:s, 64},
               "t" => {:s, 64},
               "z" => {:s, 64}
             }

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
               b: [1.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0],
               c: [0.25, 1.75, 2.75, 3.75, 4.75, 5.75, 6.75, 7.75, 8.75, 9.75],
               d: [1.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5],
               e: [1.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
               f: [1.0, 5.0, 8.0, 11.0, 14.0, 17.0, 20.0, 23.0, 26.0, 29.0],
               g: [
                 0.0,
                 0.7071067811865476,
                 0.7071067811865476,
                 0.7071067811865476,
                 0.7071067811865476,
                 0.7071067811865476,
                 0.7071067811865476,
                 0.7071067811865476,
                 0.7071067811865476,
                 0.7071067811865476
               ],
               h: [
                 1.0,
                 1.6666666666666667,
                 2.4285714285714284,
                 3.2666666666666666,
                 4.161290322580645,
                 5.095238095238095,
                 6.05511811023622,
                 7.031372549019608,
                 8.017612524461839,
                 9.009775171065494
               ],
               p: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
               q: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
               r: [1, 3, 6, 10, 15, 21, 28, 36, 45, 55],
               s: [10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
               t: [1, 2, 6, 24, 120, 720, 5040, 40320, 362_880, 3_628_800],
               z: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
             }
    end

    test "add columns with peaks values" do
      df = DF.new(a: [1, 2, 3, 2, 1, 3])

      df1 = DF.mutate(df, b: peaks(a, :max), c: peaks(a, :min))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 2, 1, 3],
               b: [false, false, true, false, false, true],
               c: [true, false, false, false, true, false]
             }

      assert df1.dtypes == %{"a" => {:s, 64}, "b" => :boolean, "c" => :boolean}
    end

    test "add columns with missing values" do
      df = DF.new(a: [1, nil, 3, 2, nil, 4])

      df1 =
        DF.mutate(df,
          b: fill_missing(a, :forward),
          c: fill_missing(a, :backward),
          d: fill_missing(a, :min),
          e: fill_missing(a, :max),
          f: fill_missing(a, :mean),
          g: fill_missing(a, 42)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, nil, 3, 2, nil, 4],
               b: [1, 1, 3, 2, 2, 4],
               c: [1, 3, 3, 2, 4, 4],
               d: [1, 1, 3, 2, 1, 4],
               e: [1, 4, 3, 2, 4, 4],
               f: [1.0, 2.5, 3.0, 2.0, 2.5, 4.0],
               g: [1, 42, 3, 2, 42, 4]
             }

      assert df1.dtypes == %{
               "a" => {:s, 64},
               "b" => {:s, 64},
               "c" => {:s, 64},
               "d" => {:s, 64},
               "e" => {:s, 64},
               "f" => {:f, 64},
               "g" => {:s, 64}
             }
    end

    test "add columns with dtype-specific missing values" do
      df = DF.new(a: [true, nil, false], b: [nil, 1.0, 2.0])

      df1 =
        DF.mutate(df,
          c: fill_missing(a, false),
          d: fill_missing(a, true),
          e: fill_missing(b, :nan)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [true, nil, false],
               b: [nil, 1.0, 2.0],
               c: [true, false, false],
               d: [true, true, false],
               e: [:nan, 1.0, 2.0]
             }

      assert df1.dtypes == %{
               "a" => :boolean,
               "b" => {:f, 64},
               "c" => :boolean,
               "d" => :boolean,
               "e" => {:f, 64}
             }
    end

    test "add columns with head+shift" do
      df = DF.new(a: [1, 2, 3])

      df1 =
        DF.mutate(df,
          b: head(shift(a, 3), 3),
          c: head(shift(a, 2), 3),
          d: head(shift(a, 1), 3),
          e: head(shift(a, 0), 3),
          f: head(shift(a, -1), 3),
          g: head(shift(a, -2), 3),
          h: head(shift(a, -3), 3)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3],
               b: [nil, nil, nil],
               c: [nil, nil, 1],
               d: [nil, 1, 2],
               e: [1, 2, 3],
               f: [2, 3, nil],
               g: [3, nil, nil],
               h: [nil, nil, nil]
             }
    end

    test "add columns with sampling" do
      df = DF.new(a: [1, 2, 3, 4, 5])

      df1 =
        DF.mutate(df,
          b: sample(a, 5, seed: 100, shuffle: true),
          c: sample(a, 1.0, seed: 99, shuffle: true),
          d: sample(a, 3, seed: 98, shuffle: true) |> concat(from_list([0, 0]))
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5],
               b: [5, 3, 2, 1, 4],
               c: [1, 3, 2, 4, 5],
               d: [4, 2, 5, 0, 0]
             }

      assert df1.dtypes == %{
               "a" => {:s, 64},
               "b" => {:s, 64},
               "c" => {:s, 64},
               "d" => {:s, 64}
             }
    end

    test "keeps the column order" do
      df = DF.new(e: [1, 2, 3], c: ["a", "b", "c"], a: [1.2, 2.3, 4.5])

      df1 = DF.mutate(df, d: 1, b: 2)

      assert df1.names == ["e", "c", "a", "d", "b"]
    end

    test "operations on boolean column" do
      df = DF.new(a: [true, false, true, nil], b: [true, false, false, true])

      df1 = DF.mutate(df, c: not a, d: a and b, e: a or b)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [true, false, true, nil],
               b: [true, false, false, true],
               c: [false, true, false, nil],
               d: [true, false, false, nil],
               e: [true, false, true, true]
             }

      assert df1.names == ["a", "b", "c", "d", "e"]

      assert df1.dtypes == %{
               "a" => :boolean,
               "b" => :boolean,
               "c" => :boolean,
               "d" => :boolean,
               "e" => :boolean
             }
    end

    test "adds a slice of another column with a list of indices" do
      df = DF.new(a: [1, 2, 4])

      # nil is needed to make the column equals the size of the df.
      df1 = DF.mutate(df, b: slice(a, [1, 2, nil]))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 4],
               b: [2, 4, nil]
             }

      assert DF.dtypes(df1) == %{
               "a" => {:s, 64},
               "b" => {:s, 64}
             }
    end

    test "adds a slice of another column with a series of indices" do
      df = DF.new(a: [1, 2, 4])

      # nil is needed to make the column equals the size of the df.
      df1 = DF.mutate(df, b: slice(a, from_list([1, 2, nil])))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 4],
               b: [2, 4, nil]
             }

      assert DF.dtypes(df1) == %{
               "a" => {:s, 64},
               "b" => {:s, 64}
             }
    end

    test "adds a column with log" do
      df = DF.new(a: [8, 16, 64])

      df1 = DF.mutate(df, b: log(a, 2), c: log(a))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [8, 16, 64],
               b: [3.0, 4.0, 6.0],
               c: [2.0794415416798357, 2.772588722239781, 4.1588830833596715]
             }

      assert df1.names == ["a", "b", "c"]

      assert df1.dtypes == %{
               "a" => {:s, 64},
               "b" => {:f, 64},
               "c" => {:f, 64}
             }
    end

    test "adds a column with round" do
      df = DF.new(a: [1.2345, 2.3456, 3.4567, 4.5678])

      df1 = DF.mutate(df, b: round(a, 2))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1.2345, 2.3456, 3.4567, 4.5678],
               b: [1.23, 2.35, 3.46, 4.57]
             }
    end

    test "adds a column with exponential" do
      df = DF.new(a: [1, 2, 3])

      df1 = DF.mutate(df, b: exp(a))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3],
               b: [2.718281828459045, 7.38905609893065, 20.085536923187668]
             }

      assert df1.names == ["a", "b"]

      assert df1.dtypes == %{
               "a" => {:s, 64},
               "b" => {:f, 64}
             }
    end

    test "add columns with trignometric functions" do
      pi = :math.pi()
      df = DF.new(a: [0, pi / 2, pi])

      df1 = DF.mutate(df, b: sin(a), c: cos(a), d: tan(a))
      df2 = DF.mutate(df1, e: asin(b), f: acos(c), g: atan(d))

      assert DF.to_columns(df2, atom_keys: true) == %{
               a: [0, pi / 2, pi],
               b: [0, 1, 1.2246467991473532e-16],
               c: [1.0, 6.123233995736766e-17, -1.0],
               d: [0.0, 1.633123935319537e16, -1.2246467991473532e-16],
               e: [0, pi / 2, 1.2246467991473532e-16],
               f: [0, pi / 2, pi],
               g: [0, pi / 2, -1.2246467991473532e-16]
             }

      assert df2.names == ["a", "b", "c", "d", "e", "f", "g"]

      assert df2.dtypes == %{
               "a" => {:f, 64},
               "b" => {:f, 64},
               "c" => {:f, 64},
               "d" => {:f, 64},
               "e" => {:f, 64},
               "f" => {:f, 64},
               "g" => {:f, 64}
             }
    end

    test "raises when adding eager series whose length does not match dataframe's length" do
      df = DF.new(a: [1, 2, 3])
      series = Series.from_list([4, 5, 6, 7])

      assert_raise RuntimeError,
                   "Polars Error: lengths don't match: unable to add a column of length 4 to a DataFrame of height 3",
                   fn ->
                     DF.mutate(df, b: ^series)
                   end
    end

    test "raises when adding list" do
      df = DF.new(a: [1, 2, 3])
      series = [4, 5, 6]

      error =
        "expecting a lazy series or scalar value, but instead got a list. " <>
          "consider using `Explorer.Series.from_list/2` to create a `Series`, " <>
          "and then `Explorer.DataFrame.put/3` to add the series to your dataframe."

      assert_raise ArgumentError, error, fn ->
        DF.mutate(df, b: ^series)
      end
    end

    test "slice strings" do
      df =
        DF.new(
          a: ["_hello", "_world", "_foo", "_bar"],
          b: ["venus", "earth", "mars", "jupiter"],
          c: ["_foo", "_bar", "_baz", "_quox"],
          d: ["_foo", "_bar", "_baz", "_quox"],
          e: ["_foo", "_bar", "_baz", "_quox"]
        )

      df1 =
        DF.mutate(df,
          f: substring(a, 1),
          g: substring(b, 2, 5),
          h: substring(c, -3),
          i: substring(d, 6, 10),
          j: substring(e, -15, 2)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: ["_hello", "_world", "_foo", "_bar"],
               b: ["venus", "earth", "mars", "jupiter"],
               c: ["_foo", "_bar", "_baz", "_quox"],
               d: ["_foo", "_bar", "_baz", "_quox"],
               e: ["_foo", "_bar", "_baz", "_quox"],
               f: ["hello", "world", "foo", "bar"],
               g: ["nus", "rth", "rs", "piter"],
               h: ["foo", "bar", "baz", "uox"],
               i: ["", "", "", ""],
               j: ["_f", "_b", "_b", "_q"]
             }
    end

    test "strip characters from string" do
      df =
        DF.new(
          a: ["£2", "3£", "£200£", "£££20"],
          b: ["   sent   ", " received", "  words  ", "lots of pound signs    "]
        )

      df1 =
        DF.mutate(df,
          c: strip(a, "£"),
          d: strip(b)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: ["£2", "3£", "£200£", "£££20"],
               b: ["   sent   ", " received", "  words  ", "lots of pound signs    "],
               c: ["2", "3", "200", "20"],
               d: ["sent", "received", "words", "lots of pound signs"]
             }
    end

    test "replace characters in a string" do
      df = DF.new(a: ["2,000", "2,000,000", ","])

      df1 =
        DF.mutate(df,
          b: replace(a, ",", "")
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: ["2,000", "2,000,000", ","],
               b: ["2000", "2000000", ""]
             }
    end

    test "strip multiple characters from string" do
      df =
        DF.new(
          a: ["ababhelloabab", "abababworldabababa", "abab", "bbbbaaaabhelloba"],
          b: ["nx_hello", "world_nx", "nx_nx_xn", "more_nx"]
        )

      df1 =
        DF.mutate(df,
          c: strip(a, "ab"),
          d: strip(b, "nx_")
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: ["ababhelloabab", "abababworldabababa", "abab", "bbbbaaaabhelloba"],
               b: ["nx_hello", "world_nx", "nx_nx_xn", "more_nx"],
               c: ["hello", "world", "", "hello"],
               d: ["hello", "world", "", "more"]
             }
    end

    test "strip trailing characters from string" do
      df =
        DF.new(
          a: ["£2", "3£", "£200£", "£££20"],
          b: ["   sent   ", " received", "  words  ", "lots of pound signs    "]
        )

      df1 =
        DF.mutate(df,
          c: rstrip(a, "£"),
          d: rstrip(b)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: ["£2", "3£", "£200£", "£££20"],
               b: ["   sent   ", " received", "  words  ", "lots of pound signs    "],
               c: ["£2", "3", "£200", "£££20"],
               d: ["   sent", " received", "  words", "lots of pound signs"]
             }
    end

    test "strip leading characters from string" do
      df =
        DF.new(
          a: ["£2", "3£", "£200£", "£££20"],
          b: ["   sent   ", " received", "  words  ", "lots of pound signs    "]
        )

      df1 =
        DF.mutate(df,
          c: lstrip(a, "£"),
          d: lstrip(b)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: ["£2", "3£", "£200£", "£££20"],
               b: ["   sent   ", " received", "  words  ", "lots of pound signs    "],
               c: ["2", "3£", "200£", "20"],
               d: ["sent   ", "received", "words  ", "lots of pound signs    "]
             }
    end

    test "conversion between string and datetime" do
      df =
        DF.new(
          a: ["2023-01-05 12:34:56", nil],
          b: ["2023/30/01 00:11:22", "XYZ"],
          c: [~N[2023-01-05 12:34:56.000000], nil],
          d: [~N[2023-01-30 00:11:22.000000], nil]
        )

      df1 =
        DF.mutate(df,
          c: strptime(a, "%Y-%m-%d %H:%M:%S"),
          d: strptime(b, "%Y/%d/%m %H:%M:%S")
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: ["2023-01-05 12:34:56", nil],
               b: ["2023/30/01 00:11:22", "XYZ"],
               c: [~N[2023-01-05 12:34:56.000000], nil],
               d: [~N[2023-01-30 00:11:22.000000], nil]
             }

      df1 =
        DF.mutate(df,
          a: strftime(c, "%Y-%m-%d %H:%M:%S"),
          b: strftime(d, "%Y/%d/%m %H:%M:%S")
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: ["2023-01-05 12:34:56", nil],
               b: ["2023/30/01 00:11:22", nil],
               c: [~N[2023-01-05 12:34:56.000000], nil],
               d: [~N[2023-01-30 00:11:22.000000], nil]
             }
    end

    test "add columns with date and datetime operations" do
      df =
        DF.new(
          a: [~D[2023-01-15], ~D[2022-02-16], ~D[2021-03-20], nil],
          b: [
            ~N[2023-01-15 01:01:01],
            ~N[2022-02-16 02:02:02],
            ~N[2021-03-20 03:03:03.003030],
            nil
          ]
        )

      df1 =
        DF.mutate(df,
          c: day_of_week(a),
          d: day_of_week(b),
          e: month(a),
          f: month(b),
          g: year(a),
          h: year(b),
          i: hour(b),
          j: minute(b),
          k: second(b),
          l: day_of_year(a),
          m: day_of_year(b),
          n: week_of_year(a),
          o: week_of_year(b)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [~D[2023-01-15], ~D[2022-02-16], ~D[2021-03-20], nil],
               b: [
                 ~N[2023-01-15 01:01:01.000000],
                 ~N[2022-02-16 02:02:02.000000],
                 ~N[2021-03-20 03:03:03.003030],
                 nil
               ],
               c: [7, 3, 6, nil],
               d: [7, 3, 6, nil],
               e: [1, 2, 3, nil],
               f: [1, 2, 3, nil],
               g: [2023, 2022, 2021, nil],
               h: [2023, 2022, 2021, nil],
               i: [1, 2, 3, nil],
               j: [1, 2, 3, nil],
               k: [1, 2, 3, nil],
               l: [15, 47, 79, nil],
               m: [15, 47, 79, nil],
               n: [2, 7, 11, nil],
               o: [2, 7, 11, nil]
             }

      assert df1.dtypes == %{
               "a" => :date,
               "b" => {:datetime, :microsecond},
               "c" => {:s, 8},
               "d" => {:s, 8},
               "e" => {:s, 8},
               "f" => {:s, 8},
               "g" => {:s, 32},
               "h" => {:s, 32},
               "i" => {:s, 8},
               "j" => {:s, 8},
               "k" => {:s, 8},
               "l" => {:s, 16},
               "m" => {:s, 16},
               "n" => {:s, 8},
               "o" => {:s, 8}
             }
    end

    test "add columns with select/3" do
      df1 = DF.new(a: [1, 2, 3]) |> DF.mutate(x: select(a <= 2.5, a, 2.5))
      assert Series.to_list(df1[:x]) == [1.0, 2.0, 2.5]

      df2 = DF.new(a: [1, 2, 3]) |> DF.mutate(x: select(a <= 2.5, :nan, a))
      assert Series.to_list(df2[:x]) == [:nan, :nan, 3.0]

      df3 = DF.new(a: [true, false, nil]) |> DF.mutate(x: select(a, false, a))
      assert Series.to_list(df3[:x]) == [false, false, nil]

      df4 = DF.new(a: [true, false, nil]) |> DF.mutate(x: select(not a, a, true))
      assert Series.to_list(df4[:x]) == [true, false, true]

      df5 = DF.new(a: ["a", "b", "c"]) |> DF.mutate(x: select(a == "a", "x", a))
      assert Series.to_list(df5[:x]) == ["x", "b", "c"]

      df6 = DF.new(a: ["a", "b", "c"]) |> DF.mutate(x: select(a == "a", a, "x"))
      assert Series.to_list(df6[:x]) == ["a", "x", "x"]

      df7 =
        DF.new(a: [~D[2023-01-15], ~D[2022-02-16], nil])
        |> DF.mutate(x: select(a == ~D[2023-01-15], a, ~D[2023-01-01]))

      assert Series.to_list(df7[:x]) == [~D[2023-01-15], ~D[2023-01-01], ~D[2023-01-01]]
    end

    test "supports list operations" do
      df =
        DF.new(
          a: [~w(a b c), ~w(d e f)],
          b: [[1, 2, 3], [4, 5, 6]],
          c: [
            [~N[2021-01-01 00:00:00], ~N[2021-01-02 00:00:00]],
            [~N[2021-01-03 00:00:00], ~N[2021-01-04 00:00:00]]
          ]
        )

      df =
        DF.mutate(df,
          join: join(a, ","),
          lengths: lengths(b),
          member?: member?(c, ~N[2021-01-02 00:00:00])
        )

      assert DF.dtypes(df) == %{
               "a" => {:list, :string},
               "b" => {:list, {:s, 64}},
               "c" => {:list, {:datetime, :microsecond}},
               "join" => :string,
               "lengths" => {:u, 32},
               "member?" => :boolean
             }

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [~w(a b c), ~w(d e f)],
               b: [[1, 2, 3], [4, 5, 6]],
               c: [
                 [~N[2021-01-01 00:00:00.000000], ~N[2021-01-02 00:00:00.000000]],
                 [~N[2021-01-03 00:00:00.000000], ~N[2021-01-04 00:00:00.000000]]
               ],
               join: ["a,b,c", "d,e,f"],
               lengths: [3, 3],
               member?: [true, false]
             }
    end
  end

  describe "sort_by/3" do
    test "raises with invalid column names", %{df: df} do
      assert_raise ArgumentError,
                   ~r"could not find column name \"test\"",
                   fn -> DF.sort_by(df, test) end
    end
  end

  describe "sort_with/2" do
    test "with a simple df and asc order" do
      df = DF.new(a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"])
      df1 = DF.sort_with(df, fn ldf -> [asc: ldf["a"]] end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5, 6],
               b: ["a", "b", "c", "d", "e", "f"]
             }
    end

    test "with a simple df one column and without order" do
      df = DF.new(a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"])
      df1 = DF.sort_with(df, fn ldf -> ldf["a"] end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5, 6],
               b: ["a", "b", "c", "d", "e", "f"]
             }
    end

    test "with a simple df and desc order" do
      df = DF.new(a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"])
      df1 = DF.sort_with(df, fn ldf -> [desc: ldf["a"]] end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [6, 5, 4, 3, 2, 1],
               b: ["f", "e", "d", "c", "b", "a"]
             }
    end

    test "with a simple df and just the lazy series" do
      df = DF.new(a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"])
      df1 = DF.sort_with(df, fn ldf -> [ldf["a"]] end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4, 5, 6],
               b: ["a", "b", "c", "d", "e", "f"]
             }
    end

    test "with a simple df and sort_by by two columns" do
      df = DF.new(a: [1, 2, 2, 3, 6, 5], b: [1.1, 2.5, 2.2, 3.3, 4.0, 5.1])
      df1 = DF.sort_with(df, fn ldf -> [asc: ldf["a"], asc: ldf["b"]] end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 2, 3, 5, 6],
               b: [1.1, 2.2, 2.5, 3.3, 5.1, 4.0]
             }
    end

    test "with a simple df and window function" do
      df = DF.new(a: [1, 2, 4, 3, 6, 5], b: ["a", "b", "d", "c", "f", "e"])
      df1 = DF.sort_with(df, fn ldf -> [desc: Series.window_mean(ldf["a"], 2)] end)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [5, 6, 3, 4, 2, 1],
               b: ["e", "f", "c", "d", "b", "a"]
             }
    end

    test "with a simple df and nils" do
      df = DF.new(a: [1, 2, nil, 3])
      df1 = DF.sort_with(df, fn ldf -> [asc: ldf["a"]] end, nils: :first)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [nil, 1, 2, 3]
             }

      df2 = DF.sort_with(df, fn ldf -> [asc: ldf["a"]] end, nils: :last)

      assert DF.to_columns(df2, atom_keys: true) == %{
               a: [1, 2, 3, nil]
             }
    end

    test "without a lazy series" do
      df = DF.new(a: [1, 2])

      assert_raise RuntimeError, "expecting a lazy series, got: :foo", fn ->
        DF.sort_with(df, fn _ldf -> [desc: :foo] end)
      end
    end

    test "with wrong direction" do
      df = DF.new(a: [1, 2])

      message = "expecting a valid direction, which is :asc or :desc, got: :descending"

      assert_raise RuntimeError, message, fn ->
        DF.sort_with(df, fn ldf -> [descending: ldf["a"]] end)
      end
    end
  end

  describe "slice/2" do
    test "slice with integer indices" do
      df = DF.new(a: [1, 2, 3, 4, 5])

      df1 = DF.slice(df, [2, 4])

      assert DF.to_columns(df1, atom_keys: true) == %{a: [3, 5]}
    end

    test "slice with series of integers indices" do
      df = DF.new(a: [1, 2, 3, 4, 5])

      for dtype <- [:u8, :s16, :s64] do
        series = Series.from_list([2, 4], dtype: dtype)

        df1 = DF.slice(df, series)

        assert DF.to_columns(df1, atom_keys: true) == %{a: [3, 5]}
      end
    end

    test "slice with ranges" do
      df = DF.new(a: [1, 2, 3, 4, 5])

      df1 = DF.slice(df, -3..-1)

      assert DF.to_columns(df1, atom_keys: true) == %{a: [3, 4, 5]}
    end

    test "raises with index out of bounds", %{df: df} do
      assert_raise ArgumentError,
                   "requested row index (2000) out of bounds (-1094:1094)",
                   fn -> DF.slice(df, [1, 2, 3, 2000]) end
    end
  end

  describe "join/3" do
    test "raises if no overlapping columns" do
      assert_raise ArgumentError,
                   ~r"could not find any overlapping columns",
                   fn ->
                     left = DF.new(a: [1, 2, 3])
                     right = DF.new(b: [1, 2, 3])
                     DF.join(left, right)
                   end
    end

    test "doesn't raise if no overlapping columns on cross join" do
      left = DF.new(a: [1, 2, 3])
      right = DF.new(b: [1, 2, 3])
      joined = DF.join(left, right, how: :cross)
      assert %DF{} = joined

      assert DF.names(joined) == ["a", "b"]
      assert DF.n_rows(joined) == 9
    end

    test "with a custom 'on'" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(d: [1, 2, 2], c: ["d", "e", "f"])

      df = DF.join(left, right, on: [{"a", "d"}])

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"]
             }
    end

    test "with a custom 'on' but with repeated column on right side" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(d: [1, 2, 2], c: ["d", "e", "f"], a: [5, 6, 7])

      df = DF.join(left, right, on: [{"a", "d"}])

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"],
               a_right: [5, 6, 7]
             }

      assert df.names == ["a", "b", "c", "a_right"]

      df1 = DF.join(left, right, on: [{"a", "d"}], how: :left)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               a_right: [5, 6, 7, nil]
             }

      assert df1.names == ["a", "b", "c", "a_right"]

      df2 = DF.join(left, right, on: [{"a", "d"}], how: :outer)

      assert DF.to_columns(df2, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               d: [1, 2, 2, nil],
               a_right: [5, 6, 7, nil]
             }

      assert df2.names == ["a", "b", "d", "c", "a_right"]

      df3 = DF.join(left, right, how: :cross)

      assert DF.to_columns(df3, atom_keys: true) == %{
               a: [1, 1, 1, 2, 2, 2, 3, 3, 3],
               a_right: [5, 6, 7, 5, 6, 7, 5, 6, 7],
               b: ["a", "a", "a", "b", "b", "b", "c", "c", "c"],
               c: ["d", "e", "f", "d", "e", "f", "d", "e", "f"],
               d: [1, 2, 2, 1, 2, 2, 1, 2, 2]
             }

      assert df3.names == ["a", "b", "d", "c", "a_right"]

      df4 = DF.join(left, right, on: [{"a", "d"}], how: :right)

      assert DF.to_columns(df4, atom_keys: true) == %{
               a: [5, 6, 7],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"],
               d: [1, 2, 2]
             }

      assert df4.names == ["d", "c", "a", "b"]
    end

    test "with a custom 'on' but with repeated column on left side" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"], d: [5, 6, 7])
      right = DF.new(d: [1, 2, 2], c: ["d", "e", "f"])

      df = DF.join(left, right, on: [{"a", "d"}])

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"],
               d: [5, 6, 6]
             }

      assert df.names == ["a", "b", "d", "c"]

      df1 = DF.join(left, right, on: [{"a", "d"}], how: :left)

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               d: [5, 6, 6, 7]
             }

      assert df1.names == ["a", "b", "d", "c"]

      df2 = DF.join(left, right, on: [{"a", "d"}], how: :outer)

      assert DF.to_columns(df2, atom_keys: true) == %{
               a: [1, 2, 2, 3],
               b: ["a", "b", "b", "c"],
               c: ["d", "e", "f", nil],
               d: [5, 6, 6, 7],
               d_right: [1, 2, 2, nil]
             }

      assert df2.names == ["a", "b", "d", "d_right", "c"]

      df3 = DF.join(left, right, how: :cross)

      assert DF.to_columns(df3, atom_keys: true) == %{
               a: [1, 1, 1, 2, 2, 2, 3, 3, 3],
               b: ["a", "a", "a", "b", "b", "b", "c", "c", "c"],
               c: ["d", "e", "f", "d", "e", "f", "d", "e", "f"],
               d: [5, 5, 5, 6, 6, 6, 7, 7, 7],
               d_right: [1, 2, 2, 1, 2, 2, 1, 2, 2]
             }

      assert df3.names == ["a", "b", "d", "d_right", "c"]

      df4 = DF.join(left, right, on: [{"a", "d"}], how: :right)

      assert DF.to_columns(df4, atom_keys: true) == %{
               b: ["a", "b", "b"],
               c: ["d", "e", "f"],
               d: [1, 2, 2],
               d_left: [5, 6, 6]
             }

      assert df4.names == ["d", "c", "b", "d_left"]
    end

    test "with invalid join strategy" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(a: [1, 2, 2], c: ["d", "e", "f"])

      msg =
        "join type is not valid: :inner_join. Valid options are: :inner, :left, :right, :outer, :cross"

      assert_raise ArgumentError, msg, fn -> DF.join(left, right, how: :inner_join) end
    end

    test "with matching column indexes as single value" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(a: [1, 2, 2], c: ["d", "e", "f"])

      df = DF.join(left, right, on: 0)

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"]
             }
    end

    test "with matching column indexes" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(a: [1, 2, 2], c: ["d", "e", "f"])

      df = DF.join(left, right, on: [0])

      assert DF.to_columns(df, atom_keys: true) == %{
               a: [1, 2, 2],
               b: ["a", "b", "b"],
               c: ["d", "e", "f"]
             }
    end

    test "with no matching column indexes" do
      left = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      right = DF.new(c: ["d", "e", "f"], a: [1, 2, 2])

      msg = "the column given to option `:on` is not the same for both dataframes"

      assert_raise ArgumentError, msg, fn -> DF.join(left, right, on: [0]) end
    end
  end

  describe "table/1" do
    test "prints 5 rows by default" do
      df = Datasets.iris()

      assert capture_io(fn -> DF.print(df) end) == """
             +-----------------------------------------------------------------------+
             |              Explorer DataFrame: [rows: 150, columns: 5]              |
             +--------------+-------------+--------------+-------------+-------------+
             | sepal_length | sepal_width | petal_length | petal_width |   species   |
             |    <f64>     |    <f64>    |    <f64>     |    <f64>    |  <string>   |
             +==============+=============+==============+=============+=============+
             | 5.1          | 3.5         | 1.4          | 0.2         | Iris-setosa |
             +--------------+-------------+--------------+-------------+-------------+
             | 4.9          | 3.0         | 1.4          | 0.2         | Iris-setosa |
             +--------------+-------------+--------------+-------------+-------------+
             | 4.7          | 3.2         | 1.3          | 0.2         | Iris-setosa |
             +--------------+-------------+--------------+-------------+-------------+
             | 4.6          | 3.1         | 1.5          | 0.2         | Iris-setosa |
             +--------------+-------------+--------------+-------------+-------------+
             | 5.0          | 3.6         | 1.4          | 0.2         | Iris-setosa |
             +--------------+-------------+--------------+-------------+-------------+

             """
    end

    test "accepts limit keyword param" do
      df = Datasets.iris()

      assert capture_io(fn -> DF.print(df, limit: 1) end) == """
             +-----------------------------------------------------------------------+
             |              Explorer DataFrame: [rows: 150, columns: 5]              |
             +--------------+-------------+--------------+-------------+-------------+
             | sepal_length | sepal_width | petal_length | petal_width |   species   |
             |    <f64>     |    <f64>    |    <f64>     |    <f64>    |  <string>   |
             +==============+=============+==============+=============+=============+
             | 5.1          | 3.5         | 1.4          | 0.2         | Iris-setosa |
             +--------------+-------------+--------------+-------------+-------------+

             """
    end

    test "accepts limit: :infinity" do
      df =
        DF.new(
          a: [1, 2, 3, 4, 5, 6, 7, 8, 9],
          b: ~w[a b c d e f g h i],
          c: [9.1, 8.2, 7.3, 6.4, 5.5, 4.6, 3.7, 2.8, 1.9]
        )

      assert capture_io(fn -> DF.print(df, limit: :infinity) end) == """
             +--------------------------------------------+
             | Explorer DataFrame: [rows: 9, columns: 3]  |
             +-------------+----------------+-------------+
             |      a      |       b        |      c      |
             |    <s64>    |    <string>    |    <f64>    |
             +=============+================+=============+
             | 1           | a              | 9.1         |
             +-------------+----------------+-------------+
             | 2           | b              | 8.2         |
             +-------------+----------------+-------------+
             | 3           | c              | 7.3         |
             +-------------+----------------+-------------+
             | 4           | d              | 6.4         |
             +-------------+----------------+-------------+
             | 5           | e              | 5.5         |
             +-------------+----------------+-------------+
             | 6           | f              | 4.6         |
             +-------------+----------------+-------------+
             | 7           | g              | 3.7         |
             +-------------+----------------+-------------+
             | 8           | h              | 2.8         |
             +-------------+----------------+-------------+
             | 9           | i              | 1.9         |
             +-------------+----------------+-------------+

             """
    end

    test "works with lazy" do
      df = Datasets.iris() |> DF.lazy()

      assert capture_io(fn -> DF.print(df, limit: 1) end) == """
             +-----------------------------------------------------------------------+
             |              Explorer DataFrame: [rows: ???, columns: 5]              |
             +--------------+-------------+--------------+-------------+-------------+
             | sepal_length | sepal_width | petal_length | petal_width |   species   |
             |    <f64>     |    <f64>    |    <f64>     |    <f64>    |  <string>   |
             +==============+=============+==============+=============+=============+
             | 5.1          | 3.5         | 1.4          | 0.2         | Iris-setosa |
             +--------------+-------------+--------------+-------------+-------------+

             """
    end

    test "works with tuple dtypes" do
      df =
        [datetime1: [~N[2023-09-14 00:00:00]], datetime2: [~N[2023-09-14 01:00:00]]]
        |> DF.new()
        |> DF.mutate(duration: datetime2 - datetime1)

      assert capture_io(fn -> DF.print(df, limit: 1) end) == """
             +--------------------------------------------------------------------------+
             |                Explorer DataFrame: [rows: 1, columns: 3]                 |
             +----------------------------+----------------------------+----------------+
             |         datetime1          |         datetime2          |    duration    |
             |       <datetime[μs]>       |       <datetime[μs]>       | <duration[μs]> |
             +============================+============================+================+
             | 2023-09-14 00:00:00.000000 | 2023-09-14 01:00:00.000000 | 1h             |
             +----------------------------+----------------------------+----------------+

             """
    end

    test "works with structs" do
      df = DF.new([%{n: %{a: 1}, m: 2}, %{n: %{a: 2}, m: 3}])

      assert capture_io(fn -> DF.print(df) end) == """
             +-------------------------------------------+
             | Explorer DataFrame: [rows: 2, columns: 2] |
             +------------------+------------------------+
             |        m         |           n            |
             |      <s64>       |      <struct[1]>       |
             +==================+========================+
             | 2                | %{"a" => 1}            |
             +------------------+------------------------+
             | 3                | %{"a" => 2}            |
             +------------------+------------------------+

             """
    end
  end

  test "fetch/2" do
    df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"], c: [4.0, 5.1, 6.2])

    assert Series.to_list(df[:a]) == [1, 2, 3]
    assert Series.to_list(df["a"]) == [1, 2, 3]
    assert DF.to_columns(df[["a"]]) == %{"a" => [1, 2, 3]}
    assert DF.to_columns(df[[:a, :c]]) == %{"a" => [1, 2, 3], "c" => [4.0, 5.1, 6.2]}
    assert DF.to_columns(df[0..-2//1]) == %{"a" => [1, 2, 3], "b" => ["a", "b", "c"]}
    assert DF.to_columns(df[-3..-1]) == DF.to_columns(df)
    assert DF.to_columns(df[..]) == DF.to_columns(df)

    assert %Series{} = s1 = df[0]
    assert Series.to_list(s1) == [1, 2, 3]

    assert %Series{} = s2 = df[2]
    assert Series.to_list(s2) == [4.0, 5.1, 6.2]

    assert %Series{} = s3 = df[-1]
    assert Series.to_list(s3) == [4.0, 5.1, 6.2]

    assert %DF{} = df2 = df[1..2]
    assert DF.names(df2) == ["b", "c"]

    assert %DF{} = df3 = df[-2..-1]
    assert DF.names(df3) == ["b", "c"]

    assert_raise ArgumentError,
                 "no column exists at index 100",
                 fn -> df[100] end

    assert_raise ArgumentError,
                 ~r"could not find column name \"class\"",
                 fn -> df[:class] end

    assert DF.to_columns(df[0..100]) == DF.to_columns(df)
  end

  test "pop/2" do
    df1 = DF.new(a: [1, 2, 3], b: ["a", "b", "c"], c: [4.0, 5.1, 6.2])

    {s1, df2} = Access.pop(df1, "a")
    assert Series.to_list(s1) == [1, 2, 3]
    assert DF.to_columns(df2) == %{"b" => ["a", "b", "c"], "c" => [4.0, 5.1, 6.2]}

    {s1, df2} = Access.pop(df1, :a)
    assert Series.to_list(s1) == [1, 2, 3]
    assert DF.to_columns(df2) == %{"b" => ["a", "b", "c"], "c" => [4.0, 5.1, 6.2]}

    {s1, df2} = Access.pop(df1, 0)
    assert Series.to_list(s1) == [1, 2, 3]
    assert DF.to_columns(df2) == %{"b" => ["a", "b", "c"], "c" => [4.0, 5.1, 6.2]}

    {s1, df2} = Access.pop(df1, -3)
    assert Series.to_list(s1) == [1, 2, 3]
    assert DF.to_columns(df2) == %{"b" => ["a", "b", "c"], "c" => [4.0, 5.1, 6.2]}

    {df3, df4} = Access.pop(df1, ["a", "c"])
    assert DF.to_columns(df3) == %{"a" => [1, 2, 3], "c" => [4.0, 5.1, 6.2]}
    assert DF.to_columns(df4) == %{"b" => ["a", "b", "c"]}

    {df3, df4} = Access.pop(df1, 0..1)
    assert DF.to_columns(df3) == %{"a" => [1, 2, 3], "b" => ["a", "b", "c"]}
    assert DF.to_columns(df4) == %{"c" => [4.0, 5.1, 6.2]}

    {df3, df4} = Access.pop(df1, 0..-2//1)
    assert DF.to_columns(df3) == %{"a" => [1, 2, 3], "b" => ["a", "b", "c"]}
    assert DF.to_columns(df4) == %{"c" => [4.0, 5.1, 6.2]}

    assert {%Series{} = s2, %DF{} = df5} = Access.pop(df1, :a)
    assert Series.to_list(s2) == Series.to_list(df1[:a])
    assert DF.names(df1) -- DF.names(df5) == ["a"]

    assert {%Series{} = s3, %DF{} = df6} = Access.pop(df1, 0)
    assert Series.to_list(s3) == Series.to_list(df1[:a])
    assert DF.names(df1) -- DF.names(df6) == ["a"]
  end

  test "get_and_update/3" do
    df1 = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

    {s, df2} =
      Access.get_and_update(df1, "a", fn current_value ->
        {current_value, Series.from_list([0, 0, 0])}
      end)

    assert s.name == "a"

    assert Series.to_list(s) == [1, 2, 3]
    assert DF.to_columns(df2, atom_keys: true) == %{a: [0, 0, 0], b: ["a", "b", "c"]}
  end

  test "pull/2" do
    df1 = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

    s = DF.pull(df1, "b")

    assert %Series{} = s
    assert s.name == "b"

    assert Series.to_list(s) == ~w(a b c)
  end

  describe "concat_rows/2" do
    test "same dtype columns" do
      df1 = DF.new(x: [1, 2, 3, 4], y: ["a", "b", nil, "c"])
      df2 = DF.new(x: [4, 5, 6, 7], y: ["d", "e", "f", nil])
      df3 = DF.concat_rows(df1, df2)

      assert DF.dtypes(df3) == %{"x" => {:s, 64}, "y" => :string}

      assert Series.to_list(df3["x"]) == [1, 2, 3, 4, 4, 5, 6, 7]
      assert Series.to_list(df3["y"]) == ["a", "b", nil] ++ ~w(c d e f) ++ [nil]
    end

    test "mixing integer and floats" do
      df1 = DF.new(x: [1, 2, 3], y: ["a", "b", "c"])
      df2 = DF.new(x: [4.0, 5.0, 6.0], y: ["d", "e", "f"])
      df3 = DF.concat_rows(df1, df2)

      assert DF.dtypes(df3) == %{"x" => {:f, 64}, "y" => :string}

      assert Series.to_list(df3["x"]) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    end

    test "mixing signed and unsigned integers" do
      df1 =
        DF.new(x: Series.from_list([1, 2, 3], dtype: :u16), y: Series.from_list(["a", "b", "c"]))

      df2 =
        DF.new(x: Series.from_list([4, 5, 6], dtype: :s16), y: Series.from_list(["d", "e", "f"]))

      df3 = DF.concat_rows(df1, df2)

      assert DF.dtypes(df3) == %{"x" => {:s, 32}, "y" => :string}

      assert Series.to_list(df3["x"]) == [1, 2, 3, 4, 5, 6]
    end

    test "mixing floats" do
      df1 =
        DF.new(
          x: Series.from_list([1.0, 2.0, 3.0], dtype: :f32),
          y: Series.from_list(["a", "b", "c"])
        )

      df2 =
        DF.new(
          x: Series.from_list([4.0, 5.0, 6.0], dtype: :f64),
          y: Series.from_list(["d", "e", "f"])
        )

      df3 = DF.concat_rows(df1, df2)

      assert DF.dtypes(df3) == %{"x" => {:f, 64}, "y" => :string}

      assert Series.to_list(df3["x"]) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    end

    test "mixing nulls, signed, unsigned integers, and floats" do
      df1 =
        DF.new(x: Series.from_list([1, 2], dtype: :u16), y: Series.from_list(["a", "b"]))

      df2 =
        DF.new(x: Series.from_list([3.0, 4.0], dtype: :f32), y: Series.from_list(["c", "d"]))

      df3 =
        DF.new(x: [nil, nil], y: [nil, nil])

      df4 =
        DF.new(x: Series.from_list([5, 6], dtype: :s16), y: Series.from_list(["e", "f"]))

      df5 = DF.concat_rows([df1, df2, df3, df4])

      assert DF.dtypes(df5) == %{"x" => {:f, 32}, "y" => :string}
      assert Series.to_list(df5["x"]) == [1.0, 2.0, 3.0, 4.0, nil, nil, 5.0, 6.0]
      assert Series.to_list(df5["y"]) == ["a", "b", "c", "d", nil, nil, "e", "f"]
    end

    test "concat dfs in a list" do
      df1 = DF.new(x: [1, 2, 3], y: ["a", "b", "c"])
      df2 = DF.new(x: [4, 5, 6], y: ["d", "e", "f"])
      df3 = DF.new(x: [7, 8, 9], y: ["g", "h", nil])

      df4 = DF.concat_rows([df1, df2, df3])

      assert DF.dtypes(df4) == %{"x" => {:s, 64}, "y" => :string}

      assert Series.to_list(df4["x"]) == [1, 2, 3, 4, 5, 6, 7, 8, 9]
      assert Series.to_list(df4["y"]) == ~w(a b c d e f g h) ++ [nil]
    end

    test "with incompatible columns" do
      df1 = DF.new(x: [1, 2, 3], y: ["a", "b", "c"])

      assert_raise ArgumentError,
                   "dataframes must have the same columns",
                   fn -> DF.concat_rows(df1, DF.new(z: [7, 8, 9])) end
    end

    test "with incompatible column dtypes" do
      df1 = DF.new(x: [1, 2, 3], y: ["a", "b", "c"])

      assert_raise ArgumentError,
                   "columns and dtypes must be identical for all dataframes",
                   fn -> DF.concat_rows(df1, DF.new(x: [7, 8, 9], y: [10, 11, 12])) end
    end

    test "rechunking logic when nil is introduced in the new dataframe" do
      # this may panic without forced rechunking
      df =
        DF.new(x: [1.0, 2.0], y: [2.0, 3.0])
        |> DF.concat_rows(DF.new(x: [nil], y: [nil]))
        |> DF.mutate(z: correlation(x, y))

      assert abs(df[:z][0] - 1.0) < 1.0e-4
    end
  end

  describe "distinct/2" do
    test "with lists", %{df: df} do
      df1 = DF.distinct(df, [:year, :country])
      assert DF.names(df1) == ["year", "country"]

      assert DF.shape(df1) == {1094, 2}

      df1 = DF.distinct(df, [0, 1])
      assert DF.names(df1) == ["year", "country"]

      assert df == DF.distinct(df, [])

      df2 = DF.distinct(df, [:year, :country], keep_all: true)
      assert DF.names(df2) == DF.names(df)
    end

    test "with one column", %{df: df} do
      df1 = DF.distinct(df, [:country])
      assert DF.names(df1) == ["country"]

      assert DF.shape(df1) == {222, 1}
    end

    test "with ranges", %{df: df} do
      df1 = DF.distinct(df, 0..1//1)
      assert DF.names(df1) == ["year", "country"]

      df2 = DF.distinct(df)
      assert DF.names(df2) == DF.names(df)

      df3 = DF.distinct(df, ..)
      assert DF.names(df3) == DF.names(df)

      assert df == DF.distinct(df, 100..200)
    end
  end

  test "drop_nil/2" do
    df = DF.new(a: [1, 2, nil], b: [1, nil, 3])

    df1 = DF.drop_nil(df)
    assert DF.to_columns(df1) == %{"a" => [1], "b" => [1]}

    df2 = DF.drop_nil(df, :a)
    assert DF.to_columns(df2) == %{"a" => [1, 2], "b" => [1, nil]}

    # Empty list do nothing.
    df3 = DF.drop_nil(df, [])
    assert DF.to_columns(df3) == %{"a" => [1, 2, nil], "b" => [1, nil, 3]}

    assert_raise ArgumentError,
                 "no column exists at index 3",
                 fn -> DF.drop_nil(df, [3, 4, 5]) end

    # It takes the slice of columns in the range
    df4 = DF.drop_nil(df, 0..200)
    assert DF.to_columns(df4) == %{"a" => [1], "b" => [1]}
  end

  describe "relocate/3" do
    test "with single column" do
      df =
        DF.new(
          first: ["a", "b", "a"],
          second: ["x", "y", "z"],
          third: [2.2, 3.3, nil],
          last: [1, 3, 1]
        )

      df1 = DF.relocate(df, "first", after: "second")

      assert df1.names == ["second", "first", "third", "last"]
      assert Series.to_list(df1["first"]) == Series.to_list(df["first"])
      assert Series.to_list(df1["second"]) == Series.to_list(df["second"])
      assert Series.to_list(df1["third"]) == Series.to_list(df["third"])
      assert Series.to_list(df1["last"]) == Series.to_list(df["last"])

      df2 = DF.relocate(df, "second", before: "last")
      assert df2.names == ["first", "third", "second", "last"]

      df3 = DF.relocate(df, 0, after: 3)
      assert df3.names == ["second", "third", "last", "first"]
    end

    test "with multiple columns" do
      df =
        DF.new(
          first: ["a", "b", "a"],
          second: ["x", "y", "z"],
          third: [2.2, 3.3, nil],
          last: [1, 3, 1]
        )

      df1 = DF.relocate(df, ["third", 1], before: -1)
      assert df1.names == ["first", "third", "second", "last"]

      df2 = DF.relocate(df, ["first", "last"], after: "third")
      assert df2.names == ["second", "third", "first", "last"]

      df3 = DF.relocate(df, ["second", "last"], before: 0)
      assert df3.names == ["second", "last", "first", "third"]

      df4 = DF.relocate(df, ["third", "second"], after: "second")
      assert df4.names == ["first", "third", "second", "last"]

      df5 = DF.relocate(df, [], after: "second")
      assert df5.names == ["first", "second", "third", "last"]
    end

    test "with negative index" do
      df =
        DF.new(
          a: ["a value", "some other value", "a third value!"],
          b: [0, 5, -2],
          c: [nil, nil, nil]
        )

      df1 = DF.relocate(df, "a", after: -1)
      assert df1.names == ["b", "c", "a"]

      df2 = DF.relocate(df, 0, before: -1)
      assert df2.names == ["b", "a", "c"]

      df3 = DF.relocate(df, [2, "a"], after: -1)
      assert df3.names == ["b", "c", "a"]
    end

    test "with index at start" do
      df =
        DF.new(
          a: ["a value", "some other value", "a third value!"],
          b: [0, 5, -2],
          c: [nil, nil, nil]
        )

      df1 = DF.relocate(df, "c", after: 0)
      assert df1.names == ["a", "c", "b"]

      df2 = DF.relocate(df, 2, before: 0)
      assert df2.names == ["c", "a", "b"]

      df3 = DF.relocate(df, ["b", "a"], after: 0)
      assert df3.names == ["b", "a", "c"]
    end

    test "with both positioning parameters" do
      df =
        DF.new(
          a: [0, 5, -2],
          b: [nil, nil, nil]
        )

      assert_raise ArgumentError,
                   "only one location must be given. Got both before: \"a\" and after: 1",
                   fn -> DF.relocate(df, 0, before: "a", after: 1) end
    end

    test "ordered DataFrame output after relocation" do
      df1 =
        Explorer.DataFrame.new(
          a: [1, 2],
          b: [5.1, 5.2],
          c: [4, 5],
          d: ["yes", "no"],
          e: [4, 1]
        )

      df2 = DF.relocate(df1, [4, 0], before: 2)
      assert df2.names == ["b", "e", "a", "c", "d"]

      assert DF.dump_csv(df2) ==
               {:ok, "b,e,a,c,d\n5.1,4,1,4,yes\n5.2,1,2,5,no\n"}
    end
  end

  describe "rename/2" do
    test "with lists" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      df1 = DF.rename(df, ["c", "d"])

      assert DF.names(df1) == ["c", "d"]
      assert df1.names == ["c", "d"]
      assert Series.to_list(df1["c"]) == Series.to_list(df["a"])
    end

    test "with keyword" do
      df = DF.new(a: ["a", "b", "a"], b: [1, 3, 1])
      df1 = DF.rename(df, a: "first")

      assert df1.names == ["first", "b"]
      assert Series.to_list(df1["first"]) == Series.to_list(df["a"])
    end

    test "with a map" do
      df = DF.new(a: ["a", "b", "a"], b: [1, 3, 1])
      df1 = DF.rename(df, %{"a" => "first", "b" => "second"})

      assert df1.names == ["first", "second"]
      assert Series.to_list(df1["first"]) == Series.to_list(df["a"])
      assert Series.to_list(df1["second"]) == Series.to_list(df["b"])
    end

    test "with keyword and a column that doesn't exist" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      assert_raise ArgumentError, ~r"could not find column name \"g\"", fn ->
        DF.rename(df, g: "first")
      end
    end

    test "with keyword and a column that is duplicated" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      assert_raise ArgumentError, ~r"duplicate source column for rename", fn ->
        DF.rename(df, a: "first", a: "second")
      end
    end

    test "with mix of column name and index that is duplicated" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      assert_raise ArgumentError, ~r"duplicate source column for rename", fn ->
        DF.rename(df, [{"a", "first"}, {0, "g"}])
      end
    end

    test "with string column names that are duplicated" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      assert_raise ArgumentError, ~r"duplicate source column for rename", fn ->
        DF.rename(df, [{"a", "first"}, {"a", "second"}])
      end
    end

    test "with string column names and a target that is duplicated" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      assert_raise RuntimeError, ~r"duplicate column names found", fn ->
        DF.rename(df, [{"a", "first"}, {"b", "first"}])
      end
    end

    test "with a map and a column that doesn't exist" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      assert_raise ArgumentError, ~r"could not find column name \"i\"", fn ->
        DF.rename(df, %{"a" => "first", "i" => "foo"})
      end
    end

    test "with a mismatch size of columns" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])

      assert_raise ArgumentError,
                   "list of new names must match the number of columns in the dataframe; found 3 new name(s), but the supplied dataframe has 2 column(s)",
                   fn ->
                     DF.rename(df, ["first", "second", "third"])
                   end
    end
  end

  describe "rename_with/2" do
    test "with lists", %{df: df} do
      df_names = DF.names(df)

      df1 = DF.rename_with(df, ["total", "cement"], &String.upcase/1)
      df1_names = DF.names(df1)

      assert df_names -- df1_names == ["total", "cement"]
      assert df1_names -- df_names == ["TOTAL", "CEMENT"]

      assert df1.names == [
               "year",
               "country",
               "TOTAL",
               "solid_fuel",
               "liquid_fuel",
               "gas_fuel",
               "CEMENT",
               "gas_flaring",
               "per_capita",
               "bunker_fuels"
             ]
    end

    test "with ranges", %{df: df} do
      df_names = DF.names(df)

      df1 = DF.rename_with(df, 0..1, &String.upcase/1)
      df1_names = DF.names(df1)

      assert df_names -- df1_names == ["year", "country"]
      assert df1_names -- df_names == ["YEAR", "COUNTRY"]

      df2 = DF.rename_with(df, &String.upcase/1)

      assert Enum.all?(DF.names(df2), &String.match?(&1, ~r/[A-Z]+/))
    end

    test "with a filter function", %{df: df} do
      df_names = DF.names(df)

      df1 = DF.rename_with(df, &String.starts_with?(&1, "tot"), &String.upcase/1)
      df1_names = DF.names(df1)

      assert df_names -- df1_names == ["total"]
      assert df1_names -- df_names == ["TOTAL"]

      df2 = DF.rename_with(df, &String.starts_with?(&1, "non-existent"), &String.upcase/1)

      assert df2 == df
    end
  end

  describe "transpose/2" do
    test "without options" do
      df = DF.new(a: [1, 2, 3], b: ["a", "b", "c"])
      dft = DF.transpose(df)
      assert DF.shape(df) == {3, 2}
      assert DF.shape(dft) == {2, 3}
      assert df.dtypes == %{"a" => {:s, 64}, "b" => :string}
      assert dft.dtypes == %{"column_0" => :string, "column_1" => :string, "column_2" => :string}

      assert DF.to_columns(dft) == %{
               "column_0" => ["1", "a"],
               "column_1" => ["2", "b"],
               "column_2" => ["3", "c"]
             }
    end

    test "header column without name " do
      df = Explorer.DataFrame.new(a: [32.0, 33.0], b: [1, 2], c: ["a", "b"])
      dft = DF.transpose(df, header: true)
      assert DF.shape(df) == {2, 3}
      assert DF.shape(dft) == {3, 3}
      assert df.dtypes == %{"a" => {:f, 64}, "b" => {:s, 64}, "c" => :string}
      assert dft.dtypes == %{"column_0" => :string, "column_1" => :string, "column" => :string}

      assert DF.to_columns(dft) == %{
               "column" => ["a", "b", "c"],
               "column_0" => ["32.0", "1", "a"],
               "column_1" => ["33.0", "2", "b"]
             }
    end

    test "header column with name " do
      df = Explorer.DataFrame.new(a: [32.0, 33.0], b: [1, 2], c: ["a", "b"])
      dft = DF.transpose(df, header: "name")
      assert DF.shape(df) == {2, 3}
      assert DF.shape(dft) == {3, 3}
      assert df.dtypes == %{"a" => {:f, 64}, "b" => {:s, 64}, "c" => :string}
      assert dft.dtypes == %{"column_0" => :string, "column_1" => :string, "name" => :string}

      assert DF.to_columns(dft) == %{
               "name" => ["a", "b", "c"],
               "column_0" => ["32.0", "1", "a"],
               "column_1" => ["33.0", "2", "b"]
             }
    end

    test "with column names" do
      df = Explorer.DataFrame.new(a: [32.0, 33.0], b: [1, 2], c: ["a", "b"])
      dft = DF.transpose(df, header: "name", columns: ["x", "y"])
      assert DF.shape(df) == {2, 3}
      assert DF.shape(dft) == {3, 3}
      assert df.dtypes == %{"a" => {:f, 64}, "b" => {:s, 64}, "c" => :string}
      assert dft.dtypes == %{"name" => :string, "x" => :string, "y" => :string}

      assert DF.to_columns(dft) == %{
               "name" => ["a", "b", "c"],
               "x" => ["32.0", "1", "a"],
               "y" => ["33.0", "2", "b"]
             }
    end

    test "with column names < length of rows raises" do
      df = Explorer.DataFrame.new(a: [32.0, 33.0], b: [1, 2], c: ["a", "b"])
      assert DF.shape(df) == {2, 3}

      assert_raise ArgumentError,
                   "invalid :columns option, length of column names (1) must match the row count (2)",
                   fn ->
                     DF.transpose(df,
                       header: "name",
                       columns: ["x"]
                     )
                   end
    end
  end

  describe "pivot_wider/4" do
    test "with a single id" do
      df1 = DF.new(id: [1, 1], variable: ["a", "b"], value: [1, 2])

      df2 = DF.pivot_wider(df1, "variable", "value")

      assert DF.to_columns(df2, atom_keys: true) == %{
               id: [1],
               a: [1],
               b: [2]
             }
    end

    test "with a category" do
      df1 =
        DF.new(id: [1, 1, 1])
        |> DF.put(:category, Series.from_list(["a", "b", "a"], dtype: :category))
        |> DF.pivot_wider("category", "category")

      assert DF.dtypes(df1) == %{"a" => :category, "b" => :category, "id" => {:s, 64}}
      assert DF.to_columns(df1, atom_keys: true) == %{id: [1], a: ["a"], b: ["b"]}
    end

    test "with a single id discarding any other column" do
      df1 = DF.new(id: [1, 1], x: [6, 12], variable: ["a", "b"], value: [1, 2])

      df2 = DF.pivot_wider(df1, "variable", "value", id_columns: [:id])

      assert DF.to_columns(df2, atom_keys: true) == %{
               id: [1],
               a: [1],
               b: [2]
             }
    end

    test "with a single id and names prefix" do
      df1 = DF.new(id: [1, 1], variable: ["1", "2"], value: [1.0, 2.0])

      df2 =
        DF.pivot_wider(df1, "variable", "value",
          id_columns: ["id"],
          names_prefix: "column_"
        )

      assert DF.to_columns(
               df2,
               atom_keys: true
             ) == %{id: [1], column_1: [1.0], column_2: [2.0]}

      assert df2.names == ["id", "column_1", "column_2"]
    end

    test "with a single id but with a nil value in the variable series" do
      df1 = DF.new(id: [1, 1, 1], variable: ["a", "b", nil], value: [1, 2, 3])

      df2 = DF.pivot_wider(df1, "variable", "value")

      assert DF.to_columns(df2) == %{
               "id" => [1],
               "a" => [1],
               "b" => [2],
               "nil" => [3]
             }
    end

    test "with multiple id columns" do
      df = DF.new(id: [1, 1], variable: ["a", "b"], value: [1, 2], other_id: [4, 5])
      df1 = DF.pivot_wider(df, "variable", "value")

      assert DF.names(df1) == ["id", "other_id", "a", "b"]
      assert df1.names == ["id", "other_id", "a", "b"]

      assert DF.to_columns(df1, atom_keys: true) == %{
               id: [1, 1],
               other_id: [4, 5],
               a: [1, nil],
               b: [nil, 2]
             }
    end

    test "with multiple id columns and one id equal to a variable name" do
      df = DF.new(id: [1, 1], variable: ["a", "b"], value: [1, 2], b: [4, 5])
      df1 = DF.pivot_wider(df, "variable", "value")

      assert DF.names(df1) == ["id", "b", "a", "b_1"]

      assert DF.to_columns(df1, atom_keys: true) == %{
               id: [1, 1],
               b: [4, 5],
               a: [1, nil],
               b_1: [nil, 2]
             }
    end

    test "with multiple id columns and one id equal to a variable name, but with prefix option" do
      df = DF.new(id: [1, 1], variable: ["a", "b"], value: [1, 2], b: [4, 5])
      df1 = DF.pivot_wider(df, "variable", "value", names_prefix: "col_")

      assert DF.names(df1) == ["id", "b", "col_a", "col_b"]

      assert DF.to_columns(df1, atom_keys: true) == %{
               id: [1, 1],
               b: [4, 5],
               col_a: [1, nil],
               col_b: [nil, 2]
             }
    end

    test "with a single id column ignoring other columns" do
      df = DF.new(id: [1, 1], variable: ["a", "b"], value: [1, 2], other: [4, 5])

      df2 = DF.pivot_wider(df, "variable", "value", id_columns: [:id])
      assert DF.names(df2) == ["id", "a", "b"]

      df2 = DF.pivot_wider(df, "variable", "value", id_columns: [0])
      assert DF.names(df2) == ["id", "a", "b"]
      assert df2.names == ["id", "a", "b"]

      assert DF.to_columns(df2, atom_keys: true) == %{
               id: [1],
               a: [1],
               b: [2]
             }
    end

    test "with a single id column and repeated values" do
      df = DF.new(id: [1, 1, 2, 2], variable: ["a", "b", "a", "b"], value: [1, 2, 3, 4])

      df2 = DF.pivot_wider(df, "variable", "value", id_columns: [:id])
      assert DF.names(df2) == ["id", "a", "b"]

      df2 = DF.pivot_wider(df, "variable", "value", id_columns: [0])
      assert DF.names(df2) == ["id", "a", "b"]

      assert DF.to_columns(df2, atom_keys: true) == %{
               id: [1, 2],
               a: [1, 3],
               b: [2, 4]
             }
    end

    test "with a single id column and repeated values with names prefix" do
      df = DF.new(id: [1, 1, 2, 2], variable: ["a", "b", "a", "b"], value: [1, 2, 3, 4])

      df2 = DF.pivot_wider(df, "variable", "value", id_columns: [:id], names_prefix: "prefix_")
      assert DF.names(df2) == ["id", "prefix_a", "prefix_b"]

      assert DF.to_columns(df2, atom_keys: true) == %{
               id: [1, 2],
               prefix_a: [1, 3],
               prefix_b: [2, 4]
             }
    end

    test "with a filter function for id columns" do
      df = DF.new(id_main: [1, 1], variable: ["a", "b"], value: [1, 2], other: [4, 5])

      df1 = DF.pivot_wider(df, "variable", "value", id_columns: &String.starts_with?(&1, "id"))
      assert DF.names(df1) == ["id_main", "a", "b"]

      assert DF.to_columns(df1, atom_keys: true) == %{
               id_main: [1],
               a: [1],
               b: [2]
             }
    end

    test "with multiple value columns expand the new column names" do
      df1 = DF.new(id: [1, 1], variable: ["a", "b"], value: [1, 2], another_value: [6, 9])

      df2 = DF.pivot_wider(df1, "variable", ["value", "another_value"])

      assert DF.to_columns(df2, atom_keys: true) == %{
               id: [1],
               value_variable_a: [1],
               value_variable_b: [2],
               another_value_variable_a: [6],
               another_value_variable_b: [9]
             }
    end

    test "without an id column" do
      df = DF.new(id: [1, 1], variable: ["a", "b"], value: [1, 2], other_id: [4, 5])

      assert_raise ArgumentError,
                   "id_columns must select at least one existing column, but [] selects none. Note that float columns are discarded from the selection.",
                   fn ->
                     DF.pivot_wider(df, "variable", "value", id_columns: [])
                   end

      assert_raise ArgumentError,
                   ~r/id_columns must select at least one existing column, but/,
                   fn ->
                     DF.pivot_wider(df, "variable", "value",
                       id_columns: &String.starts_with?(&1, "none")
                     )
                   end
    end

    test "with an id column of type float" do
      df = DF.new(float_id: [1.5, 1.6], variable: ["a", "b"], value: [1, 2])

      assert_raise ArgumentError,
                   "id_columns must select at least one existing column, but 0..-1//1 selects none. Note that float columns are discarded from the selection.",
                   fn ->
                     DF.pivot_wider(df, "variable", "value")
                   end

      assert_raise ArgumentError,
                   "id_columns must select at least one existing column, but [:float_id] selects none. Note that float columns are discarded from the selection.",
                   fn ->
                     DF.pivot_wider(df, "variable", "value", id_columns: [:float_id])
                   end
    end

    test "with a string values_from" do
      df1 = DF.new(id: [1, 1], variable: ["a", "b"], value: ["x", "y"])

      df2 = DF.pivot_wider(df1, "variable", "value")

      assert DF.to_columns(df2, atom_keys: true) == %{
               id: [1],
               a: ["x"],
               b: ["y"]
             }
    end

    test "with a date values_from" do
      df1 = DF.new(id: [1, 1], variable: ["a", "b"], value: [~D[2022-01-01], ~D[2023-01-01]])

      df2 = DF.pivot_wider(df1, "variable", "value")

      assert DF.to_columns(df2, atom_keys: true) == %{
               id: [1],
               a: [~D[2022-01-01]],
               b: [~D[2023-01-01]]
             }
    end
  end

  describe "pivot_longer/3" do
    test "without selecting columns", %{df: df} do
      df = DF.pivot_longer(df, &String.ends_with?(&1, "fuel"), select: [])

      assert df.names == ["variable", "value"]
      assert df.dtypes == %{"variable" => :string, "value" => {:s, 64}}
      assert DF.shape(df) == {3282, 2}
    end

    test "selecting some columns", %{df: df} do
      df = DF.pivot_longer(df, &String.ends_with?(&1, "fuel"), select: ["year", "country"])

      assert df.names == ["year", "country", "variable", "value"]

      assert df.dtypes == %{
               "year" => {:s, 64},
               "country" => :string,
               "variable" => :string,
               "value" => {:s, 64}
             }

      assert DF.shape(df) == {3282, 4}
    end

    test "selecting all the columns (not passing select option)", %{df: df} do
      df = DF.pivot_longer(df, &String.ends_with?(&1, ["fuel", "fuels"]))

      assert df.names == [
               "year",
               "country",
               "total",
               "cement",
               "gas_flaring",
               "per_capita",
               "variable",
               "value"
             ]

      assert DF.shape(df) == {4376, 8}
    end

    test "dropping some columns", %{df: df} do
      df =
        DF.pivot_longer(df, &String.ends_with?(&1, ["fuel", "fuels"]),
          discard: ["gas_flaring", "cement"]
        )

      assert df.names == [
               "year",
               "country",
               "total",
               "per_capita",
               "variable",
               "value"
             ]
    end

    test "select and discard with the same columns discards the columns", %{df: df} do
      df =
        DF.pivot_longer(df, &String.ends_with?(&1, ["fuel", "fuels"]),
          select: ["gas_flaring", "cement"],
          discard: fn name -> name == "cement" end
        )

      assert df.names == [
               "gas_flaring",
               "variable",
               "value"
             ]
    end

    test "with pivot column in the same list of select columns", %{df: df} do
      assert_raise ArgumentError,
                   "selected columns must not include columns to pivot, but found \"solid_fuel\" in both",
                   fn ->
                     DF.pivot_longer(df, &String.ends_with?(&1, "fuel"),
                       select: ["year", "country", "solid_fuel"]
                     )
                   end
    end

    test "with multiple types of columns to pivot", %{df: df} do
      assert_raise ArgumentError,
                   "columns to pivot must include columns with the same dtype, but found multiple dtypes: :string and {:s, 64}",
                   fn ->
                     DF.pivot_longer(df, &(&1 in ["solid_fuel", "country"]))
                   end
    end
  end

  describe "table reader integration" do
    test "eager" do
      df = DF.new(x: [1, 2, 3], y: ["a", "b", "c"])

      assert df |> Table.to_rows() |> Enum.to_list() == [
               %{"x" => 1, "y" => "a"},
               %{"x" => 2, "y" => "b"},
               %{"x" => 3, "y" => "c"}
             ]

      columns = Table.to_columns(df)
      assert Enum.to_list(columns["x"]) == [1, 2, 3]
      assert Enum.to_list(columns["y"]) == ["a", "b", "c"]

      assert {:columns, %{count: 3}, _} = Table.Reader.init(df)
    end

    test "lazy" do
      df = DF.new(x: [1, 2, 3], y: ["a", "b", "c"]) |> DF.lazy()

      assert df |> Table.to_rows() |> Enum.to_list() == [
               %{"x" => 1, "y" => "a"},
               %{"x" => 2, "y" => "b"},
               %{"x" => 3, "y" => "c"}
             ]

      columns = Table.to_columns(df)
      assert Enum.to_list(columns["x"]) == [1, 2, 3]
      assert Enum.to_list(columns["y"]) == ["a", "b", "c"]

      assert {:columns, %{count: 3}, _} = Table.Reader.init(df)
    end
  end

  test "collect/1 is no-op", %{df: df} do
    assert DF.collect(df) == df
  end

  test "lazy/1", %{df: df} do
    assert %Explorer.PolarsBackend.LazyFrame{} = DF.lazy(df).data
  end

  describe "to_rows/2" do
    test "converts rows to maps" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])

      assert [
               %{"a" => "a", "b" => 1},
               %{"a" => "b", "b" => 2},
               %{"a" => "c", "b" => 3}
             ] == DF.to_rows(df)
    end

    test "converts rows to maps with atom keys" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])

      assert [
               %{a: "a", b: 1},
               %{a: "b", b: 2},
               %{a: "c", b: 3}
             ] == DF.to_rows(df, atom_keys: true)
    end
  end

  defp lazy?(stream) do
    match?(%Stream{}, stream) or is_function(stream, 2)
  end

  describe "to_rows_stream/2" do
    test "converts rows to stream of maps" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])
      stream = DF.to_rows_stream(df)

      assert lazy?(stream)

      assert [
               %{"a" => "a", "b" => 1},
               %{"a" => "b", "b" => 2},
               %{"a" => "c", "b" => 3}
             ] == Enum.to_list(stream)
    end

    test "converts rows to stream of maps with atom keys" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])
      stream = DF.to_rows_stream(df, atom_keys: true)

      assert lazy?(stream)

      assert [
               %{a: "a", b: 1},
               %{a: "b", b: 2},
               %{a: "c", b: 3}
             ] == Enum.to_list(stream)
    end
  end

  describe "select/2" do
    test "keep column names" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])
      df = DF.select(df, ["a"])

      assert DF.names(df) == ["a"]
      assert df.names == ["a"]
    end

    test "keep column positions" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])
      df = DF.select(df, [1])

      assert DF.names(df) == ["b"]
      assert df.names == ["b"]
    end

    test "keep column range" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3], c: [42.0, 42.1, 42.2])
      df = DF.select(df, 1..2)

      assert DF.names(df) == ["b", "c"]
      assert df.names == ["b", "c"]
    end

    test "keep columns matching callback" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3], c: [42.0, 42.1, 42.2])
      df = DF.select(df, fn name -> name in ~w(a c) end)

      assert DF.names(df) == ["a", "c"]
      assert df.names == ["a", "c"]
    end

    test "keep column raises error with non-existent column" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])

      assert_raise ArgumentError, ~r"could not find column name \"g\"", fn ->
        DF.select(df, ["g"])
      end
    end
  end

  describe "discard/2" do
    test "drop column names" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])
      df = DF.discard(df, ["a"])

      assert DF.names(df) == ["b"]
      assert df.names == ["b"]
    end

    test "drop column positions" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])
      df = DF.discard(df, [1])

      assert DF.names(df) == ["a"]
      assert df.names == ["a"]
    end

    test "drop column range" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3], c: [42.0, 42.1, 42.2])
      df = DF.discard(df, 1..2)

      assert DF.names(df) == ["a"]
      assert df.names == ["a"]
    end

    test "drop columns matching callback" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3], c: [42.0, 42.1, 42.2])
      df = DF.discard(df, fn name -> name in ~w(a c) end)

      assert DF.names(df) == ["b"]
      assert df.names == ["b"]
    end

    test "dropping a non-existent column fails silently" do
      df = DF.new(a: ["a", "b", "c"], b: [1, 2, 3])

      df1 = DF.discard(df, ["g"])
      assert df.names == df1.names
    end
  end

  describe "head/2" do
    test "selects the first 5 rows by default", %{df: df} do
      df1 = DF.head(df)
      assert DF.shape(df1) == {5, 10}
    end

    test "selects the first 2 rows", %{df: df} do
      df1 = DF.head(df, 2)
      assert DF.shape(df1) == {2, 10}
    end
  end

  describe "tail/2" do
    test "selects the last 5 rows by default", %{df: df} do
      df1 = DF.tail(df)
      assert DF.shape(df1) == {5, 10}
    end

    test "selects the last 2 rows", %{df: df} do
      df1 = DF.tail(df, 2)
      assert DF.shape(df1) == {2, 10}
    end
  end

  describe "put/3" do
    test "adds a new column to a dataframe" do
      df = DF.new(a: [1, 2, 3])
      df1 = DF.put(df, :b, Series.transform(df[:a], fn n -> n * 2 end))

      assert DF.names(df1) == ["a", "b"]
      assert DF.dtypes(df1) == %{"a" => {:s, 64}, "b" => {:s, 64}}

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3],
               b: [2, 4, 6]
             }
    end

    test "replaces a column in the dataframe" do
      df = DF.new(a: [1, 2, 3])
      df1 = DF.put(df, :a, Series.transform(df[:a], fn n -> n * 2 end))

      assert DF.names(df1) == ["a"]
      assert DF.dtypes(df1) == %{"a" => {:s, 64}}

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [2, 4, 6]
             }
    end

    test "with tensors" do
      i = Nx.tensor(1)
      f = Nx.tensor([1.0], type: :f64)
      d = Nx.tensor([-1, 0, 1], type: :s32)

      df =
        DF.new(
          a: [1, 2, 3],
          b: [4.0, 5.0, 6.0],
          c: ["a", "b", "c"],
          d: [~D[1970-01-01], ~D[1980-01-01], ~D[1990-01-01]]
        )

      assert DF.put(df, :a, i)[:a] |> Series.to_list() == [1, 1, 1]
      assert DF.put(df, :a, f, dtype: {:f, 64})[:a] |> Series.to_list() == [1.0, 1.0, 1.0]

      assert DF.put(df, :a, d, dtype: :date)[:a] |> Series.to_list() == [
               ~D[1969-12-31],
               ~D[1970-01-01],
               ~D[1970-01-02]
             ]

      assert DF.put(df, :c, i, dtype: :integer)[:c] |> Series.to_list() == [1, 1, 1]
      assert DF.put(df, :c, f, dtype: {:f, 64})[:c] |> Series.to_list() == [1.0, 1.0, 1.0]

      assert DF.put(df, :c, d, dtype: :date)[:c] |> Series.to_list() == [
               ~D[1969-12-31],
               ~D[1970-01-01],
               ~D[1970-01-02]
             ]

      assert DF.put(df, :d, i, dtype: :integer)[:d] |> Series.to_list() == [1, 1, 1]
      assert DF.put(df, :d, f, dtype: {:f, 64})[:d] |> Series.to_list() == [1.0, 1.0, 1.0]

      assert DF.put(df, :d, d)[:d] |> Series.to_list() == [
               ~D[1969-12-31],
               ~D[1970-01-01],
               ~D[1970-01-02]
             ]

      assert_raise ArgumentError,
                   "cannot convert dtype string into a binary/tensor type",
                   fn -> DF.put(df, :c, i) end
    end
  end

  describe "describe/2" do
    test "default percentiles" do
      df = DF.new(a: ["d", nil, "f"], b: [1, 2, 3], c: [10, 20, 30])
      df1 = DF.describe(df)

      assert df1.dtypes == %{
               "a" => :string,
               "b" => {:f, 64},
               "c" => {:f, 64},
               "describe" => :string
             }

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: ["2", "1", nil, nil, nil, nil, nil, nil, nil],
               b: [3.0, 0.0, 2.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0],
               c: [3.0, 0.0, 20.0, 10.0, 10.0, 20.0, 20.0, 30.0, 30.0],
               describe: ["count", "nil_count", "mean", "std", "min", "25%", "50%", "75%", "max"]
             }
    end

    test "custom percentiles" do
      df = DF.new(b: [1, 2, 3])
      df1 = DF.describe(df, percentiles: [0.3, 0.5, 0.8])
      df2 = DF.describe(df, percentiles: [0.5])

      assert df1.dtypes == %{
               "b" => {:f, 64},
               "describe" => :string
             }

      assert DF.to_columns(df1, atom_keys: true) == %{
               b: [3.0, 0.0, 2.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0],
               describe: ["count", "nil_count", "mean", "std", "min", "30%", "50%", "80%", "max"]
             }

      assert DF.to_columns(df2, atom_keys: true) == %{
               b: [3.0, 0.0, 2.0, 1.0, 1.0, 2.0, 3.0],
               describe: ["count", "nil_count", "mean", "std", "min", "50%", "max"]
             }
    end

    test "no percentiles" do
      df = DF.new(b: [1, 2, 3])
      df1 = DF.describe(df, percentiles: [])

      assert DF.to_columns(df1, atom_keys: true) == %{
               b: [3.0, 0.0, 2.0, 1.0, 1.0, 3.0],
               describe: ["count", "nil_count", "mean", "std", "min", "max"]
             }
    end

    test "describe columns - max, min columns" do
      df =
        DF.new(
          number: [1, 2, nil, 3],
          list: [[], [], [], []],
          # TODO: enable "null" columns when problem with polars is solved.
          # null: [nil, nil, nil, nil],
          string: ["a", "b", "c", "nil"],
          date: [~D[2021-01-01], ~D[1999-12-31], nil, ~D[2023-01-01]],
          time: [~T[00:02:03.000212], ~T[00:05:04.000456], ~T[00:07:04.000776], nil],
          datetime: [
            nil,
            ~N[2021-01-01 00:00:00],
            ~N[1999-12-31 00:00:00],
            ~N[2023-12-13 17:38:00]
          ],
          duration: [
            nil,
            ~N[2020-01-01 00:00:00],
            ~N[1999-11-30 00:00:00],
            ~N[2023-12-12 17:38:00]
          ]
        )

      df = DF.mutate(df, duration: datetime - duration)

      assert df.dtypes == %{
               "date" => :date,
               "datetime" => {:datetime, :microsecond},
               "duration" => {:duration, :microsecond},
               "list" => {:list, :null},
               # "null" => :null,
               "number" => {:s, 64},
               "string" => :string,
               "time" => :time
             }

      describe_df = DF.describe(df)

      assert describe_df.dtypes == %{
               "date" => :string,
               "datetime" => :string,
               "describe" => :string,
               "duration" => :string,
               "list" => :string,
               # "null" => :string,
               "number" => {:f, 64},
               "string" => :string,
               "time" => :string
             }

      assert DF.to_columns(describe_df, atom_keys: true) == %{
               date: ["3", "1", nil, nil, nil, nil, nil, nil, nil],
               datetime: [
                 "3",
                 "1",
                 nil,
                 nil,
                 "1999-12-31 00:00:00.000000",
                 nil,
                 nil,
                 nil,
                 "2023-12-13 17:38:00.000000"
               ],
               describe: ["count", "nil_count", "mean", "std", "min", "25%", "50%", "75%", "max"],
               duration: ["3", "1", nil, nil, "1d", nil, nil, nil, "366d"],
               list: ["4", "0", nil, nil, nil, nil, nil, nil, nil],
               # null: ["0", "4", nil, nil, nil, nil, nil, nil, nil],
               number: [3.0, 1.0, 2.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0],
               string: ["4", "0", nil, nil, nil, nil, nil, nil, nil],
               time: ["3", "1", nil, nil, nil, nil, nil, nil, nil]
             }
    end
  end

  describe "frequencies/1" do
    test "multiple columns with and without nils" do
      df =
        DF.new(
          a: [1, 1, 1, 2, 3, 3],
          b: [true, true, true, false, false, false],
          c: ["a", nil, "a", nil, "a", nil]
        )

      assert DF.frequencies(df, [:a, :b]) |> DF.to_columns(atom_keys: true) ==
               %{a: [1, 3, 2], b: [true, false, false], counts: [3, 2, 1]}

      assert DF.frequencies(df, [:a, :c]) |> DF.to_columns(atom_keys: true) ==
               %{a: [1, 1, 2, 3, 3], c: ["a", nil, nil, "a", nil], counts: [2, 1, 1, 1, 1]}
    end

    test "invalid columns args" do
      assert_raise ArgumentError,
                   ~r/cannot be empty/,
                   fn -> DF.new(a: [1]) |> DF.frequencies([]) end

      assert_raise ArgumentError,
                   ~r/could not find column name/,
                   fn -> DF.new(a: [1]) |> DF.frequencies([:x]) end
    end
  end

  describe "nil_count/1" do
    test "various dtypes" do
      require Explorer.DataFrame, as: DF

      df =
        DF.new(
          a: [1, nil, 3],
          b: [6.0, nil, :nan],
          c: ["a", "b", "a"],
          d: ["a", nil, nil],
          e: [nil, nil, nil]
        )

      df1 = DF.nil_count(df)
      assert DF.to_columns(df1, atom_keys: true) == %{a: [1], b: [1], c: [0], d: [2], e: [3]}
    end
  end

  describe "concat_columns/1" do
    test "combine columns of both data frames" do
      df1 = DF.new(x: [1, 2, 3], y: ["a", "b", "c"])
      df2 = DF.new(z: [4, 5, 6], a: ["d", "e", "f"])

      df = DF.concat_columns([df1, df2])

      assert df.names == ["x", "y", "z", "a"]

      assert DF.to_columns(df, atom_keys: true) == %{
               x: [1, 2, 3],
               y: ["a", "b", "c"],
               z: [4, 5, 6],
               a: ["d", "e", "f"]
             }
    end

    test "with conflicting names add number suffix" do
      df1 = DF.new(x: [1, 2, 3], y: ["a", "b", "c"])
      df2 = DF.new(x: [4, 5, 6], a: ["d", "e", "f"])

      df = DF.concat_columns([df1, df2])
      assert df.names == ["x", "y", "x_1", "a"]

      assert DF.to_columns(df, atom_keys: true) == %{
               x: [1, 2, 3],
               y: ["a", "b", "c"],
               x_1: [4, 5, 6],
               a: ["d", "e", "f"]
             }
    end
  end

  describe "sample/3" do
    test "sampling by integer" do
      df = DF.new(letters: ~w(a b c d e f g h i j), numbers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

      df1 = DF.sample(df, 3, seed: 100)

      assert DF.to_columns(df1, atom_keys: true) == %{
               letters: ["j", "f", "h"],
               numbers: [10, 6, 8]
             }
    end

    test "sampling by fraction (float)" do
      df = DF.new(letters: ~w(a b c d e f g h i j), numbers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

      df1 = DF.sample(df, 0.2, seed: 100)

      assert DF.to_columns(df1, atom_keys: true) == %{
               letters: ["f", "g"],
               numbers: [6, 7]
             }
    end

    test "sampling by integer with same size of the dataframe" do
      df = DF.new(letters: ~w(a b c d e f g h i j), numbers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

      df1 = DF.sample(df, 10, seed: 100)

      # Without "shuffle", returns the same DF.
      assert DF.to_columns(df1, atom_keys: true) == %{
               letters: ~w(a b c d e f g h i j),
               numbers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
             }
    end

    test "sampling by integer with same size of the dataframe and with shuffle" do
      df = DF.new(letters: ~w(a b c d e f g h i j), numbers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

      df1 = DF.sample(df, 10, seed: 100, shuffle: true)

      assert DF.to_columns(df1, atom_keys: true) == %{
               letters: ["h", "j", "c", "a", "e", "b", "d", "i", "f", "g"],
               numbers: [8, 10, 3, 1, 5, 2, 4, 9, 6, 7]
             }
    end

    test "sampling by fraction the all rows of the dataframe" do
      df = DF.new(letters: ~w(a b c d e f g h i j), numbers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

      df1 = DF.sample(df, 1.0, seed: 100)

      # Without "shuffle", returns the same DF.
      assert DF.to_columns(df1, atom_keys: true) == %{
               letters: ~w(a b c d e f g h i j),
               numbers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
             }
    end

    test "sampling by fraction with all rows and with shuffle" do
      df = DF.new(letters: ~w(a b c d e f g h i j), numbers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

      df1 = DF.sample(df, 1.0, seed: 100, shuffle: true)

      assert DF.to_columns(df1, atom_keys: true) == %{
               letters: ["h", "j", "c", "a", "e", "b", "d", "i", "f", "g"],
               numbers: [8, 10, 3, 1, 5, 2, 4, 9, 6, 7]
             }
    end
  end

  describe "shuffle/2" do
    test "shuffles the dataframe rows" do
      df = DF.new(letters: ~w(a b c d e f g h i j), numbers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

      df1 = DF.shuffle(df, seed: 100)

      assert DF.to_columns(df1, atom_keys: true) == %{
               letters: ["h", "j", "c", "a", "e", "b", "d", "i", "f", "g"],
               numbers: [8, 10, 3, 1, 5, 2, 4, 9, 6, 7]
             }
    end
  end

  describe "summarise/2" do
    test "summarise with nil" do
      df = DF.new(strs: ["a", "b", "c"], nums: [1, 2, 3])
      df1 = DF.summarise(df, c: nil)
      assert DF.dtypes(df1) == %{"c" => :null}
      assert DF.to_columns(df1, atom_keys: true) == %{c: [nil]}
    end

    test "one column with aggregation and without groups", %{df: df} do
      df1 =
        DF.summarise(df,
          total: count(total),
          solid_fuel_mean: mean(solid_fuel),
          gas_fuel_max: max(gas_fuel)
        )

      assert DF.names(df1) == ["total", "solid_fuel_mean", "gas_fuel_max"]

      assert DF.dtypes(df1) == %{
               "total" => {:u, 32},
               "solid_fuel_mean" => {:f, 64},
               "gas_fuel_max" => {:s, 64}
             }

      assert DF.groups(df1) == []

      assert DF.to_columns(df1, atom_keys: true) == %{
               total: [1094],
               gas_fuel_max: [390_719],
               solid_fuel_mean: [18212.27970749543]
             }
    end

    test "allows returning the group as a list" do
      df =
        DF.new(
          letters: ~w(a b c d e f g h i j),
          is_vowel: [true, false, false, false, true, false, false, false, true, false]
        )
        |> DF.group_by(:is_vowel)
        |> DF.summarise(letters: letters)

      assert DF.to_columns(df, atom_keys: true) == %{
               is_vowel: [true, false],
               letters: [["a", "e", "i"], ["b", "c", "d", "f", "g", "h", "j"]]
             }

      assert DF.dtypes(df) == %{"is_vowel" => :boolean, "letters" => {:list, :string}}
    end

    test "mode/1 without groups (see grouped_test for groups)" do
      df = DF.new(petal_width: [1, 2, 2, 3, 3, 120])

      df1 =
        DF.summarise(df, petal_width_mode: mode(petal_width), petal_width_sum: sum(petal_width))

      assert DF.dtypes(df1) == %{
               "petal_width_mode" => {:list, {:s, 64}},
               "petal_width_sum" => {:s, 64}
             }

      result = DF.to_columns(df1)

      assert result["petal_width_sum"] == [131]
      # Since this does not maintain order, we need to check differently.
      assert [[a, b]] = result["petal_width_mode"]

      assert a in [2, 3]
      assert b in [2, 3]
    end

    test "argmax/1 and argmin/1" do
      df =
        DF.new(
          a: [1, 2, 3, nil],
          b: [1.3, nil, 5.4, 2.6],
          c: [nil, ~D[2023-01-01], ~D[2022-01-01], ~D[2021-01-01]],
          d: [
            ~N[2023-01-01 00:00:00],
            ~N[2022-01-01 00:00:00],
            ~N[2021-01-01 00:00:00],
            nil
          ],
          e: [
            ~N[2023-01-01 10:00:00],
            ~N[2022-01-01 01:00:00],
            ~N[2021-01-01 00:10:00],
            nil
          ],
          f: [1.0, :infinity, :neg_infinity, nil]
        )
        |> DF.mutate(e: cast(e, :time))

      df1 =
        DF.summarise(df,
          a: argmax(a),
          b: argmax(b),
          c: argmax(c),
          d: argmax(d),
          e: argmax(e),
          f: argmax(f)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [2],
               b: [2],
               c: [1],
               d: [0],
               e: [0],
               f: [1]
             }

      df2 =
        DF.summarise(df,
          a: argmin(fill_missing(a, :max)),
          b: argmin(fill_missing(b, :max)),
          c: argmin(fill_missing(c, :max)),
          d: argmin(fill_missing(d, :max)),
          e: argmin(fill_missing(e, :max)),
          f: argmin(fill_missing(f, :max))
        )

      assert DF.to_columns(df2, atom_keys: true) == %{
               a: [0],
               b: [0],
               c: [3],
               d: [2],
               e: [2],
               f: [2]
             }

      df3 =
        DF.summarise(df,
          a: argmin(a),
          b: argmin(b),
          c: argmin(c),
          d: argmin(d),
          e: argmin(e),
          f: argmin(f)
        )

      assert DF.to_columns(df3, atom_keys: true) == %{
               a: [0],
               b: [0],
               c: [3],
               d: [2],
               e: [2],
               f: [2]
             }
    end

    test "all?/1 and any?/1" do
      df = DF.new([a: [true, false, true], nils: [nil, nil, nil]], dtypes: [nils: :boolean])

      df1 =
        DF.summarise(df,
          all?: all?(a),
          any?: any?(a),
          all_nils?: all?(nils),
          any_nils?: any?(nils)
        )

      assert DF.to_columns(df1, atom_keys: true) == %{
               all?: [false],
               any?: [true],
               all_nils?: [true],
               any_nils?: [false]
             }
    end
  end

  describe "explode/2" do
    test "explodes a list column" do
      df = DF.new(letters: [~w(a e), ~w(b c d)], is_vowel: [true, false])

      df1 = DF.explode(df, :letters)

      assert DF.to_columns(df1, atom_keys: true) == %{
               letters: ["a", "e", "b", "c", "d"],
               is_vowel: [true, true, false, false, false]
             }

      assert DF.dtypes(df1) == %{"is_vowel" => :boolean, "letters" => :string}
    end

    test "works with multiple columns" do
      df = DF.new(a: [[1, 2], [3, 4]], b: [[5, 6], [7, 8]], c: ["a", "b"])

      df1 = DF.explode(df, [:a, :b])

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 2, 3, 4],
               b: [5, 6, 7, 8],
               c: ["a", "a", "b", "b"]
             }
    end

    test "raises if the columns are not of the list type" do
      df = DF.new(a: [1, 2, 3], b: [[1, 2], [3, 4], [5, 6]])

      assert_raise ArgumentError,
                   "explode/2 expects list columns, but the given columns have the types: {:s, 64}",
                   fn -> DF.explode(df, :a) end

      assert_raise ArgumentError,
                   "explode/2 expects list columns, but the given columns have the types: {:list, {:s, 64}} and {:s, 64}",
                   fn -> DF.explode(df, [:a, :b]) end
    end
  end

  describe "unnest/2" do
    test "unnests a struct column" do
      df = DF.new(a: [%{x: 1, y: 2}, %{x: 3, y: 4}])

      df1 = DF.unnest(df, :a)

      assert DF.names(df1) == ["x", "y"]
      assert DF.dtypes(df1) == %{"x" => {:s, 64}, "y" => {:s, 64}}

      assert DF.to_columns(df1, atom_keys: true) == %{
               x: [1, 3],
               y: [2, 4]
             }
    end

    test "unnests multiple struct columns at once" do
      df = DF.new(a: [%{x: 1, y: 2}, %{x: 3, y: 4}], b: [%{z: 5}, %{z: 6}])

      df1 = DF.unnest(df, [:a, :b])

      assert DF.names(df1) == ["x", "y", "z"]
      assert DF.dtypes(df1) == %{"x" => {:s, 64}, "y" => {:s, 64}, "z" => {:s, 64}}

      assert DF.to_columns(df1, atom_keys: true) == %{
               x: [1, 3],
               y: [2, 4],
               z: [5, 6]
             }
    end

    test "unnests the columns in the right order" do
      df =
        DF.new(
          before: [1, 2],
          a: [%{x: 1}, %{x: 2}],
          between: [3, 4],
          x: [%{y: 1}, %{y: 2}],
          after: [5, 6]
        )

      df1 = DF.unnest(df, [:a, :x])

      assert DF.names(df1) == ["before", "x", "between", "y", "after"]

      assert DF.dtypes(df1) == %{
               "before" => {:s, 64},
               "x" => {:s, 64},
               "between" => {:s, 64},
               "y" => {:s, 64},
               "after" => {:s, 64}
             }
    end

    test "errors if the column is not of the struct type" do
      df = DF.new(a: [1, 2, 3])

      assert_raise ArgumentError,
                   "unnest/2 expects struct columns, but the given columns have the types: {:s, 64}",
                   fn -> DF.unnest(df, :a) end
    end

    test "errors when unnesting columns with clashing names" do
      df = DF.new(a: [%{x: 1}, %{x: 2}], x: [3, 4])

      assert_raise RuntimeError,
                   ~r/column with name 'x' has more than one occurrences/,
                   fn -> DF.unnest(df, :a) end
    end

    test "errors when unnesting multiple columns with clashing names" do
      df = DF.new(a: [%{x: 1, y: 2}, %{x: 3, y: 4}], b: [%{x: 5}, %{x: 6}])

      assert_raise RuntimeError,
                   ~r/column with name 'x' has more than one occurrences/,
                   fn -> DF.unnest(df, [:a, :b]) end
    end
  end

  describe "correlation/2" do
    test "two integer columns" do
      df = DF.new(dogs: [1, 8, 3], cats: [4, 5, 2])
      df1 = DF.correlation(df)

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["dogs", "cats"],
               dogs: [1.0000000000000002, 0.5447047794019219],
               cats: [0.5447047794019219, 1.0]
             }
    end

    test "spearman rank method" do
      df = DF.new(dogs: [1, 8, 3], cats: [4, 5, 2])
      df1 = DF.correlation(df, method: :spearman)

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["dogs", "cats"],
               dogs: [1.0, 0.5],
               cats: [0.5, 1.0]
             }
    end

    test "three integer columns and custom column name" do
      df = DF.new(dogs: [1, 2, 3], cats: [3, 2, 1], frogs: [7, 8, 9])
      df1 = DF.correlation(df, column_name: "variables")

      assert DF.to_columns(df1, atom_keys: true) == %{
               variables: ["dogs", "cats", "frogs"],
               dogs: [1.0, -1.0, 1.0],
               cats: [-1.0, 1.0, -1.0],
               frogs: [1.0, -1.0, 1.0]
             }
    end

    test "two float columns" do
      df = DF.new(dogs: [1.4, 8.6, 3.7], cats: [4.1, 5.3, 2.2])
      df1 = DF.correlation(df)

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["dogs", "cats"],
               dogs: [0.9999999999999999, 0.5642328261411999],
               cats: [0.5642328261411999, 0.9999999999999998]
             }
    end

    test "one column" do
      df = DF.new(cats: [4, 5, 2])
      df1 = DF.correlation(df)

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["cats"],
               cats: [1.0]
             }
    end

    test "no numeric columns" do
      df = DF.new(cats: ["susie", "tuka", "tobias", "terror"])
      df1 = DF.correlation(df)

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: []
             }
    end

    test "ignores non numeric columns" do
      df = DF.new(dogs: [1, 8, 3], cats: [4, 5, 2], frogs: ["a", "b", "c"])
      df1 = DF.correlation(df)

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["dogs", "cats"],
               dogs: [1.0000000000000002, 0.5447047794019219],
               cats: [0.5447047794019219, 1.0]
             }
    end

    test "subset of numeric columns" do
      df = DF.new(dogs: [1, 8, 3], cats: [4, 5, 2], frogs: [7, 8, 9])
      df1 = DF.correlation(df, columns: [:dogs, :cats])

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["dogs", "cats"],
               dogs: [1.0000000000000002, 0.5447047794019219],
               cats: [0.5447047794019219, 1.0]
             }
    end

    test "the returned matrix preserves the order" do
      df = DF.new(dogs: [1, 2, 3], cats: [3, 2, 1])
      df1 = DF.correlation(df, columns: [:cats, :dogs])

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["cats", "dogs"],
               dogs: [-1.0, 1.0],
               cats: [1.0, -1.0]
             }
    end
  end

  describe "covariance/2" do
    test "two integer columns" do
      df = DF.new(dogs: [1, 0, 2, 1], cats: [2, 3, 0, 1])
      df1 = DF.covariance(df)

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["dogs", "cats"],
               dogs: [0.6666666666666666, -1.0],
               cats: [-1.0, 1.6666666666666667]
             }
    end

    test "three integer columns and custom column name" do
      df = DF.new(dogs: [1, 2, 3], cats: [3, 2, 1], frogs: [7, 8, 9])
      df1 = DF.covariance(df, column_name: "variables")

      assert DF.to_columns(df1, atom_keys: true) == %{
               variables: ["dogs", "cats", "frogs"],
               dogs: [1.0, -1.0, 1.0],
               cats: [-1.0, 1.0, -1.0],
               frogs: [1.0, -1.0, 1.0]
             }
    end

    test "two float columns" do
      df = DF.new(dogs: [1.4, 8.6, 3.7], cats: [4.1, 5.3, 2.2])
      df1 = DF.covariance(df)

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["dogs", "cats"],
               dogs: [13.52333333333333, 3.2433333333333394],
               cats: [3.2433333333333394, 2.4433333333333422]
             }
    end

    test "one column" do
      df = DF.new(cats: [4, 5, 2])
      df1 = DF.covariance(df)

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["cats"],
               cats: [2.3333333333333357]
             }
    end

    test "no numeric columns" do
      df = DF.new(cats: ["susie", "tuka", "tobias", "terror"])
      df1 = DF.covariance(df)

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: []
             }
    end

    test "ignores non numeric columns" do
      df = DF.new(dogs: [1, 0, 2, 1], cats: [2, 3, 0, 1], frogs: ["a", "b", "c", "d"])
      df1 = DF.covariance(df)

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["dogs", "cats"],
               dogs: [0.6666666666666666, -1.0],
               cats: [-1.0, 1.6666666666666667]
             }
    end

    test "subset of numeric columns" do
      df = DF.new(dogs: [1, 0, 2, 1], cats: [2, 3, 0, 1], frogs: [6, 7, 8, 9])
      df1 = DF.covariance(df, columns: [:dogs, :cats])

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["dogs", "cats"],
               dogs: [0.6666666666666666, -1.0],
               cats: [-1.0, 1.6666666666666667]
             }
    end

    test "the returned matrix preserves the order" do
      df = DF.new(dogs: [1, 0, 2, 1], cats: [2, 3, 0, 1])
      df1 = DF.covariance(df, columns: [:cats, :dogs])

      assert DF.to_columns(df1, atom_keys: true) == %{
               names: ["cats", "dogs"],
               dogs: [-1.0, 0.6666666666666666],
               cats: [1.6666666666666667, -1.0]
             }
    end
  end

  describe "json_decode/2" do
    test "decodes primitives, lists, structs" do
      df = DF.new([%{st: "{\"n\": 1}", f: "1.0", l: "[1]", dt: "1"}], lazy: true)

      df1 =
        DF.mutate(df,
          st: json_decode(st, {:struct, %{"n" => {:s, 64}}}),
          f: json_decode(f, {:f, 64}),
          l: json_decode(l, {:list, {:s, 64}}),
          dt: json_decode(dt, {:datetime, :microsecond})
        )

      assert df.dtypes == %{"dt" => :string, "f" => :string, "l" => :string, "st" => :string}

      assert df1.dtypes == %{
               "dt" => {:datetime, :microsecond},
               "f" => {:f, 64},
               "l" => {:list, {:s, 64}},
               "st" => {:struct, [{"n", {:s, 64}}]}
             }

      assert df1 |> DF.collect() |> DF.to_columns() == %{
               "dt" => [~N[1970-01-01 00:00:00.000001]],
               "f" => [1.0],
               "l" => [[1]],
               "st" => [%{"n" => 1}]
             }
    end
  end

  describe "row_index/1" do
    test "works as row_count(), including offset" do
      df = DF.new(a: [1, 3, 5], b: [2, 4, 6])
      df1 = DF.mutate(df, index: row_index(a))

      assert DF.to_columns(df1, atom_keys: true) == %{
               a: [1, 3, 5],
               b: [2, 4, 6],
               index: [0, 1, 2]
             }

      df2 = DF.mutate(df, id: row_index(a) + 1000)

      assert DF.to_columns(df2, atom_keys: true) == %{
               a: [1, 3, 5],
               b: [2, 4, 6],
               id: [1000, 1001, 1002]
             }
    end
  end
end
