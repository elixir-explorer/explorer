defmodule Explorer.RemoteTest do
  use ExUnit.Case, async: true
  import Explorer.RemoteHelpers

  @moduletag :distributed
  require Explorer.DataFrame, as: DF
  alias Explorer.Series, as: S

  @node2 :"secondary@127.0.0.1"
  @node3 :"tertiary@127.0.0.1"

  @csv1 """
  city,lat,lng
  Elgin,57.653484,-3.335724
  Stoke-on-Trent,53.002666,-2.179404
  """

  @csv2 """
  city,lat,lng
  Solihull,52.412811,-1.778197
  Cardiff,51.481583,-3.17909
  """

  describe "remote calls" do
    test "on unary non-placed series" do
      {remote, _} =
        remote_eval @node2 do
          Explorer.Series.from_list([1, 2, 3])
          |> Explorer.RemoteHelpers.keep()
        end

      assert inspect(remote) =~ """
             #Explorer.Series<
               secondary@127.0.0.1
               Polars[3]
               s64 [1, 2, 3]
             >\
             """
    end

    test "on binary mixed series" do
      {remote2, _} =
        remote_eval @node2 do
          Explorer.Series.from_list([1, 2, 3])
          |> Explorer.RemoteHelpers.keep()
        end

      # Remote resources are placed even if not placed before
      placed = S.add(remote2, 3)
      assert placed.remote != nil

      assert inspect(placed) =~ """
             #Explorer.Series<
               secondary@127.0.0.1
               Polars[3]
               s64 [4, 5, 6]
             >\
             """

      # Placed and remote from the same node
      placed = S.add(placed, remote2)
      assert placed.remote != nil

      assert inspect(placed) =~ """
             #Explorer.Series<
               secondary@127.0.0.1
               Polars[3]
               s64 [5, 7, 9]
             >\
             """

      # Placed and remote from the different node
      {remote3, _} =
        remote_eval @node3 do
          Explorer.Series.from_list([1, 2, 3])
          |> Explorer.RemoteHelpers.keep()
        end

      placed = S.add(remote3, placed)
      assert placed.remote != nil

      assert inspect(placed) =~ """
             #Explorer.Series<
               secondary@127.0.0.1
               Polars[3]
               s64 [6, 9, 12]
             >\
             """

      # And now varargs
      placed = S.format([placed, "/", remote2, "/", remote3])
      assert placed.remote != nil

      assert inspect(placed) =~ """
             #Explorer.Series<
               secondary@127.0.0.1
               Polars[3]
               string ["6/1/1", "9/2/2", "12/3/3"]
             >\
             """
    end

    test "on varargs placed dataframes" do
      df1 = DF.load_csv!(@csv1, node: @node2)
      df2 = DF.load_csv!(@csv2, node: @node3)

      placed = DF.concat_rows([df1, df2])
      assert placed.remote != nil

      assert inspect(placed) =~ """
             #Explorer.DataFrame<
               secondary@127.0.0.1
               Polars[4 x 3]
               city string ["Elgin", "Stoke-on-Trent", "Solihull", "Cardiff"]
               lat f64 [57.653484, 53.002666, 52.412811, 51.481583]
               lng f64 [-3.335724, -2.179404, -1.778197, -3.17909]
             >\
             """

      placed = DF.concat_rows([df2, df1])
      assert placed.remote != nil

      assert inspect(placed) =~ """
             #Explorer.DataFrame<
               tertiary@127.0.0.1
               Polars[4 x 3]
               city string ["Solihull", "Cardiff", "Elgin", "Stoke-on-Trent"]
               lat f64 [52.412811, 51.481583, 57.653484, 53.002666]
               lng f64 [-1.778197, -3.17909, -3.335724, -2.179404]
             >\
             """
    end

    test "on local dataframe and remote series" do
      df = DF.load_csv!(@csv1, node: @node2)

      df = DF.put(df, "column", S.from_list([1, 2]))
      assert df.remote != nil

      df = DF.put(df, "column", S.from_list([1, 2], node: @node3))
      assert df.remote != nil
    end

    test "on remote dataframe and local series" do
      df = DF.load_csv!(@csv1)
      df = DF.put(df, "column", S.from_list([1, 2], node: @node3))
      assert df.remote == nil
    end
  end

  describe "lazy series" do
    test "with local dataframe" do
      df = DF.new(a: [1, 2, 3])
      assert df.remote == nil

      s2 = S.from_list([4, 5, 6], node: @node2)
      assert s2.remote != nil

      {s3, _} =
        remote_eval @node3 do
          Explorer.Series.from_list([7, 8, 9])
          |> Explorer.RemoteHelpers.keep()
        end

      df = DF.mutate(df, a1: a - ^s2 + ^s3, a2: a + (^s2 - ^s3), a3: ^s3 + ^s2 + a)
      assert df.remote == nil

      assert DF.to_columns(df) == %{
               "a" => [1, 2, 3],
               "a1" => [4, 5, 6],
               "a2" => [-2, -1, 0],
               "a3" => [12, 15, 18]
             }
    end

    test "with an unplaced remote dataframe" do
      {df, _} =
        remote_eval @node3 do
          Explorer.DataFrame.new(a: [1, 2, 3])
          |> Explorer.RemoteHelpers.keep()
        end

      s2 = S.from_list([4, 5, 6], node: @node2)
      assert s2.remote != nil

      s3 = Explorer.Series.from_list([7, 8, 9])
      assert s3.remote == nil

      df = DF.mutate(df, a1: a - ^s2 + ^s3, a2: a + (^s2 - ^s3), a3: ^s3 + ^s2 + a)
      assert df.remote != nil

      assert DF.to_columns(df) == %{
               "a" => [1, 2, 3],
               "a1" => [4, 5, 6],
               "a2" => [-2, -1, 0],
               "a3" => [12, 15, 18]
             }
    end

    test "with a placed remote dataframe" do
      df =
        DF.load_csv!(
          """
          a
          1
          2
          3
          """,
          node: @node3
        )

      assert df.remote != nil

      s2 = S.from_list([4, 5, 6], node: @node2)
      assert s2.remote != nil

      s3 = Explorer.Series.from_list([7, 8, 9])
      assert s3.remote == nil

      df = DF.mutate(df, a1: a - ^s2 + ^s3, a2: a + (^s2 - ^s3), a3: ^s3 + ^s2 + a)
      assert df.remote != nil

      assert DF.to_columns(df) == %{
               "a" => [1, 2, 3],
               "a1" => [4, 5, 6],
               "a2" => [-2, -1, 0],
               "a3" => [12, 15, 18]
             }
    end

    test "with a placed remote dataframe and filter" do
      df =
        DF.load_csv!(
          """
          a
          1
          2
          3
          """,
          node: @node3
        )

      result_df = DF.filter(df, a >= 2)
      assert DF.to_columns(result_df) == %{"a" => [2, 3]}

      result_df = DF.filter(df, 1 < a)
      assert DF.to_columns(result_df) == %{"a" => [2, 3]}
    end
  end

  describe "garbage collection" do
    test "happens once the resource is deallocated" do
      {resource, _} =
        remote_eval @node2 do
          Explorer.Series.from_list([1, 2, 3])
          |> Explorer.RemoteHelpers.keep()
        end

      {%{"foo" => [{resource}]}, [pid]} = Explorer.Remote.place(%{"foo" => [{resource}]})
      assert node(pid) == @node2

      ref = Process.monitor(pid)
      assert :erpc.call(@node2, Process, :alive?, [pid])

      # Hold a reference until before it is garbage collected
      List.flatten([resource])
      :erlang.garbage_collect(self())
      assert_receive {:DOWN, ^ref, _, _, _}
    end
  end

  describe "init placement" do
    test "series" do
      series = S.from_list([1, 2, 3], node: @node2)
      assert series.remote != nil

      assert inspect(series) =~ """
             #Explorer.Series<
               secondary@127.0.0.1
               Polars[3]
               s64 [1, 2, 3]
             >\
             """

      collected = S.collect(series)
      assert collected.remote == nil

      series = S.from_binary(<<4, 5, 6>>, {:s, 8}, node: @node3)
      assert series.remote != nil

      assert inspect(series) =~ """
             #Explorer.Series<
               tertiary@127.0.0.1
               Polars[3]
               s8 [4, 5, 6]
             >\
             """

      collected = S.collect(series)
      assert collected.remote == nil
    end

    test "df" do
      df = DF.load_csv!(@csv1, node: @node2)
      assert df.remote != nil

      assert inspect(df) =~ """
             #Explorer.DataFrame<
               secondary@127.0.0.1
               Polars[2 x 3]
               city string ["Elgin", "Stoke-on-Trent"]
               lat f64 [57.653484, 53.002666]
               lng f64 [-3.335724, -2.179404]
             >\
             """
    end
  end

  describe "with FLAME" do
    setup config do
      start_supervised!(
        {FLAME.Pool,
         name: config.test, min: 0, max: 2, max_concurrency: 2, idle_shutdown_after: 100}
      )

      :ok
    end

    test "automatically places", config do
      # Since FLAME by default runs on the current node,
      # we cannot assert on the result, we need to use tracing instead.

      # First do a warmup to load the code:
      %Explorer.Series{} =
        FLAME.call(
          config.test,
          fn -> Explorer.Series.from_list([1, 2, 3]) end,
          track_resources: true
        )

      # Now setup the tracer
      :erlang.trace(:all, true, [:call, tracer: self()])
      :erlang.trace_pattern({FLAME.Trackable.Explorer.Series, :_, :_}, [])

      spawn_link(fn ->
        %Explorer.Series{} =
          FLAME.call(
            config.test,
            fn -> Explorer.Series.from_list([1, 2, 3]) end,
            track_resources: true
          )
      end)

      assert_receive {:trace, _, :call, {FLAME.Trackable.Explorer.Series, :track, _}}
    end
  end
end
