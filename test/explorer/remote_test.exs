defmodule Explorer.RemoteTest do
  use ExUnit.Case, async: true
  import Explorer.RemoteHelpers

  @moduletag :distributed
  alias Explorer.Series, as: S
  alias Explorer.DataFrame, as: DF

  @node2 :"secondary@127.0.0.1"
  @node3 :"tertiary@127.0.0.1"

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
      {remote, _} =
        remote_eval @node2 do
          Explorer.Series.from_list([1, 2, 3])
          |> Explorer.RemoteHelpers.keep()
        end

      # Remote resources are placed even if not placed before
      placed = S.add(remote, 3)
      assert placed.remote != nil

      assert inspect(placed) =~ """
             #Explorer.Series<
               secondary@127.0.0.1
               Polars[3]
               s64 [4, 5, 6]
             >\
             """

      # Placed and remote from the same node
      placed = S.add(placed, remote)
      assert placed.remote != nil

      assert inspect(placed) =~ """
             #Explorer.Series<
               secondary@127.0.0.1
               Polars[3]
               s64 [5, 7, 9]
             >\
             """

      # Placed and remote from the different node
      {remote, _} =
        remote_eval @node3 do
          Explorer.Series.from_list([1, 2, 3])
          |> Explorer.RemoteHelpers.keep()
        end

      placed = S.add(remote, placed)
      assert placed.remote != nil

      assert inspect(placed) =~ """
             #Explorer.Series<
               secondary@127.0.0.1
               Polars[3]
               s64 [6, 9, 12]
             >\
             """
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
end
