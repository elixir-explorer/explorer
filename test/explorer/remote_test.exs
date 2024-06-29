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
      # {resource, _} =
      #   remote_eval @node2 do
      #     Explorer.Series.from_list([1, 2, 3])
      #     |> Explorer.RemoteHelpers.keep()
      #   end

      # assert inspect(resource) =~ "OMG"
    end
  end

  # defp pry_request(sessions) do
  #   :erlang.trace(Process.whereis(IEx.Broker), true, [:receive, tracer: self()])
  #   patterns = for %{pid: pid} <- sessions, do: {[:_, pid, :_], [], []}
  #   :erlang.trace_pattern(:receive, patterns, [])

  #   task =
  #     Task.async(fn ->
  #       iex_context = :inside_pry
  #       IEx.pry()
  #     end)

  #   for _ <- sessions do
  #     assert_receive {:trace, _, :receive, {_, _, call}} when elem(call, 0) in [:accept, :refuse]
  #   end

  #   task
  # after
  #   :erlang.trace(Process.whereis(IEx.Broker), false, [:receive, tracer: self()])
  # end
end
