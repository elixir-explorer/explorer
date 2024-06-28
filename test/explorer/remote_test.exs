defmodule Explorer.RemoteTest do
  use ExUnit.Case, async: true

  @moduletag :distributed
  alias Explorer.Series
  alias Explorer.DataFrame

  @node1 :"primary@127.0.0.1"
  @node2 :"secondary@127.0.0.1"

  defmacrop remote_eval(node, binding \\ [], do: block) do
    quote do
      :erpc.call(unquote(node), Code, :eval_quoted, [
        unquote(Macro.escape(block)),
        unquote(binding)
      ])
    end
  end

  describe "garbage collection" do
    test "happens once the resource is deallocated" do
      remote_eval @node2 do
        s = Explorer.Series.from_list([1, 2, 3])
        Agent.start(fn -> s end)
        s
      end
      |> dbg()
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
