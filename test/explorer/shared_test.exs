defmodule Explorer.SharedTest do
  use ExUnit.Case, async: true
  alias Explorer.Shared

  defmodule FakeImpl do
    defstruct op: nil

    def ping(left, right) do
      send(self(), {:pong, left, right})
      :ok
    end
  end

  describe "apply_binary_op_impl/1" do
    test "applies when series is on the left-hand side" do
      :ok = Shared.apply_binary_op_impl(:ping, %{data: %FakeImpl{}}, 42)

      assert_receive {:pong, %{data: %FakeImpl{}}, 42}
    end

    test "applies when series is on the right-hand side" do
      :ok = Shared.apply_binary_op_impl(:ping, 42, %{data: %FakeImpl{}})

      assert_receive {:pong, 42, %{data: %FakeImpl{}}}
    end

    test "raise an error if is not possible to find the implementation" do
      error_message =
        "could not find implementation for function :ping. " <>
          "One of the sides must be a series, but they are: " <>
          "42 (left-hand side) and 13 (right-hand side)."

      assert_raise ArgumentError, error_message, fn ->
        Shared.apply_binary_op_impl(:ping, 42, 13)
      end
    end
  end
end
