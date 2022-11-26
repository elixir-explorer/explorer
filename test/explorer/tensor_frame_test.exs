defmodule Explorer.TensorFrameTest do
  use ExUnit.Case, async: true
  import Nx.Defn

  # Used by doctests
  defn add_columns(tf) do
    tf[:a] + tf[:b]
  end

  doctest Explorer.TensorFrame
  alias Explorer.DataFrame, as: DF
  alias Explorer.TensorFrame, as: TF

  defp tf(data) do
    Nx.Defn.jit_apply(&Function.identity/1, [DF.new(data)])
  end

  describe "defn integration" do
    test "works even if dataframe has unsupported columns" do
      df = DF.new(a: [1, 2, 3], b: [4.0, 5.0, 6.0], c: ["a", "b", "c"])
      assert add_columns(df) == Nx.tensor([5.0, 7.0, 9.0], type: :f64)
    end

    defnp put_column(data) do
      TF.put(data, "d", data["a"] + TF.pull(data, "b"))
    end

    test "handles deftransform functions" do
      tf = put_column(tf(a: [1, 2, 3], b: [4.0, 5.0, 6.0], c: ["a", "b", "c"]))
      assert tf[:a] == Nx.tensor([1, 2, 3])
      assert tf["a"] == Nx.tensor([1, 2, 3])
      assert tf[:b] == Nx.tensor([4.0, 5.0, 6.0], type: :f64)
      assert tf["b"] == Nx.tensor([4.0, 5.0, 6.0], type: :f64)
      assert tf[:d] == Nx.tensor([5.0, 7.0, 9.0], type: :f64)
      assert tf["d"] == Nx.tensor([5.0, 7.0, 9.0], type: :f64)
    end
  end

  describe "put" do
    test "broadcasts" do
      i = 1
      f = Nx.tensor([1.0], type: :f64)
      tf = tf(a: [1, 2, 3], b: [4.0, 5.0, 6.0], c: ["a", "b", "c"])
      assert TF.put(tf, :a, i)[:a] == Nx.tensor([1, 1, 1])
      assert TF.put(tf, :a, f)[:a] == Nx.tensor([1.0, 1.0, 1.0], type: :f64)

      assert TF.put(tf, :c, i)[:c] == Nx.tensor([1, 1, 1])
      assert TF.put(tf, :c, f)[:c] == Nx.tensor([1.0, 1.0, 1.0], type: :f64)

      assert TF.put(tf, :d, i)[:d] == Nx.tensor([1, 1, 1])
      assert TF.put(tf, :d, f)[:d] == Nx.tensor([1.0, 1.0, 1.0], type: :f64)
    end
  end

  describe "inspect" do
    test "columns and tensors" do
      assert inspect(tf(a: [1, 2, 3], b: [4.0, 5.0, 6.0], c: ["a", "b", "c"])) == """
             #Explorer.TensorFrame<
               [3 x 2]
               a #Nx.Tensor<
                 s64[3]
                 [1, 2, 3]
               >
               b #Nx.Tensor<
                 f64[3]
                 [4.0, 5.0, 6.0]
               >
             >\
             """
    end
  end

  describe "access" do
    test "get" do
      tf = tf(a: [1, 2, 3], b: [4.0, 5.0, 6.0], c: ["a", "b", "c"])
      assert tf[:a] == Nx.tensor([1, 2, 3])
      assert tf["a"] == Nx.tensor([1, 2, 3])
      assert tf[:b] == Nx.tensor([4.0, 5.0, 6.0], type: :f64)
      assert tf["b"] == Nx.tensor([4.0, 5.0, 6.0], type: :f64)

      assert_raise ArgumentError,
                   "Explorer.TensorFrame only accepts atoms and strings as column names, got: 0",
                   fn -> tf[0] end

      assert_raise ArgumentError,
                   "could not find column \"c\" either because it doesn't exist or its dtype is not supported in Explorer.TensorFrame",
                   fn -> tf[:c] end
    end

    test "get_and_update" do
      tf = tf(a: [1, 2, 3], b: [4.0, 5.0, 6.0], c: ["a", "b", "c"])
      {get, update} = Access.get_and_update(tf, :a, fn a -> {a, Nx.multiply(a, 2)} end)
      assert get == Nx.tensor([1, 2, 3])
      assert update[:a] == Nx.tensor([2, 4, 6])

      {get, update} = Access.get_and_update(tf, :a, fn a -> {a, 123} end)
      assert get == Nx.tensor([1, 2, 3])
      assert update[:a] == Nx.tensor([123, 123, 123])

      assert_raise ArgumentError,
                   ~r"cannot add tensor that does not match the frame size. Expected a tensor of shape \{3\}",
                   fn ->
                     Access.get_and_update(tf, :a, fn a -> {a, Nx.tensor([1, 2])} end)
                   end
    end

    test "pop" do
      tf = tf(a: [1, 2, 3], b: [4.0, 5.0, 6.0], c: ["a", "b", "c"])
      {tensor, popped} = Access.pop(tf, :a)
      assert tensor == Nx.tensor([1, 2, 3])
      assert popped.data[:a] == nil
    end
  end
end
