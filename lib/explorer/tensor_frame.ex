if Code.ensure_loaded?(Nx) do
  alias Explorer.Series, as: S, warn: false
  alias Explorer.DataFrame, as: DF, warn: false
  alias Explorer.TensorFrame, as: TF, warn: false

  defmodule Explorer.TensorFrame do
    @moduledoc """
    TensorFrame is a representation of `Explorer.DataFrame`
    that is designed to work inside Nx's `defn` expressions.

    For example, imagine the following `defn`:

        defn add_columns(tf) do
          tf[:a] + tf[:b]
        end

    We can now pass a DataFrame as argument:

        iex> add_columns(Explorer.DataFrame.new(a: [11, 12], b: [21, 22]))
        #Nx.Tensor<
          s64[2]
          [32, 34]
        >

    Passing an `Explorer.DataFrame` to a `defn` will automatically
    convert it to a TensorFrame. The TensorFrame will lazily
    build tensors out of the used dataframe fields.

    ## Warning: returning TensorFrames

    It is not recommended to return a TensorFrame from a `defn`,
    as that would force all columns to be sent to the CPU/GPU
    and then copied back. Return only the columns that have been
    modified during the computation. For example, in the example
    above we used `Nx` to add two columns, if you want to
    put the result of the computation back into a DataFrame,
    you can use `Explorer.DataFrame.put/4`, which also accepts
    tensors:

        iex> df = Explorer.DataFrame.new(a: [11, 12], b: [21, 22])
        iex> Explorer.DataFrame.put(df, "result", add_columns(df))
        #Explorer.DataFrame<
          Polars[2 x 3]
          a integer [11, 12]
          b integer [21, 22]
          result integer [32, 34]
        >

    One benefit of using `Explorer.DataFrame.put/4` is that it will
    preserve the type of the column if one already exists. Alternatively,
    use `Explorer.Series.from_tensor/1` to explicitly convert a tensor
    back to a series.

    ## Supported dtypes

    The following dtypes can be converted to tensors:

      * `:integer`
      * `:float`
      * `:boolean`
      * `:date`
      * `:datetime`

    See `Explorer.Series.to_iovec/1` and `Explorer.Series.to_tensor/1`
    to learn more about their internal representation.
    """

    @enforce_keys [:data, :names, :n_rows]
    defstruct [:data, :names, :n_rows]
    @type t :: %__MODULE__{}
    @compile {:no_warn_undefined, Nx}

    ## Nx

    import Nx.Defn

    @doc """
    Pulls a tensor from the TensorFrame.

    This is equivalent to using the `tf[name]` to access
    a tensor.

    ## Examples

        Explorer.TensorFrame.pull(tf, "some_column")

    """
    deftransform pull(%TF{} = tf, name) do
      fetch!(tf, to_column_name(name))
    end

    @doc """
    Puts a tensor in the TensorFrame.

    This function can be invoked from within `defn`.

    ## Examples

        Explorer.TensorFrame.put(tf, "result", some_tensor)

    """
    deftransform put(%TF{} = tf, name, tensor) do
      put!(tf, to_column_name(name), tensor)
    end

    ## Access

    @behaviour Access

    @impl Access
    def fetch(tf, name) do
      {:ok, fetch!(tf, to_column_name(name))}
    end

    @impl Access
    def get_and_update(tf, name, callback) do
      name = to_column_name(name)
      {get, update} = callback.(fetch!(tf, name))
      {get, put!(tf, name, update)}
    end

    @impl Access
    def pop(%TF{data: data, names: names} = tf, name) do
      name = to_column_name(name)

      {fetch!(tf, name), %{tf | data: Map.delete(data, name), names: names -- [name]}}
    end

    defp fetch!(%TF{data: data}, name) when is_binary(name) do
      case data do
        %{^name => data} ->
          data

        %{} ->
          raise ArgumentError,
                List.to_string([
                  "could not find column \"#{name}\" either because it doesn't exist or its dtype is not supported in Explorer.TensorFrame"
                  | Explorer.Shared.did_you_mean(name, Map.keys(data))
                ])
      end
    end

    ## Helpers

    defp to_column_name(name) when is_atom(name), do: Atom.to_string(name)
    defp to_column_name(name) when is_binary(name), do: name

    defp to_column_name(name) do
      raise ArgumentError,
            "Explorer.TensorFrame only accepts atoms and strings as column names, got: #{inspect(name)}"
    end

    defp put!(%{n_rows: n_rows, names: names, data: data} = tf, name, value) when is_binary(name) do
      names = if name in names, do: names, else: names ++ [name]
      data = Map.put(data, name, value |> Nx.to_tensor() |> broadcast!(n_rows))
      %{tf | names: names, data: data}
    end

    defp broadcast!(%{shape: {}} = tensor, n_rows), do: Nx.broadcast(tensor, {n_rows})
    defp broadcast!(%{shape: {1}} = tensor, n_rows), do: Nx.broadcast(tensor, {n_rows})
    defp broadcast!(%{shape: {n_rows}} = tensor, n_rows), do: tensor

    defp broadcast!(tensor, n_rows) do
      raise ArgumentError,
            "cannot add tensor that does not match the frame size. " <>
              "Expected a tensor of shape {#{n_rows}} but got tensor #{inspect(tensor)}"
    end

    defimpl Inspect do
      import Inspect.Algebra

      def inspect(tf, opts) do
        force_unfit(
          concat([
            color("#Explorer.TensorFrame<", :map, opts),
            nest(concat([line(), inner(tf, opts)]), 2),
            line(),
            color(">", :map, opts)
          ])
        )
      end

      @default_limit 5

      defp inner(%{data: data, n_rows: n_rows}, opts) do
        opts = %{opts | limit: @default_limit}
        open = color("[", :list, opts)
        close = color("]", :list, opts)

        pairs =
          for {name, tensor} <- Enum.sort(data) do
            concat([
              line(),
              color("#{name} ", :map, opts),
              Inspect.Algebra.to_doc(tensor, opts)
            ])
          end

        concat([open, "#{n_rows} x #{map_size(data)}", close | pairs])
      end
    end
  end

  defimpl Nx.LazyContainer, for: S do
    def traverse(series, acc, fun) do
      size = S.size(series)
      template = Nx.template({size}, S.iotype(series))
      fun.(template, fn -> S.to_tensor(series) end, acc)
    end
  end

  defimpl Nx.LazyContainer, for: DF do
    @supported [:boolean,:category,:date,:time,:datetime,:float,:integer]

    def traverse(df, acc, fun) do
      n_rows = DF.n_rows(df)

      {data, acc} =
        Enum.flat_map_reduce(DF.names(df), acc, fn name, acc ->
          series = df[name]

          if series.dtype in @supported do
            template = Nx.template({n_rows}, S.iotype(series))
            {result, acc} = fun.(template, fn -> S.to_tensor(series) end, acc)
            {[{name, result}], acc}
          else
            {[], acc}
          end
        end)

      {%TF{data: Map.new(data), names: Enum.map(data, &elem(&1, 0)), n_rows: n_rows}, acc}
    end
  end

  defimpl Nx.Container, for: TF do
    def traverse(tf, acc, fun) do
      {data, acc} =
        Enum.map_reduce(tf.names, acc, fn name, acc ->
          {contents, acc} = fun.(tf[name], acc)
          {{name, contents}, acc}
        end)

      {%{tf | data: Map.new(data)}, acc}
    end

    def reduce(tf, acc, fun) do
      Enum.reduce(tf.names, acc, fn name, acc ->
        fun.(tf[name], acc)
      end)
    end

    def serialize(%TF{data: data, names: names, n_rows: n_rows}) do
      {__MODULE__, Map.to_list(data), {names, n_rows}}
    end

    def deserialize(data, {names, n_rows}) do
      %TF{data: Map.new(data), names: names, n_rows: n_rows}
    end
  end
end
