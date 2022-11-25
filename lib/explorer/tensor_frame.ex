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

    defstruct [:data, :names, :dtypes, :n_rows]
    @type t :: %__MODULE__{}

    @doc false
    def new(data, names, dtypes, n_rows)
        when is_map(data) and is_list(names) and is_map(dtypes) and
               is_integer(n_rows) and n_rows > 0 do
      %TF{data: data, names: names, dtypes: dtypes, n_rows: n_rows}
    end

    @compile {:no_warn_undefined, Nx}

    ## Nx

    import Nx.Defn

    @doc """
    Puts a tensor in the TensorFrame.

    This function can be invoked from within `defn`.

    A `:dtype` can be given as option. If none is given,
    one is retrieved from the existing dtypes (if there
    is a column of matching name), or it is automatically
    inferred from the tensor.

    ## Examples

        Explorer.TensorFrame.put(tf, "result", some_tensor)
    """
    deftransform put(%TF{} = tf, name, tensor, opts \\ []) do
      tensor = Nx.to_tensor(tensor)
      opts = Keyword.validate!(opts, [:dtype])
      name = to_column_name(name)
      dtype = opts[:dtype] || tf.dtypes[name] || dtype_from_tensor!(tensor.type)
      put!(tf, name, tensor, dtype)
    end

    defp dtype_from_tensor!({:u, 8}), do: :boolean
    defp dtype_from_tensor!({:s, 64}), do: :integer
    defp dtype_from_tensor!({:f, 64}), do: :float

    defp dtype_from_tensor!(type) do
      raise ArgumentError,
            "cannot find dtype for tensor of type #{inspect(type)}, please pass the :dtype option instead"
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
      {get, put!(tf, name, Nx.to_tensor(update), tf.dtypes[name])}
    end

    @impl Access
    def pop(%TF{data: data, names: names, dtypes: dtypes} = tf, name) do
      name = to_column_name(name)

      {fetch!(tf, name),
       %{
         tf
         | data: Map.delete(data, name),
           names: List.delete(names, name),
           dtypes: Map.delete(dtypes, name)
       }}
    end

    defp fetch!(%TF{} = tf, name) when is_binary(name) do
      case tf.data do
        %{^name => data} ->
          data

        %{} ->
          case tf.dtypes do
            %{^name => dtype} ->
              raise ArgumentError,
                    "cannot access \"#{name}\" because its dtype #{dtype} is not supported in Explorer.TensorFrame"

            %{} ->
              Explorer.Shared.raise_column_not_found!(name, tf.names)
          end
      end
    end

    ## Helpers

    defp to_column_name(name) when is_atom(name), do: Atom.to_string(name)
    defp to_column_name(name) when is_binary(name), do: name

    defp to_column_name(name) do
      raise ArgumentError,
            "Explorer.TensorFrame only accepts atoms and strings as column names, got: #{inspect(name)}"
    end

    defp put!(%{dtypes: dtypes, n_rows: n_rows} = tf, name, value, dtype) when is_binary(name) do
      cond do
        Explorer.Shared.dtype_to_bintype(dtype) != value.type ->
          raise ArgumentError,
                "cannot add column \"#{name}\" to TensorFrame with a tensor that does not match its dtype. " <>
                  "The dtype #{dtype} expects a tensor of type #{inspect(Explorer.Shared.dtype_to_bintype(dtype))} " <>
                  "but got tensor #{inspect(value)}"

        Map.has_key?(dtypes, name) ->
          put_in(tf.data[name], broadcast!(value, n_rows))

        true ->
          %{data: data, names: names} = tf

          %{
            tf
            | data: Map.put(data, name, broadcast!(value, n_rows)),
              names: names ++ [name],
              dtypes: Map.put(dtypes, name, dtype)
          }
      end
    end

    defp broadcast!(%{shape: {}} = tensor, n_rows), do: Nx.broadcast(tensor, {n_rows})
    defp broadcast!(%{shape: {1}} = tensor, n_rows), do: Nx.broadcast(tensor, {n_rows})
    defp broadcast!(%{shape: {n_rows}} = tensor, n_rows), do: tensor

    defp broadcast!(tensor, n_rows) do
      raise ArgumentError,
            "cannot add column to TensorFrame with a tensor that does not match its size. " <>
              "Expected a tensor of shape {#{n_rows}} but got tensor #{inspect(tensor)}"
    end
  end

  defimpl Nx.LazyContainer, for: DF do
    @unsupported [:string]

    def traverse(df, acc, fun) do
      n_rows = DF.n_rows(df)
      dtypes = DF.dtypes(df)

      {data, acc} =
        Enum.reduce(dtypes, {[], acc}, fn
          {_name, dtype}, {data, acc} when dtype in @unsupported ->
            {data, acc}

          {name, _dtype}, {data, acc} ->
            series = DF.pull(df, name)
            template = Nx.template({n_rows}, S.bintype(series))
            {result, acc} = fun.(template, fn -> S.to_tensor(series) end, acc)
            {[{name, result} | data], acc}
        end)

      # We keep original names and dtypes for display
      {TF.new(Map.new(data), DF.names(df), dtypes, n_rows), acc}
    end
  end

  defimpl Nx.Container, for: TF do
    def traverse(tf, acc, fun) do
      {data, acc} =
        Enum.map_reduce(tf.data, acc, fn {name, contents}, acc ->
          {contents, acc} = fun.(contents, acc)
          {{name, contents}, acc}
        end)

      {%{tf | data: Map.new(data)}, acc}
    end

    def reduce(tf, acc, fun) do
      Enum.reduce(tf.data, acc, fn {_name, contents}, acc ->
        fun.(contents, acc)
      end)
    end
  end
end
