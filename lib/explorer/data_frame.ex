defmodule Explorer.DataFrame do
  @moduledoc """
  The DataFrame struct and API.
  """

  alias __MODULE__, as: DataFrame

  import Explorer.Shared, only: [impl!: 1]
  import Nx.Defn.Kernel, only: [keyword!: 2]

  @type data :: Explorer.Backend.DataFrame.t()
  @type t :: %DataFrame{data: data}

  @enforce_keys [:data, :groups]
  defstruct [:data, :groups]

  # Access

  @behaviour Access

  @impl true
  def fetch(df, columns) when is_list(columns) do
    {:ok, select(df, columns)}
  end

  @impl true
  def fetch(df, column) when is_binary(column) do
    {:ok, pull(df, column)}
  end

  @impl true
  def pop(df, column) when is_binary(column) do
    {pull(df, column), select(df, [column], :drop)}
  end

  def pop(df, columns) when is_list(columns) do
    {select(df, columns), select(df, columns, :drop)}
  end

  @impl true
  def get_and_update(df, column, fun) when is_binary(column) do
    value = pull(df, column)
    {current_value, new_value} = fun.(value)
    new_data = mutate(df, [{String.to_atom(column), new_value}])
    {current_value, new_data}
  end

  # IO

  @doc """
  Reads a delimited file into a dataframe.

  ## Options

    * `delimiter` - A single character used to separate fields within a record. (default: `","`)
    * `dtypes` - A keyword list of `[column_name: dtype]`. If `nil`, dtypes are imputed from the first 1000 rows. (default: `nil`)
    * `header?` - Does the file have a header of column names as the first row or not? (default: `true`)
    * `max_rows` - Maximum number of lines to read. (default: `Inf`)
    * `names` - A list of column names. Must match the width of the dataframe. (default: nil)
    * `null_character` - The string that should be interpreted as a nil value. (default: `"NA"`)
    * `skip_rows` - The number of lines to skip at the beginning of the file. (default: `0`)
    * `with_columns` - A list of column names to keep. If present, only these columns are read
    * into the dataframe. (default: `nil`)
  """
  @spec read_csv(filename :: String.t(), opts :: Keyword.t()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def read_csv(filename, opts \\ []) do
    opts =
      keyword!(opts,
        delimiter: ",",
        dtypes: nil,
        encoding: "utf8",
        header?: true,
        max_rows: Inf,
        names: nil,
        null_character: "NA",
        skip_rows: 0,
        with_columns: nil
      )

    backend = backend_from_options!(opts)

    backend.read_csv(
      filename,
      opts[:names],
      opts[:dtypes],
      opts[:delimiter],
      opts[:null_character],
      opts[:skip_rows],
      opts[:header?],
      opts[:encoding],
      opts[:max_rows],
      opts[:with_columns]
    )
  end

  @doc """
  Similar to `read_csv/2` but raises if there is a problem reading the CSV.
  """
  @spec read_csv!(filename :: String.t(), opts :: Keyword.t()) :: DataFrame.t()
  def read_csv!(filename, opts \\ []) do
    case read_csv(filename, opts) do
      {:ok, df} -> df
      {:error, error} -> raise "#{error}"
    end
  end

  @doc """
  Writes a dataframe to a delimited file.

  ## Options

    * `header?` - Should the column names be written as the first line of the file? (default: `true`)
    * `delimiter` - A single character used to separate fields within a record. (default: `","`)
  """
  @spec write_csv(df :: DataFrame.t(), filename :: String.t(), opts :: Keyword.t()) ::
          {:ok, String.t()} | {:error, term()}
  def write_csv(df, filename, opts \\ []) do
    opts = keyword!(opts, header?: true, delimiter: ",")
    apply_impl(df, :write_csv, [filename, opts[:header?], opts[:delimiter]])
  end

  @doc """
  Similar to `write_csv/3` but raises if there is a problem reading the CSV.
  """
  @spec write_csv!(df :: DataFrame.t(), filename :: String.t(), opts :: Keyword.t()) :: String.t()
  def write_csv!(df, filename, opts \\ []) do
    case write_csv(df, filename, opts) do
      {:ok, filename} -> filename
      {:error, error} -> raise "#{error}"
    end
  end

  @doc """
  Creates a new dataframe from a map of lists.

  Lists must be the same length. This function calls `Explorer.Series.from_list/2` so lists must
  conform to the requirements for making a series.

  ## Options

    * `backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.default_backend/0`.

  ## Examples

      iex> Explorer.DataFrame.from_map(%{floats: [1.0, 2.0], ints: [1, nil]})
      #Explorer.DataFrame<
        [rows: 2, columns: 2]
        floats float [1.0, 2.0]
        ints integer [1, nil]
      >
  """
  @spec from_map(map :: map(), opts :: Keyword.t()) :: DataFrame.t()
  def from_map(map, opts \\ []) do
    backend = backend_from_options!(opts)
    backend.from_map(map)
  end

  @doc """
  Converts a dataframe to a map.

  By default, the constituent series of the dataframe are converted to Elixir lists.

  ## Examples

      iex> df = Explorer.DataFrame.from_map(%{floats: [1.0, 2.0], ints: [1, nil]})
      iex> Explorer.DataFrame.to_map(df)
      %{floats: [1.0, 2.0], ints: [1, nil]}
  """
  @spec to_map(df :: DataFrame.t(), convert_series? :: boolean()) :: map()
  def to_map(df, convert_series? \\ true), do: apply_impl(df, :to_map, [convert_series?])

  # Introspection

  @doc """
  Gets the names of the dataframe columns.

  ## Examples

      iex> df = Explorer.DataFrame.from_map(%{floats: [1.0, 2.0], ints: [1, 2]})
      iex> Explorer.DataFrame.names(df)
      ["floats", "ints"]
  """
  @spec names(df :: DataFrame.t()) :: [String.t()]
  def names(df), do: apply_impl(df, :names)

  @doc """
  Gets the dtypes of the dataframe columns.

  ## Examples

      iex> df = Explorer.DataFrame.from_map(%{floats: [1.0, 2.0], ints: [1, 2]})
      iex> Explorer.DataFrame.dtypes(df)
      [:float, :integer]
  """
  @spec dtypes(df :: DataFrame.t()) :: [atom()]
  def dtypes(df), do: apply_impl(df, :dtypes)

  @doc """
  Gets the shape of the dataframe as a `{height, width}` tuple.

  ## Examples

      iex> df = Explorer.DataFrame.from_map(%{floats: [1.0, 2.0, 3.0], ints: [1, 2, 3]})
      iex> Explorer.DataFrame.shape(df)
      {3, 2}
  """
  @spec shape(df :: DataFrame.t()) :: {integer(), integer()}
  def shape(df), do: apply_impl(df, :shape)

  @doc """
  Returns the number of rows in the dataframe.
  """
  @spec n_rows(df :: DataFrame.t()) :: integer()
  def n_rows(df), do: apply_impl(df, :n_rows)

  @doc """
  Returns the number of columns in the dataframe.
  """
  @spec n_cols(df :: DataFrame.t()) :: integer()
  def n_cols(df), do: apply_impl(df, :n_cols)

  @doc """
  Returns the groups of a dataframe.
  """
  @spec groups(df :: DataFrame.t()) :: list(String.t())
  def groups(%DataFrame{groups: groups}), do: groups

  # Single table verbs

  @doc """
  Returns the first *n* rows of the dataframe.
  """
  def head(df, nrows \\ 6), do: apply_impl(df, :head, [nrows])

  @doc """
  Returns the last *n* rows of the dataframe.
  """
  def tail(df, nrows \\ 6), do: apply_impl(df, :tail, [nrows])

  @doc """
  Selects a subset of columns by name.

  Can optionally return all *but* the named columns if `:drop` is passed as the last argument.

  ## Examples

    You can select columns with a list of names:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "c"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.select(df, ["a"])
      #Explorer.DataFrame<
        [rows: 3, columns: 1]
        a string ["a", "b", "c"]
      >

    Or you can use a callback function that takes the dataframe's names as its first argument:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "c"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.select(df, &String.starts_with?(&1, "b"))
      #Explorer.DataFrame<
        [rows: 3, columns: 1]
        b integer [1, 2, 3]
      >

    If you pass `:drop` as the third argument, it will return all but the named columns:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "c"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.select(df, ["b"], :drop)
      #Explorer.DataFrame<
        [rows: 3, columns: 1]
        a string ["a", "b", "c"]
      >
  """
  @spec select(
          df :: DataFrame.t(),
          cols_or_callback :: [String.t()] | function(),
          keep_or_drop ::
            :keep | :drop
        ) :: DataFrame.t()
  def select(df, cols_or_callback, keep_or_drop \\ :keep),
    do: apply_impl(df, :select, [cols_or_callback, keep_or_drop])

  @doc """
  Subset rows using column values.

  ## Examples

    You can pass a mask directly:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "c"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.filter(df, Explorer.Series.gt(df["b"], 1))
      #Explorer.DataFrame<
        [rows: 2, columns: 2]
        a string ["b", "c"]
        b integer [2, 3]
      >

    Or you can invoke a callback on the dataframe:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "c"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.filter(df, &Explorer.Series.gt(&1["b"], 1))
      #Explorer.DataFrame<
        [rows: 2, columns: 2]
        a string ["b", "c"]
        b integer [2, 3]
      >
  """
  def filter(df, mask), do: apply_impl(df, :filter, [mask])

  @doc """
  Creates an modifies columns.

  Columns are added as keyword list arguments. New variables overwrite existing variables of the 
  same name. Column names are coerced from atoms to strings.

  ## Examples

    You can pass in a list directly as a new column:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "c"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.mutate(df, c: [4, 5, 6])
      #Explorer.DataFrame<
        [rows: 3, columns: 3]
        a string ["a", "b", "c"]
        b integer [1, 2, 3]
        c integer [4, 5, 6]
      >

    Or you can pass in a series:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "c"], b: [1, 2, 3]})
      iex> s = Explorer.Series.from_list([4, 5, 6])
      iex> Explorer.DataFrame.mutate(df, c: s)
      #Explorer.DataFrame<
        [rows: 3, columns: 3]
        a string ["a", "b", "c"]
        b integer [1, 2, 3]
        c integer [4, 5, 6]
      >

    Or you can invoke a callback on the dataframe:

      iex> df = Explorer.DataFrame.from_map(%{a: [4, 5, 6], b: [1, 2, 3]})
      iex> Explorer.DataFrame.mutate(df, c: &Explorer.Series.add(&1["a"], &1["b"]))
      #Explorer.DataFrame<
        [rows: 3, columns: 3]
        a integer [4, 5, 6]
        b integer [1, 2, 3]
        c integer [5, 7, 9]
      >

    You can overwrite existing columns:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "c"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.mutate(df, a: [4, 5, 6])
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        a integer [4, 5, 6]
        b integer [1, 2, 3]
      >
  """
  def mutate(df, with_columns), do: apply_impl(df, :mutate, [with_columns])

  @doc """
  Arranges/sorts rows by columns.

  ## Examples

      iex> df = Explorer.DataFrame.from_map(%{a: ["b", "c", "a"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.arrange(df, ["a"])
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        a string ["a", "b", "c"]
        b integer [3, 1, 2]
      >

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "a"], b: [1, 3, 2]})
      iex> Explorer.DataFrame.arrange(df, ["a", "b"])
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        a string ["a", "a", "b"]
        b integer [1, 2, 3]
      >
  """
  def arrange(df, column, direction \\ :asc), do: apply_impl(df, :arrange, [column, direction])

  @doc """
  Takes distinct rows by a selection of columns.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.distinct(df, ["year", "country"])
      #Explorer.DataFrame<
        [rows: 1094, columns: 2]
        year integer [2010, 2010, 2010, 2010, 2010, "..."]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", "..."]
      >
  """
  def distinct(df, columns, keep_all? \\ false),
    do: apply_impl(df, :distinct, [columns, keep_all?])

  @doc """
  Renames columns.

  ## Examples

    You can pass in a list of new names:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "a"], b: [1, 3, 1]})
      iex> Explorer.DataFrame.rename(df, ["c", "d"])
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        c string ["a", "b", "a"]
        d integer [1, 3, 1]
      >

    Or you can rename individual columns using keyword args:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "a"], b: [1, 3, 1]})
      iex> Explorer.DataFrame.rename(df, a: "first")
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        first string ["a", "b", "a"]
        b integer [1, 3, 1]
      >

    Or you can rename individual columns using a map:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "a"], b: [1, 3, 1]})
      iex> Explorer.DataFrame.rename(df, %{"a" => "first"})
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        first string ["a", "b", "a"]
        b integer [1, 3, 1]
      >
  """
  def rename(df, names) when is_list(names) do
    case Keyword.keyword?(names) do
      false ->
        names =
          Enum.map(names, fn
            name when is_atom(name) -> Atom.to_string(name)
            name -> name
          end)

        width = n_cols(df)
        n_new_names = length(names)

        if width != n_new_names,
          do:
            raise(ArgumentError,
              message:
                "List of new names must match the number of columns in the dataframe. " <>
                  "Found #{n_new_names} new name(s), but the supplied dataframe has #{width} " <>
                  "column(s)."
            )

        apply_impl(df, :rename, [names])

      true ->
        names
        |> Enum.reduce(%{}, &rename_reducer/2)
        |> then(&rename(df, &1))
    end
  end

  def rename(df, names) when is_map(names) do
    names = Enum.reduce(names, %{}, &rename_reducer/2)
    name_keys = Map.keys(names)
    old_names = names(df)

    Enum.each(name_keys, fn name ->
      maybe_raise_column_not_found(old_names, name)
    end)

    old_names
    |> Enum.map(fn name -> if name in name_keys, do: Map.get(names, name), else: name end)
    |> then(&rename(df, &1))
  end

  defp rename_reducer({k, v}, acc) when is_atom(k) and is_binary(v),
    do: Map.put(acc, Atom.to_string(k), v)

  defp rename_reducer({k, v}, acc) when is_binary(k) and is_binary(v), do: Map.put(acc, k, v)

  defp rename_reducer({k, v}, acc) when is_atom(k) and is_atom(v),
    do: Map.put(acc, Atom.to_string(k), Atom.to_string(v))

  defp rename_reducer({k, v}, acc) when is_binary(k) and is_atom(v),
    do: Map.put(acc, k, Atom.to_string(v))

      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        first string ["a", "b", "a"]
        b integer [1, 3, 1]
      >
  """
  def rename(df, names), do: apply_impl(df, :rename, [names])

  @doc """
  Turns a set of columns to dummy variables.
  """
  def dummies(df, columns), do: apply_impl(df, :dummies, [columns])

  @doc """
  Extracts a single column as a series.
  """
  def pull(df, column), do: apply_impl(df, :pull, [column])

  @doc """
  Subset a continuous set of rows.

  Negative offset will be counted from the end of the dataframe.
  """
  def slice(df, offset, length), do: apply_impl(df, :slice, [offset, length])

  @doc """
  Subset rows with a list of indices.
  """
  def take(df, row_indices), do: apply_impl(df, :take, [row_indices])

  # Two table verbs

  @doc """
  Join two tables.
  """
  def join(left, right, opts \\ []) do
    opts = keyword!(opts, on: find_overlapping_cols(left, right), how: :inner)
    apply_impl(left, :join, [right, opts[:on], opts[:how]])
  end

  # Helpers

  defp backend_from_options!(opts) do
    backend = Explorer.Shared.backend_from_options!(opts) || Explorer.default_backend()
    Module.concat(backend, "DataFrame")
  end

  defp apply_impl(df, fun, args \\ []) do
    impl = impl!(df)
    apply(impl, fun, [df | args])
  end

  defp find_overlapping_cols(left, right) do
    left_cols = left |> names() |> MapSet.new()
    right_cols = right |> names() |> MapSet.new()
    left_cols |> MapSet.intersection(right_cols) |> MapSet.to_list()
  end
end
