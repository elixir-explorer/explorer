defmodule Explorer.DataFrame do
  @moduledoc """
  The DataFrame struct and API.
  """

  alias __MODULE__, as: DataFrame
  alias Explorer.Series

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

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.n_rows(df)
      1094
  """
  @spec n_rows(df :: DataFrame.t()) :: integer()
  def n_rows(df), do: apply_impl(df, :n_rows)

  @doc """
  Returns the number of columns in the dataframe.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.n_cols(df)
      10
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

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.head(df)
      #Explorer.DataFrame<
        [rows: 5, columns: 10]
        year integer [2010, 2010, 2010, 2010, 2010]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA"]
        total integer [2308, 1254, 32500, 141, 7924]
        solid_fuel integer [627, 117, 332, 0, 0]
        liquid_fuel integer [1601, 953, 12381, 141, 3649]
        gas_fuel integer [74, 7, 14565, 0, 374]
        cement integer [5, 177, 2598, 0, 204]
        gas_flaring integer [0, 0, 2623, 0, 3697]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37]
        bunker_fuels integer [9, 7, 663, 0, 321]
      >
  """
  def head(df, nrows \\ 5), do: apply_impl(df, :head, [nrows])

  @doc """
  Returns the last *n* rows of the dataframe.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.tail(df)
      #Explorer.DataFrame<
        [rows: 5, columns: 10]
        year integer [2014, 2014, 2014, 2014, 2014]
        country string ["VIET NAM", "WALLIS AND FUTUNA ISLANDS", "YEMEN", "ZAMBIA", "ZIMBABWE"]
        total integer [45517, 6, 6190, 1228, 3278]
        solid_fuel integer [19246, 0, 137, 132, 2097]
        liquid_fuel integer [12694, 6, 5090, 797, 1005]
        gas_fuel integer [5349, 0, 581, 0, 0]
        cement integer [8229, 0, 381, 299, 177]
        gas_flaring integer [0, 0, 0, 0, 0]
        per_capita float [0.49, 0.44, 0.24, 0.08, 0.22]
        bunker_fuels integer [761, 1, 153, 33, 9]
      >
  """
  def tail(df, nrows \\ 5), do: apply_impl(df, :tail, [nrows])

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
          columns :: [String.t()],
          keep_or_drop ::
            :keep | :drop
        ) :: DataFrame.t()
  def select(df, columns, keep_or_drop \\ :keep)

  def select(df, columns, keep_or_drop) when is_list(columns) do
    column_names = names(df)

    Enum.each(columns, fn name ->
      maybe_raise_column_not_found(column_names, name)
    end)

    apply_impl(df, :select, [columns, keep_or_drop])
  end

  @spec select(
          df :: DataFrame.t(),
          callback :: function(),
          keep_or_drop ::
            :keep | :drop
        ) :: DataFrame.t()
  def select(df, callback, keep_or_drop) when is_function(callback),
    do: df |> names() |> Enum.filter(callback) |> then(&select(df, &1, keep_or_drop))

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

    Including a list:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "c"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.filter(df, [false, true, false])
      #Explorer.DataFrame<
        [rows: 1, columns: 2]
        a string ["b"]
        b integer [2]
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
  def filter(df, %Series{} = mask) do
    s_len = Series.length(mask)
    df_len = n_rows(df)

    case s_len == df_len do
      false ->
        raise(ArgumentError,
          message:
            "Length of the mask (#{s_len}) must match number of rows in the dataframe (#{df_len})."
        )

      true ->
        apply_impl(df, :filter, [mask])
    end
  end

  def filter(df, mask) when is_list(mask), do: mask |> Series.from_list() |> then(&filter(df, &1))

  def filter(df, callback) when is_function(callback),
    do:
      df
      |> callback.()
      |> then(
        &filter(
          df,
          &1
        )
      )

  @doc """
  Creates and modifies columns.

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

    Alternatively, all of the above works with a map instead of a keyword list:

      iex> df = Explorer.DataFrame.from_map(%{a: ["a", "b", "c"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.mutate(df, %{"c" => [4, 5, 6]})
      #Explorer.DataFrame<
        [rows: 3, columns: 3]
        a string ["a", "b", "c"]
        b integer [1, 2, 3]
        c integer [4, 5, 6]
      >
  """
  def mutate(df, with_columns) when is_map(with_columns) do
    with_columns = Enum.reduce(with_columns, %{}, &mutate_reducer(&1, &2, df))

    df_len = n_rows(df)

    Enum.each(with_columns, fn {colname, series} ->
      s_len = Series.length(series)

      if s_len != df_len,
        do:
          raise(ArgumentError,
            message:
              "Length of new column #{colname} (#{s_len}) must match number of rows in the " <>
                "dataframe (#{df_len})."
          )
    end)

    apply_impl(df, :mutate, [with_columns])
  end

  def mutate(df, with_columns) when is_list(with_columns) do
    if not Keyword.keyword?(with_columns), do: raise(ArgumentError, message: "Expected second
      argument to be a keyword list.")

    with_columns
    |> Enum.reduce(%{}, fn {colname, value}, acc ->
      Map.put(acc, Atom.to_string(colname), value)
    end)
    |> then(&mutate(df, &1))
  end

  defp mutate_reducer({colname, %Series{} = series}, acc, %DataFrame{} = _df)
       when is_binary(colname) and is_map(acc),
       do: Map.put(acc, colname, series)

  defp mutate_reducer({colname, value}, acc, %DataFrame{} = df)
       when is_atom(colname) and is_map(acc),
       do: mutate_reducer({Atom.to_string(colname), value}, acc, df)

  defp mutate_reducer({colname, callback}, acc, %DataFrame{} = df)
       when is_function(callback) and is_map(acc),
       do: mutate_reducer({colname, callback.(df)}, acc, df)

  defp mutate_reducer({colname, values}, acc, df) when is_list(values) and is_map(acc),
    do: mutate_reducer({colname, Series.from_list(values)}, acc, df)

  @doc """
  Arranges/sorts rows by columns.

  ## Examples

    A single column name will sort ascending by that column:

      iex> df = Explorer.DataFrame.from_map(%{a: ["b", "c", "a"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.arrange(df, ["a"])
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        a string ["a", "b", "c"]
        b integer [3, 1, 2]
      >

    You can also sort descending:

      iex> df = Explorer.DataFrame.from_map(%{a: ["b", "c", "a"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.arrange(df, [{"a", :desc}])
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        a string ["c", "b", "a"]
        b integer [2, 1, 3]
      >

    Atoms will be converted to the string column name:

      iex> df = Explorer.DataFrame.from_map(%{a: ["b", "c", "a"], b: [1, 2, 3]})
      iex> Explorer.DataFrame.arrange(df, a: :desc)
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        a string ["c", "b", "a"]
        b integer [2, 1, 3]
      >

    Sorting by two columns sorts them in the order they are entered:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.arrange(df, ["total", "country"])
      #Explorer.DataFrame<
        [rows: 1094, columns: 10]
        year integer [2010, 2014, 2013, 2012, 2011, "..."]
        country string ["AFGHANISTAN", "AFGHANISTAN", "AFGHANISTAN", "AFGHANISTAN", "AFGHANISTAN", "..."]
        total integer [2308, 2675, 2731, 2933, 3338, "..."]
        solid_fuel integer [627, 1194, 1075, 1000, 1174, "..."]
        liquid_fuel integer [1601, 1393, 1568, 1844, 2075, "..."]
        gas_fuel integer [74, 74, 81, 84, 84, "..."]
        cement integer [5, 14, 7, 5, 5, "..."]
        gas_flaring integer [0, 0, 0, 0, 0, "..."]
        per_capita float [0.08, 0.08, 0.09, 0.1, 0.12, "..."]
        bunker_fuels integer [9, 9, 9, 9, 9, "..."]
      >

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.arrange(df, total: :asc, country: :desc)
      #Explorer.DataFrame<
        [rows: 1094, columns: 10]
        year integer [2010, 2012, 2011, 2013, 2014, "..."]
        country string ["ZIMBABWE", "ZIMBABWE", "ZIMBABWE", "ZIMBABWE", "ZIMBABWE", "..."]
        total integer [2121, 2125, 2608, 3184, 3278, "..."]
        solid_fuel integer [1531, 917, 1584, 1902, 2097, "..."]
        liquid_fuel integer [481, 1006, 888, 1119, 1005, "..."]
        gas_fuel integer [0, 0, 0, 0, 0, "..."]
        cement integer [109, 201, 136, 162, 177, "..."]
        gas_flaring integer [0, 0, 0, 0, 0, "..."]
        per_capita float [0.15, 0.15, 0.18, 0.21, 0.22, "..."]
        bunker_fuels integer [7, 9, 8, 9, 9, "..."]
      >
  """
  def arrange(df, columns) when is_list(columns) do
    columns = columns |> Enum.reduce([], &arrange_reducer/2) |> Enum.reverse()

    column_names = names(df)

    Enum.each(columns, fn {name, _dir} ->
      maybe_raise_column_not_found(column_names, name)
    end)

    apply_impl(df, :arrange, [columns])
  end

  defp arrange_reducer({name, dir}, acc)
       when is_binary(name) and is_list(acc) and dir in [:asc, :desc],
       do: [{name, dir} | acc]

  defp arrange_reducer({name, dir}, acc)
       when is_atom(name) and is_list(acc) and dir in [:asc, :desc],
       do: arrange_reducer({Atom.to_string(name), dir}, acc)

  defp arrange_reducer(name, acc)
       when is_binary(name) and is_list(acc),
       do: arrange_reducer({name, :asc}, acc)

  defp arrange_reducer(name, acc)
       when is_atom(name) and is_list(acc),
       do: arrange_reducer(Atom.to_string(name), acc)

  @doc """
  Takes distinct rows by a selection of columns.

  ## Examples

    By default will return unique values of the requested columns:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.distinct(df, ["year", "country"])
      #Explorer.DataFrame<
        [rows: 1094, columns: 2]
        year integer [2010, 2010, 2010, 2010, 2010, "..."]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", "..."]
      >

    If `keep_all?` is set to `true`, then the first value of each column not in the requested
    columns will be returned:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.distinct(df, ["year", "country"], true)
      #Explorer.DataFrame<
        [rows: 1094, columns: 10]
        year integer [2010, 2010, 2010, 2010, 2010, "..."]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", "..."]
        total integer [2308, 1254, 32500, 141, 7924, "..."]
        solid_fuel integer [627, 117, 332, 0, 0, "..."]
        liquid_fuel integer [1601, 953, 12381, 141, 3649, "..."]
        gas_fuel integer [74, 7, 14565, 0, 374, "..."]
        cement integer [5, 177, 2598, 0, 204, "..."]
        gas_flaring integer [0, 0, 2623, 0, 3697, "..."]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, "..."]
        bunker_fuels integer [9, 7, 663, 0, 321, "..."]
      >

    A callback on the dataframe's names can be passed instead of a list (like `select/3`):

      iex> df = Explorer.DataFrame.from_map(%{x1: [1, 3, 3], x2: ["a", "c", "c"], y1: [1, 2, 3]})
      iex> Explorer.DataFrame.distinct(df, &String.starts_with?(&1, "x"))
      #Explorer.DataFrame<
        [rows: 2, columns: 2]
        x1 integer [1, 3]
        x2 string ["a", "c"]
      >
  """
  @spec distinct(df :: DataFrame.t(), columns :: [String.t()], keep_all? :: boolean()) ::
          DataFrame.t()
  def distinct(df, columns, keep_all? \\ false)

  def distinct(df, columns, keep_all?) when is_list(columns) do
    column_names = names(df)

    Enum.each(columns, fn name ->
      maybe_raise_column_not_found(column_names, name)
    end)

    apply_impl(df, :distinct, [columns, keep_all?])
  end

  @spec distinct(df :: DataFrame.t(), callback :: function(), keep_all? :: boolean()) ::
          DataFrame.t()
  def distinct(df, callback, keep_all?) when is_function(callback) do
    columns = df |> names() |> Enum.filter(callback)
    distinct(df, columns, keep_all?)
  end

  @doc """
  Renames columns.

  To apply a function to a subset of columns, see `rename_with/3`.

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

  @doc """
  Renames columns with a function.

  ## Examples

    If no columns are specified, it will apply the function to all column names:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.rename_with(df, &String.upcase/1)
      #Explorer.DataFrame<
        [rows: 1094, columns: 10]
        YEAR integer [2010, 2010, 2010, 2010, 2010, "..."]
        COUNTRY string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", "..."]
        TOTAL integer [2308, 1254, 32500, 141, 7924, "..."]
        SOLID_FUEL integer [627, 117, 332, 0, 0, "..."]
        LIQUID_FUEL integer [1601, 953, 12381, 141, 3649, "..."]
        GAS_FUEL integer [74, 7, 14565, 0, 374, "..."]
        CEMENT integer [5, 177, 2598, 0, 204, "..."]
        GAS_FLARING integer [0, 0, 2623, 0, 3697, "..."]
        PER_CAPITA float [0.08, 0.43, 0.9, 1.68, 0.37, "..."]
        BUNKER_FUELS integer [9, 7, 663, 0, 321, "..."]
      >

    A callback can be used to filter the column names that will be renamed, similarly to `select/3`:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.rename_with(df, &String.trim_trailing(&1, "_fuel"), &String.ends_with?(&1, "_fuel"))
      #Explorer.DataFrame<
        [rows: 1094, columns: 10]
        year integer [2010, 2010, 2010, 2010, 2010, "..."]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", "..."]
        total integer [2308, 1254, 32500, 141, 7924, "..."]
        solid integer [627, 117, 332, 0, 0, "..."]
        liquid integer [1601, 953, 12381, 141, 3649, "..."]
        gas integer [74, 7, 14565, 0, 374, "..."]
        cement integer [5, 177, 2598, 0, 204, "..."]
        gas_flaring integer [0, 0, 2623, 0, 3697, "..."]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, "..."]
        bunker_fuels integer [9, 7, 663, 0, 321, "..."]
      >

    Or you can just pass in the list of column names you'd like to apply the function to:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.rename_with(df, &String.upcase/1, ["total", "cement"])
      #Explorer.DataFrame<
        [rows: 1094, columns: 10]
        year integer [2010, 2010, 2010, 2010, 2010, "..."]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", "..."]
        TOTAL integer [2308, 1254, 32500, 141, 7924, "..."]
        solid_fuel integer [627, 117, 332, 0, 0, "..."]
        liquid_fuel integer [1601, 953, 12381, 141, 3649, "..."]
        gas_fuel integer [74, 7, 14565, 0, 374, "..."]
        CEMENT integer [5, 177, 2598, 0, 204, "..."]
        gas_flaring integer [0, 0, 2623, 0, 3697, "..."]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, "..."]
        bunker_fuels integer [9, 7, 663, 0, 321, "..."]
      >
  """
  def rename_with(df, callback, columns \\ [])

  def rename_with(df, callback, []) when is_function(callback),
    do: df |> names() |> Enum.map(callback) |> then(&rename(df, &1))

  def rename_with(df, callback, columns) when is_function(callback) and is_list(columns) do
    old_names = names(df)

    Enum.each(columns, fn name ->
      maybe_raise_column_not_found(old_names, name)
    end)

    old_names
    |> Enum.map(fn name -> if name in columns, do: callback.(name), else: name end)
    |> then(&rename(df, &1))
  end

  def rename_with(df, callback, columns) when is_function(callback) and is_function(columns) do
    case df |> names() |> Enum.filter(columns) do
      [_ | _] = columns ->
        rename_with(df, callback, columns)

      [] ->
        raise ArgumentError, message: "Function to select column names did not return any names."
    end
  end

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

  defp maybe_raise_column_not_found(names, name) do
    if name not in names,
      do:
        raise(ArgumentError,
          message:
            List.to_string(
              ["Could not find column name \"#{name}\""] ++ did_you_mean(name, names)
            )
        )
  end

  @threshold 0.77
  @max_suggestions 5
  defp did_you_mean(missing_key, available_keys) do
    suggestions =
      for key <- available_keys,
          distance = String.jaro_distance(missing_key, key),
          distance >= @threshold,
          do: {distance, key}

    case suggestions do
      [] -> []
      suggestions -> [". Did you mean:\n\n" | format_suggestions(suggestions)]
    end
  end

  defp format_suggestions(suggestions) do
    suggestions
    |> Enum.sort(&(elem(&1, 0) >= elem(&2, 0)))
    |> Enum.take(@max_suggestions)
    |> Enum.sort(&(elem(&1, 1) <= elem(&2, 1)))
    |> Enum.map(fn {_, key} -> ["      * ", inspect(key), ?\n] end)
  end
end
