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
  @default_infer_schema_length 1000
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
    * `dtypes` - A list of `{"column_name", dtype}` tuples. Uses column names as read, not as defined in options. If `nil`, dtypes are imputed from the first 1000 rows. (default: `nil`)
    * `header?` - Does the file have a header of column names as the first row or not? (default: `true`)
    * `max_rows` - Maximum number of lines to read. (default: `Inf`)
    * `names` - A list of column names. Must match the width of the dataframe. (default: nil)
    * `null_character` - The string that should be interpreted as a nil value. (default: `"NA"`)
    * `skip_rows` - The number of lines to skip at the beginning of the file. (default: `0`)
    * `with_columns` - A list of column names to keep. If present, only these columns are read into the dataframe. (default: `nil`)
    * `infer_schema_length` Maximum number of rows read for schema inference. Setting this to nil will do a full table scan and will be slow (default: `1000`).
    * `parse_dates` - Automatically try to parse dates/ datetimes and time. If parsing fails, columns remain of dtype `[DataType::Utf8]`
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
        with_columns: nil,
        infer_schema_length: @default_infer_schema_length,
        parse_dates: false
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
      opts[:with_columns],
      opts[:infer_schema_length],
      opts[:parse_dates]
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
  Reads a parquet file into a dataframe.
  """
  @spec read_parquet(filename :: String.t()) :: {:ok, DataFrame.t()} | {:error, term()}
  def read_parquet(filename), do: Explorer.PolarsBackend.DataFrame.read_parquet(filename)

  @doc """
  Writes a dataframe to a parquet file.
  """
  @spec write_parquet(df :: DataFrame.t(), filename :: String.t()) ::
          {:ok, String.t()} | {:error, term()}
  def write_parquet(df, filename) do
    apply_impl(df, :write_parquet, [filename])
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
  Creates a new dataframe from a map or keyword of lists or series.

  Lists and series must be the same length. This function calls `Explorer.Series.from_list/2`
  for lists, so they must conform to the requirements for making a series.

  ## Options

    * `backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.default_backend/0`.

  ## Examples

      iex> Explorer.DataFrame.from_columns(%{floats: [1.0, 2.0], ints: [1, nil]})
      #Explorer.DataFrame<
        [rows: 2, columns: 2]
        floats float [1.0, 2.0]
        ints integer [1, nil]
      >

      iex> Explorer.DataFrame.from_columns([floats: [1.0, 2.0], ints: [1, nil]])
      #Explorer.DataFrame<
        [rows: 2, columns: 2]
        floats float [1.0, 2.0]
        ints integer [1, nil]
      >

      iex> Explorer.DataFrame.from_columns(floats: Explorer.Series.from_list([1.0, 2.0]), ints: Explorer.Series.from_list([1, nil]))
      #Explorer.DataFrame<
        [rows: 2, columns: 2]
        floats float [1.0, 2.0]
        ints integer [1, nil]
      >
  """
  @spec from_columns(series :: map() | Keyword.t(), opts :: Keyword.t()) :: DataFrame.t()
  def from_columns(series, opts \\ []) do
    backend = backend_from_options!(opts)
    backend.from_columns(series)
  end

  @doc """
  Creates a new dataframe from a list of maps or keyword lists.

  Each map in the list should have the same keys, but missing keys will yield a null value for
  that row. All values for a given key should be of the same dtype.

  Keyword lists should all be in the same order.

  ## Options

    * `backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.default_backend/0`.

  ## Examples

      iex> rows = [%{id: 1, name: "José"}, %{id: 2, name: "Christopher"}, %{id: 3, name: "Cristine"}]
      iex> Explorer.DataFrame.from_rows(rows)
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        id integer [1, 2, 3]
        name string ["José", "Christopher", "Cristine"]
      >

      iex> rows = [[id: 1, name: "José"], [id: 2, name: "Christopher"], [id: 3, name: "Cristine"]]
      iex> Explorer.DataFrame.from_rows(rows)
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        id integer [1, 2, 3]
        name string ["José", "Christopher", "Cristine"]
      >

    With a list of maps, missing keys will yield a null value.

      iex> rows = [%{id: 1, name: "José", date: ~D[2001-01-01]}, %{id: 2, date: ~D[1993-01-01]}, %{id: 3, name: "Cristine"}]
      iex> Explorer.DataFrame.from_rows(rows)
      #Explorer.DataFrame<
        [rows: 3, columns: 3]
        date date [2001-01-01, 1993-01-01, nil]
        id integer [1, 2, 3]
        name string ["José", nil, "Cristine"]
      >
  """
  @spec from_rows(rows :: list(map()) | Keyword.t(), opts :: Keyword.t()) :: DataFrame.t()
  def from_rows(rows, opts \\ []) do
    backend = backend_from_options!(opts)
    backend.from_rows(rows)
  end

  @doc """
  Converts a dataframe to a map.

  By default, the constituent series of the dataframe are converted to Elixir lists.

  ## Examples

      iex> df = Explorer.DataFrame.from_columns(floats: [1.0, 2.0], ints: [1, nil])
      iex> Explorer.DataFrame.to_map(df)
      %{floats: [1.0, 2.0], ints: [1, nil]}
  """
  @spec to_map(df :: DataFrame.t(), convert_series? :: boolean()) :: map()
  def to_map(df, convert_series? \\ true), do: apply_impl(df, :to_map, [convert_series?])

  @doc """
  Writes a dataframe to a binary representation of a delimited file.

  ## Options

    * `header?` - Should the column names be written as the first line of the file? (default: `true`)
    * `delimiter` - A single character used to separate fields within a record. (default: `","`)

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> df |> Explorer.DataFrame.head() |> Explorer.DataFrame.to_binary()
      "year,country,total,solid_fuel,liquid_fuel,gas_fuel,cement,gas_flaring,per_capita,bunker_fuels\\n2010,AFGHANISTAN,2308,627,1601,74,5,0,0.08,9\\n2010,ALBANIA,1254,117,953,7,177,0,0.43,7\\n2010,ALGERIA,32500,332,12381,14565,2598,2623,0.9,663\\n2010,ANDORRA,141,0,141,0,0,0,1.68,0\\n2010,ANGOLA,7924,0,3649,374,204,3697,0.37,321\\n"
  """
  @spec to_binary(df :: DataFrame.t(), opts :: Keyword.t()) :: String.t()
  def to_binary(df, opts \\ []) do
    opts = keyword!(opts, header?: true, delimiter: ",")
    apply_impl(df, :to_binary, [opts[:header?], opts[:delimiter]])
  end

  # Introspection

  @doc """
  Gets the names of the dataframe columns.

  ## Examples

      iex> df = Explorer.DataFrame.from_columns(floats: [1.0, 2.0], ints: [1, 2])
      iex> Explorer.DataFrame.names(df)
      ["floats", "ints"]
  """
  @spec names(df :: DataFrame.t()) :: [String.t()]
  def names(df), do: apply_impl(df, :names)

  @doc """
  Gets the dtypes of the dataframe columns.

  ## Examples

      iex> df = Explorer.DataFrame.from_columns(floats: [1.0, 2.0], ints: [1, 2])
      iex> Explorer.DataFrame.dtypes(df)
      [:float, :integer]
  """
  @spec dtypes(df :: DataFrame.t()) :: [atom()]
  def dtypes(df), do: apply_impl(df, :dtypes)

  @doc """
  Gets the shape of the dataframe as a `{height, width}` tuple.

  ## Examples

      iex> df = Explorer.DataFrame.from_columns(floats: [1.0, 2.0, 3.0], ints: [1, 2, 3])
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

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> df = Explorer.DataFrame.group_by(df, "country")
      iex> Explorer.DataFrame.groups(df)
      ["country"]
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
  @spec head(df :: DataFrame.t(), nrows :: integer()) :: DataFrame.t()
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
  @spec tail(df :: DataFrame.t(), nrows :: integer()) :: DataFrame.t()
  def tail(df, nrows \\ 5), do: apply_impl(df, :tail, [nrows])

  @doc """
  Selects a subset of columns by name.

  Can optionally return all *but* the named columns if `:drop` is passed as the last argument.

  ## Examples

    You can select columns with a list of names:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.select(df, ["a"])
      #Explorer.DataFrame<
        [rows: 3, columns: 1]
        a string ["a", "b", "c"]
      >

    Or you can use a callback function that takes the dataframe's names as its first argument:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.select(df, &String.starts_with?(&1, "b"))
      #Explorer.DataFrame<
        [rows: 3, columns: 1]
        b integer [1, 2, 3]
      >

    If you pass `:drop` as the third argument, it will return all but the named columns:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.select(df, ["b"], :drop)
      #Explorer.DataFrame<
        [rows: 3, columns: 1]
        a string ["a", "b", "c"]
      >

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3], c: [4, 5, 6])
      iex> Explorer.DataFrame.select(df, ["a", "b"], :drop)
      #Explorer.DataFrame<
        [rows: 3, columns: 1]
        c integer [4, 5, 6]
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

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, Explorer.Series.greater(df["b"], 1))
      #Explorer.DataFrame<
        [rows: 2, columns: 2]
        a string ["b", "c"]
        b integer [2, 3]
      >

    You can combine masks using `Explorer.Series.and/2` or `Explorer.Series.or/2`:
      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> b_gt = Explorer.Series.greater(df["b"], 1)
      iex> a_eq = Explorer.Series.equal(df["a"], "b")
      iex> Explorer.DataFrame.filter(df, Explorer.Series.and(a_eq, b_gt))
      #Explorer.DataFrame<
        [rows: 1, columns: 2]
        a string ["b"]
        b integer [2]
      >

    Including a list:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, [false, true, false])
      #Explorer.DataFrame<
        [rows: 1, columns: 2]
        a string ["b"]
        b integer [2]
      >

    Or you can invoke a callback on the dataframe:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, &Explorer.Series.greater(&1["b"], 1))
      #Explorer.DataFrame<
        [rows: 2, columns: 2]
        a string ["b", "c"]
        b integer [2, 3]
      >
  """
  @spec filter(df :: DataFrame.t(), mask :: Series.t() | [boolean()]) :: DataFrame.t()
  def filter(df, %Series{} = mask) do
    s_len = Series.length(mask)
    df_len = n_rows(df)

    case s_len == df_len do
      false ->
        raise(
          ArgumentError,
          "length of the mask (#{s_len}) must match number of rows in the dataframe (#{df_len})"
        )

      true ->
        apply_impl(df, :filter, [mask])
    end
  end

  def filter(df, mask) when is_list(mask), do: mask |> Series.from_list() |> then(&filter(df, &1))

  @spec filter(df :: DataFrame.t(), callback :: function()) :: DataFrame.t()
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

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate(df, c: [4, 5, 6])
      #Explorer.DataFrame<
        [rows: 3, columns: 3]
        a string ["a", "b", "c"]
        b integer [1, 2, 3]
        c integer [4, 5, 6]
      >

    Or you can pass in a series:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> s = Explorer.Series.from_list([4, 5, 6])
      iex> Explorer.DataFrame.mutate(df, c: s)
      #Explorer.DataFrame<
        [rows: 3, columns: 3]
        a string ["a", "b", "c"]
        b integer [1, 2, 3]
        c integer [4, 5, 6]
      >

    Or you can invoke a callback on the dataframe:

      iex> df = Explorer.DataFrame.from_columns(a: [4, 5, 6], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate(df, c: &Explorer.Series.add(&1["a"], &1["b"]))
      #Explorer.DataFrame<
        [rows: 3, columns: 3]
        a integer [4, 5, 6]
        b integer [1, 2, 3]
        c integer [5, 7, 9]
      >

    You can overwrite existing columns:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate(df, a: [4, 5, 6])
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        a integer [4, 5, 6]
        b integer [1, 2, 3]
      >

    Scalar values are repeated to fill the series:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate(df, a: 4)
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        a integer [4, 4, 4]
        b integer [1, 2, 3]
      >

    Including when a callback returns a scalar:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate(df, a: &Explorer.Series.max(&1["b"]))
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        a integer [3, 3, 3]
        b integer [1, 2, 3]
      >

    Alternatively, all of the above works with a map instead of a keyword list:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate(df, %{"c" => [4, 5, 6]})
      #Explorer.DataFrame<
        [rows: 3, columns: 3]
        a string ["a", "b", "c"]
        b integer [1, 2, 3]
        c integer [4, 5, 6]
      >
  """
  @spec mutate(df :: DataFrame.t(), with_columns :: map() | Keyword.t()) :: DataFrame.t()
  def mutate(df, with_columns) when is_map(with_columns) do
    with_columns = Enum.reduce(with_columns, %{}, &mutate_reducer(&1, &2, df))

    apply_impl(df, :mutate, [with_columns])
  end

  def mutate(df, with_columns) when is_list(with_columns) do
    if not Keyword.keyword?(with_columns),
      do: raise(ArgumentError, "expected second argument to be a keyword list")

    with_columns
    |> Enum.reduce(%{}, fn {colname, value}, acc ->
      Map.put(acc, Atom.to_string(colname), value)
    end)
    |> then(&mutate(df, &1))
  end

  defp mutate_reducer({colname, value}, acc, %DataFrame{} = _df)
       when is_binary(colname) and is_map(acc),
       do: Map.put(acc, colname, value)

  defp mutate_reducer({colname, value}, acc, %DataFrame{} = df)
       when is_atom(colname) and is_map(acc),
       do: mutate_reducer({Atom.to_string(colname), value}, acc, df)

  @doc """
  Arranges/sorts rows by columns.

  ## Examples

    A single column name will sort ascending by that column:

      iex> df = Explorer.DataFrame.from_columns(a: ["b", "c", "a"], b: [1, 2, 3])
      iex> Explorer.DataFrame.arrange(df, "a")
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        a string ["a", "b", "c"]
        b integer [3, 1, 2]
      >

    You can also sort descending:

      iex> df = Explorer.DataFrame.from_columns(a: ["b", "c", "a"], b: [1, 2, 3])
      iex> Explorer.DataFrame.arrange(df, desc: "a")
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        a string ["c", "b", "a"]
        b integer [2, 1, 3]
      >

    Sorting by more than one column sorts them in the order they are entered:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.arrange(df, asc: "total", desc: "country")
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
  @spec arrange(
          df :: DataFrame.t(),
          columns ::
            String.t() | [String.t() | {:asc | :desc, String.t()}]
        ) :: DataFrame.t()
  def arrange(df, columns) when is_list(columns) do
    columns = columns |> Enum.reduce([], &arrange_reducer/2) |> Enum.reverse()

    column_names = names(df)

    Enum.each(columns, fn {_dir, name} ->
      maybe_raise_column_not_found(column_names, name)
    end)

    apply_impl(df, :arrange, [columns])
  end

  def arrange(df, column) when is_binary(column), do: arrange(df, [column])

  defp arrange_reducer({dir, name}, acc)
       when is_binary(name) and is_list(acc) and dir in [:asc, :desc],
       do: [{dir, name} | acc]

  defp arrange_reducer({dir, name}, acc)
       when is_atom(name) and is_list(acc) and dir in [:asc, :desc],
       do: arrange_reducer({dir, Atom.to_string(name)}, acc)

  defp arrange_reducer(name, acc)
       when is_binary(name) and is_list(acc),
       do: arrange_reducer({:asc, name}, acc)

  defp arrange_reducer(name, acc)
       when is_atom(name) and is_list(acc),
       do: arrange_reducer(Atom.to_string(name), acc)

  @doc """
  Takes distinct rows by a selection of columns.

  ## Examples

    By default will return unique values of the requested columns:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.distinct(df, columns: ["year", "country"])
      #Explorer.DataFrame<
        [rows: 1094, columns: 2]
        year integer [2010, 2010, 2010, 2010, 2010, "..."]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", "..."]
      >

    If `keep_all?` is set to `true`, then the first value of each column not in the requested
    columns will be returned:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.distinct(df, columns: ["year", "country"], keep_all?: true)
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

      iex> df = Explorer.DataFrame.from_columns(x1: [1, 3, 3], x2: ["a", "c", "c"], y1: [1, 2, 3])
      iex> Explorer.DataFrame.distinct(df, columns: &String.starts_with?(&1, "x"))
      #Explorer.DataFrame<
        [rows: 2, columns: 2]
        x1 integer [1, 3]
        x2 string ["a", "c"]
      >
  """
  @spec distinct(df :: DataFrame.t(), opts :: Keyword.t()) :: DataFrame.t()
  def distinct(df, opts \\ [])

  def distinct(df, opts) do
    opts = keyword!(opts, columns: [], keep_all?: false)

    column_names = names(df)

    columns =
      case opts[:columns] do
        [] -> column_names
        callback when is_function(callback) -> Enum.filter(column_names, callback)
        columns -> columns
      end

    Enum.each(columns, fn name ->
      maybe_raise_column_not_found(column_names, name)
    end)

    apply_impl(df, :distinct, [columns, opts[:keep_all?]])
  end

  @doc """
  Drop nil values.

  Optionally accepts a subset of columns.

  ## Examples

      iex> df = Explorer.DataFrame.from_columns(a: [1, 2, nil], b: [1, nil, 3])
      iex> Explorer.DataFrame.drop_nil(df)
      #Explorer.DataFrame<
        [rows: 1, columns: 2]
        a integer [1]
        b integer [1]
      >
  """
  @spec drop_nil(df :: DataFrame.t(), columns_or_column :: [String.t()] | String.t()) ::
          DataFrame.t()
  def drop_nil(df, columns_or_column \\ [])

  def drop_nil(df, []), do: drop_nil(df, names(df))

  def drop_nil(df, column) when is_binary(column), do: drop_nil(df, [column])

  def drop_nil(df, columns) when is_list(columns) do
    column_names = names(df)

    Enum.each(columns, fn name ->
      maybe_raise_column_not_found(column_names, name)
    end)

    apply_impl(df, :drop_nil, [columns])
  end

  @doc """
  Renames columns.

  To apply a function to a subset of columns, see `rename_with/3`.

  ## Examples

    You can pass in a list of new names:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "a"], b: [1, 3, 1])
      iex> Explorer.DataFrame.rename(df, ["c", "d"])
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        c string ["a", "b", "a"]
        d integer [1, 3, 1]
      >

    Or you can rename individual columns using keyword args:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "a"], b: [1, 3, 1])
      iex> Explorer.DataFrame.rename(df, a: "first")
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        first string ["a", "b", "a"]
        b integer [1, 3, 1]
      >

    Or you can rename individual columns using a map:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "a"], b: [1, 3, 1])
      iex> Explorer.DataFrame.rename(df, %{"a" => "first"})
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        first string ["a", "b", "a"]
        b integer [1, 3, 1]
      >

    Or if you want to use a function:

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "a"], b: [1, 3, 1])
      iex> Explorer.DataFrame.rename(df, &(&1 <> "_test"))
      #Explorer.DataFrame<
        [rows: 3, columns: 2]
        a_test string ["a", "b", "a"]
        b_test integer [1, 3, 1]
      >

  """
  @spec rename(df :: DataFrame.t(), names :: [String.t() | atom()] | map()) :: DataFrame.t()
  def rename(df, names) when is_list(names) do
    case Keyword.keyword?(names) do
      false ->
        names =
          Enum.map(names, fn
            name when is_atom(name) -> Atom.to_string(name)
            name -> name
          end)

        check_new_names_length(df, names)

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

  def rename(df, names) when is_function(names),
    do: df |> names() |> Enum.map(names) |> then(&rename(df, &1))

  defp rename_reducer({k, v}, acc) when is_atom(k) and is_binary(v),
    do: Map.put(acc, Atom.to_string(k), v)

  defp rename_reducer({k, v}, acc) when is_binary(k) and is_binary(v), do: Map.put(acc, k, v)

  defp rename_reducer({k, v}, acc) when is_atom(k) and is_atom(v),
    do: Map.put(acc, Atom.to_string(k), Atom.to_string(v))

  defp rename_reducer({k, v}, acc) when is_binary(k) and is_atom(v),
    do: Map.put(acc, k, Atom.to_string(v))

  defp check_new_names_length(df, names) do
    width = n_cols(df)
    n_new_names = length(names)

    if width != n_new_names,
      do:
        raise(
          ArgumentError,
          "list of new names must match the number of columns in the dataframe; found " <>
            "#{n_new_names} new name(s), but the supplied dataframe has #{width} column(s)"
        )
  end

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
  @spec rename_with(df :: DataFrame.t(), callback :: function(), columns :: list() | function()) ::
          DataFrame.t()
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
        raise ArgumentError, "function to select column names did not return any names"
    end
  end

  @doc """
  Turns a set of columns to dummy variables.

  ## Examples

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "a", "c"], b: ["b", "a", "b", "d"])
      iex> Explorer.DataFrame.dummies(df, ["a"])
      #Explorer.DataFrame<
        [rows: 4, columns: 3]
        a_a integer [1, 0, 1, 0]
        a_b integer [0, 1, 0, 0]
        a_c integer [0, 0, 0, 1]
      >

      iex> df = Explorer.DataFrame.from_columns(a: ["a", "b", "a", "c"], b: ["b", "a", "b", "d"])
      iex> Explorer.DataFrame.dummies(df, ["a", "b"])
      #Explorer.DataFrame<
        [rows: 4, columns: 6]
        a_a integer [1, 0, 1, 0]
        a_b integer [0, 1, 0, 0]
        a_c integer [0, 0, 0, 1]
        b_a integer [0, 1, 0, 0]
        b_b integer [1, 0, 1, 0]
        b_d integer [0, 0, 0, 1]
      >
  """
  def dummies(df, columns), do: apply_impl(df, :dummies, [columns])

  @doc """
  Extracts a single column as a series.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.pull(df, "total")
      #Explorer.Series<
        integer[1094]
        [2308, 1254, 32500, 141, 7924, 41, 143, 51246, 1150, 684, 106589, 18408, 8366, 451, 7981, 16345, 403, 17192, 30222, 147, 1388, 166, 133, 5802, 1278, 114468, 47, 2237, 12030, 535, 58, 1367, 145806, 152, 152, 72, 141, 19703, 2393248, 20773, 44, 540, 19, 2064, 1900, 5501, 10465, 2102, 30428, 18122, ...]
      >
  """
  @spec pull(df :: DataFrame.t(), column :: String.t()) :: Series.t()
  def pull(df, column) do
    names = names(df)
    maybe_raise_column_not_found(names, column)
    apply_impl(df, :pull, [column])
  end

  @doc """
  Subset a continuous set of rows.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.slice(df, 1, 2)
      #Explorer.DataFrame<
        [rows: 2, columns: 10]
        year integer [2010, 2010]
        country string ["ALBANIA", "ALGERIA"]
        total integer [1254, 32500]
        solid_fuel integer [117, 332]
        liquid_fuel integer [953, 12381]
        gas_fuel integer [7, 14565]
        cement integer [177, 2598]
        gas_flaring integer [0, 2623]
        per_capita float [0.43, 0.9]
        bunker_fuels integer [7, 663]
      >

    Negative offsets count from the end of the series:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.slice(df, -10, 2)
      #Explorer.DataFrame<
        [rows: 2, columns: 10]
        year integer [2014, 2014]
        country string ["UNITED STATES OF AMERICA", "URUGUAY"]
        total integer [1432855, 1840]
        solid_fuel integer [450047, 2]
        liquid_fuel integer [576531, 1700]
        gas_fuel integer [390719, 25]
        cement integer [11314, 112]
        gas_flaring integer [4244, 0]
        per_capita float [4.43, 0.54]
        bunker_fuels integer [30722, 251]
      >

    If the length would run past the end of the dataframe, the result may be shorter than the length:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.slice(df, -10, 20)
      #Explorer.DataFrame<
        [rows: 10, columns: 10]
        year integer [2014, 2014, 2014, 2014, 2014, "..."]
        country string ["UNITED STATES OF AMERICA", "URUGUAY", "UZBEKISTAN", "VANUATU", "VENEZUELA", "..."]
        total integer [1432855, 1840, 28692, 42, 50510, "..."]
        solid_fuel integer [450047, 2, 1677, 0, 204, "..."]
        liquid_fuel integer [576531, 1700, 2086, 42, 28445, "..."]
        gas_fuel integer [390719, 25, 23929, 0, 12731, "..."]
        cement integer [11314, 112, 1000, 0, 1088, "..."]
        gas_flaring integer [4244, 0, 0, 0, 8042, "..."]
        per_capita float [4.43, 0.54, 0.97, 0.16, 1.65, "..."]
        bunker_fuels integer [30722, 251, 0, 10, 1256, "..."]
      >
  """
  def slice(df, offset, length), do: apply_impl(df, :slice, [offset, length])

  @doc """
  Subset rows with a list of indices.

  ## Examples

      iex> df = Explorer.DataFrame.from_columns(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> Explorer.DataFrame.take(df, [0, 2])
      #Explorer.DataFrame<
        [rows: 2, columns: 2]
        a integer [1, 3]
        b string ["a", "c"]
      >
  """
  def take(df, row_indices) when is_list(row_indices) do
    n_rows = n_rows(df)

    Enum.each(row_indices, fn idx ->
      if idx > n_rows or idx < -n_rows,
        do:
          raise(
            ArgumentError,
            "requested row index (#{idx}) out of bounds (-#{n_rows}:#{n_rows})"
          )
    end)

    apply_impl(df, :take, [row_indices])
  end

  @doc """
  Sample rows from a dataframe.

  If given an integer as the second argument, it will return N samples. If given a float, it will
  return that proportion of the series.

  Can sample with or without replacement.

  ## Options

    * `with_replacement?` - If set to `true`, each sample will be independent and therefore values may repeat. Required to be `true` for `n` greater then the number of rows in the dataframe or `frac` > 1.0. (default: `false`)
    * `seed` - An integer to be used as a random seed. If nil, a random value between 1 and 1e12 will be used. (default: nil)

  ## Examples

    You can sample N rows:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.sample(df, 3, seed: 100)
      #Explorer.DataFrame<
        [rows: 3, columns: 10]
        year integer [2012, 2012, 2013]
        country string ["ZIMBABWE", "NICARAGUA", "NIGER"]
        total integer [2125, 1260, 529]
        solid_fuel integer [917, 0, 93]
        liquid_fuel integer [1006, 1176, 432]
        gas_fuel integer [0, 0, 0]
        cement integer [201, 84, 4]
        gas_flaring integer [0, 0, 0]
        per_capita float [0.15, 0.21, 0.03]
        bunker_fuels integer [9, 18, 19]
      >

    Or you can sample a proportion of rows:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.sample(df, 0.03, seed: 100)
      #Explorer.DataFrame<
        [rows: 33, columns: 10]
        year integer [2013, 2012, 2013, 2012, 2010, "..."]
        country string ["BAHAMAS", "POLAND", "SLOVAKIA", "MOZAMBIQUE", "OMAN", "..."]
        total integer [764, 81792, 9024, 851, 12931, "..."]
        solid_fuel integer [1, 53724, 3657, 11, 0, "..."]
        liquid_fuel integer [763, 17353, 2090, 632, 2331, "..."]
        gas_fuel integer [0, 8544, 2847, 47, 9309, "..."]
        cement integer [0, 2165, 424, 161, 612, "..."]
        gas_flaring integer [0, 6, 7, 0, 679, "..."]
        per_capita float [2.02, 2.12, 1.67, 0.03, 4.39, "..."]
        bunker_fuels integer [167, 573, 34, 56, 1342, "..."]
      >

  """
  @spec sample(df :: DataFrame.t(), n_or_frac :: number(), opts :: Keyword.t()) :: DataFrame.t()
  def sample(df, n_or_frac, opts \\ [])

  def sample(df, n, opts) when is_integer(n) do
    opts = keyword!(opts, with_replacement?: false, seed: Enum.random(1..1_000_000_000_000))
    n_rows = n_rows(df)

    case {n > n_rows, opts[:with_replacement?]} do
      {true, false} ->
        raise ArgumentError,
              "in order to sample more rows than are in the dataframe (#{n_rows}), sampling " <>
                "`with_replacement?` must be true"

      _ ->
        :ok
    end

    apply_impl(df, :sample, [n, opts[:with_replacement?], opts[:seed]])
  end

  def sample(df, frac, opts) when is_float(frac) do
    n_rows = n_rows(df)
    n = round(frac * n_rows)
    sample(df, n, opts)
  end

  @doc """
  Pivot data from wide to long.

  `Explorer.DataFrame.pivot_longer/3` "lengthens" data, increasing the number of rows and
  decreasing the number of columns. The inverse transformation is
  `Explorer.DataFrame.pivot_wider/4`.

  The second argument (`columns`) can be either an array of column names to use or a filter callback on
  the dataframe's names.

  `value_cols` must all have the same dtype.

  ## Options

    * `value_cols` - Columns to use for values. May be a filter callback on the dataframe's column names. Defaults to an empty list, using all variables except the columns to pivot.
    * `names_to` - A string specifying the name of the column to create from the data stored in the column names of the dataframe. Defaults to `"variable"`.
    * `values_to` - A string specifying the name of the column to create from the data stored in series element values. Defaults to `"value"`.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.pivot_longer(df, ["year", "country"], value_cols: &String.ends_with?(&1, "fuel"))
      #Explorer.DataFrame<
        [rows: 3282, columns: 4]
        year integer [2010, 2010, 2010, 2010, 2010, "..."]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", "..."]
        variable string ["solid_fuel", "solid_fuel", "solid_fuel", "solid_fuel", "solid_fuel", "..."]
        value integer [627, 117, 332, 0, 0, "..."]
      >

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.pivot_longer(df, ["year", "country"], value_cols: ["total"])
      #Explorer.DataFrame<
        [rows: 1094, columns: 4]
        year integer [2010, 2010, 2010, 2010, 2010, "..."]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", "..."]
        variable string ["total", "total", "total", "total", "total", "..."]
        value integer [2308, 1254, 32500, 141, 7924, "..."]
      >
  """
  @spec pivot_longer(
          df :: DataFrame.t(),
          columns :: [String.t()] | function(),
          opts :: Keyword.t()
        ) :: DataFrame.t()
  def pivot_longer(df, columns, opts \\ [])

  def pivot_longer(df, columns, opts) when is_list(columns) do
    opts = keyword!(opts, value_cols: [], names_to: "variable", values_to: "value")

    names = names(df)
    dtypes = names |> Enum.zip(dtypes(df)) |> Enum.into(%{})

    Enum.each(columns, fn name -> maybe_raise_column_not_found(names, name) end)

    value_cols =
      case opts[:value_cols] do
        [] ->
          Enum.filter(names, fn name -> name not in columns end)

        [_ | _] = cols ->
          Enum.each(cols, fn col ->
            if col in columns,
              do:
                raise(
                  ArgumentError,
                  "value columns may not also be ID columns but found #{col} in both"
                )
          end)

          cols

        callback when is_function(callback) ->
          Enum.filter(names, fn name -> name not in columns && callback.(name) end)
      end

    Enum.each(value_cols, fn name -> maybe_raise_column_not_found(names, name) end)

    dtypes
    |> Map.take(value_cols)
    |> Map.values()
    |> Enum.uniq()
    |> length()
    |> case do
      1 ->
        :ok

      _ ->
        raise ArgumentError,
              "value columns may only include one dtype but found multiple dtypes"
    end

    apply_impl(df, :pivot_longer, [columns, value_cols, opts[:names_to], opts[:values_to]])
  end

  def pivot_longer(df, columns, opts) when is_function(columns),
    do:
      df
      |> names()
      |> columns.()
      |> then(&pivot_longer(df, &1, opts))

  @doc """
  Pivot data from long to wide.

  `Explorer.DataFrame.pivot_wider/4` "widens" data, increasing the number of columns and
  decreasing the number of rows. The inverse transformation is
  `Explorer.DataFrame.pivot_longer/3`.

  Due to a restriction upstream, `values_from` must be a numeric type.

  ## Options

  * `id_cols` - A set of columns that uniquely identifies each observation. Defaults to all columns in data except for the columns specified in `names_from` and `values_from`. Typically used when you have redundant variables, i.e. variables whose values are perfectly correlated with existing variables. May accept a filter callback or list of column names.
  * `names_prefix` - String added to the start of every variable name. This is particularly useful if `names_from` is a numeric vector and you want to create syntactic variable names.

  ## Examples

      iex> df = Explorer.DataFrame.from_columns(id: [1, 1], variable: ["a", "b"], value: [1, 2])
      iex> Explorer.DataFrame.pivot_wider(df, "variable", "value")
      #Explorer.DataFrame<
        [rows: 1, columns: 3]
        id integer [1]
        a integer [1]
        b integer [2]
      >
  """
  @spec pivot_wider(
          df :: DataFrame.t(),
          names_from :: String.t(),
          values_from :: String.t(),
          opts ::
            Keyword.t()
        ) :: DataFrame.t()
  def pivot_wider(df, names_from, values_from, opts \\ []) do
    opts = keyword!(opts, id_cols: [], names_prefix: "")

    names = names(df)
    dtypes = names |> Enum.zip(dtypes(df)) |> Enum.into(%{})

    case Map.get(dtypes, values_from) do
      :integer ->
        :ok

      :float ->
        :ok

      :date ->
        :ok

      :datetime ->
        :ok

      dtype ->
        raise ArgumentError, "the values_from column must be numeric, but found #{dtype}"
    end

    id_cols =
      case opts[:id_cols] do
        [] ->
          Enum.filter(names, &(&1 not in [names_from, values_from]))

        [_ | _] = names ->
          Enum.filter(names, &(&1 not in [names_from, values_from]))

        fun when is_function(fun) ->
          Enum.filter(names, fn name -> fun.(name) && name not in [names_from, values_from] end)
      end

    Enum.each(id_cols ++ [names_from, values_from], fn name ->
      maybe_raise_column_not_found(names, name)
    end)

    apply_impl(df, :pivot_wider, [id_cols, names_from, values_from, opts[:names_prefix]])
  end

  # Two table verbs

  @doc """
  Join two tables.

  ## Join types

    * `inner` - Returns all rows from `left` where there are matching values in `right`, and all columns from `left` and `right`.
    * `left` - Returns all rows from `left` and all columns from `left` and `right`. Rows in `left` with no match in `right` will have `nil` values in the new columns.
    * `right` - Returns all rows from `right` and all columns from `left` and `right`. Rows in `right` with no match in `left` will have `nil` values in the new columns.
    * `outer` - Returns all rows and all columns from both `left` and `right`. Where there are not matching values, returns `nil` for the one missing.
    * `cross` - Also known as a cartesian join. Returns all combinations of `left` and `right`. Can be very computationally expensive.

  ## Options

    * `on` - The columns to join on. Defaults to overlapping columns. Does not apply to cross join.
    * `how` - One of the join types (as an atom) described above. Defaults to `:inner`.

  ## Examples

    Inner join:

      iex> left = Explorer.DataFrame.from_columns(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.from_columns(a: [1, 2, 2], c: ["d", "e", "f"])
      iex> Explorer.DataFrame.join(left, right)
      #Explorer.DataFrame<
        [rows: 3, columns: 3]
        a integer [1, 2, 2]
        b string ["a", "b", "b"]
        c string ["d", "e", "f"]
      >

    Left join:

      iex> left = Explorer.DataFrame.from_columns(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.from_columns(a: [1, 2, 2], c: ["d", "e", "f"])
      iex> Explorer.DataFrame.join(left, right, how: :left)
      #Explorer.DataFrame<
        [rows: 4, columns: 3]
        a integer [1, 2, 2, 3]
        b string ["a", "b", "b", "c"]
        c string ["d", "e", "f", nil]
      >

    Right join:

      iex> left = Explorer.DataFrame.from_columns(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.from_columns(a: [1, 2, 4], c: ["d", "e", "f"])
      iex> Explorer.DataFrame.join(left, right, how: :right)
      #Explorer.DataFrame<
        [rows: 3, columns: 3]
        a integer [1, 2, 4]
        c string ["d", "e", "f"]
        b string ["a", "b", nil]
      >

    Outer join:

      iex> left = Explorer.DataFrame.from_columns(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.from_columns(a: [1, 2, 4], c: ["d", "e", "f"])
      iex> Explorer.DataFrame.join(left, right, how: :outer)
      #Explorer.DataFrame<
        [rows: 4, columns: 3]
        a integer [1, 2, 4, 3]
        b string ["a", "b", nil, "c"]
        c string ["d", "e", "f", nil]
      >

    Cross join:

      iex> left = Explorer.DataFrame.from_columns(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.from_columns(a: [1, 2, 4], c: ["d", "e", "f"])
      iex> Explorer.DataFrame.join(left, right, how: :cross)
      #Explorer.DataFrame<
        [rows: 9, columns: 4]
        a integer [1, 1, 1, 2, 2, "..."]
        b string ["a", "a", "a", "b", "b", "..."]
        a_right integer [1, 2, 4, 1, 2, "..."]
        c string ["d", "e", "f", "d", "e", "..."]
      >

    Inner join with different names:

      iex> left = Explorer.DataFrame.from_columns(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.from_columns(d: [1, 2, 2], c: ["d", "e", "f"])
      iex> Explorer.DataFrame.join(left, right, on: [{"a", "d"}])
      #Explorer.DataFrame<
        [rows: 3, columns: 3]
        a integer [1, 2, 2]
        b string ["a", "b", "b"]
        c string ["d", "e", "f"]
      >

  """
  @spec join(left :: DataFrame.t(), right :: DataFrame.t(), opts :: Keyword.t()) :: DataFrame.t()
  def join(%DataFrame{} = left, %DataFrame{} = right, opts \\ []) do
    left_cols = names(left)
    right_cols = names(right)
    cols = left_cols ++ right_cols

    opts = keyword!(opts, on: find_overlapping_cols(left_cols, right_cols), how: :inner)

    case {opts[:on], opts[:how]} do
      {_, :cross} ->
        nil

      {[], _} ->
        raise(ArgumentError, "could not find any overlapping columns")

      {[_ | _] = on, _} ->
        Enum.each(on, fn
          {l_name, r_name} -> Enum.each([l_name, r_name], &maybe_raise_column_not_found(cols, &1))
          name -> maybe_raise_column_not_found(cols, name)
        end)

      _ ->
        nil
    end

    apply_impl(left, :join, [right, opts[:on], opts[:how]])
  end

  defp find_overlapping_cols(left_cols, right_cols) do
    left_cols = MapSet.new(left_cols)
    right_cols = MapSet.new(right_cols)
    left_cols |> MapSet.intersection(right_cols) |> MapSet.to_list()
  end

  @doc """
  Combine two or more dataframes row-wise (stack).

  Column names and dtypes must match exactly.

  ## Examples

      iex> df1 = Explorer.DataFrame.from_columns(x: [1, 2, 3], y: ["a", "b", "c"])
      iex> df2 = Explorer.DataFrame.from_columns(x: [4, 5, 6], y: ["d", "e", "f"])
      iex> Explorer.DataFrame.concat_rows([df1, df2])
      #Explorer.DataFrame<
        [rows: 6, columns: 2]
        x integer [1, 2, 3, 4, 5, "..."]
        y string ["a", "b", "c", "d", "e", "..."]
      >
  """
  def concat_rows([%DataFrame{} = h | t] = dfs) do
    key = Map.new(Enum.zip(names(h), dtypes(h)))

    for df <- t, key != Map.new(Enum.zip(names(df), dtypes(df))) do
      raise ArgumentError, "columns and dtypes must be identical for all dataframes"
    end

    apply_impl(dfs, :concat_rows)
  end

  @doc """
  Combine two dataframes row-wise.

  `concat_rows(df1, df2)` is equivalent to `concat_rows([df1, df2])`.
  """
  def concat_rows(%DataFrame{} = df1, %DataFrame{} = df2), do: concat_rows([df1, df2])
  def concat_rows(%DataFrame{} = df, [%DataFrame{} | _] = dfs), do: concat_rows([df | dfs])

  # Groups
  @doc """
  Group the dataframe by one or more variables.

  When the dataframe has grouping variables, operations are performed per group.
  `Explorer.DataFrame.ungroup/2` removes grouping.

  ## Examples

    You can group by a single variable:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.group_by(df, "country")
      #Explorer.DataFrame<
        [rows: 1094, columns: 10, groups: ["country"]]
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

    Or you can group by multiple:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.group_by(df, ["country", "year"])
      #Explorer.DataFrame<
        [rows: 1094, columns: 10, groups: ["country", "year"]]
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
  """
  @spec group_by(df :: DataFrame.t(), groups_or_group :: [String.t()] | String.t()) ::
          DataFrame.t()
  def group_by(df, groups) when is_list(groups) do
    names = names(df)
    Enum.each(groups, fn name -> maybe_raise_column_not_found(names, name) end)

    apply_impl(df, :group_by, [groups])
  end

  def group_by(df, group) when is_binary(group), do: group_by(df, [group])

  @doc """
  Removes grouping variables.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> df = Explorer.DataFrame.group_by(df, ["country", "year"])
      iex> Explorer.DataFrame.ungroup(df, ["country"])
      #Explorer.DataFrame<
        [rows: 1094, columns: 10, groups: ["year"]]
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
  """
  @spec ungroup(df :: DataFrame.t(), groups_or_group :: [String.t()] | String.t()) ::
          DataFrame.t()
  def ungroup(df, groups \\ [])

  def ungroup(df, groups) when is_list(groups) do
    current_groups = groups(df)

    Enum.each(groups, fn group ->
      if group not in current_groups,
        do:
          raise(
            ArgumentError,
            "could not find #{group} in current groups (#{current_groups})"
          )
    end)

    apply_impl(df, :ungroup, [groups])
  end

  def ungroup(df, group) when is_binary(group), do: ungroup(df, [group])

  @supported_aggs ~w[min max sum mean median first last count n_unique]a

  @doc """
  Summarise each group to a single row.

  Implicitly ungroups.

  ## Supported operations

    The following aggregations may be performed:

    * `:min` - Take the minimum value within the group. See `Explorer.Series.min/1`.
    * `:max` - Take the maximum value within the group. See `Explorer.Series.max/1`.
    * `:sum` - Take the sum of the series within the group. See `Explorer.Series.sum/1`.
    * `:mean` - Take the mean of the series within the group. See `Explorer.Series.mean/1`.
    * `:median` - Take the median of the series within the group. See `Explorer.Series.median/1`.
    * `:first` - Take the first value within the group. See `Explorer.Series.first/1`.
    * `:last` - Take the last value within the group. See `Explorer.Series.last/1`.
    * `:count` - Count the number of rows per group.
    * `:n_unique` - Count the number of unique rows per group.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> df |> Explorer.DataFrame.group_by("year") |> Explorer.DataFrame.summarise(total: [:max, :min], country: [:n_unique])
      #Explorer.DataFrame<
        [rows: 5, columns: 4]
        year integer [2010, 2011, 2012, 2013, 2014]
        country_n_unique integer [217, 217, 220, 220, 220]
        total_max integer [2393248, 2654360, 2734817, 2797384, 2806634]
        total_min integer [1, 2, 2, 2, 3]
      >
  """
  @spec summarise(df :: DataFrame.t(), with_columns :: Keyword.t() | map()) :: DataFrame.t()
  def summarise(%DataFrame{groups: []}, _),
    do:
      raise(
        ArgumentError,
        "dataframe must be grouped in order to perform summarisation"
      )

  def summarise(df, with_columns) when is_map(with_columns) do
    with_columns = Enum.reduce(with_columns, %{}, &summarise_reducer(&1, &2, df))
    columns = names(df)

    Enum.each(with_columns, fn {name, values} ->
      maybe_raise_column_not_found(columns, name)

      unless values |> MapSet.new() |> MapSet.subset?(MapSet.new(@supported_aggs)) do
        unsupported = values |> MapSet.difference(MapSet.new(@supported_aggs)) |> MapSet.to_list()
        raise ArgumentError, "found unsupported aggregations #{inspect(unsupported)}"
      end
    end)

    apply_impl(df, :summarise, [with_columns])
  end

  def summarise(df, with_columns) when is_list(with_columns) do
    if not Keyword.keyword?(with_columns),
      do: raise(ArgumentError, "expected second argument to be a keyword list")

    with_columns
    |> Enum.reduce(%{}, fn {colname, value}, acc ->
      Map.put(acc, Atom.to_string(colname), value)
    end)
    |> then(&summarise(df, &1))
  end

  defp summarise_reducer({colname, value}, acc, %DataFrame{} = _df)
       when is_binary(colname) and is_map(acc) and is_list(value),
       do: Map.put(acc, colname, value)

  defp summarise_reducer({colname, value}, acc, %DataFrame{} = df)
       when is_atom(colname) and is_map(acc) and is_list(value),
       do: mutate_reducer({Atom.to_string(colname), value}, acc, df)

  @doc """
  Display the DataFrame in a tabular fashion.

  ## Examples

     df = Explorer.Datasets.iris()
     Explorer.DataFrame.table(df)
  """
  def table(df, nrow \\ 5) when nrow >= 0 do
    {rows, cols} = shape(df)
    headers = names(df)

    df = slice(df, 0, nrow)

    types =
      df
      |> dtypes()
      |> Enum.map(&"\n<#{Atom.to_string(&1)}>")

    values =
      headers
      |> Enum.map(&Series.to_list(df[&1]))
      |> Enum.zip_with(& &1)

    name_type = Enum.zip_with(headers, types, fn x, y -> x <> y end)

    TableRex.Table.new()
    |> TableRex.Table.put_title("Explorer DataFrame: [rows: #{rows}, columns: #{cols}]")
    |> TableRex.Table.put_header(name_type)
    |> TableRex.Table.put_header_meta(0..cols, align: :center)
    |> TableRex.Table.add_rows(values)
    |> TableRex.Table.render!(
      header_separator_symbol: "=",
      horizontal_style: :all
    )
    |> IO.puts()
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

  defp maybe_raise_column_not_found(names, name) do
    if name not in names,
      do:
        raise(
          ArgumentError,
          List.to_string(["could not find column name \"#{name}\""] ++ did_you_mean(name, names))
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
