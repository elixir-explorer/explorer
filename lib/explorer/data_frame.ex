defmodule Explorer.DataFrame do
  @moduledoc """
  The DataFrame struct and API.

  Dataframes are two-dimensional tabular data structures similar to a spreadsheet.
  For example, the Iris dataset:

      iex> Explorer.Datasets.iris()
      #Explorer.DataFrame<
        Polars[150 x 5]
        sepal_length float [5.1, 4.9, 4.7, 4.6, 5.0, ...]
        sepal_width float [3.5, 3.0, 3.2, 3.1, 3.6, ...]
        petal_length float [1.4, 1.4, 1.3, 1.5, 1.4, ...]
        petal_width float [0.2, 0.2, 0.2, 0.2, 0.2, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
      >

  This dataframe has 150 rows and five columns. Each column is an `Explorer.Series`
  of the same size (150):

      iex> df = Explorer.Datasets.iris()
      iex> df["sepal_length"]
      #Explorer.Series<
        Polars[150]
        float [5.1, 4.9, 4.7, 4.6, 5.0, 5.4, 4.6, 5.0, 4.4, 4.9, 5.4, 4.8, 4.8, 4.3, 5.8, 5.7, 5.4, 5.1, 5.7, 5.1, 5.4, 5.1, 4.6, 5.1, 4.8, 5.0, 5.0, 5.2, 5.2, 4.7, 4.8, 5.4, 5.2, 5.5, 4.9, 5.0, 5.5, 4.9, 4.4, 5.1, 5.0, 4.5, 4.4, 5.0, 5.1, 4.8, 5.1, 4.6, 5.3, 5.0, ...]
      >

  ## Creating dataframes

  Dataframes can be created from normal Elixir terms. The main way you might do this is
  with the `new/1` function. For example:

      iex> Explorer.DataFrame.new(a: ["a", "b"], b: [1, 2])
      #Explorer.DataFrame<
        Polars[2 x 2]
        a string ["a", "b"]
        b integer [1, 2]
      >

  Or with a list of maps:

      iex> Explorer.DataFrame.new([%{"col1" => "a", "col2" => 1}, %{"col1" => "b", "col2" => 2}])
      #Explorer.DataFrame<
        Polars[2 x 2]
        col1 string ["a", "b"]
        col2 integer [1, 2]
      >

  ## Verbs

  Explorer uses the idea of a consistent set of SQL-like `verbs` like
  [`dplyr`](https://dplyr.tidyverse.org) which can help solve common
  data manipulation challenges. These are split into single table verbs,
  multiple table verbs, and row-based verbs:

  ### Single table verbs

  Single table verbs are (unsurprisingly) used for manipulating a single dataframe.
  Those operations typically driven by column names. These are:

  - `select/2` for picking columns and `discard/2` to discard them
  - `filter/2` for picking rows based on predicates
  - `mutate/2` for adding or replacing columns that are functions of existing columns
  - `arrange/2` for changing the ordering of rows
  - `distinct/2` for picking unique rows
  - `summarise/2` for reducing multiple rows down to a single summary
  - `pivot_longer/3` and `pivot_wider/4` for massaging dataframes into longer or
    wider forms, respectively

  Each of these combine with `Explorer.DataFrame.group_by/2` for operating by group.

  ### Multiple table verbs

  Multiple table verbs are used for combining tables. These are:

  - `join/3` for performing SQL-like joins
  - `concat_columns/1` for horizontally "stacking" dataframes
  - `concat_rows/1` for vertically "stacking" dataframes

  ### Row-based verbs

  Those operations are driven by the row index. These are:

  - `head/2` for picking the first rows
  - `tail/2` for picking the last rows
  - `slice/2` for slicing the dataframe by row indexes or a range
  - `slice/3` for slicing a section by an offset
  - `sample/2` for sampling the data-frame by row

  ## IO operations

  Explorer supports reading and writing of:

  - delimited files (such as CSV or TSV)
  - [Parquet](https://databricks.com/glossary/what-is-parquet)
  - [Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format)
  - [Arrow Streaming IPC](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
  - [Newline Delimited JSON](http://ndjson.org)
  - Databases via `Adbc` in `from_query/3`

  The convention Explorer uses is to have `from_*` and `to_*` functions to read and write
  to files in the formats above. `load_*` and `dump_*` versions are also available to read
  and write those formats directly in memory.

  Files can be fetched from local or remote file system, such as S3, using the following formats:

      # path to a file in disk
      Explorer.DataFrame.from_parquet("/path/to/file.parquet")

      # path to a URL schema (with optional configuration)
      Explorer.DataFrame.from_parquet("s3://bucket/file.parquet", config: FSS.S3.config_from_system_env())

      # it's possible to configure using keyword lists
      Explorer.DataFrame.from_parquet("s3://bucket/file.parquet", config: [access_key_id: "my-key", secret_access_key: "my-secret"])

      # a FSS entry (it already includes its config)
      Explorer.DataFrame.from_parquet(FSS.S3.parse("s3://bucket/file.parquet"))

  The `:config` option of `from_*` functions is only required if the filename is a path
  to a remote resource. In case it's a FSS entry, the requirement is that the config is passed
  inside the entry struct.

  ## Selecting columns and access

  Several functions in this module, such as `select/2`, `discard/2`, `drop_nil/2`, and so
  forth accept a single or multiple columns as arguments. The columns can be specified in
  a variety of formats, which we describe below.

  `Explorer.DataFrame` also implements the `Access` behaviour (also known as the brackets
  syntax). This should be familiar for users coming from other language with dataframes
  such as R or Python. For example:

      iex> df = Explorer.Datasets.wine()
      iex> df["class"]
      #Explorer.Series<
        Polars[178]
        integer [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, ...]
      >

  Accessing the dataframe with a column name either as a string or an atom, will return
  the column. You can also pass an integer representing the column order:

      iex> df = Explorer.Datasets.wine()
      iex> df[0]
      #Explorer.Series<
        Polars[178]
        integer [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, ...]
      >

  You can also pass a list, a range, or a regex to return a dataframe matching
  the given data type. For example, by passing a list:

      iex> df = Explorer.Datasets.wine()
      iex> df[["class", "hue"]]
      #Explorer.DataFrame<
        Polars[178 x 2]
        class integer [1, 1, 1, 1, 1, ...]
        hue float [1.04, 1.05, 1.03, 0.86, 1.04, ...]
      >

  Or a range for the given positions:

      iex> df = Explorer.Datasets.wine()
      iex> df[0..2]
      #Explorer.DataFrame<
        Polars[178 x 3]
        class integer [1, 1, 1, 1, 1, ...]
        alcohol float [14.23, 13.2, 13.16, 14.37, 13.24, ...]
        malic_acid float [1.71, 1.78, 2.36, 1.95, 2.59, ...]
      >

  Or a regex to keep only columns matching a given pattern:

      iex> df = Explorer.Datasets.wine()
      iex> df[~r/(class|hue)/]
      #Explorer.DataFrame<
        Polars[178 x 2]
        class integer [1, 1, 1, 1, 1, ...]
        hue float [1.04, 1.05, 1.03, 0.86, 1.04, ...]
      >

  Given you can also access a series using its index, you can use
  multiple accesses to select a column and row at the same time:

      iex> df = Explorer.Datasets.wine()
      iex> df["class"][3]
      1

  """

  alias __MODULE__, as: DataFrame
  alias Explorer.Series
  alias Explorer.Shared
  alias Explorer.Backend.LazySeries

  alias FSS.Local
  alias FSS.S3

  @valid_dtypes Explorer.Shared.dtypes()

  @enforce_keys [:data, :groups, :names, :dtypes]
  defstruct [:data, :groups, :names, :dtypes]

  @typedoc """
  Represents a column name as atom or string.
  """
  @type column_name :: atom() | String.t()

  @typedoc """
  Represents a column name or its index.
  """
  @type column :: column_name() | non_neg_integer()

  @typedoc """
  Represents multiple columns.

  The columns may be specified as one of:

    * a list of columns indexes or names as atoms and strings

    * a range

    * a regex that keeps only the names matching the regex

    * a one-arity function that receives column names and returns
      true for column names to keep

    * a two-arity function that receives column names and types and
      returns true for column names to keep

  """
  @type columns ::
          [column]
          | Range.t()
          | (String.t() -> boolean())
          | (String.t(), Explorer.Series.dtype() -> boolean())

  @typedoc """
  Represents multiple column names as atoms or strings.
  """
  @type column_names :: [column_name]

  @typedoc """
  Represents a column pair where the value is a column name or
  a column index, and the value is of type `value`.
  """
  @type column_pairs(value) :: [{column(), value}] | %{column() => value}

  @typedoc """
  Represents a filesystem entry, that can be local or S3.
  """
  @type fs_entry :: Local.Entry.t() | S3.Entry.t()

  @typedoc """
  Represents a dataframe.
  """
  @type t :: %DataFrame{
          data: Explorer.Backend.DataFrame.t(),
          groups: [String.t()],
          names: [String.t()],
          dtypes: %{String.t() => Explorer.Series.dtype()}
        }

  @default_infer_schema_length 1000
  @default_sample_nrows 5

  # Guards and helpers for columns

  defguardp is_column(column) when is_binary(column) or is_atom(column) or is_integer(column)
  defguardp is_column_name(column) when is_binary(column) or is_atom(column)
  defguardp is_column_pairs(columns) when is_list(columns) or is_map(columns)

  # Normalize a column name to string
  defp to_column_name(column) when is_binary(column), do: column
  defp to_column_name(column) when is_atom(column), do: Atom.to_string(column)

  # Normalize pairs of `{column, value}` where value can be anything.
  # The `column` is only validated if it's an integer. We check that the index is present.
  # The function allows to change the `value` for each pair.
  defp to_column_pairs(df, pairs, value_fun)
       when is_column_pairs(pairs) and is_function(value_fun, 1) do
    existing_columns = df.names

    pairs
    |> Enum.map_reduce(nil, fn
      {column, value}, maybe_map when is_integer(column) ->
        map = maybe_map || column_index_map(existing_columns)
        existing_column = fetch_column_at!(map, column)
        {{existing_column, value_fun.(value)}, map}

      {column, value}, maybe_map when is_atom(column) ->
        column = Atom.to_string(column)
        {{column, value_fun.(value)}, maybe_map}

      {column, value}, maybe_map when is_binary(column) ->
        {{column, value_fun.(value)}, maybe_map}
    end)
    |> then(fn {pairs, _} -> pairs end)
  end

  defp fetch_column_at!(map, index) do
    normalized = if index < 0, do: index + map_size(map), else: index

    case map do
      %{^normalized => column} -> column
      %{} -> raise ArgumentError, "no column exists at index #{index}"
    end
  end

  defp column_index_map(names),
    do: for({name, idx} <- Enum.with_index(names), into: %{}, do: {idx, name})

  # Normalizes column names and raise if column does not exist.
  defp to_existing_columns(df, columns) do
    Explorer.Shared.to_existing_columns(df, columns)
  end

  # Normalizes the "columns" option for some IO operations.
  defp to_columns_for_io(nil), do: nil

  defp to_columns_for_io([h | _] = columns) when is_integer(h) do
    if Enum.all?(columns, &is_integer/1) do
      columns
    else
      raise ArgumentError,
            "expected :columns to be a list of only integers, only atoms, or only binaries, " <>
              "got: #{inspect(columns)}"
    end
  end

  defp to_columns_for_io([h | _] = columns) when is_column_name(h),
    do: Enum.map(columns, &to_column_name/1)

  defp to_columns_for_io(other) do
    raise ArgumentError,
          "expected :columns to be a list of only integers, only atoms, or only binaries, " <>
            "got: #{inspect(other)}"
  end

  defp check_dtypes!(dtypes) do
    Map.new(dtypes, fn
      {key, value} when is_atom(key) ->
        {Atom.to_string(key), check_dtype!(key, value)}

      {key, value} when is_binary(key) ->
        {key, check_dtype!(key, value)}

      _ ->
        raise ArgumentError,
              "dtypes must be a list/map of column names as keys and types as values, " <>
                "where the keys are atoms or binaries. Got: #{inspect(dtypes)}"
    end)
  end

  defp check_dtype!(_key, value) when value in @valid_dtypes, do: value

  defp check_dtype!(key, value) do
    raise ArgumentError,
          "invalid dtype #{inspect(value)} for #{inspect(key)} (expected one of #{inspect(@valid_dtypes)})"
  end

  # Access

  @behaviour Access
  @compile {:no_warn_undefined, Nx}

  @impl true
  def fetch(df, column) when is_column(column) do
    {:ok, pull(df, column)}
  end

  def fetch(df, columns) do
    columns = to_existing_columns(df, columns)

    {:ok, select(df, columns)}
  end

  @impl true
  def pop(df, column) when is_column(column) do
    [column] = to_existing_columns(df, [column])
    {pull_existing(df, column), discard(df, [column])}
  end

  def pop(df, columns) do
    columns = to_existing_columns(df, columns)
    {select(df, columns), discard(df, columns)}
  end

  # Notice that the resultant series is going to have the `:name` field
  # equals to the column name.
  @impl true
  def get_and_update(df, column, fun) when is_column(column) do
    [column] = to_existing_columns(df, [column])
    value = pull_existing(df, column)
    {current_value, new_value} = fun.(value)
    new_df = put(df, column, new_value)
    {%{current_value | name: column}, new_df}
  end

  # IO

  @doc """
  Reads data from a query.

  `conn` must be a `Adbc.Connection` process. `sql` is a string representing
  the sql query and `params` is the list of query parameters. See `Adbc`
  for more information.

  ## Example

  In order to read data from a database, you must list `:adbc` as a dependency,
  download the relevant driver, and start both database and connection processes
  in your supervision tree.

  First, add `:adbc` as a dependency in your `mix.exs`:

      {:adbc, "~> 0.1"}

  Now, in your config/config.exs, configure the drivers you are going to use
  (see `Adbc` module docs for more information on supported drivers):

      config :adbc, :drivers, [:sqlite]

  If you are using a notebook or scripting, you can also use `Adbc.download_driver!/1`
  to dynamically download one.

  Then start the database and the relevant connection processes in your
  supervision tree:

      children = [
        {Adbc.Database,
         driver: :sqlite,
         process_options: [name: MyApp.DB]},
        {Adbc.Connection,
         database: MyApp.DB,
         process_options: [name: MyApp.Conn]}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  In a notebook, the above would look like this:

      db = Kino.start_child!({Adbc.Database, driver: :sqlite})
      conn = Kino.start_child!({Adbc.Connection, database: db})

  And now you can make queries with:

      # For named connections
      {:ok, _} = Explorer.DataFrame.from_query(MyApp.Conn, "SELECT 123")

      # When using the conn PID directly
      {:ok, _} = Explorer.DataFrame.from_query(conn, "SELECT 123")

  ## Options

    * `:backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.Backend.get/0`.

    * `:lazy` - force the results into the lazy version of the current backend.
  """
  @doc type: :io
  @spec from_query(
          Adbc.Connection.t(),
          query :: String.t(),
          params :: list(term),
          opts :: Keyword.t()
        ) ::
          {:ok, DataFrame.t()} | {:error, Exception.t()}
  def from_query(conn, query, params, opts \\ [])

  if Code.ensure_loaded?(Adbc) do
    def from_query(conn, query, params, opts)
        when is_binary(query) and is_list(params) and is_list(opts) do
      backend = backend_from_options!(opts)
      backend.from_query(conn, query, params)
    end
  else
    def from_query(_conn, _query, _params, _opts) do
      raise "you must install :adbc as a dependency in order to use from_query/3"
    end
  end

  @doc """
  Similar to `from_query/4` but raises if there is an error.
  """
  @doc type: :io
  @spec from_query!(
          Adbc.Connection.t(),
          query :: String.t(),
          params :: list(term),
          opts :: Keyword.t()
        ) ::
          DataFrame.t()
  def from_query!(conn, query, params, opts \\ []) do
    case from_query(conn, query, params, opts) do
      {:ok, df} -> df
      {:error, error} -> raise error
    end
  end

  @doc """
  Reads a delimited file into a dataframe.

  It accepts a filename that can be a local file, a "s3://" schema, or
  a `FSS` entry like `FSS.S3.Entry`.

  If the CSV is compressed, it is automatically decompressed.

  ## Options

    * `:delimiter` - A single character used to separate fields within a record. (default: `","`)

    * `:dtypes` - A list/map of `{"column_name", dtype}` tuples. Any non-specified column has its type
      imputed from the first 1000 rows. (default: `[]`)

    * `:header` - Does the file have a header of column names as the first row or not? (default: `true`)

    * `:max_rows` - Maximum number of lines to read. (default: `nil`)

    * `:null_character` - The string that should be interpreted as a nil value. (default: `"NA"`)

    * `:skip_rows` - The number of lines to skip at the beginning of the file. (default: `0`)

    * `:columns` - A list of column names or indexes to keep.
      If present, only these columns are read into the dataframe. (default: `nil`)

    * `:infer_schema_length` Maximum number of rows read for schema inference.
      Setting this to nil will do a full table scan and will be slow (default: `1000`).

    * `:parse_dates` - Automatically try to parse dates/ datetimes and time.
      If parsing fails, columns remain of dtype `string`

    * `:eol_delimiter` - A single character used to represent new lines. (default: `"\n"`)

    * `:config` - An optional config struct or map, normally associated with remote
      file systems. See [IO section](#module-io-operations) for more details. (default: `nil`)

    * `:backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.Backend.get/0`.

    * `:lazy` - force the results into the lazy version of the current backend.

  """
  @doc type: :io
  @spec from_csv(filename :: String.t() | fs_entry(), opts :: Keyword.t()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def from_csv(filename, opts \\ []) do
    {backend_opts, opts} = Keyword.split(opts, [:backend, :lazy])

    opts =
      Keyword.validate!(opts,
        config: nil,
        delimiter: ",",
        dtypes: [],
        encoding: "utf8",
        header: true,
        max_rows: nil,
        null_character: "NA",
        skip_rows: 0,
        columns: nil,
        infer_schema_length: @default_infer_schema_length,
        parse_dates: false,
        eol_delimiter: nil
      )

    backend = backend_from_options!(backend_opts)

    with {:ok, entry} <- normalise_entry(filename, opts[:config]) do
      backend.from_csv(
        entry,
        check_dtypes!(opts[:dtypes]),
        opts[:delimiter],
        opts[:null_character],
        opts[:skip_rows],
        opts[:header],
        opts[:encoding],
        opts[:max_rows],
        to_columns_for_io(opts[:columns]),
        opts[:infer_schema_length],
        opts[:parse_dates],
        opts[:eol_delimiter]
      )
    end
  end

  @doc """
  Similar to `from_csv/2` but raises if there is a problem reading the CSV.
  """
  @doc type: :io
  @spec from_csv!(filename :: String.t(), opts :: Keyword.t()) :: DataFrame.t()
  def from_csv!(filename, opts \\ []) do
    case from_csv(filename, opts) do
      {:ok, df} ->
        df

      {:error, %module{} = e} when module in [ArgumentError, RuntimeError] ->
        raise module, "from_csv failed: #{inspect(e.message)}"

      {:error, error} ->
        raise "from_csv failed: #{inspect(error)}"
    end
  end

  @doc """
  Reads a representation of a CSV file into a dataframe.

  If the CSV is compressed, it is automatically decompressed.

  ## Options

    * `:delimiter` - A single character used to separate fields within a record. (default: `","`)
    * `:dtypes` - A list/map of `{"column_name", dtype}` tuples. Any non-specified column has its type
      imputed from the first 1000 rows. (default: `[]`)
    * `:header` - Does the file have a header of column names as the first row or not? (default: `true`)
    * `:max_rows` - Maximum number of lines to read. (default: `nil`)
    * `:null_character` - The string that should be interpreted as a nil value. (default: `"NA"`)
    * `:skip_rows` - The number of lines to skip at the beginning of the file. (default: `0`)
    * `:columns` - A list of column names or indexes to keep. If present, only these columns are read into the dataframe. (default: `nil`)
    * `:infer_schema_length` Maximum number of rows read for schema inference. Setting this to nil will do a full table scan and will be slow (default: `1000`).
    * `:parse_dates` - Automatically try to parse dates/ datetimes and time. If parsing fails, columns remain of dtype `string`
    * `:eol_delimiter` - A single character used to represent new lines. (default: `"\n"`)
    * `:backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.Backend.get/0`.
    * `:lazy` - force the results into the lazy version of the current backend.
  """
  @doc type: :io
  @spec load_csv(contents :: String.t(), opts :: Keyword.t()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def load_csv(contents, opts \\ []) do
    {backend_opts, opts} = Keyword.split(opts, [:backend, :lazy])

    opts =
      Keyword.validate!(opts,
        delimiter: ",",
        dtypes: [],
        encoding: "utf8",
        header: true,
        max_rows: nil,
        null_character: "NA",
        skip_rows: 0,
        columns: nil,
        infer_schema_length: @default_infer_schema_length,
        parse_dates: false,
        eol_delimiter: nil
      )

    backend = backend_from_options!(backend_opts)

    backend.load_csv(
      contents,
      check_dtypes!(opts[:dtypes]),
      opts[:delimiter],
      opts[:null_character],
      opts[:skip_rows],
      opts[:header],
      opts[:encoding],
      opts[:max_rows],
      to_columns_for_io(opts[:columns]),
      opts[:infer_schema_length],
      opts[:parse_dates],
      opts[:eol_delimiter]
    )
  end

  @doc """
  Similar to `load_csv/2` but raises if there is a problem reading the CSV.
  """
  @doc type: :io
  @spec load_csv!(contents :: String.t(), opts :: Keyword.t()) :: DataFrame.t()
  def load_csv!(contents, opts \\ []) do
    case load_csv(contents, opts) do
      {:ok, df} -> df
      {:error, error} -> raise "load_csv failed: #{inspect(error)}"
    end
  end

  @doc """
  Reads a parquet file into a dataframe.

  It accepts a filename that can be a local file, a "s3://" schema, or
  a `FSS` entry like `FSS.S3.Entry`.

  ## Options

    * `:max_rows` - Maximum number of lines to read. (default: `nil`)

    * `:columns` - A list of column names or indexes to keep. If present,
      only these columns are read into the dataframe. (default: `nil`)

    * `:config` - An optional config struct or map, normally associated with remote
      file systems. See [IO section](#module-io-operations) for more details. (default: `nil`)

    * `:backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.Backend.get/0`.

    * `:lazy` - force the results into the lazy version of the current backend.
  """
  @doc type: :io
  @spec from_parquet(filename :: String.t() | fs_entry(), opts :: Keyword.t()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def from_parquet(filename, opts \\ []) do
    {backend_opts, opts} = Keyword.split(opts, [:backend, :lazy])

    opts =
      Keyword.validate!(opts,
        max_rows: nil,
        columns: nil,
        config: nil
      )

    backend = backend_from_options!(backend_opts)

    with {:ok, entry} <- normalise_entry(filename, opts[:config]) do
      backend.from_parquet(
        entry,
        opts[:max_rows],
        to_columns_for_io(opts[:columns])
      )
    end
  end

  defp normalise_entry(%_{} = entry, config) when config != nil do
    {:error,
     ArgumentError.message(
       ":config key is only supported when the argument is a string, got #{inspect(entry)} with config #{inspect(config)}"
     )}
  end

  defp normalise_entry(%Local.Entry{} = entry, nil), do: {:ok, entry}
  defp normalise_entry(%S3.Entry{config: %S3.Config{}} = entry, nil), do: {:ok, entry}

  defp normalise_entry("s3://" <> _rest = entry, config) do
    S3.Entry.parse(entry, config: config)
  end

  defp normalise_entry("file://" <> path, _config), do: {:ok, %Local.Entry{path: path}}

  defp normalise_entry(filepath, _config) when is_binary(filepath) do
    {:ok, %Local.Entry{path: filepath}}
  end

  @doc """
  Similar to `from_parquet/2` but raises if there is a problem reading the Parquet file.
  """
  @doc type: :io
  @spec from_parquet!(filename :: String.t(), opts :: Keyword.t()) :: DataFrame.t()
  def from_parquet!(filename, opts \\ []) do
    case from_parquet(filename, opts) do
      {:ok, df} ->
        df

      {:error, %module{} = e} when module in [ArgumentError, RuntimeError] ->
        raise module, "from_parquet failed: #{inspect(e.message)}"

      {:error, error} ->
        raise "from_parquet failed: #{inspect(error)}"
    end
  end

  @doc """
  Writes a dataframe to a parquet file.

  Groups are ignored if the dataframe is using any.

  ## Options

    * `:compression` - The compression algorithm to use when writing files.
      Where a compression level is available, this can be passed as a tuple,
      such as `{:zstd, 3}`. Supported options are:

        * `nil` (uncompressed, default)
        * `:snappy`
        * `:gzip` (with levels 1-9)
        * `:brotli` (with levels 1-11)
        * `:zstd` (with levels -7-22)
        * `:lz4raw`.

    * `:streaming` - Tells the backend if it should use streaming, which means
      that the dataframe is not loaded to the memory at once, and instead it is
      written in chunks from a lazy dataframe.

      This option has no effect on eager - the default - dataframes.
      It defaults to `true`.

  """
  @doc type: :io
  @spec to_parquet(df :: DataFrame.t(), filename :: String.t(), opts :: Keyword.t()) ::
          :ok | {:error, term()}
  def to_parquet(df, filename, opts \\ []) do
    opts = Keyword.validate!(opts, compression: nil, streaming: true)
    compression = parquet_compression(opts[:compression])
    Shared.apply_impl(df, :to_parquet, [filename, compression, opts[:streaming]])
  end

  defp parquet_compression(nil), do: {nil, nil}

  defp parquet_compression(algorithm) when algorithm in ~w(snappy gzip brotli zstd lz4raw)a do
    {algorithm, nil}
  end

  for {algorithm, min, max} <- [{:gzip, 1, 9}, {:brotli, 1, 11}, {:zstd, -7, 22}] do
    defp parquet_compression({unquote(algorithm), level}) do
      if level in unquote(min)..unquote(max) or is_nil(level) do
        {unquote(algorithm), level}
      else
        raise ArgumentError,
              "#{unquote(algorithm)} compression level must be between #{unquote(min)} and #{unquote(max)} inclusive or nil, got #{level}"
      end
    end
  end

  defp parquet_compression(other) do
    raise ArgumentError, "unsupported :compression #{inspect(other)} for Parquet"
  end

  @doc """
  Similar to `to_parquet/3`, but raises in case of error.
  """
  @doc type: :io
  @spec to_parquet!(df :: DataFrame.t(), filename :: String.t()) :: :ok
  def to_parquet!(df, filename, opts \\ []) do
    case to_parquet(df, filename, opts) do
      :ok -> :ok
      {:error, error} -> raise "to_parquet failed: #{inspect(error)}"
    end
  end

  @doc """
  Writes a dataframe to a binary representation of a Parquet file.

  Groups are ignored if the dataframe is using any.

  ## Options

    * `:compression` - The compression algorithm to use when writing files.
      Where a compression level is available, this can be passed as a tuple,
      such as `{:zstd, 3}`. Supported options are:

        * `nil` (uncompressed, default)
        * `:snappy`
        * `:gzip` (with levels 1-9)
        * `:brotli` (with levels 1-11)
        * `:zstd` (with levels -7-22)
        * `:lz4raw`.

  """
  @doc type: :io
  @spec dump_parquet(df :: DataFrame.t(), opts :: Keyword.t()) ::
          {:ok, binary()} | {:error, term()}
  def dump_parquet(df, opts \\ []) do
    opts = Keyword.validate!(opts, compression: nil)
    compression = parquet_compression(opts[:compression])

    Shared.apply_impl(df, :dump_parquet, [compression])
  end

  @doc """
  Similar to `dump_parquet/2`, but raises in case of error.
  """
  @doc type: :io
  @spec dump_parquet!(df :: DataFrame.t(), opts :: Keyword.t()) :: binary()
  def dump_parquet!(df, opts \\ []) do
    case dump_parquet(df, opts) do
      {:ok, parquet} -> parquet
      {:error, error} -> raise "dump_parquet failed: #{inspect(error)}"
    end
  end

  @doc """
  Reads a binary representation of a parquet file into a dataframe.
  """
  @doc type: :io
  @spec load_parquet(contents :: binary(), opts :: Keyword.t()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def load_parquet(contents, opts \\ []) do
    backend = backend_from_options!(opts)
    backend.load_parquet(contents)
  end

  @doc """
  Similar to `load_parquet/2` but raises if there is a problem reading the Parquet file.
  """
  @doc type: :io
  @spec load_parquet!(contents :: binary(), opts :: Keyword.t()) :: DataFrame.t()
  def load_parquet!(contents, opts \\ []) do
    case load_parquet(contents, opts) do
      {:ok, df} -> df
      {:error, error} -> raise "load_parquet failed: #{inspect(error)}"
    end
  end

  @doc """
  Reads an IPC file into a dataframe.

  It accepts a filename that can be a local file, a "s3://" schema, or
  a `FSS` entry like `FSS.S3.Entry`.

  ## Options

    * `:columns` - List with the name or index of columns to be selected.
      Defaults to all columns.

    * `:config` - An optional config struct or map, normally associated with remote
      file systems. See [IO section](#module-io-operations) for more details. (default: `nil`)

    * `:backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.Backend.get/0`.

    * `:lazy` - force the results into the lazy version of the current backend.
  """
  @doc type: :io
  @spec from_ipc(filename :: String.t() | fs_entry(), opts :: Keyword.t()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def from_ipc(filename, opts \\ []) do
    {backend_opts, opts} = Keyword.split(opts, [:backend, :lazy])

    opts =
      Keyword.validate!(opts,
        columns: nil,
        config: nil
      )

    backend = backend_from_options!(backend_opts)

    with {:ok, entry} <- normalise_entry(filename, opts[:config]) do
      backend.from_ipc(
        entry,
        to_columns_for_io(opts[:columns])
      )
    end
  end

  @doc """
  Similar to `from_ipc/2` but raises if there is a problem reading the IPC file.
  """
  @doc type: :io
  @spec from_ipc!(filename :: String.t(), opts :: Keyword.t()) :: DataFrame.t()
  def from_ipc!(filename, opts \\ []) do
    case from_ipc(filename, opts) do
      {:ok, df} ->
        df

      {:error, %module{} = e} when module in [ArgumentError, RuntimeError] ->
        raise module, "from_ipc failed: #{inspect(e.message)}"

      {:error, error} ->
        raise "from_ipc failed: #{inspect(error)}"
    end
  end

  @doc """
  Writes a dataframe to an IPC file.

  Apache IPC is a language-agnostic columnar data structure that can be used to store dataframes.
  It excels as a format for quickly exchange data between different programming languages.

  Groups are ignored if the dataframe is using any.

  ## Options

    * `:compression` - The compression algorithm to use when writing files.
      Supported options are:

        * `nil` (uncompressed, default)
        * `:zstd`
        * `:lz4`.

    * `:streaming` - Tells the backend if it should use streaming, which means
      that the dataframe is not loaded to the memory at once, and instead it is
      written in chunks from a lazy dataframe.

      This option has no effect on eager - the default - dataframes.
      It defaults to `true`.

  """
  @doc type: :io
  @spec to_ipc(df :: DataFrame.t(), filename :: String.t(), opts :: Keyword.t()) ::
          :ok | {:error, term()}
  def to_ipc(df, filename, opts \\ []) do
    opts = Keyword.validate!(opts, compression: nil, streaming: true)
    compression = ipc_compression(opts[:compression])

    Shared.apply_impl(df, :to_ipc, [filename, compression, opts[:streaming]])
  end

  defp ipc_compression(nil), do: {nil, nil}
  defp ipc_compression(algorithm) when algorithm in ~w(zstd lz4)a, do: {algorithm, nil}

  defp ipc_compression(other),
    do: raise(ArgumentError, "unsupported :compression #{inspect(other)} for IPC")

  @doc """
  Similar to `to_ipc/3`, but raises in case of error.
  """
  @doc type: :io
  @spec to_ipc!(df :: DataFrame.t(), filename :: String.t(), opts :: Keyword.t()) :: :ok
  def to_ipc!(df, filename, opts \\ []) do
    case to_ipc(df, filename, opts) do
      :ok -> :ok
      {:error, error} -> raise "to_ipc failed: #{inspect(error)}"
    end
  end

  @doc """
  Writes a dataframe to a binary representation of an IPC file.

  Groups are ignored if the dataframe is using any.

  ## Options

    * `:compression` - The compression algorithm to use when writing files.
      Supported options are:

        * `nil` (uncompressed, default)
        * `:zstd`
        * `:lz4`.

  """
  @doc type: :io
  @spec dump_ipc(df :: DataFrame.t(), opts :: Keyword.t()) ::
          {:ok, binary()} | {:error, term()}
  def dump_ipc(df, opts \\ []) do
    opts = Keyword.validate!(opts, compression: nil)
    compression = ipc_compression(opts[:compression])

    Shared.apply_impl(df, :dump_ipc, [compression])
  end

  @doc """
  Similar to `dump_ipc/2`, but raises in case of error.
  """
  @doc type: :io
  @spec dump_ipc!(df :: DataFrame.t(), opts :: Keyword.t()) :: binary()
  def dump_ipc!(df, opts \\ []) do
    case dump_ipc(df, opts) do
      {:ok, ipc} -> ipc
      {:error, error} -> raise "dump_ipc failed: #{inspect(error)}"
    end
  end

  @doc """
  Reads a binary representing an IPC file into a dataframe.

  ## Options

    * `:columns` - List with the name or index of columns to be selected. Defaults to all columns.

    * `:backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.Backend.get/0`.

    * `:lazy` - force the results into the lazy version of the current backend.
  """
  @doc type: :io
  @spec load_ipc(contents :: binary(), opts :: Keyword.t()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def load_ipc(contents, opts \\ []) do
    {backend_opts, opts} = Keyword.split(opts, [:backend, :lazy])

    opts =
      Keyword.validate!(opts,
        columns: nil
      )

    backend = backend_from_options!(backend_opts)

    backend.load_ipc(
      contents,
      to_columns_for_io(opts[:columns])
    )
  end

  @doc """
  Similar to `load_ipc/2` but raises if there is a problem reading the IPC file.
  """
  @doc type: :io
  @spec load_ipc!(contents :: binary(), opts :: Keyword.t()) :: DataFrame.t()
  def load_ipc!(contents, opts \\ []) do
    case load_ipc(contents, opts) do
      {:ok, df} -> df
      {:error, error} -> raise "load_ipc failed: #{inspect(error)}"
    end
  end

  @doc """
  Reads an IPC Streaming file into a dataframe.

  It's possible to read from an IPC Streaming file using the lazy Polars
  backend, but the implementation is not truly lazy. We are going to read it
  first using the eager backend, and then convert the dataframe to lazy.

  ## Options

    * `:columns` - List with the name or index of columns to be selected.
      Defaults to all columns.

    * `:backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.Backend.get/0`.

    * `:lazy` - force the results into the lazy version of the current backend.

    * `:config` - An optional config struct or map, normally associated with remote
      file systems. See [IO section](#module-io-operations) for more details. (default: `nil`)

  """
  @doc type: :io
  @spec from_ipc_stream(filename :: String.t() | fs_entry()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def from_ipc_stream(filename, opts \\ []) do
    {backend_opts, opts} = Keyword.split(opts, [:backend, :lazy])

    opts = Keyword.validate!(opts, columns: nil, config: nil)
    backend = backend_from_options!(backend_opts)

    with {:ok, entry} <- normalise_entry(filename, opts[:config]) do
      backend.from_ipc_stream(
        entry,
        to_columns_for_io(opts[:columns])
      )
    end
  end

  @doc """
  Similar to `from_ipc_stream/2` but raises if there is a problem reading the IPC Stream file.
  """
  @doc type: :io
  @spec from_ipc_stream!(filename :: String.t() | fs_entry(), opts :: Keyword.t()) ::
          DataFrame.t()
  def from_ipc_stream!(filename, opts \\ []) do
    case from_ipc_stream(filename, opts) do
      {:ok, df} ->
        df

      {:error, %module{} = e} when module in [ArgumentError, RuntimeError] ->
        raise module, "from_ipc_stream failed: #{inspect(e.message)}"

      {:error, error} ->
        raise "from_ipc_stream failed: #{inspect(error)}"
    end
  end

  @doc """
  Writes a dataframe to an IPC Stream file.

  Arrow IPC Streams provide a streaming protocol or “format" for sending an arbitrary
  length sequence of record batches.
  The format must be processed from start to end, and does not support random access.

  ## Options

    * `:compression` - The compression algorithm to use when writing files.
      Supported options are:

        * `nil` (uncompressed, default)
        * `:zstd`
        * `:lz4`.

  """
  @doc type: :io
  @spec to_ipc_stream(df :: DataFrame.t(), filename :: String.t()) ::
          :ok | {:error, term()}
  def to_ipc_stream(df, filename, opts \\ []) do
    opts = Keyword.validate!(opts, compression: nil)
    compression = ipc_compression(opts[:compression])

    Shared.apply_impl(df, :to_ipc_stream, [filename, compression])
  end

  @doc """
  Similar to `to_ipc_stream/3`, but raises in case of error.
  """
  @doc type: :io
  @spec to_ipc_stream!(df :: DataFrame.t(), filename :: String.t(), opts :: Keyword.t()) :: :ok
  def to_ipc_stream!(df, filename, opts \\ []) do
    case to_ipc_stream(df, filename, opts) do
      :ok -> :ok
      {:error, error} -> raise "to_ipc_stream failed: #{inspect(error)}"
    end
  end

  @doc """
  Writes a dataframe to a binary representation of an IPC Stream file.

  Groups are ignored if the dataframe is using any.

  ## Options

    * `:compression` - The compression algorithm to use when writing files.
      Supported options are:

        * `nil` (uncompressed, default)
        * `:zstd`
        * `:lz4`.

  """
  @doc type: :io
  @spec dump_ipc_stream(df :: DataFrame.t(), opts :: Keyword.t()) ::
          {:ok, binary()} | {:error, term()}
  def dump_ipc_stream(df, opts \\ []) do
    opts = Keyword.validate!(opts, compression: nil)
    compression = ipc_compression(opts[:compression])

    Shared.apply_impl(df, :dump_ipc_stream, [compression])
  end

  @doc """
  Similar to `dump_ipc_stream/2`, but raises in case of error.
  """
  @doc type: :io
  @spec dump_ipc_stream!(df :: DataFrame.t(), opts :: Keyword.t()) :: binary()
  def dump_ipc_stream!(df, opts \\ []) do
    case dump_ipc_stream(df, opts) do
      {:ok, ipc} -> ipc
      {:error, error} -> raise "dump_ipc_stream failed: #{inspect(error)}"
    end
  end

  @doc """
  Reads a binary representing an IPC Stream file into a dataframe.

  ## Options

    * `:columns` - List with the name or index of columns to be selected. Defaults to all columns.

    * `:backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.Backend.get/0`.

    * `:lazy` - force the results into the lazy version of the current backend.
  """
  @doc type: :io
  @spec load_ipc_stream(contents :: binary(), opts :: Keyword.t()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def load_ipc_stream(contents, opts \\ []) do
    {backend_opts, opts} = Keyword.split(opts, [:backend, :lazy])

    opts =
      Keyword.validate!(opts,
        columns: nil
      )

    backend = backend_from_options!(backend_opts)

    backend.load_ipc_stream(
      contents,
      to_columns_for_io(opts[:columns])
    )
  end

  @doc """
  Similar to `load_ipc_stream/2` but raises if there is a problem.
  """
  @doc type: :io
  @spec load_ipc_stream!(contents :: binary(), opts :: Keyword.t()) :: DataFrame.t()
  def load_ipc_stream!(contents, opts \\ []) do
    case load_ipc_stream(contents, opts) do
      {:ok, df} -> df
      {:error, error} -> raise "load_ipc_stream failed: #{inspect(error)}"
    end
  end

  @doc """
  Writes a dataframe to a delimited file.

  Groups are ignored if the dataframe is using any.

  ## Options

    * `:header` - Should the column names be written as the first line of the file? (default: `true`)
    * `:delimiter` - A single character used to separate fields within a record. (default: `","`)
  """
  @doc type: :io
  @spec to_csv(df :: DataFrame.t(), filename :: String.t(), opts :: Keyword.t()) ::
          :ok | {:error, term()}
  def to_csv(df, filename, opts \\ []) do
    opts = Keyword.validate!(opts, header: true, delimiter: ",")
    Shared.apply_impl(df, :to_csv, [filename, opts[:header], opts[:delimiter]])
  end

  @doc """
  Similar to `to_csv/3` but raises if there is a problem reading the CSV.
  """
  @doc type: :io
  @spec to_csv!(df :: DataFrame.t(), filename :: String.t(), opts :: Keyword.t()) :: :ok
  def to_csv!(df, filename, opts \\ []) do
    case to_csv(df, filename, opts) do
      :ok -> :ok
      {:error, error} -> raise "to_csv failed: #{inspect(error)}"
    end
  end

  @doc """
  Writes a dataframe to a binary representation of a delimited file.

  ## Options

    * `:header` - Should the column names be written as the first line of the file? (default: `true`)
    * `:delimiter` - A single character used to separate fields within a record. (default: `","`)

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels() |> Explorer.DataFrame.head(2)
      iex> Explorer.DataFrame.dump_csv(df)
      {:ok, "year,country,total,solid_fuel,liquid_fuel,gas_fuel,cement,gas_flaring,per_capita,bunker_fuels\\n2010,AFGHANISTAN,2308,627,1601,74,5,0,0.08,9\\n2010,ALBANIA,1254,117,953,7,177,0,0.43,7\\n"}
  """
  @doc type: :io
  @spec dump_csv(df :: DataFrame.t(), opts :: Keyword.t()) :: {:ok, String.t()} | {:error, term()}
  def dump_csv(df, opts \\ []) do
    opts = Keyword.validate!(opts, header: true, delimiter: ",")
    Shared.apply_impl(df, :dump_csv, [opts[:header], opts[:delimiter]])
  end

  @doc """
  Similar to `dump_csv/2`, but raises in case of error.
  """
  @doc type: :io
  @spec dump_csv!(df :: DataFrame.t(), opts :: Keyword.t()) :: String.t()
  def dump_csv!(df, opts \\ []) do
    case dump_csv(df, opts) do
      {:ok, csv} -> csv
      {:error, error} -> raise "dump_csv failed: #{inspect(error)}"
    end
  end

  @doc """
  Read a file of JSON objects or lists separated by new lines

  ## Options

    * `:batch_size` - Sets the batch size for reading rows.
      This value may have significant impact in performance,
      so adjust it for your needs (default: `1000`).

    * `:infer_schema_length` - Maximum number of rows read for schema inference.
      Setting this to nil will do a full table scan and will be slow (default: `1000`).

    * `:backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.Backend.get/0`.

    * `:lazy` - force the results into the lazy version of the current backend.

    * `:config` - An optional config struct or map, normally associated with remote
      file systems. See [IO section](#module-io-operations) for more details. (default: `nil`)
  """
  @doc type: :io
  @spec from_ndjson(filename :: String.t() | fs_entry(), opts :: Keyword.t()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def from_ndjson(filename, opts \\ []) do
    {backend_opts, opts} = Keyword.split(opts, [:backend, :lazy])

    opts =
      Keyword.validate!(opts,
        config: nil,
        batch_size: 1000,
        infer_schema_length: @default_infer_schema_length
      )

    backend = backend_from_options!(backend_opts)

    with {:ok, entry} <- normalise_entry(filename, opts[:config]) do
      backend.from_ndjson(
        entry,
        opts[:infer_schema_length],
        opts[:batch_size]
      )
    end
  end

  @doc """
  Similar to `from_ndjson/2`, but raises in case of error.
  """
  @doc type: :io
  @spec from_ndjson!(filename :: String.t() | fs_entry(), opts :: Keyword.t()) ::
          DataFrame.t()
  def from_ndjson!(filename, opts \\ []) do
    case from_ndjson(filename, opts) do
      {:ok, df} ->
        df

      {:error, %module{} = e} when module in [ArgumentError, RuntimeError] ->
        raise module, "from_ndjson failed: #{inspect(e.message)}"

      {:error, error} ->
        raise "from_ndjson failed: #{inspect(error)}"
    end
  end

  @doc """
  Writes a dataframe to a ndjson file.

  Groups are ignored if the dataframe is using any.

  NDJSON are files that contains JSON files separated by new lines.
  They are often used as structured logs.
  """
  @doc type: :io
  @spec to_ndjson(df :: DataFrame.t(), filename :: String.t(), opts :: Keyword.t()) ::
          :ok | {:error, term()}
  def to_ndjson(df, filename, _opts \\ []) do
    Shared.apply_impl(df, :to_ndjson, [filename])
  end

  @doc """
  Similar to `to_ndjson/3`, but raises in case of error.
  """
  @doc type: :io
  @spec to_ndjson!(df :: DataFrame.t(), filename :: String.t(), opts :: Keyword.t()) :: :ok
  def to_ndjson!(df, filename, opts \\ []) do
    case to_ndjson(df, filename, opts) do
      :ok -> :ok
      {:error, error} -> raise "to_ndjson failed: #{inspect(error)}"
    end
  end

  @doc """
  Writes a dataframe to a binary representation of a NDJSON file.

  Groups are ignored if the dataframe is using any.

  ## Examples

      iex> df = Explorer.DataFrame.new(col_a: [1, 2], col_b: [5.1, 5.2])
      iex> Explorer.DataFrame.dump_ndjson(df)
      {:ok, ~s({"col_a":1,"col_b":5.1}\\n{"col_a":2,"col_b":5.2}\\n)}

  """
  @doc type: :io
  @spec dump_ndjson(df :: DataFrame.t()) :: {:ok, binary()} | {:error, term()}
  def dump_ndjson(df) do
    Shared.apply_impl(df, :dump_ndjson, [])
  end

  @doc """
  Similar to `dump_ndjson!/2`, but raises in case of error.
  """
  @doc type: :io
  @spec dump_ndjson!(df :: DataFrame.t()) :: binary()
  def dump_ndjson!(df) do
    case dump_ndjson(df) do
      {:ok, ndjson} -> ndjson
      {:error, error} -> raise "dump_ndjson failed: #{inspect(error)}"
    end
  end

  @doc """
  Reads a representation of a NDJSON file into a dataframe.

  ## Options

    * `:batch_size` - Sets the batch size for reading rows.
      This value may have significant impact in performance,
      so adjust it for your needs (default: `1000`).

    * `:infer_schema_length` - Maximum number of rows read for schema inference.
      Setting this to nil will do a full table scan and will be slow (default: `1000`).

    * `:backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.Backend.get/0`.

    * `:lazy` - force the results into the lazy version of the current backend.

  """
  @doc type: :io
  @spec load_ndjson(contents :: String.t(), opts :: Keyword.t()) ::
          {:ok, DataFrame.t()} | {:error, term()}
  def load_ndjson(contents, opts \\ []) do
    {backend_opts, opts} = Keyword.split(opts, [:backend, :lazy])

    opts =
      Keyword.validate!(opts,
        batch_size: 1000,
        infer_schema_length: @default_infer_schema_length
      )

    backend = backend_from_options!(backend_opts)

    backend.load_ndjson(
      contents,
      opts[:infer_schema_length],
      opts[:batch_size]
    )
  end

  @doc """
  Similar to `load_ndjson/2`, but raises in case of error.

  ## Examples

      iex> contents = ~s({"col_a":1,"col_b":5.1}\\n{"col_a":2,"col_b":5.2}\\n)
      iex> Explorer.DataFrame.load_ndjson!(contents)
      #Explorer.DataFrame<
        Polars[2 x 2]
        col_a integer [1, 2]
        col_b float [5.1, 5.2]
      >

  """
  @doc type: :io
  @spec load_ndjson!(contents :: String.t(), opts :: Keyword.t()) ::
          DataFrame.t()
  def load_ndjson!(contents, opts \\ []) do
    case load_ndjson(contents, opts) do
      {:ok, df} ->
        df

      {:error, %module{} = e} when module in [ArgumentError, RuntimeError] ->
        raise module, "load_ndjson failed: #{inspect(e.message)}"

      {:error, error} ->
        raise "load_ndjson failed: #{inspect(error)}"
    end
  end

  ## Conversion

  @doc """
  Converts the dataframe to the lazy version of the current backend.

  If already lazy, this is a noop.

  Converting a grouped dataframe should return a lazy dataframe with groups.
  """
  @doc type: :conversion
  @spec to_lazy(df :: DataFrame.t()) :: DataFrame.t()
  def to_lazy(df), do: Shared.apply_impl(df, :to_lazy)

  @doc """
  This collects the lazy data frame into an eager one, computing the query.

  If already eager, this is a noop.

  Collecting a grouped dataframe should return a grouped dataframe.
  """
  @doc type: :conversion
  @spec collect(df :: DataFrame.t()) :: {:ok, DataFrame.t()} | {:error, term()}
  def collect(df), do: Shared.apply_impl(df, :collect)

  @doc """
  Creates a new dataframe.

  It accepts any of:

    * a map or keyword list of string/atom keys and series as values
    * a map or keyword list of string/atom keys and tensors as values
    * any data structure adhering to the `Table.Reader` protocol

  ## Options

    * `:backend` - The Explorer backend to use. Defaults to the value returned by `Explorer.Backend.get/0`.
    * `:dtypes` - A list/map of `{column_name, dtype}` pairs. (default: `[]`)

  ## Examples

  ### From series

  Series can be given either as keyword lists or maps
  where the keys are the name and the values are series:

      iex> Explorer.DataFrame.new(%{
      ...>   floats: Explorer.Series.from_list([1.0, 2.0]),
      ...>   ints: Explorer.Series.from_list([1, nil])
      ...> })
      #Explorer.DataFrame<
        Polars[2 x 2]
        floats float [1.0, 2.0]
        ints integer [1, nil]
      >

  ### From tensors

  To create dataframe from tensors, you can pass a matrix as argument.
  Each matrix column becomes a dataframe column with names x1, x2, x3,
  etc:

      iex> Explorer.DataFrame.new(Nx.tensor([
      ...>   [1, 2, 3],
      ...>   [4, 5, 6]
      ...> ]))
      #Explorer.DataFrame<
        Polars[2 x 3]
        x1 integer [1, 4]
        x2 integer [2, 5]
        x3 integer [3, 6]
      >

  Explorer expects tensors to have certain types, so you may need to cast
  the data accordingly. See `Explorer.Series.from_tensor/2` for more info.

  You can also pass a keyword list or maps of vectors (rank 1 tensors):

      iex> Explorer.DataFrame.new(%{
      ...>   floats: Nx.tensor([1.0, 2.0], type: :f64),
      ...>   ints: Nx.tensor([3, 4])
      ...> })
      #Explorer.DataFrame<
        Polars[2 x 2]
        floats float [1.0, 2.0]
        ints integer [3, 4]
      >

  Use dtypes to force a particular representation:

      iex> Explorer.DataFrame.new([
      ...>   floats: Nx.tensor([1.0, 2.0], type: :f64),
      ...>   times: Nx.tensor([3_000, 4_000])
      ...> ], dtypes: [times: :time])
      #Explorer.DataFrame<
        Polars[2 x 2]
        floats float [1.0, 2.0]
        times time [00:00:00.000003, 00:00:00.000004]
      >

  ### From tabular

  Tabular data can be either columnar or row-based.
  Let's start with column data:

      iex> Explorer.DataFrame.new(%{floats: [1.0, 2.0], ints: [1, nil]})
      #Explorer.DataFrame<
        Polars[2 x 2]
        floats float [1.0, 2.0]
        ints integer [1, nil]
      >

      iex> Explorer.DataFrame.new(floats: [1.0, 2.0], ints: [1, nil])
      #Explorer.DataFrame<
        Polars[2 x 2]
        floats float [1.0, 2.0]
        ints integer [1, nil]
      >

      iex> Explorer.DataFrame.new([floats: [1.0, 2.0], ints: [1, nil], binaries: [<<239, 191, 19>>, nil]], dtypes: [{:binaries, :binary}])
      #Explorer.DataFrame<
        Polars[2 x 3]
        floats float [1.0, 2.0]
        ints integer [1, nil]
        binaries binary [<<239, 191, 19>>, nil]
      >

      iex> Explorer.DataFrame.new(%{floats: [1.0, 2.0], ints: [1, "wrong"]})
      ** (ArgumentError) cannot create series "ints": the value "wrong" does not match the inferred series dtype :integer

  From row data:

      iex> rows = [%{id: 1, name: "José"}, %{id: 2, name: "Christopher"}, %{id: 3, name: "Cristine"}]
      iex> Explorer.DataFrame.new(rows)
      #Explorer.DataFrame<
        Polars[3 x 2]
        id integer [1, 2, 3]
        name string ["José", "Christopher", "Cristine"]
      >

      iex> rows = [[id: 1, name: "José"], [id: 2, name: "Christopher"], [id: 3, name: "Cristine"]]
      iex> Explorer.DataFrame.new(rows)
      #Explorer.DataFrame<
        Polars[3 x 2]
        id integer [1, 2, 3]
        name string ["José", "Christopher", "Cristine"]
      >
  """
  @doc type: :conversion
  @spec new(
          Table.Reader.t() | series_pairs,
          opts :: Keyword.t()
        ) :: DataFrame.t()
        when series_pairs: %{column_name() => Series.t()} | [{column_name(), Series.t()}]
  def new(data, opts \\ []) do
    opts = Keyword.validate!(opts, lazy: false, backend: nil, dtypes: [])
    backend = backend_from_options!(opts)
    dtypes = check_dtypes!(opts[:dtypes])

    case data do
      %DataFrame{data: %^backend{}} ->
        data

      data ->
        case classify_data(data) do
          {:series, data} ->
            pairs =
              for {name, series} <- data do
                if not is_struct(series, Series) do
                  raise ArgumentError,
                        "expected a list or map of only series, got: #{inspect(series)}"
                end

                {to_column_name(name), series}
              end

            backend.from_series(pairs)

          {:tensor, data} ->
            s_backend = df_backend_to_s_backend(backend)

            pairs =
              for {name, tensor} <- data do
                if not is_struct(tensor, Nx.Tensor) do
                  raise ArgumentError,
                        "expected a list or map of only tensors, got: #{inspect(tensor)}"
                end

                name = to_column_name(name)
                dtype = dtypes[name]
                opts = [backend: s_backend]
                opts = if dtype, do: [dtype: dtype] ++ opts, else: opts
                {name, Series.from_tensor(tensor, opts)}
              end

            backend.from_series(pairs)

          {:tabular, data} ->
            backend.from_tabular(data, dtypes)
        end
    end
  end

  defp classify_data([{_, %struct{}} | _] = data) when struct == Series, do: {:series, data}
  defp classify_data([{_, %struct{}} | _] = data) when struct == Nx.Tensor, do: {:tensor, data}

  defp classify_data(%struct{} = data) do
    if struct == Nx.Tensor do
      case data.shape do
        {_, cols} ->
          {:tensor, Enum.map(1..cols, fn i -> {"x#{i}", data[[0..-1//1, i - 1]]} end)}

        _ ->
          raise ArgumentError,
                "Explorer.DataFrame.new/2 only accepts tensor of rank 2, got: #{inspect(data)}"
      end
    else
      {:tabular, data}
    end
  end

  defp classify_data(data) when is_map(data) do
    case :maps.next(:maps.iterator(data)) do
      {_key, %struct{}, _} when struct == Series -> {:series, data}
      {_key, %struct{}, _} when struct == Nx.Tensor -> {:tensor, data}
      _ -> {:tabular, data}
    end
  end

  defp classify_data(data), do: {:tabular, data}

  @doc """
  Converts a dataframe to a list of columns with lists as values.

  See `to_series/2` if you want a list of columns with series as values.
  Note that this function does not take into account groups.

  > #### Warning {: .warning}
  >
  > This is an expensive operation since it converts series to lists and doing
  > so will copy the whole dataframe. Prefer to use the operations in this and
  > the `Explorer.Series` module rather than the ones in `Enum` whenever possible,
  > as Explorer is optimized for large series.

  ## Options

    * `:atom_keys` - Configure if the resultant map should have atom keys. (default: `false`)

  ## Examples

      iex> df = Explorer.DataFrame.new(ints: [1, nil], floats: [1.0, 2.0])
      iex> Explorer.DataFrame.to_columns(df)
      %{"floats" => [1.0, 2.0], "ints" => [1, nil]}

      iex> df = Explorer.DataFrame.new(floats: [1.0, 2.0], ints: [1, nil])
      iex> Explorer.DataFrame.to_columns(df, atom_keys: true)
      %{floats: [1.0, 2.0], ints: [1, nil]}

  """
  @doc type: :conversion
  @spec to_columns(df :: DataFrame.t(), Keyword.t()) :: map()
  def to_columns(df, opts \\ []) do
    opts = Keyword.validate!(opts, atom_keys: false)
    atom_keys = opts[:atom_keys]

    for name <- df.names, into: %{} do
      series = Shared.apply_impl(df, :pull, [name])
      key = if atom_keys, do: String.to_atom(name), else: name
      {key, Series.to_list(series)}
    end
  end

  @doc """
  Converts a dataframe to a list of columns with series as values.

  See `to_columns/2` if you want a list of columns with lists as values.
  Note that this function does not take into account groups.

  ## Options

    * `:atom_keys` - Configure if the resultant map should have atom keys. (default: `false`)

  ## Examples

      iex> df = Explorer.DataFrame.new(ints: [1, nil], floats: [1.0, 2.0])
      iex> map = Explorer.DataFrame.to_series(df)
      iex> Explorer.Series.to_list(map["floats"])
      [1.0, 2.0]
      iex> Explorer.Series.to_list(map["ints"])
      [1, nil]

  """
  @doc type: :conversion
  @spec to_series(df :: DataFrame.t(), Keyword.t()) :: map()
  def to_series(df, opts \\ []) do
    opts = Keyword.validate!(opts, atom_keys: false)
    atom_keys = opts[:atom_keys]

    for name <- df.names, into: %{} do
      key = if atom_keys, do: String.to_atom(name), else: name
      {key, Shared.apply_impl(df, :pull, [name])}
    end
  end

  @doc """
  Converts a dataframe to a list of maps (rows).

  > #### Warning {: .warning}
  >
  > This is an expensive operation since data is stored in a columnar format.
  > You must avoid converting a dataframe to rows, as that will transform and
  > copy the whole dataframe in memory. Prefer to use the operations in this
  > module rather than the ones in `Enum` whenever possible, as this module is
  > optimized for large series.

  ## Options

    * `:atom_keys` - Configure if the resultant maps should have atom keys. (default: `false`)

  ## Examples

      iex> df = Explorer.DataFrame.new(floats: [1.0, 2.0], ints: [1, nil])
      iex> Explorer.DataFrame.to_rows(df)
      [%{"floats" => 1.0, "ints" => 1}, %{"floats" => 2.0 ,"ints" => nil}]

      iex> df = Explorer.DataFrame.new(floats: [1.0, 2.0], ints: [1, nil])
      iex> Explorer.DataFrame.to_rows(df, atom_keys: true)
      [%{floats: 1.0, ints: 1}, %{floats: 2.0, ints: nil}]
  """
  @doc type: :conversion
  @spec to_rows(df :: DataFrame.t(), Keyword.t()) :: [map()]
  def to_rows(df, opts \\ []) do
    opts = Keyword.validate!(opts, atom_keys: false)

    Shared.apply_impl(df, :to_rows, [opts[:atom_keys]])
  end

  @doc """
  Converts a dataframe to a stream of maps (rows).

  > #### Warning {: .warning}
  >
  > This is an expensive operation since data is stored in a columnar format.
  > Prefer to use the operations in this module rather than the ones in `Enum`
  > whenever possible, as this module is optimized for large series.

  ## Options

    * `:atom_keys` - Configure if the resultant maps should have atom keys. (default: `false`)
    * `:chunk_size` - Number of rows passed to `to_rows/2` while streaming over the data. (default: `1000`)

  ## Examples

      iex> df = Explorer.DataFrame.new(floats: [1.0, 2.0], ints: [1, nil])
      iex> Explorer.DataFrame.to_rows_stream(df) |> Enum.map(& &1)
      [%{"floats" => 1.0, "ints" => 1}, %{"floats" => 2.0 ,"ints" => nil}]

      iex> df = Explorer.DataFrame.new(floats: [1.0, 2.0], ints: [1, nil])
      iex> Explorer.DataFrame.to_rows_stream(df, atom_keys: true) |> Enum.map(& &1)
      [%{floats: 1.0, ints: 1}, %{floats: 2.0, ints: nil}]
  """
  @doc type: :conversion
  @spec to_rows_stream(df :: DataFrame.t(), Keyword.t()) :: Enumerable.t()
  def to_rows_stream(df, opts \\ []) do
    opts = Keyword.validate!(opts, atom_keys: false, chunk_size: 1_000)

    Shared.apply_impl(df, :to_rows_stream, [opts[:atom_keys], opts[:chunk_size]])
  end

  # Introspection

  @doc """
  Gets the names of the dataframe columns.

  ## Examples

      iex> df = Explorer.DataFrame.new(floats: [1.0, 2.0], ints: [1, 2])
      iex> Explorer.DataFrame.names(df)
      ["floats", "ints"]
  """
  @doc type: :introspection
  @spec names(df :: DataFrame.t()) :: [String.t()]
  def names(df), do: df.names

  @doc """
  Gets the dtypes of the dataframe columns.

  ## Examples

      iex> df = Explorer.DataFrame.new(floats: [1.0, 2.0], ints: [1, 2])
      iex> Explorer.DataFrame.dtypes(df)
      %{"floats" => :float, "ints" => :integer}
  """
  @doc type: :introspection
  @spec dtypes(df :: DataFrame.t()) :: %{String.t() => atom()}
  def dtypes(df), do: df.dtypes

  @doc """
  Gets the shape of the dataframe as a `{height, width}` tuple.

  This function works the same way for grouped dataframes, considering the entire
  dataframe in the counting of rows.

  ## Examples

      iex> df = Explorer.DataFrame.new(floats: [1.0, 2.0, 3.0], ints: [1, 2, 3])
      iex> Explorer.DataFrame.shape(df)
      {3, 2}
  """
  @doc type: :introspection
  @spec shape(df :: DataFrame.t()) :: {integer(), integer()}
  def shape(df), do: {n_rows(df), n_columns(df)}

  @doc """
  Returns the number of rows in the dataframe.

  This function works the same way for grouped dataframes, considering the entire
  dataframe in the counting of rows.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.n_rows(df)
      1094
  """
  @doc type: :introspection
  @spec n_rows(df :: DataFrame.t()) :: integer()
  def n_rows(df), do: Shared.apply_impl(df, :n_rows)

  @doc """
  Returns the number of columns in the dataframe.

  This function works the same way for grouped dataframes.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.n_columns(df)
      10
  """
  @doc type: :introspection
  @spec n_columns(df :: DataFrame.t()) :: integer()
  def n_columns(df), do: map_size(df.dtypes)

  @doc """
  Returns the groups of a dataframe.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> df = Explorer.DataFrame.group_by(df, "country")
      iex> Explorer.DataFrame.groups(df)
      ["country"]

      iex> df = Explorer.Datasets.iris()
      iex> Explorer.DataFrame.groups(df)
      []
  """
  @doc type: :introspection
  @spec groups(df :: DataFrame.t()) :: list(String.t())
  def groups(%DataFrame{groups: groups}), do: groups

  # Single table verbs

  @doc """
  Returns the first *n* rows of the dataframe.

  By default it returns the first 5 rows.

  If the dataframe is using groups, then the first *n* rows of each group is
  returned.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.head(df)
      #Explorer.DataFrame<
        Polars[5 x 10]
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

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.head(df, 2)
      #Explorer.DataFrame<
        Polars[2 x 10]
        year integer [2010, 2010]
        country string ["AFGHANISTAN", "ALBANIA"]
        total integer [2308, 1254]
        solid_fuel integer [627, 117]
        liquid_fuel integer [1601, 953]
        gas_fuel integer [74, 7]
        cement integer [5, 177]
        gas_flaring integer [0, 0]
        per_capita float [0.08, 0.43]
        bunker_fuels integer [9, 7]
      >

  ## Grouped examples

  Using grouped dataframes makes `head/2` return *n* rows from each group.
  Here is an example using the Iris dataset, and returning two rows from each group:

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.head(grouped, 2)
      #Explorer.DataFrame<
        Polars[6 x 5]
        Groups: ["species"]
        sepal_length float [5.1, 4.9, 7.0, 6.4, 6.3, ...]
        sepal_width float [3.5, 3.0, 3.2, 3.2, 3.3, ...]
        petal_length float [1.4, 1.4, 4.7, 4.5, 6.0, ...]
        petal_width float [0.2, 0.2, 1.4, 1.5, 2.5, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-versicolor", "Iris-versicolor", "Iris-virginica", ...]
      >

  """
  @doc type: :rows
  @spec head(df :: DataFrame.t(), nrows :: integer()) :: DataFrame.t()
  def head(df, nrows \\ @default_sample_nrows), do: Shared.apply_impl(df, :head, [nrows])

  @doc """
  Returns the last *n* rows of the dataframe.

  By default it returns the last 5 rows.

  If the dataframe is using groups, then the last *n* rows of each group is
  returned.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.tail(df)
      #Explorer.DataFrame<
        Polars[5 x 10]
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

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.tail(df, 2)
      #Explorer.DataFrame<
        Polars[2 x 10]
        year integer [2014, 2014]
        country string ["ZAMBIA", "ZIMBABWE"]
        total integer [1228, 3278]
        solid_fuel integer [132, 2097]
        liquid_fuel integer [797, 1005]
        gas_fuel integer [0, 0]
        cement integer [299, 177]
        gas_flaring integer [0, 0]
        per_capita float [0.08, 0.22]
        bunker_fuels integer [33, 9]
      >

  ## Grouped examples

  Using grouped dataframes makes `tail/2` return **n rows** from each group.
  Here is an example using the Iris dataset, and returning two rows from each group:

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.tail(grouped, 2)
      #Explorer.DataFrame<
        Polars[6 x 5]
        Groups: ["species"]
        sepal_length float [5.3, 5.0, 5.1, 5.7, 6.2, ...]
        sepal_width float [3.7, 3.3, 2.5, 2.8, 3.4, ...]
        petal_length float [1.5, 1.4, 3.0, 4.1, 5.4, ...]
        petal_width float [0.2, 0.2, 1.1, 1.3, 2.3, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-versicolor", "Iris-versicolor", "Iris-virginica", ...]
      >

  """
  @doc type: :rows
  @spec tail(df :: DataFrame.t(), nrows :: integer()) :: DataFrame.t()
  def tail(df, nrows \\ @default_sample_nrows), do: Shared.apply_impl(df, :tail, [nrows])

  @doc """
  Selects a subset of columns by name.

  It's important to notice that groups are kept:
  you can't select off grouping columns.

  ## Examples

  You can select a single column:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.select(df, "a")
      #Explorer.DataFrame<
        Polars[3 x 1]
        a string ["a", "b", "c"]
      >

  Or a list of names:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.select(df, ["a"])
      #Explorer.DataFrame<
        Polars[3 x 1]
        a string ["a", "b", "c"]
      >

  You can also use a range or a list of integers:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3], c: [4, 5, 6])
      iex> Explorer.DataFrame.select(df, [0, 1])
      #Explorer.DataFrame<
        Polars[3 x 2]
        a string ["a", "b", "c"]
        b integer [1, 2, 3]
      >

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3], c: [4, 5, 6])
      iex> Explorer.DataFrame.select(df, 0..1)
      #Explorer.DataFrame<
        Polars[3 x 2]
        a string ["a", "b", "c"]
        b integer [1, 2, 3]
      >

  Or you can use a callback function that takes the dataframe's names as its first argument:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.select(df, &String.starts_with?(&1, "b"))
      #Explorer.DataFrame<
        Polars[3 x 1]
        b integer [1, 2, 3]
      >

  Or, if you prefer, a regex:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.select(df, ~r/^b$/)
      #Explorer.DataFrame<
        Polars[3 x 1]
        b integer [1, 2, 3]
      >

  Or a callback function that takes names and types:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.select(df, fn _name, type -> type == :integer end)
      #Explorer.DataFrame<
        Polars[3 x 1]
        b integer [1, 2, 3]
      >

  ## Grouped examples

  Columns that are also groups cannot be removed,
  you need to ungroup before removing these columns.

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.select(grouped, ["sepal_width"])
      #Explorer.DataFrame<
        Polars[150 x 2]
        Groups: ["species"]
        sepal_width float [3.5, 3.0, 3.2, 3.1, 3.6, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
      >

  """
  @doc type: :single
  @spec select(df :: DataFrame.t(), column | columns) :: DataFrame.t()
  def select(df, columns_or_column)

  def select(df, column) when is_column(column) do
    select(df, [column])
  end

  def select(df, columns) do
    columns = to_existing_columns(df, columns)
    columns_to_keep = Enum.uniq(columns ++ df.groups)
    out_df = %{df | names: columns_to_keep, dtypes: Map.take(df.dtypes, columns_to_keep)}
    Shared.apply_impl(df, :select, [out_df])
  end

  @doc """
  Discards a subset of columns by name.

  It's important to notice that groups are kept:
  you can't discard grouping columns.

  ## Examples

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.discard(df, ["b"])
      #Explorer.DataFrame<
        Polars[3 x 1]
        a string ["a", "b", "c"]
      >

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3], c: [4, 5, 6])
      iex> Explorer.DataFrame.discard(df, ["a", "b"])
      #Explorer.DataFrame<
        Polars[3 x 1]
        c integer [4, 5, 6]
      >

  Ranges, regexes, and functions are also accepted in column names, as in `select/2`.

  ## Grouped examples

  You cannot discard grouped columns. You need to ungroup before removing them:

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.discard(grouped, ["species"])
      #Explorer.DataFrame<
        Polars[150 x 5]
        Groups: ["species"]
        sepal_length float [5.1, 4.9, 4.7, 4.6, 5.0, ...]
        sepal_width float [3.5, 3.0, 3.2, 3.1, 3.6, ...]
        petal_length float [1.4, 1.4, 1.3, 1.5, 1.4, ...]
        petal_width float [0.2, 0.2, 0.2, 0.2, 0.2, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
      >

  """
  @doc type: :single
  @spec discard(df :: DataFrame.t(), column | columns) :: DataFrame.t()
  def discard(df, columns_or_column)

  def discard(df, column) when is_column(column) do
    discard(df, [column])
  end

  def discard(df, columns) do
    columns = to_existing_columns(df, columns) -- df.groups
    columns_to_keep = df.names -- columns
    out_df = %{df | names: columns_to_keep, dtypes: Map.take(df.dtypes, columns_to_keep)}
    Shared.apply_impl(df, :select, [out_df])
  end

  @doc """
  Picks rows based on a list or series of values.

  ## Examples

  This function must only be used when you need to select rows based
  on external values that are not available to the dataframe. For example,
  you can pass a list:

      iex> df = Explorer.DataFrame.new(col1: ["a", "b", "c"], col2: [1, 2, 3])
      iex> Explorer.DataFrame.mask(df, [false, true, false])
      #Explorer.DataFrame<
        Polars[1 x 2]
        col1 string ["b"]
        col2 integer [2]
      >

  You must avoid using masks when the masks themselves are computed from
  other columns. For example, DO NOT do this:

      iex> df = Explorer.DataFrame.new(col1: ["a", "b", "c"], col2: [1, 2, 3])
      iex> Explorer.DataFrame.mask(df, Explorer.Series.greater(df["col2"], 1))
      #Explorer.DataFrame<
        Polars[2 x 2]
        col1 string ["b", "c"]
        col2 integer [2, 3]
      >

  Instead, do this:

      iex> df = Explorer.DataFrame.new(col1: ["a", "b", "c"], col2: [1, 2, 3])
      iex> Explorer.DataFrame.filter_with(df, fn df -> Explorer.Series.greater(df["col2"], 1) end)
      #Explorer.DataFrame<
        Polars[2 x 2]
        col1 string ["b", "c"]
        col2 integer [2, 3]
      >

  The `filter_with/2` version is much more efficient because it doesn't need
  to create intermediate series representations to apply the mask.
  """
  @doc type: :single
  @spec mask(df :: DataFrame.t(), mask :: Series.t() | [boolean()]) :: DataFrame.t()
  def mask(df, %Series{} = mask) do
    s_len = Series.size(mask)
    df_len = n_rows(df)

    case s_len == df_len do
      false ->
        raise(
          ArgumentError,
          "size of the mask (#{s_len}) must match number of rows in the dataframe (#{df_len})"
        )

      true ->
        Shared.apply_impl(df, :mask, [mask])
    end
  end

  def mask(df, mask) when is_list(mask), do: mask |> Series.from_list() |> then(&mask(df, &1))

  @doc """
  Picks rows based on `Explorer.Query`.

  The query is compiled and runs efficiently against the dataframe.
  The query must return a boolean expression or a list of boolean expressions.
  When a list is returned, they are joined as `and` expressions.

  > #### Notice {: .notice}
  >
  > This is a macro. You must `require  Explorer.DataFrame` before using it.

  Besides element-wise series operations, you can also use window functions
  and aggregations inside comparisons. In such cases, grouped dataframes
  may have different results than ungrouped ones, because the filtering
  is computed withing groups. See examples below.

  See `filter_with/2` for a callback version of this function without
  `Explorer.Query`.

  ## Examples

      iex> df = Explorer.DataFrame.new(col1: ["a", "b", "c"], col2: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, col2 > 2)
      #Explorer.DataFrame<
        Polars[1 x 2]
        col1 string ["c"]
        col2 integer [3]
      >

      iex> df = Explorer.DataFrame.new(col1: ["a", "b", "c"], col2: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, col1 == "b")
      #Explorer.DataFrame<
        Polars[1 x 2]
        col1 string ["b"]
        col2 integer [2]
      >

      iex> df = Explorer.DataFrame.new(col1: [5, 4, 3], col2: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, [col1 > 3, col2 < 3])
      #Explorer.DataFrame<
        Polars[2 x 2]
        col1 integer [5, 4]
        col2 integer [1, 2]
      >

  Returning a non-boolean expression errors:

      iex> df = Explorer.DataFrame.new(col1: ["a", "b", "c"], col2: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, cumulative_max(col2))
      ** (ArgumentError) expecting the function to return a boolean LazySeries, but instead it returned a LazySeries of type :integer

  Which can be addressed by converting it to boolean:

      iex> df = Explorer.DataFrame.new(col1: ["a", "b", "c"], col2: [1, 2, 3])
      iex> Explorer.DataFrame.filter(df, cumulative_max(col2) == 1)
      #Explorer.DataFrame<
        Polars[1 x 2]
        col1 string ["a"]
        col2 integer [1]
      >

  ## Grouped examples

  In a grouped dataframe, the aggregation is calculated within each group.

  In the following example we select the flowers of the Iris dataset that have the "petal length"
  above the average of each species group.

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.filter(grouped, petal_length > mean(petal_length))
      #Explorer.DataFrame<
        Polars[79 x 5]
        Groups: ["species"]
        sepal_length float [4.6, 5.4, 5.0, 4.9, 5.4, ...]
        sepal_width float [3.1, 3.9, 3.4, 3.1, 3.7, ...]
        petal_length float [1.5, 1.7, 1.5, 1.5, 1.5, ...]
        petal_width float [0.2, 0.4, 0.2, 0.1, 0.2, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
      >

  """
  @doc type: :single
  defmacro filter(df, query) do
    quote do
      require Explorer.Query
      Explorer.DataFrame.filter_with(unquote(df), Explorer.Query.query(unquote(query)))
    end
  end

  @doc """
  Picks rows based on a callback function.

  The callback receives a lazy dataframe. A lazy dataframe does
  not hold any values, instead it stores all operations in order to
  execute all filtering performantly.

  This is a callback version of `filter/2`.

  ## Examples

      iex> df = Explorer.DataFrame.new(col1: ["a", "b", "c"], col2: [1, 2, 3])
      iex> Explorer.DataFrame.filter_with(df, &Explorer.Series.greater(&1["col2"], 2))
      #Explorer.DataFrame<
        Polars[1 x 2]
        col1 string ["c"]
        col2 integer [3]
      >

      iex> df = Explorer.DataFrame.new(col1: ["a", "b", "c"], col2: [1, 2, 3])
      iex> Explorer.DataFrame.filter_with(df, fn df -> Explorer.Series.equal(df["col1"], "b") end)
      #Explorer.DataFrame<
        Polars[1 x 2]
        col1 string ["b"]
        col2 integer [2]
      >

  ## Grouped examples

  In a grouped dataframe, the aggregation is calculated within each group.

  In the following example we select the flowers of the Iris dataset that have the "petal length"
  above the average of each species group.

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.filter_with(grouped, &Explorer.Series.greater(&1["petal_length"], Explorer.Series.mean(&1["petal_length"])))
      #Explorer.DataFrame<
        Polars[79 x 5]
        Groups: ["species"]
        sepal_length float [4.6, 5.4, 5.0, 4.9, 5.4, ...]
        sepal_width float [3.1, 3.9, 3.4, 3.1, 3.7, ...]
        petal_length float [1.5, 1.7, 1.5, 1.5, 1.5, ...]
        petal_width float [0.2, 0.4, 0.2, 0.1, 0.2, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
      >
  """
  @doc type: :single
  @spec filter_with(
          df :: DataFrame.t(),
          callback :: (Explorer.Backend.LazyFrame.t() -> Series.lazy_t())
        ) :: DataFrame.t()
  def filter_with(df, fun) when is_function(fun, 1) do
    ldf = Explorer.Backend.LazyFrame.new(df)

    case fun.(ldf) do
      %Series{dtype: :boolean, data: %LazySeries{} = data} ->
        Shared.apply_impl(df, :filter_with, [df, data])

      %Series{dtype: dtype, data: %LazySeries{}} ->
        raise ArgumentError,
              "expecting the function to return a boolean LazySeries, " <>
                "but instead it returned a LazySeries of type " <>
                inspect(dtype)

      [%Series{dtype: :boolean, data: %LazySeries{}} | _rest] = lazy_series ->
        first_non_boolean =
          Enum.find(lazy_series, &(!match?(%Series{dtype: :boolean, data: %LazySeries{}}, &1)))

        if is_nil(first_non_boolean) do
          series = Enum.reduce(lazy_series, &Explorer.Backend.LazySeries.binary_and(&2, &1))

          Shared.apply_impl(df, :filter_with, [df, series.data])
        else
          filter_without_boolean_series_error(first_non_boolean)
        end

      other ->
        filter_without_boolean_series_error(other)
    end
  end

  defp filter_without_boolean_series_error(term) do
    raise ArgumentError,
          "expecting the function to return a single or a list of boolean LazySeries, " <>
            "but instead it contains:\n#{inspect(term)}"
  end

  @doc """
  Creates or modifies columns based on `Explorer.Query`.

  The query is compiled and runs efficiently against the dataframe.
  New variables overwrite existing variables of the same name.
  Column names are coerced from atoms to strings.

  > #### Notice {: .notice}
  >
  > This is a macro. You must `require  Explorer.DataFrame` before using it.

  Besides element-wise series operations, you can also use window functions
  and aggregations inside mutations. In such cases, grouped dataframes
  may have different results than ungrouped ones, because the mutation
  is computed withing groups. See examples below.

  See `mutate_with/2` for a callback version of this function without
  `Explorer.Query`. If your mutation cannot be expressed with queries,
  you may compute the values using the `Explorer.Series` API directly
  and then add it to the dataframe using `put/3`.

  ## Examples

  Mutations are useful to add or modify columns in your dataframe:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate(df, c: b + 1)
      #Explorer.DataFrame<
        Polars[3 x 3]
        a string ["a", "b", "c"]
        b integer [1, 2, 3]
        c integer [2, 3, 4]
      >

  It's also possible to overwrite existing columns:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate(df, a: b * 2)
      #Explorer.DataFrame<
        Polars[3 x 2]
        a integer [2, 4, 6]
        b integer [1, 2, 3]
      >

  Scalar values are repeated to fill the series:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate(df, a: 4)
      #Explorer.DataFrame<
        Polars[3 x 2]
        a integer [4, 4, 4]
        b integer [1, 2, 3]
      >

  It's also possible to use functions from the Series module, like `Explorer.Series.window_sum/3`:

      iex> df = Explorer.DataFrame.new(a: [1, 2, 3])
      iex> Explorer.DataFrame.mutate(df, b: window_sum(a, 2))
      #Explorer.DataFrame<
        Polars[3 x 2]
        a integer [1, 2, 3]
        b integer [1, 3, 5]
      >

  Alternatively, all of the above works with a map instead of a keyword list:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate(df, %{"c" => cast(b, :float)})
      #Explorer.DataFrame<
        Polars[3 x 3]
        a string ["a", "b", "c"]
        b integer [1, 2, 3]
        c float [1.0, 2.0, 3.0]
      >

  ## Grouped examples

  Mutations in grouped dataframes takes the context of the group.
  This enables some aggregations to be made considering each group. It's almost like `summarise/2`,
  but repeating the results for each member in the group.
  For example, if we want to count how many elements of a given group, we can add a new
  column with that aggregation:

      iex> df = Explorer.DataFrame.new(id: ["a", "a", "b"], b: [1, 2, 3])
      iex> grouped = Explorer.DataFrame.group_by(df, :id)
      iex> Explorer.DataFrame.mutate(grouped, count: count(b))
      #Explorer.DataFrame<
        Polars[3 x 3]
        Groups: ["id"]
        id string ["a", "a", "b"]
        b integer [1, 2, 3]
        count integer [2, 2, 1]
      >

  In case we want to get the average size of the petal length from the Iris dataset, we can:

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.mutate(grouped, petal_length_avg: mean(petal_length))
      #Explorer.DataFrame<
        Polars[150 x 6]
        Groups: ["species"]
        sepal_length float [5.1, 4.9, 4.7, 4.6, 5.0, ...]
        sepal_width float [3.5, 3.0, 3.2, 3.1, 3.6, ...]
        petal_length float [1.4, 1.4, 1.3, 1.5, 1.4, ...]
        petal_width float [0.2, 0.2, 0.2, 0.2, 0.2, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
        petal_length_avg float [1.4640000000000004, 1.4640000000000004, 1.4640000000000004, 1.4640000000000004, 1.4640000000000004, ...]
      >
  """
  @doc type: :single
  defmacro mutate(df, mutations) do
    quote do
      require Explorer.Query
      Explorer.DataFrame.mutate_with(unquote(df), Explorer.Query.query(unquote(mutations)))
    end
  end

  @doc """
  Creates or modifies columns using a callback function.

  The callback receives a lazy dataframe. A lazy dataframe doesn't
  hold any values, instead it stores all operations in order to
  execute all mutations performantly.

  This is a callback version of `mutate/2`. If your mutation
  cannot be expressed with lazy dataframes, you may compute the
  values using the `Explorer.Series` API directly and then add
  it to the dataframe using `put/3`.

  ## Examples

  Here is an example of a new column that sums the value of two other columns:

      iex> df = Explorer.DataFrame.new(a: [4, 5, 6], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate_with(df, &[c: Explorer.Series.add(&1["a"], &1["b"])])
      #Explorer.DataFrame<
        Polars[3 x 3]
        a integer [4, 5, 6]
        b integer [1, 2, 3]
        c integer [5, 7, 9]
      >

  You can overwrite existing columns as well:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "c"], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate_with(df, &[b: Explorer.Series.pow(&1["b"], 2)])
      #Explorer.DataFrame<
        Polars[3 x 2]
        a string ["a", "b", "c"]
        b float [1.0, 4.0, 9.0]
      >

  It's possible to "reuse" a variable for different computations:

      iex> df = Explorer.DataFrame.new(a: [4, 5, 6], b: [1, 2, 3])
      iex> Explorer.DataFrame.mutate_with(df, fn ldf ->
      iex>   c = Explorer.Series.add(ldf["a"], ldf["b"])
      iex>   [c: c, d: Explorer.Series.window_sum(c, 2)]
      iex> end)
      #Explorer.DataFrame<
        Polars[3 x 4]
        a integer [4, 5, 6]
        b integer [1, 2, 3]
        c integer [5, 7, 9]
        d integer [5, 12, 16]
      >

  ## Grouped examples

  Mutations in grouped dataframes takes the context of the group.
  For example, if we want to count how many elements of a given group,
  we can add a new column with that aggregation:

      iex> df = Explorer.DataFrame.new(id: ["a", "a", "b"], b: [1, 2, 3])
      iex> grouped = Explorer.DataFrame.group_by(df, :id)
      iex> Explorer.DataFrame.mutate_with(grouped, &[count: Explorer.Series.count(&1["b"])])
      #Explorer.DataFrame<
        Polars[3 x 3]
        Groups: ["id"]
        id string ["a", "a", "b"]
        b integer [1, 2, 3]
        count integer [2, 2, 1]
      >

  """
  @doc type: :single
  @spec mutate_with(
          df :: DataFrame.t(),
          callback :: (Explorer.Backend.LazyFrame.t() -> column_pairs(Series.lazy_t()))
        ) :: DataFrame.t()
  def mutate_with(%DataFrame{} = df, fun) when is_function(fun) do
    ldf = Explorer.Backend.LazyFrame.new(df)

    result = fun.(ldf)

    column_pairs =
      to_column_pairs(df, result, fn value ->
        case value do
          %Series{data: %LazySeries{}} = lazy_series ->
            lazy_series

          %Series{data: _other} ->
            raise ArgumentError,
                  "expecting a lazy series. Consider using `Explorer.DataFrame.put/3` " <>
                    "to add eager series to your dataframe."

          list when is_list(list) ->
            raise ArgumentError,
                  "expecting a lazy series or scalar value, but instead got a list. " <>
                    "consider using `Explorer.Series.from_list/2` to create a `Series`, " <>
                    "and then `Explorer.DataFrame.put/3` to add the series to your dataframe."

          number when is_number(number) ->
            dtype = if is_integer(number), do: :integer, else: :float
            lazy_s = LazySeries.new(:to_lazy, [number])

            Explorer.Backend.Series.new(lazy_s, dtype)

          string when is_binary(string) ->
            lazy_s = LazySeries.new(:to_lazy, [string])

            Explorer.Backend.Series.new(lazy_s, :string)

          boolean when is_boolean(boolean) ->
            lazy_s = LazySeries.new(:to_lazy, [boolean])

            Explorer.Backend.Series.new(lazy_s, :boolean)

          date = %Date{} ->
            lazy_s = LazySeries.new(:to_lazy, [date])

            Explorer.Backend.Series.new(lazy_s, :date)

          datetime = %NaiveDateTime{} ->
            lazy_s = LazySeries.new(:to_lazy, [datetime])

            Explorer.Backend.Series.new(lazy_s, :datetime)

          other ->
            raise ArgumentError,
                  "expecting a lazy series or scalar value, but instead got #{inspect(other)}"
        end
      end)

    new_dtypes =
      for {column_name, series} <- column_pairs, into: %{} do
        {column_name, series.dtype}
      end

    mut_names = Enum.map(column_pairs, &elem(&1, 0))
    new_names = Enum.uniq(df.names ++ mut_names)

    df_out = %{df | names: new_names, dtypes: Map.merge(df.dtypes, new_dtypes)}

    column_pairs = for {name, %Series{data: lazy_series}} <- column_pairs, do: {name, lazy_series}

    Shared.apply_impl(df, :mutate_with, [df_out, column_pairs])
  end

  @doc """
  Creates or modifies a single column.

  This is a simplified way to add or modify one column,
  accepting a series, tensor, or a list.

  If you are computing a series, it is preferrable to use
  `mutate/2` or `mutate_with/2` to compute the series and
  modify it in a single step, as it is more powerful and
  it handles both expressions and scalar values accordingly.

  If you are passing tensors or lists, they will be automatically
  converted to a series. By default, the new series will have the
  same dtype as the existing series, unless the `:dtype` option
  is given. If there is no existing series, one is inferred from
  the tensor/list.

  ## Examples

      iex> df = Explorer.DataFrame.new(a: [1, 2, 3])
      iex> Explorer.DataFrame.put(df, :b, Explorer.Series.transform(df[:a], fn n -> n * 2 end))
      #Explorer.DataFrame<
        Polars[3 x 2]
        a integer [1, 2, 3]
        b integer [2, 4, 6]
      >

      iex> df = Explorer.DataFrame.new(a: [1, 2, 3])
      iex> Explorer.DataFrame.put(df, :b, Explorer.Series.from_list([4, 5, 6]))
      #Explorer.DataFrame<
        Polars[3 x 2]
        a integer [1, 2, 3]
        b integer [4, 5, 6]
      >

  ## Grouped examples

  If the dataframe is grouped, `put/3` is going to ignore the groups.
  So the series must be of the same size of the entire dataframe.

      iex> df = Explorer.DataFrame.new(a: [1, 2, 3])
      iex> grouped = Explorer.DataFrame.group_by(df, "a")
      iex> series = Explorer.Series.from_list([9, 8, 7])
      iex> Explorer.DataFrame.put(grouped, :b, series)
      #Explorer.DataFrame<
        Polars[3 x 2]
        Groups: ["a"]
        a integer [1, 2, 3]
        b integer [9, 8, 7]
      >

  ## Tensor examples

  You can also put tensors into the dataframe:

      iex> df = Explorer.DataFrame.new([])
      iex> Explorer.DataFrame.put(df, :a, Nx.tensor([1, 2, 3]))
      #Explorer.DataFrame<
        Polars[3 x 1]
        a integer [1, 2, 3]
      >

  You can specify which dtype the tensor represents.
  For example, a tensor of s64 represents integers
  by default, but it may also represent timestamps
  in microseconds from the Unix epoch:

      iex> df = Explorer.DataFrame.new([])
      iex> Explorer.DataFrame.put(df, :a, Nx.tensor([1, 2, 3]), dtype: :datetime)
      #Explorer.DataFrame<
        Polars[3 x 1]
        a datetime [1970-01-01 00:00:00.000001, 1970-01-01 00:00:00.000002, 1970-01-01 00:00:00.000003]
      >

  If there is already a column where we want to place the tensor,
  the column dtype will be automatically used, this means that
  updating dataframes in place while preserving their types is
  straight-forward:

      iex> df = Explorer.DataFrame.new(a: [~N[1970-01-01 00:00:00]])
      iex> Explorer.DataFrame.put(df, :a, Nx.tensor(529550625987654))
      #Explorer.DataFrame<
        Polars[1 x 1]
        a datetime [1986-10-13 01:23:45.987654]
      >

  This is particularly useful for categorical columns:

      iex> cat = Explorer.Series.from_list(["foo", "bar", "baz"], dtype: :category)
      iex> df = Explorer.DataFrame.new(a: cat)
      iex> Explorer.DataFrame.put(df, :a, Nx.tensor([2, 1, 0]))
      #Explorer.DataFrame<
        Polars[3 x 1]
        a category ["baz", "bar", "foo"]
      >

  On the other hand, if you try to put a floating tensor on
  an integer column, an error will be raised unless a dtype
  or `dtype: :infer` is given:

      iex> df = Explorer.DataFrame.new(a: [1, 2, 3])
      iex> Explorer.DataFrame.put(df, :a, Nx.tensor(1.0, type: :f64))
      ** (ArgumentError) dtype integer expects a tensor of type {:s, 64} but got type {:f, 64}

      iex> df = Explorer.DataFrame.new(a: [1, 2, 3])
      iex> Explorer.DataFrame.put(df, :a, Nx.tensor(1.0, type: :f64), dtype: :float)
      #Explorer.DataFrame<
        Polars[3 x 1]
        a float [1.0, 1.0, 1.0]
      >

      iex> df = Explorer.DataFrame.new(a: [1, 2, 3])
      iex> Explorer.DataFrame.put(df, :a, Nx.tensor(1.0, type: :f64), dtype: :infer)
      #Explorer.DataFrame<
        Polars[3 x 1]
        a float [1.0, 1.0, 1.0]
      >

  ## List examples

  Similar to tensors, we can also put lists in the dataframe:

      iex> df = Explorer.DataFrame.new([])
      iex> Explorer.DataFrame.put(df, :a, [1, 2, 3])
      #Explorer.DataFrame<
        Polars[3 x 1]
        a integer [1, 2, 3]
      >

  The same considerations as above apply.
  """
  @doc type: :single
  @spec put(DataFrame.t(), column_name(), Series.t() | Nx.Tensor.t() | list()) :: DataFrame.t()
  def put(df, column_name, series_or_tensor_or_list, opts \\ [])

  def put(%DataFrame{} = df, column_name, %Series{} = series, opts)
      when is_column_name(column_name) do
    Keyword.validate!(opts, [])
    name = to_column_name(column_name)
    new_names = append_unless_present(df.names, name)

    out_df = %{df | names: new_names, dtypes: Map.put(df.dtypes, name, series.dtype)}
    Shared.apply_impl(df, :put, [out_df, name, series])
  end

  def put(%DataFrame{} = df, column_name, tensor, opts)
      when is_column_name(column_name) and is_struct(tensor, Nx.Tensor) do
    put(df, column_name, :from_tensor, tensor, opts)
  end

  def put(%DataFrame{} = df, column_name, list, opts)
      when is_column_name(column_name) and is_list(list) do
    put(df, column_name, :from_list, list, opts)
  end

  defp put(%DataFrame{} = df, column_name, fun, arg, opts) when is_column_name(column_name) do
    opts = Keyword.validate!(opts, [:dtype])
    name = to_column_name(column_name)
    dtype = opts[:dtype]

    if dtype == nil and is_map_key(df.dtypes, name) do
      put(df, name, Series.replace(pull(df, name), arg), [])
    else
      backend = df_backend_to_s_backend(df.data.__struct__)

      opts =
        if dtype == nil or dtype == :infer do
          [backend: backend]
        else
          [dtype: dtype, backend: backend]
        end

      put(df, name, apply(Series, fun, [arg, opts]), [])
    end
  end

  defp df_backend_to_s_backend(backend) do
    backend_df_string = Atom.to_string(backend)
    backend_string = binary_part(backend_df_string, 0, byte_size(backend_df_string) - 10)
    String.to_atom(backend_string)
  end

  defp append_unless_present([name | tail], name), do: [name | tail]
  defp append_unless_present([head | tail], name), do: [head | append_unless_present(tail, name)]
  defp append_unless_present([], name), do: [name]

  @doc """
  Arranges/sorts rows by columns using `Explorer.Query`.

  > #### Notice {: .notice}
  >
  > This is a macro. You must `require  Explorer.DataFrame` before using it.

  See `arrange_with/2` for a callback version of this function without
  `Explorer.Query`.

  ## Examples

  A single column name will sort ascending by that column:

      iex> df = Explorer.DataFrame.new(a: ["b", "c", "a"], b: [1, 2, 3])
      iex> Explorer.DataFrame.arrange(df, a)
      #Explorer.DataFrame<
        Polars[3 x 2]
        a string ["a", "b", "c"]
        b integer [3, 1, 2]
      >

  You can also sort descending:

      iex> df = Explorer.DataFrame.new(a: ["b", "c", "a"], b: [1, 2, 3])
      iex> Explorer.DataFrame.arrange(df, desc: a)
      #Explorer.DataFrame<
        Polars[3 x 2]
        a string ["c", "b", "a"]
        b integer [2, 1, 3]
      >

  Sorting by more than one column sorts them in the order they are entered:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.arrange(df, asc: total, desc: country)
      #Explorer.DataFrame<
        Polars[1094 x 10]
        year integer [2010, 2010, 2011, 2011, 2012, ...]
        country string ["NIUE", "TUVALU", "TUVALU", "NIUE", "NIUE", ...]
        total integer [1, 2, 2, 2, 2, ...]
        solid_fuel integer [0, 0, 0, 0, 0, ...]
        liquid_fuel integer [1, 2, 2, 2, 2, ...]
        gas_fuel integer [0, 0, 0, 0, 0, ...]
        cement integer [0, 0, 0, 0, 0, ...]
        gas_flaring integer [0, 0, 0, 0, 0, ...]
        per_capita float [0.52, 0.0, 0.0, 1.04, 1.04, ...]
        bunker_fuels integer [0, 0, 0, 0, 0, ...]
      >

  ## Grouped examples

  When used in a grouped dataframe, arrange is going to sort each group individually and
  then return the entire dataframe with the existing groups. If one of the arrange columns
  is also a group, the sorting for that column is not going to work. It is necessary to
  first summarise the desired column and then arrange it.

  Here is an example using the Iris dataset. We group by species and then we try to sort
  the dataframe by species and petal length, but only "petal length" is taken into account
  because "species" is a group.

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.arrange(grouped, desc: species, asc: sepal_width)
      #Explorer.DataFrame<
        Polars[150 x 5]
        Groups: ["species"]
        sepal_length float [4.5, 4.4, 4.9, 4.8, 4.3, ...]
        sepal_width float [2.3, 2.9, 3.0, 3.0, 3.0, ...]
        petal_length float [1.3, 1.4, 1.4, 1.4, 1.1, ...]
        petal_width float [0.3, 0.2, 0.2, 0.1, 0.1, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
      >
  """
  @doc type: :single
  defmacro arrange(df, query) do
    quote do
      require Explorer.Query
      Explorer.DataFrame.arrange_with(unquote(df), Explorer.Query.query(unquote(query)))
    end
  end

  @doc """
  Arranges/sorts rows by columns using a callback function.

  The callback receives a lazy dataframe. A lazy dataframe does
  hold any values, instead it stores all operations in order to
  execute all sorting performantly.

  This is a callback version of `arrange/2`.

  Sorting is stable by default.

  ## Examples

  A single column name will sort ascending by that column:

      iex> df = Explorer.DataFrame.new(a: ["b", "c", "a"], b: [1, 2, 3])
      iex> Explorer.DataFrame.arrange_with(df, &(&1["a"]))
      #Explorer.DataFrame<
        Polars[3 x 2]
        a string ["a", "b", "c"]
        b integer [3, 1, 2]
      >

  You can also sort descending:

      iex> df = Explorer.DataFrame.new(a: ["b", "c", "a"], b: [1, 2, 3])
      iex> Explorer.DataFrame.arrange_with(df, &[desc: &1["a"]])
      #Explorer.DataFrame<
        Polars[3 x 2]
        a string ["c", "b", "a"]
        b integer [2, 1, 3]
      >

  Sorting by more than one column sorts them in the order they are entered:

      iex> df = Explorer.DataFrame.new(a: [3, 1, 3], b: [2, 1, 3])
      iex> Explorer.DataFrame.arrange_with(df, &[desc: &1["a"], asc: &1["b"]])
      #Explorer.DataFrame<
        Polars[3 x 2]
        a integer [3, 3, 1]
        b integer [2, 3, 1]
      >

  ## Grouped examples

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.arrange_with(grouped, &[desc: &1["species"], asc: &1["sepal_width"]])
      #Explorer.DataFrame<
        Polars[150 x 5]
        Groups: ["species"]
        sepal_length float [4.5, 4.4, 4.9, 4.8, 4.3, ...]
        sepal_width float [2.3, 2.9, 3.0, 3.0, 3.0, ...]
        petal_length float [1.3, 1.4, 1.4, 1.4, 1.1, ...]
        petal_width float [0.3, 0.2, 0.2, 0.1, 0.1, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
      >
  """
  @doc type: :single
  @spec arrange_with(
          df :: DataFrame.t(),
          (Explorer.Backend.LazyFrame.t() ->
             Series.lazy_t() | [Series.lazy_t()] | [{:asc | :desc, Series.lazy_t()}])
        ) :: DataFrame.t()
  def arrange_with(%DataFrame{} = df, fun) when is_function(fun, 1) do
    ldf = Explorer.Backend.LazyFrame.new(df)

    result = fun.(ldf)

    dir_and_lazy_series_pairs =
      result
      |> List.wrap()
      |> Enum.map(fn
        {dir, %Series{data: %LazySeries{} = lazy_series}} when dir in [:asc, :desc] ->
          {dir, lazy_series}

        {wrong_dir, %Series{data: %LazySeries{}}} ->
          raise "expecting a valid direction, which is :asc or :desc, got: #{inspect(wrong_dir)}"

        {_, other} ->
          raise "expecting a lazy series, got: #{inspect(other)}"

        %Series{data: %LazySeries{} = lazy_series} ->
          {:asc, lazy_series}

        other ->
          raise "not a valid lazy series or arrange instruction: #{inspect(other)}"
      end)

    Shared.apply_impl(df, :arrange_with, [df, dir_and_lazy_series_pairs])
  end

  @doc """
  Takes distinct rows by a selection of columns.

  Distinct is not affected by groups, although groups are kept in the
  columns selection if `keep_all` option is false (the default).

  ## Options

    * `:keep_all` - If set to `true`, keep all columns. Default is `false`.

  ## Examples

  By default will return unique values of the requested columns:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.distinct(df, ["year", "country"])
      #Explorer.DataFrame<
        Polars[1094 x 2]
        year integer [2010, 2010, 2010, 2010, 2010, ...]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
      >

  If `keep_all` is set to `true`, then the first value of each column not in the requested
  columns will be returned:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.distinct(df, ["year", "country"], keep_all: true)
      #Explorer.DataFrame<
        Polars[1094 x 10]
        year integer [2010, 2010, 2010, 2010, 2010, ...]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
        total integer [2308, 1254, 32500, 141, 7924, ...]
        solid_fuel integer [627, 117, 332, 0, 0, ...]
        liquid_fuel integer [1601, 953, 12381, 141, 3649, ...]
        gas_fuel integer [74, 7, 14565, 0, 374, ...]
        cement integer [5, 177, 2598, 0, 204, ...]
        gas_flaring integer [0, 0, 2623, 0, 3697, ...]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, ...]
        bunker_fuels integer [9, 7, 663, 0, 321, ...]
      >

  A callback on the dataframe's names can be passed instead of a list (like `select/2`):

      iex> df = Explorer.DataFrame.new(x1: [1, 3, 3], x2: ["a", "c", "c"], y1: [1, 2, 3])
      iex> Explorer.DataFrame.distinct(df, &String.starts_with?(&1, "x"))
      #Explorer.DataFrame<
        Polars[2 x 2]
        x1 integer [1, 3]
        x2 string ["a", "c"]
      >

  If the dataframe has groups, then the columns of each group will be added to the distinct columns:

      iex> df = Explorer.DataFrame.new(x1: [1, 3, 3], x2: ["a", "c", "c"], y1: [1, 2, 3])
      iex> df = Explorer.DataFrame.group_by(df, "x1")
      iex> Explorer.DataFrame.distinct(df, ["x2"])
      #Explorer.DataFrame<
        Polars[2 x 2]
        Groups: ["x1"]
        x1 integer [1, 3]
        x2 string ["a", "c"]
      >

  """
  @doc type: :single
  @spec distinct(df :: DataFrame.t(), columns :: columns(), opts :: Keyword.t()) :: DataFrame.t()
  def distinct(df, columns \\ 0..-1//1, opts \\ [])

  def distinct(df, columns, opts) do
    opts = Keyword.validate!(opts, keep_all: false)

    columns = to_existing_columns(df, columns)

    if columns != [] do
      out_df =
        if opts[:keep_all] do
          df
        else
          groups = df.groups
          keep = if groups == [], do: columns, else: Enum.uniq(groups ++ columns)
          %{df | names: keep, dtypes: Map.take(df.dtypes, keep)}
        end

      Shared.apply_impl(df, :distinct, [out_df, columns])
    else
      df
    end
  end

  @doc """
  Drop nil values.

  Optionally accepts a subset of columns.

  ## Examples

  To drop nils on all columns:

      iex> df = Explorer.DataFrame.new(a: [1, 2, nil], b: [1, nil, 3])
      iex> Explorer.DataFrame.drop_nil(df)
      #Explorer.DataFrame<
        Polars[1 x 2]
        a integer [1]
        b integer [1]
      >

   To drop nils on a single column:

      iex> df = Explorer.DataFrame.new(a: [1, 2, nil], b: [1, nil, 3])
      iex> Explorer.DataFrame.drop_nil(df, :a)
      #Explorer.DataFrame<
        Polars[2 x 2]
        a integer [1, 2]
        b integer [1, nil]
      >

  To drop some columns:

      iex> df = Explorer.DataFrame.new(a: [1, 2, nil], b: [1, nil, 3], c: [nil, 5, 6])
      iex> Explorer.DataFrame.drop_nil(df, [:a, :c])
      #Explorer.DataFrame<
        Polars[1 x 3]
        a integer [2]
        b integer [nil]
        c integer [5]
      >

  Ranges, regexes, and functions are also accepted in column names, as in `select/2`.
  """
  @doc type: :single
  @spec drop_nil(df :: DataFrame.t(), column() | columns()) ::
          DataFrame.t()
  def drop_nil(df, columns_or_column \\ 0..-1//1)

  def drop_nil(df, column) when is_column(column), do: drop_nil(df, [column])

  def drop_nil(df, []), do: df

  def drop_nil(df, columns) do
    columns = to_existing_columns(df, columns)
    Shared.apply_impl(df, :drop_nil, [columns])
  end

  @doc """
  Relocates columns.

  Change column order within a DataFrame. The `before` and `after` options are mutually exclusive.
  Providing no options will relocate the columns to beginning of the DataFrame.

  ## Options

    * `:before` - Specifies to relocate before the given column.
      It can be an index or a column name.

    * `:after` - Specifies to relocate after the given column.
      It can be an index or a column name.

  ## Examples

  Relocate a single column

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "a"], b: [1, 3, 1], c: [nil, 5, 6])
      iex> Explorer.DataFrame.relocate(df, "a", after: "c")
      #Explorer.DataFrame<
        Polars[3 x 3]
        b integer [1, 3, 1]
        c integer [nil, 5, 6]
        a string ["a", "b", "a"]
      >

  Relocate (and reorder) multiple columns to the beginning

      iex> df = Explorer.DataFrame.new(a: [1, 2], b: [5.1, 5.2], c: [4, 5], d: ["yes", "no"])
      iex> Explorer.DataFrame.relocate(df, ["d", 1], before: 0)
      #Explorer.DataFrame<
        Polars[2 x 4]
        d string ["yes", "no"]
        b float [5.1, 5.2]
        a integer [1, 2]
        c integer [4, 5]
      >

  Relocate before another column

      iex> df = Explorer.DataFrame.new(a: [1, 2], b: [5.1, 5.2], c: [4, 5], d: ["yes", "no"])
      iex> Explorer.DataFrame.relocate(df, ["a", "c"], before: "b")
      #Explorer.DataFrame<
        Polars[2 x 4]
        a integer [1, 2]
        c integer [4, 5]
        b float [5.1, 5.2]
        d string ["yes", "no"]
      >
  """

  @doc type: :single
  @spec relocate(
          df :: DataFrame.t(),
          columns :: [column()] | column(),
          opts :: Keyword.t()
        ) :: DataFrame.t()

  def relocate(df, columns_or_column, opts)

  def relocate(df, column, opts) when is_column(column),
    do: relocate(df, [column], opts)

  def relocate(df, columns, opts) do
    opts = Keyword.validate!(opts, before: nil, after: nil)

    columns = to_existing_columns(df, columns) |> Enum.uniq()

    columns =
      case {opts[:before], opts[:after]} do
        {before_col, nil} ->
          {:before, before_col}

        {nil, after_col} ->
          {:after, after_col}

        {before_col, after_col} ->
          raise(
            ArgumentError,
            "only one location must be given. Got both " <>
              "before: #{inspect(before_col)} and after: #{inspect(after_col)}"
          )
      end
      |> relocate_columns(df, columns)

    select(df, columns)
  end

  defp relocate_columns({direction, target_column}, df, columns_to_relocate) do
    [target_column] = to_existing_columns(df, [target_column])

    offset =
      case direction do
        :before -> 0
        :after -> 1
      end

    target_index = Enum.find_index(df.names, fn col -> col == target_column end) + offset

    df.names
    |> Enum.split(target_index)
    |> Kernel.then(fn {before_cols, after_cols} ->
      Enum.reject(before_cols, &(&1 in columns_to_relocate)) ++
        columns_to_relocate ++ Enum.reject(after_cols, &(&1 in columns_to_relocate))
    end)
  end

  @doc """
  Renames columns.

  Renaming a column that is also a group is going to rename the group as well.
  To apply a function to a subset of columns, see `rename_with/3`.

  ## Examples

  You can pass in a list of new names:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "a"], b: [1, 3, 1])
      iex> Explorer.DataFrame.rename(df, ["c", "d"])
      #Explorer.DataFrame<
        Polars[3 x 2]
        c string ["a", "b", "a"]
        d integer [1, 3, 1]
      >

  Or you can rename individual columns using keyword args:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "a"], b: [1, 3, 1])
      iex> Explorer.DataFrame.rename(df, a: "first")
      #Explorer.DataFrame<
        Polars[3 x 2]
        first string ["a", "b", "a"]
        b integer [1, 3, 1]
      >

  Or you can rename individual columns using a map:

      iex> df = Explorer.DataFrame.new(a: ["a", "b", "a"], b: [1, 3, 1])
      iex> Explorer.DataFrame.rename(df, %{"a" => "first"})
      #Explorer.DataFrame<
        Polars[3 x 2]
        first string ["a", "b", "a"]
        b integer [1, 3, 1]
      >

  """
  @doc type: :single
  @spec rename(
          df :: DataFrame.t(),
          names :: column_names() | column_pairs(column_name())
        ) ::
          DataFrame.t()
  def rename(df, [name | _] = names) when is_column_name(name) do
    check_new_names_length!(df, names)
    rename(df, Enum.zip(df.names, names))
  end

  def rename(df, names) when is_column_pairs(names) do
    case to_column_pairs(df, names, &to_column_name(&1)) do
      [] ->
        df

      pairs ->
        pairs_map = Map.new(pairs)
        old_dtypes = df.dtypes

        for {name, _} <- pairs do
          Shared.maybe_raise_column_not_found(df, name)
        end

        new_dtypes =
          for name <- df.names do
            {Map.get(pairs_map, name, name), Map.fetch!(old_dtypes, name)}
          end

        new_names = Enum.map(new_dtypes, &elem(&1, 0))

        new_groups =
          for group <- df.groups do
            Map.get(pairs_map, group, group)
          end

        out_df = %{df | names: new_names, dtypes: Map.new(new_dtypes), groups: new_groups}
        Shared.apply_impl(df, :rename, [out_df, pairs])
    end
  end

  defp check_new_names_length!(df, names) do
    width = n_columns(df)
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

  Renaming a column that is also a group is going to rename the group as well.

  ## Examples

  If no columns are specified, it will apply the function to all column names:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.rename_with(df, &String.upcase/1)
      #Explorer.DataFrame<
        Polars[1094 x 10]
        YEAR integer [2010, 2010, 2010, 2010, 2010, ...]
        COUNTRY string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
        TOTAL integer [2308, 1254, 32500, 141, 7924, ...]
        SOLID_FUEL integer [627, 117, 332, 0, 0, ...]
        LIQUID_FUEL integer [1601, 953, 12381, 141, 3649, ...]
        GAS_FUEL integer [74, 7, 14565, 0, 374, ...]
        CEMENT integer [5, 177, 2598, 0, 204, ...]
        GAS_FLARING integer [0, 0, 2623, 0, 3697, ...]
        PER_CAPITA float [0.08, 0.43, 0.9, 1.68, 0.37, ...]
        BUNKER_FUELS integer [9, 7, 663, 0, 321, ...]
      >

  A callback can be used to filter the column names that will be renamed, similarly to `select/2`:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.rename_with(df, &String.ends_with?(&1, "_fuel"), &String.trim_trailing(&1, "_fuel"))
      #Explorer.DataFrame<
        Polars[1094 x 10]
        year integer [2010, 2010, 2010, 2010, 2010, ...]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
        total integer [2308, 1254, 32500, 141, 7924, ...]
        solid integer [627, 117, 332, 0, 0, ...]
        liquid integer [1601, 953, 12381, 141, 3649, ...]
        gas integer [74, 7, 14565, 0, 374, ...]
        cement integer [5, 177, 2598, 0, 204, ...]
        gas_flaring integer [0, 0, 2623, 0, 3697, ...]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, ...]
        bunker_fuels integer [9, 7, 663, 0, 321, ...]
      >

  Or you can just pass in the list of column names you'd like to apply the function to:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.rename_with(df, ["total", "cement"], &String.upcase/1)
      #Explorer.DataFrame<
        Polars[1094 x 10]
        year integer [2010, 2010, 2010, 2010, 2010, ...]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
        TOTAL integer [2308, 1254, 32500, 141, 7924, ...]
        solid_fuel integer [627, 117, 332, 0, 0, ...]
        liquid_fuel integer [1601, 953, 12381, 141, 3649, ...]
        gas_fuel integer [74, 7, 14565, 0, 374, ...]
        CEMENT integer [5, 177, 2598, 0, 204, ...]
        gas_flaring integer [0, 0, 2623, 0, 3697, ...]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, ...]
        bunker_fuels integer [9, 7, 663, 0, 321, ...]
      >

  Ranges, regexes, and functions are also accepted in column names, as in `select/2`.
  """
  @doc type: :single
  @spec rename_with(df :: DataFrame.t(), columns :: columns(), callback :: function()) ::
          DataFrame.t()
  def rename_with(df, columns \\ 0..-1//1, callback)

  def rename_with(df, 0..-1//1, callback) when is_function(callback) do
    df.names
    |> Enum.map(callback)
    |> then(&rename(df, &1))
  end

  def rename_with(df, columns, callback) when is_function(callback) do
    columns = to_existing_columns(df, columns)
    renames = for name <- df.names, name in columns, do: {name, callback.(name)}
    rename(df, renames)
  end

  @doc """
  Turns a set of columns to dummy variables.

  In case the dataframe is using groups, all groups will be removed.

  ## Examples

  To mark a single column as dummy:

      iex> df = Explorer.DataFrame.new(col_x: ["a", "b", "a", "c"], col_y: ["b", "a", "b", "d"])
      iex> Explorer.DataFrame.dummies(df, "col_x")
      #Explorer.DataFrame<
        Polars[4 x 3]
        col_x_a integer [1, 0, 1, 0]
        col_x_b integer [0, 1, 0, 0]
        col_x_c integer [0, 0, 0, 1]
      >

  Or multiple columns:

      iex> df = Explorer.DataFrame.new(col_x: ["a", "b", "a", "c"], col_y: ["b", "a", "b", "d"])
      iex> Explorer.DataFrame.dummies(df, ["col_x", "col_y"])
      #Explorer.DataFrame<
        Polars[4 x 6]
        col_x_a integer [1, 0, 1, 0]
        col_x_b integer [0, 1, 0, 0]
        col_x_c integer [0, 0, 0, 1]
        col_y_b integer [1, 0, 1, 0]
        col_y_a integer [0, 1, 0, 0]
        col_y_d integer [0, 0, 0, 1]
      >

  Or all string columns:

      iex> df = Explorer.DataFrame.new(num: [1, 2, 3, 4], col_y: ["b", "a", "b", "d"])
      iex> Explorer.DataFrame.dummies(df, fn _name, type -> type == :string end)
      #Explorer.DataFrame<
        Polars[4 x 3]
        col_y_b integer [1, 0, 1, 0]
        col_y_a integer [0, 1, 0, 0]
        col_y_d integer [0, 0, 0, 1]
      >

  Ranges, regexes, and functions are also accepted in column names, as in `select/2`.
  """
  @doc type: :single
  @spec dummies(df :: DataFrame.t(), column() | columns()) ::
          DataFrame.t()
  def dummies(df, columns_or_column)

  def dummies(df, column) when is_column(column), do: dummies(df, [column])

  def dummies(df, columns) do
    columns = to_existing_columns(df, columns)

    out_columns =
      for column <- columns,
          value <- Series.to_list(Series.distinct(df[column])),
          do: column <> "_#{value}"

    out_dtypes = for new_column <- out_columns, into: %{}, do: {new_column, :integer}

    out_df = %{df | groups: [], names: out_columns, dtypes: out_dtypes}
    Shared.apply_impl(df, :dummies, [out_df, columns])
  end

  @doc """
  Extracts a single column as a series.

  This is equivalent to `df[field]` for retrieving a single field.
  The returned series will have its `:name` field set to the column name.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.pull(df, "total")
      #Explorer.Series<
        Polars[1094]
        integer [2308, 1254, 32500, 141, 7924, 41, 143, 51246, 1150, 684, 106589, 18408, 8366, 451, 7981, 16345, 403, 17192, 30222, 147, 1388, 166, 133, 5802, 1278, 114468, 47, 2237, 12030, 535, 58, 1367, 145806, 152, 152, 72, 141, 19703, 2393248, 20773, 44, 540, 19, 2064, 1900, 5501, 10465, 2102, 30428, 18122, ...]
      >

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.pull(df, 2)
      #Explorer.Series<
        Polars[1094]
        integer [2308, 1254, 32500, 141, 7924, 41, 143, 51246, 1150, 684, 106589, 18408, 8366, 451, 7981, 16345, 403, 17192, 30222, 147, 1388, 166, 133, 5802, 1278, 114468, 47, 2237, 12030, 535, 58, 1367, 145806, 152, 152, 72, 141, 19703, 2393248, 20773, 44, 540, 19, 2064, 1900, 5501, 10465, 2102, 30428, 18122, ...]
      >
  """
  @doc type: :single
  @spec pull(df :: DataFrame.t(), column :: column()) :: Series.t()
  def pull(df, column) when is_column(column) do
    [column] = to_existing_columns(df, [column])
    pull_existing(df, column)
  end

  defp pull_existing(df, column) do
    series = Shared.apply_impl(df, :pull, [column])
    %{series | name: column}
  end

  @doc """
  Subset a continuous set of rows.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.slice(df, 1, 2)
      #Explorer.DataFrame<
        Polars[2 x 10]
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
        Polars[2 x 10]
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
        Polars[10 x 10]
        year integer [2014, 2014, 2014, 2014, 2014, ...]
        country string ["UNITED STATES OF AMERICA", "URUGUAY", "UZBEKISTAN", "VANUATU", "VENEZUELA", ...]
        total integer [1432855, 1840, 28692, 42, 50510, ...]
        solid_fuel integer [450047, 2, 1677, 0, 204, ...]
        liquid_fuel integer [576531, 1700, 2086, 42, 28445, ...]
        gas_fuel integer [390719, 25, 23929, 0, 12731, ...]
        cement integer [11314, 112, 1000, 0, 1088, ...]
        gas_flaring integer [4244, 0, 0, 0, 8042, ...]
        per_capita float [4.43, 0.54, 0.97, 0.16, 1.65, ...]
        bunker_fuels integer [30722, 251, 0, 10, 1256, ...]
      >

  ## Grouped examples

  We want to take the first 3 rows of each group. We need the offset 0 and the length 3:

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.slice(grouped, 0, 3)
      #Explorer.DataFrame<
        Polars[9 x 5]
        Groups: ["species"]
        sepal_length float [5.1, 4.9, 4.7, 7.0, 6.4, ...]
        sepal_width float [3.5, 3.0, 3.2, 3.2, 3.2, ...]
        petal_length float [1.4, 1.4, 1.3, 4.7, 4.5, ...]
        petal_width float [0.2, 0.2, 0.2, 1.4, 1.5, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-versicolor", "Iris-versicolor", ...]
      >

  We can also pass a negative offset:

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.slice(grouped, -6, 3)
      #Explorer.DataFrame<
        Polars[9 x 5]
        Groups: ["species"]
        sepal_length float [5.1, 4.8, 5.1, 5.6, 5.7, ...]
        sepal_width float [3.8, 3.0, 3.8, 2.7, 3.0, ...]
        petal_length float [1.9, 1.4, 1.6, 4.2, 4.2, ...]
        petal_width float [0.4, 0.3, 0.2, 1.3, 1.2, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-versicolor", "Iris-versicolor", ...]
      >

  """
  @doc type: :rows
  def slice(df, offset, length) when is_integer(offset) and is_integer(length) and length >= 0,
    do: Shared.apply_impl(df, :slice, [offset, length])

  @doc """
  Slices rows at the given indices as a new dataframe.

  The indices may be a list or series of indices, or a range.
  A list of indices does not support negative numbers.
  Ranges may be negative on either end, which are then
  normalized. Note ranges in Elixir are inclusive.

  Slice works differently when a dataframe is grouped.
  It is going to consider the indices of each group
  instead of the entire dataframe. See the examples below.

  If your intention is to grab a portion of each group,
  prefer to use `sample/3` instead.

  ## Examples

      iex> df = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> Explorer.DataFrame.slice(df, [0, 2])
      #Explorer.DataFrame<
        Polars[2 x 2]
        a integer [1, 3]
        b string ["a", "c"]
      >

  With a series

      iex> df = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> Explorer.DataFrame.slice(df, Explorer.Series.from_list([0, 2]))
      #Explorer.DataFrame<
        Polars[2 x 2]
        a integer [1, 3]
        b string ["a", "c"]
      >

  With a range:

      iex> df = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> Explorer.DataFrame.slice(df, 1..2)
      #Explorer.DataFrame<
        Polars[2 x 2]
        a integer [2, 3]
        b string ["b", "c"]
      >

  With a range with negative first and last:

      iex> df = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> Explorer.DataFrame.slice(df, -2..-1)
      #Explorer.DataFrame<
        Polars[2 x 2]
        a integer [2, 3]
        b string ["b", "c"]
      >

  ## Grouped examples

  We are going to once again use the Iris dataset.
  In this example we want to take elements at indexes
  0 and 2:

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.slice(grouped, [0, 2])
      #Explorer.DataFrame<
        Polars[6 x 5]
        Groups: ["species"]
        sepal_length float [5.1, 4.7, 7.0, 6.9, 6.3, ...]
        sepal_width float [3.5, 3.2, 3.2, 3.1, 3.3, ...]
        petal_length float [1.4, 1.3, 4.7, 4.9, 6.0, ...]
        petal_width float [0.2, 0.2, 1.4, 1.5, 2.5, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-versicolor", "Iris-versicolor", "Iris-virginica", ...]
      >

  Now we want to take the first 3 rows of each group.
  This is going to work with the range `0..2`:

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.slice(grouped, 0..2)
      #Explorer.DataFrame<
        Polars[9 x 5]
        Groups: ["species"]
        sepal_length float [5.1, 4.9, 4.7, 7.0, 6.4, ...]
        sepal_width float [3.5, 3.0, 3.2, 3.2, 3.2, ...]
        petal_length float [1.4, 1.4, 1.3, 4.7, 4.5, ...]
        petal_width float [0.2, 0.2, 0.2, 1.4, 1.5, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-versicolor", "Iris-versicolor", ...]
      >

  """
  @doc type: :rows
  def slice(%DataFrame{groups: []} = df, row_indices) when is_list(row_indices) do
    n_rows = n_rows(df)

    Enum.each(row_indices, fn idx ->
      if idx > n_rows or idx < -n_rows,
        do:
          raise(
            ArgumentError,
            "requested row index (#{idx}) out of bounds (-#{n_rows}:#{n_rows})"
          )
    end)

    Shared.apply_impl(df, :slice, [row_indices])
  end

  def slice(%DataFrame{} = df, %Series{dtype: :integer} = indices) do
    Shared.apply_impl(df, :slice, [indices])
  end

  def slice(%DataFrame{groups: []} = df, first..last//1) do
    first = if first < 0, do: first + n_rows(df), else: first
    last = if last < 0, do: last + n_rows(df), else: last
    size = last - first + 1

    if first >= 0 and size >= 0 do
      Shared.apply_impl(df, :slice, [first, size])
    else
      Shared.apply_impl(df, :slice, [[]])
    end
  end

  def slice(%DataFrame{groups: []} = df, %Range{} = range) do
    slice(df, Enum.slice(0..(n_rows(df) - 1)//1, range))
  end

  def slice(%DataFrame{groups: [_ | _]} = df, row_indices) when is_list(row_indices),
    do: Shared.apply_impl(df, :slice, [row_indices])

  def slice(%DataFrame{groups: [_ | _]} = df, %Range{} = range),
    do: Shared.apply_impl(df, :slice, [range])

  @doc """
  Sample rows from a dataframe.

  If given an integer as the second argument, it will return N samples. If given a float, it will
  return that proportion of the series.

  Can sample with or without replacement.

  For grouped dataframes, sample will take into account the rows of each group, meaning that
  if you try to get N samples and you have G groups, you will get N * G rows. See the examples
  below.

  ## Options

    * `:replace` - If set to `true`, each sample will be independent and therefore
      values may repeat. Required to be `true` for `n` greater then the number of rows
      in the dataframe or `frac` > 1.0. (default: `false`)

    * `:seed` - An integer to be used as a random seed. If nil, a random value between 0
      and 2^64 - 1 will be used. (default: `nil`)

    * `:shuffle` - If set to `true`, the resultant dataframe is going to be shuffle
      if the sample is equal to the size of the dataframe. (default: `false`)

  ## Examples

  You can sample N rows:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.sample(df, 3, seed: 100)
      #Explorer.DataFrame<
        Polars[3 x 10]
        year integer [2011, 2012, 2011]
        country string ["SERBIA", "FALKLAND ISLANDS (MALVINAS)", "SWAZILAND"]
        total integer [13422, 15, 286]
        solid_fuel integer [9355, 3, 102]
        liquid_fuel integer [2537, 12, 184]
        gas_fuel integer [1188, 0, 0]
        cement integer [342, 0, 0]
        gas_flaring integer [0, 0, 0]
        per_capita float [1.49, 5.21, 0.24]
        bunker_fuels integer [39, 0, 1]
      >

  Or you can sample a proportion of rows:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.sample(df, 0.03, seed: 100)
      #Explorer.DataFrame<
        Polars[32 x 10]
        year integer [2011, 2012, 2012, 2013, 2010, ...]
        country string ["URUGUAY", "FRENCH POLYNESIA", "ICELAND", "PERU", "TUNISIA", ...]
        total integer [2117, 222, 491, 15586, 7543, ...]
        solid_fuel integer [1, 0, 96, 784, 15, ...]
        liquid_fuel integer [1943, 222, 395, 7097, 3138, ...]
        gas_fuel integer [40, 0, 0, 3238, 3176, ...]
        cement integer [132, 0, 0, 1432, 1098, ...]
        gas_flaring integer [0, 0, 0, 3036, 116, ...]
        per_capita float [0.63, 0.81, 1.52, 0.51, 0.71, ...]
        bunker_fuels integer [401, 45, 170, 617, 219, ...]
      >

  ## Grouped examples

  In the following example we have the Iris dataset grouped by species, and we want
  to take a sample of two plants from each group. Since we have three species, the
  resultant dataframe is going to have six rows (2 * 3).

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.sample(grouped, 2, seed: 100)
      #Explorer.DataFrame<
        Polars[6 x 5]
        Groups: ["species"]
        sepal_length float [5.3, 5.1, 5.1, 5.6, 6.2, ...]
        sepal_width float [3.7, 3.8, 2.5, 2.7, 3.4, ...]
        petal_length float [1.5, 1.9, 3.0, 4.2, 5.4, ...]
        petal_width float [0.2, 0.4, 1.1, 1.3, 2.3, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-versicolor", "Iris-versicolor", "Iris-virginica", ...]
      >

  The behaviour is similar when you want to take a fraction of the rows from each group. The main
  difference is that each group can have more or less rows, depending on its size.

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.sample(grouped, 0.1, seed: 100)
      #Explorer.DataFrame<
        Polars[15 x 5]
        Groups: ["species"]
        sepal_length float [5.3, 5.1, 4.7, 5.7, 5.1, ...]
        sepal_width float [3.7, 3.8, 3.2, 3.8, 3.5, ...]
        petal_length float [1.5, 1.9, 1.3, 1.7, 1.4, ...]
        petal_width float [0.2, 0.4, 0.2, 0.3, 0.3, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
      >

  """
  @doc type: :rows
  @spec sample(df :: DataFrame.t(), n_or_frac :: number(), opts :: Keyword.t()) :: DataFrame.t()
  def sample(df, n_or_frac, opts \\ [])

  def sample(df, n_or_frac, opts) when is_number(n_or_frac) do
    opts = Keyword.validate!(opts, replace: false, shuffle: false, seed: nil)

    if groups(df) == [] do
      n_rows = n_rows(df)
      n = if is_integer(n_or_frac), do: n_or_frac, else: round(n_or_frac * n_rows)

      if n > n_rows && opts[:replace] == false do
        raise ArgumentError,
              "in order to sample more rows than are in the dataframe (#{n_rows}), sampling " <>
                "`replace` must be true"
      end
    end

    Shared.apply_impl(df, :sample, [n_or_frac, opts[:replace], opts[:shuffle], opts[:seed]])
  end

  @doc """
  Change the order of the rows of a dataframe randomly.

  This function is going to ignore groups.

  ## Options

    * `:seed` - An integer to be used as a random seed. If nil, a random value between 0
      and 2^64 - 1 will be used. (default: `nil`)

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.shuffle(df, seed: 100)
      #Explorer.DataFrame<
        Polars[1094 x 10]
        year integer [2014, 2014, 2014, 2012, 2010, ...]
        country string ["ISRAEL", "ARGENTINA", "NETHERLANDS", "YEMEN", "GRENADA", ...]
        total integer [17617, 55638, 45624, 5091, 71, ...]
        solid_fuel integer [6775, 1588, 9070, 129, 0, ...]
        liquid_fuel integer [6013, 25685, 18272, 4173, 71, ...]
        gas_fuel integer [3930, 26368, 18010, 414, 0, ...]
        cement integer [898, 1551, 272, 375, 0, ...]
        gas_flaring integer [0, 446, 0, 0, 0, ...]
        per_capita float [2.22, 1.29, 2.7, 0.2, 0.68, ...]
        bunker_fuels integer [1011, 2079, 14210, 111, 4, ...]
      >

  """
  @doc type: :rows
  @spec shuffle(df :: DataFrame.t(), opts :: Keyword.t()) :: DataFrame.t()
  def shuffle(df, opts \\ [])

  def shuffle(df, opts) do
    opts = Keyword.validate!(opts, seed: nil)

    sample(df, 1.0, seed: opts[:seed], shuffle: true)
  end

  @doc """
  Pivot data from wide to long.

  `pivot_longer/3` "lengthens" data, increasing the number of rows and
  decreasing the number of columns. The inverse transformation is `pivot_wider/4`.

  The second argument, `columns_to_pivot`, can be either list of column names to pivot.
  Ranges, regexes, and functions are also accepted in column names, as in `select/2`.
  The selected columns must always have the same data type.

  In case the dataframe is using groups, the groups that are also in the list of columns
  to pivot will be removed from the resultant dataframe. See the examples below.

  ## Options

    * `:select` - Columns that are not in the list of pivot and should be kept in the dataframe.
      Ranges, regexes, and functions are also accepted in column names, as in `select/2`.
      Defaults to all columns except the ones to pivot.

    * `:discard` - Columns that are not in the list of pivot and should be dropped from the dataframe.
      Ranges, regexes, and functions are also accepted in column names, as in `select/2`.
      This list of columns is going to be subtracted from the list of `select`.
      Defaults to an empty list.

    * `:names_to` - A string specifying the name of the column to create from the data stored
      in the column names of the dataframe. Defaults to `"variable"`.

    * `:values_to` - A string specifying the name of the column to create from the data stored
      in series element values. Defaults to `"value"`.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.pivot_longer(df, &String.ends_with?(&1, "fuel"))
      #Explorer.DataFrame<
        Polars[3282 x 9]
        year integer [2010, 2010, 2010, 2010, 2010, ...]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
        total integer [2308, 1254, 32500, 141, 7924, ...]
        cement integer [5, 177, 2598, 0, 204, ...]
        gas_flaring integer [0, 0, 2623, 0, 3697, ...]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, ...]
        bunker_fuels integer [9, 7, 663, 0, 321, ...]
        variable string ["solid_fuel", "solid_fuel", "solid_fuel", "solid_fuel", "solid_fuel", ...]
        value integer [627, 117, 332, 0, 0, ...]
      >

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.pivot_longer(df, &String.ends_with?(&1, "fuel"), select: ["year", "country"])
      #Explorer.DataFrame<
        Polars[3282 x 4]
        year integer [2010, 2010, 2010, 2010, 2010, ...]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
        variable string ["solid_fuel", "solid_fuel", "solid_fuel", "solid_fuel", "solid_fuel", ...]
        value integer [627, 117, 332, 0, 0, ...]
      >

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.pivot_longer(df, ["total"], select: ["year", "country"], discard: ["country"])
      #Explorer.DataFrame<
        Polars[1094 x 3]
        year integer [2010, 2010, 2010, 2010, 2010, ...]
        variable string ["total", "total", "total", "total", "total", ...]
        value integer [2308, 1254, 32500, 141, 7924, ...]
      >

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.pivot_longer(df, ["total"], select: [], names_to: "my_var", values_to: "my_value")
      #Explorer.DataFrame<
        Polars[1094 x 2]
        my_var string ["total", "total", "total", "total", "total", ...]
        my_value integer [2308, 1254, 32500, 141, 7924, ...]
      >

  ## Grouped examples

  In the following example we want to take the Iris dataset and increase the number of rows by
  pivoting the "sepal_length" column. This dataset is grouped by "species", so the resultant
  dataframe is going to keep the "species" group:

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.pivot_longer(grouped, ["sepal_length"])
      #Explorer.DataFrame<
        Polars[150 x 6]
        Groups: ["species"]
        sepal_width float [3.5, 3.0, 3.2, 3.1, 3.6, ...]
        petal_length float [1.4, 1.4, 1.3, 1.5, 1.4, ...]
        petal_width float [0.2, 0.2, 0.2, 0.2, 0.2, ...]
        species string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
        variable string ["sepal_length", "sepal_length", "sepal_length", "sepal_length", "sepal_length", ...]
        value float [5.1, 4.9, 4.7, 4.6, 5.0, ...]
      >

  Now we want to do something different: we want to pivot the "species" column that is also a group.
  This is going to remove the group in the resultant dataframe:

      iex> df = Explorer.Datasets.iris()
      iex> grouped = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.pivot_longer(grouped, ["species"])
      #Explorer.DataFrame<
        Polars[150 x 6]
        sepal_length float [5.1, 4.9, 4.7, 4.6, 5.0, ...]
        sepal_width float [3.5, 3.0, 3.2, 3.1, 3.6, ...]
        petal_length float [1.4, 1.4, 1.3, 1.5, 1.4, ...]
        petal_width float [0.2, 0.2, 0.2, 0.2, 0.2, ...]
        variable string ["species", "species", "species", "species", "species", ...]
        value string ["Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", "Iris-setosa", ...]
      >

  """
  @doc type: :single
  @spec pivot_longer(
          df :: DataFrame.t(),
          columns_to_pivot :: columns(),
          opts :: Keyword.t()
        ) :: DataFrame.t()
  def pivot_longer(df, columns_to_pivot, opts \\ [])

  def pivot_longer(df, columns_to_pivot, opts) do
    opts =
      Keyword.validate!(opts,
        select: 0..-1//1,
        discard: [],
        names_to: "variable",
        values_to: "value"
      )

    names_to = to_column_name(opts[:names_to])
    values_to = to_column_name(opts[:values_to])

    columns_to_pivot = to_existing_columns(df, columns_to_pivot)
    dtypes = df.dtypes

    columns_to_keep =
      case opts[:select] do
        keep when is_list(keep) ->
          Enum.each(keep, fn column ->
            if column in columns_to_pivot do
              raise ArgumentError,
                    "selected columns must not include columns to pivot, but found #{inspect(column)} in both"
            end
          end)

          to_existing_columns(df, keep)

        keep ->
          to_existing_columns(df, keep)
      end

    columns_to_keep =
      (columns_to_keep -- columns_to_pivot) -- to_existing_columns(df, opts[:discard])

    values_dtype =
      dtypes
      |> Map.take(columns_to_pivot)
      |> Map.values()
      |> Enum.uniq()
      |> case do
        [dtype] ->
          dtype

        [_ | _] = dtypes ->
          raise ArgumentError,
                "columns to pivot must include columns with the same dtype, but found multiple dtypes: #{inspect(dtypes)}"
      end

    new_dtypes =
      dtypes
      |> Map.take(columns_to_keep)
      |> Map.put(names_to, :string)
      |> Map.put(values_to, values_dtype)

    out_df = %{
      df
      | names: columns_to_keep ++ [names_to, values_to],
        dtypes: new_dtypes,
        groups: df.groups -- columns_to_pivot
    }

    args = [out_df, columns_to_pivot, columns_to_keep, names_to, values_to]
    Shared.apply_impl(df, :pivot_longer, args)
  end

  @doc """
  Pivot data from long to wide.

  `pivot_wider/4` "widens" data, increasing the number of columns and decreasing the number of rows.
  The inverse transformation is `pivot_longer/3`.

  Due to a restriction upstream, `values_from` must be a numeric type.

  In case the dataframe is using groups, the groups that are also in the list of columns
  to pivot will be removed from the resultant dataframe. See the examples below.

  ## Options

  * `:id_columns` - A set of columns that uniquely identifies each observation.

    Defaults to all columns in data except for the columns specified in `names_from` and `values_from`,
    and columns that are of the `:float` dtype.

    Typically used when you have redundant variables, i.e. variables whose values are perfectly correlated
    with existing variables. May accept a filter callback, a list or a range of column names.
    Default value is `0..-1//1`. If an empty list is passed, or a range that results in a empty list of
    column names, it raises an error.

    ID columns cannot be of the float type and any columns of this dtype is discarded.
    If you need to use float columns as IDs, you must carefully consider rounding
    or truncating the column and converting it to integer, as long as doing so
    preserves the properties of the column.

  * `:names_prefix` - String added to the start of every variable name.
    This is particularly useful if `names_from` is a numeric vector and you want to create syntactic variable names.

  ## Examples

  Suppose we have a basketball court and multiple teams that want to train in that court. They need
  to share a schedule with the hours each team is going to use it. Here is a dataframe representing
  that schedule:

      iex> Explorer.DataFrame.new(
      iex>   weekday: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
      iex>   team: ["A", "B", "C", "A", "B", "C", "A", "B", "C", "A"],
      iex>   hour: [10, 9, 10, 10, 11, 15, 14, 16, 14, 16]
      iex> )

  This dataframe is going to look like this - using `table/2`:

        +----------------------------------------------+
        |  Explorer DataFrame: [rows: 10, columns: 3]  |
        +---------------+--------------+---------------+
        |    weekday    |     team     |     hour      |
        |   <string>    |   <string>   |   <integer>   |
        +===============+==============+===============+
        | Monday        | A            | 10            |
        +---------------+--------------+---------------+
        | Tuesday       | B            | 9             |
        +---------------+--------------+---------------+
        | Wednesday     | C            | 10            |
        +---------------+--------------+---------------+
        | Thursday      | A            | 10            |
        +---------------+--------------+---------------+
        | Friday        | B            | 11            |
        +---------------+--------------+---------------+
        | Monday        | C            | 15            |
        +---------------+--------------+---------------+
        | Tuesday       | A            | 14            |
        +---------------+--------------+---------------+
        | Wednesday     | B            | 16            |
        +---------------+--------------+---------------+
        | Thursday      | C            | 14            |
        +---------------+--------------+---------------+
        | Friday        | A            | 16            |
        +---------------+--------------+---------------+

  You can see that the "weekday" repeats, and it's not clear how free the agenda is.
  We can solve that by pivoting the "weekday" column in multiple columns, making each weekday
  a new column in the resultant dataframe.

      iex> df = Explorer.DataFrame.new(
      iex>   weekday: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
      iex>   team: ["A", "B", "C", "A", "B", "C", "A", "B", "C", "A"],
      iex>   hour: [10, 9, 10, 10, 11, 15, 14, 16, 14, 16]
      iex> )
      iex> Explorer.DataFrame.pivot_wider(df, "weekday", "hour")
      #Explorer.DataFrame<
        Polars[3 x 6]
        team string ["A", "B", "C"]
        Monday integer [10, nil, 15]
        Tuesday integer [14, 9, nil]
        Wednesday integer [nil, 16, 10]
        Thursday integer [10, nil, 14]
        Friday integer [16, 11, nil]
      >

  Now if we print that same dataframe with `table/2`, we get a better picture of the schedule:

        +----------------------------------------------------------------------+
        |              Explorer DataFrame: [rows: 3, columns: 6]               |
        +----------+-----------+-----------+-----------+-----------+-----------+
        |   team   |  Monday   |  Tuesday  | Wednesday | Thursday  |  Friday   |
        | <string> | <integer> | <integer> | <integer> | <integer> | <integer> |
        +==========+===========+===========+===========+===========+===========+
        | A        | 10        | 14        |           | 10        | 16        |
        +----------+-----------+-----------+-----------+-----------+-----------+
        | B        |           | 9         | 16        |           | 11        |
        +----------+-----------+-----------+-----------+-----------+-----------+
        | C        | 15        |           | 10        | 14        |           |
        +----------+-----------+-----------+-----------+-----------+-----------+

  Pivot wider can create unpredictable column names, and sometimes they can conflict with ID columns.
  In that scenario, we add a number as suffix to duplicated column names. Here is an example:

      iex> df = Explorer.DataFrame.new(
      iex>   product_id: [1, 1, 1, 1, 2, 2, 2, 2],
      iex>   property: ["product_id", "width_cm", "height_cm", "length_cm", "product_id", "width_cm", "height_cm", "length_cm"],
      iex>   property_value: [1, 42, 40, 64, 2, 35, 20, 40]
      iex> )
      iex> Explorer.DataFrame.pivot_wider(df, "property", "property_value")
      #Explorer.DataFrame<
        Polars[2 x 5]
        product_id integer [1, 2]
        product_id_1 integer [1, 2]
        width_cm integer [42, 35]
        height_cm integer [40, 20]
        length_cm integer [64, 40]
      >

  But if the option `:names_prefix` is used, that suffix is not added:

      iex> df = Explorer.DataFrame.new(
      iex>   product_id: [1, 1, 1, 1, 2, 2, 2, 2],
      iex>   property: ["product_id", "width_cm", "height_cm", "length_cm", "product_id", "width_cm", "height_cm", "length_cm"],
      iex>   property_value: [1, 42, 40, 64, 2, 35, 20, 40]
      iex> )
      iex> Explorer.DataFrame.pivot_wider(df, "property", "property_value", names_prefix: "col_")
      #Explorer.DataFrame<
        Polars[2 x 5]
        product_id integer [1, 2]
        col_product_id integer [1, 2]
        col_width_cm integer [42, 35]
        col_height_cm integer [40, 20]
        col_length_cm integer [64, 40]
      >

  Multiple columns are accepted for the `values_from` parameter, but the behaviour is slightly
  different for the naming of new columns in the resultant dataframe. The new columns are going
  to be prefixed by the name of the original value column, followed by an underscore and the
  original column name, followed by the name of the variable.

      iex> df = Explorer.DataFrame.new(
      iex>   product_id: [1, 1, 1, 1, 2, 2, 2, 2],
      iex>   property: ["product_id", "width_cm", "height_cm", "length_cm", "product_id", "width_cm", "height_cm", "length_cm"],
      iex>   property_value: [1, 42, 40, 64, 2, 35, 20, 40],
      iex>   another_value: [1, 43, 41, 65, 2, 36, 21, 42]
      iex> )
      iex> Explorer.DataFrame.pivot_wider(df, "property", ["property_value", "another_value"])
      #Explorer.DataFrame<
        Polars[2 x 9]
        product_id integer [1, 2]
        property_value_property_product_id integer [1, 2]
        property_value_property_width_cm integer [42, 35]
        property_value_property_height_cm integer [40, 20]
        property_value_property_length_cm integer [64, 40]
        another_value_property_product_id integer [1, 2]
        another_value_property_width_cm integer [43, 36]
        another_value_property_height_cm integer [41, 21]
        another_value_property_length_cm integer [65, 42]
      >

  ## Grouped examples

  Now using the same idea, we can see that there is not much difference for grouped dataframes.
  The only detail is that groups that are not ID columns are discarded.

      iex> df = Explorer.DataFrame.new(
      iex>   weekday: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
      iex>   team: ["A", "B", "C", "A", "B", "C", "A", "B", "C", "A"],
      iex>   hour: [10, 9, 10, 10, 11, 15, 14, 16, 14, 16]
      iex> )
      iex> grouped = Explorer.DataFrame.group_by(df, "team")
      iex> Explorer.DataFrame.pivot_wider(grouped, "weekday", "hour")
      #Explorer.DataFrame<
        Polars[3 x 6]
        Groups: ["team"]
        team string ["A", "B", "C"]
        Monday integer [10, nil, 15]
        Tuesday integer [14, 9, nil]
        Wednesday integer [nil, 16, 10]
        Thursday integer [10, nil, 14]
        Friday integer [16, 11, nil]
      >

  In the following example the group "weekday" is going to be removed, because the column is going
  to be pivoted in multiple columns:

      iex> df = Explorer.DataFrame.new(
      iex>   weekday: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
      iex>   team: ["A", "B", "C", "A", "B", "C", "A", "B", "C", "A"],
      iex>   hour: [10, 9, 10, 10, 11, 15, 14, 16, 14, 16]
      iex> )
      iex> grouped = Explorer.DataFrame.group_by(df, "weekday")
      iex> Explorer.DataFrame.pivot_wider(grouped, "weekday", "hour")
      #Explorer.DataFrame<
        Polars[3 x 6]
        team string ["A", "B", "C"]
        Monday integer [10, nil, 15]
        Tuesday integer [14, 9, nil]
        Wednesday integer [nil, 16, 10]
        Thursday integer [10, nil, 14]
        Friday integer [16, 11, nil]
      >

  """
  @doc type: :single
  @spec pivot_wider(
          df :: DataFrame.t(),
          names_from :: column(),
          values_from :: column() | columns(),
          opts ::
            Keyword.t()
        ) :: DataFrame.t()

  def pivot_wider(df, names_from, values_from, opts \\ [])

  def pivot_wider(df, names_from, values_from, opts) when is_column(values_from) do
    pivot_wider(df, names_from, [values_from], opts)
  end

  def pivot_wider(df, names_from, values_from, opts) when is_list(values_from) do
    opts = Keyword.validate!(opts, id_columns: 0..-1//1, names_prefix: "")

    [names_from | values_from] = to_existing_columns(df, [names_from | values_from])
    dtypes = df.dtypes

    for column <- values_from do
      unless dtypes[column] in [:integer, :float, :date, :datetime, :category] do
        raise ArgumentError,
              "the values_from column must be numeric, but found #{dtypes[values_from]}"
      end
    end

    id_columns =
      for column_name <- to_existing_columns(df, opts[:id_columns]) -- [names_from | values_from],
          df.dtypes[column_name] != :float,
          do: column_name

    if id_columns == [] do
      raise ArgumentError,
            "id_columns must select at least one existing column, but #{inspect(opts[:id_columns])} selects none. " <>
              "Note that float columns are discarded from the selection."
    end

    out_df =
      Shared.apply_impl(df, :pivot_wider, [
        id_columns,
        names_from,
        values_from,
        opts[:names_prefix]
      ])

    %{out_df | groups: Enum.filter(df.groups, &(&1 in id_columns))}
  end

  # Two table verbs

  @valid_join_types [:inner, :left, :right, :outer, :cross]

  @doc """
  Join two tables.

  ## Join types

    * `:inner` - Returns all rows from `left` where there are matching values in `right`, and all columns from `left` and `right`.
    * `:left` - Returns all rows from `left` and all columns from `left` and `right`. Rows in `left` with no match in `right` will have `nil` values in the new columns.
    * `:right` - Returns all rows from `right` and all columns from `left` and `right`. Rows in `right` with no match in `left` will have `nil` values in the new columns.
    * `:outer` - Returns all rows and all columns from both `left` and `right`. Where there are not matching values, returns `nil` for the one missing.
    * `:cross` - Also known as a cartesian join. Returns all combinations of `left` and `right`. Can be very computationally expensive.

  ## Options

    * `:on` - The columns to join on. Defaults to overlapping columns. Does not apply to cross join.
    * `:how` - One of the join types (as an atom) described above. Defaults to `:inner`.

  ## Examples

  Inner join:

      iex> left = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.new(a: [1, 2, 2], c: ["d", "e", "f"])
      iex> Explorer.DataFrame.join(left, right)
      #Explorer.DataFrame<
        Polars[3 x 3]
        a integer [1, 2, 2]
        b string ["a", "b", "b"]
        c string ["d", "e", "f"]
      >

  Left join:

      iex> left = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.new(a: [1, 2, 2], c: ["d", "e", "f"])
      iex> Explorer.DataFrame.join(left, right, how: :left)
      #Explorer.DataFrame<
        Polars[4 x 3]
        a integer [1, 2, 2, 3]
        b string ["a", "b", "b", "c"]
        c string ["d", "e", "f", nil]
      >

  Right join:

      iex> left = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.new(a: [1, 2, 4], c: ["d", "e", "f"])
      iex> Explorer.DataFrame.join(left, right, how: :right)
      #Explorer.DataFrame<
        Polars[3 x 3]
        a integer [1, 2, 4]
        c string ["d", "e", "f"]
        b string ["a", "b", nil]
      >

  Outer join:

      iex> left = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.new(a: [1, 2, 4], c: ["d", "e", "f"])
      iex> Explorer.DataFrame.join(left, right, how: :outer)
      #Explorer.DataFrame<
        Polars[4 x 3]
        a integer [1, 2, 4, 3]
        b string ["a", "b", nil, "c"]
        c string ["d", "e", "f", nil]
      >

  Cross join:

      iex> left = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.new(a: [1, 2, 4], c: ["d", "e", "f"])
      iex> Explorer.DataFrame.join(left, right, how: :cross)
      #Explorer.DataFrame<
        Polars[9 x 4]
        a integer [1, 1, 1, 2, 2, ...]
        b string ["a", "a", "a", "b", "b", ...]
        a_right integer [1, 2, 4, 1, 2, ...]
        c string ["d", "e", "f", "d", "e", ...]
      >

  Inner join with different names:

      iex> left = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.new(d: [1, 2, 2], c: ["d", "e", "f"])
      iex> Explorer.DataFrame.join(left, right, on: [{"a", "d"}])
      #Explorer.DataFrame<
        Polars[3 x 3]
        a integer [1, 2, 2]
        b string ["a", "b", "b"]
        c string ["d", "e", "f"]
      >

  ## Grouped examples

  When doing a join operation with grouped dataframes, the joined dataframe
  may keep the groups from only one side.

  An inner join operation will keep the groups from the left-hand side dataframe:

      iex> left = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.new(a: [1, 2, 2], c: ["d", "e", "f"])
      iex> grouped_left = Explorer.DataFrame.group_by(left, "b")
      iex> grouped_right = Explorer.DataFrame.group_by(right, "c")
      iex> Explorer.DataFrame.join(grouped_left, grouped_right)
      #Explorer.DataFrame<
        Polars[3 x 3]
        Groups: ["b"]
        a integer [1, 2, 2]
        b string ["a", "b", "b"]
        c string ["d", "e", "f"]
      >

  A left join operation will keep the groups from the left-hand side dataframe:

      iex> left = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.new(a: [1, 2, 2], c: ["d", "e", "f"])
      iex> grouped_left = Explorer.DataFrame.group_by(left, "b")
      iex> grouped_right = Explorer.DataFrame.group_by(right, "c")
      iex> Explorer.DataFrame.join(grouped_left, grouped_right, how: :left)
      #Explorer.DataFrame<
        Polars[4 x 3]
        Groups: ["b"]
        a integer [1, 2, 2, 3]
        b string ["a", "b", "b", "c"]
        c string ["d", "e", "f", nil]
      >

  A right join operation will keep the groups from the right-hand side dataframe:

      iex> left = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.new(a: [1, 2, 4], c: ["d", "e", "f"])
      iex> grouped_left = Explorer.DataFrame.group_by(left, "b")
      iex> grouped_right = Explorer.DataFrame.group_by(right, "c")
      iex> Explorer.DataFrame.join(grouped_left, grouped_right, how: :right)
      #Explorer.DataFrame<
        Polars[3 x 3]
        Groups: ["c"]
        a integer [1, 2, 4]
        c string ["d", "e", "f"]
        b string ["a", "b", nil]
      >

  An outer join operation is going to keep the groups from the left-hand side dataframe:

      iex> left = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.new(a: [1, 2, 4], c: ["d", "e", "f"])
      iex> grouped_left = Explorer.DataFrame.group_by(left, "b")
      iex> grouped_right = Explorer.DataFrame.group_by(right, "c")
      iex> Explorer.DataFrame.join(grouped_left, grouped_right, how: :outer)
      #Explorer.DataFrame<
        Polars[4 x 3]
        Groups: ["b"]
        a integer [1, 2, 4, 3]
        b string ["a", "b", nil, "c"]
        c string ["d", "e", "f", nil]
      >

  A cross join operation is going to keep the groups from the left-hand side dataframe:

      iex> left = Explorer.DataFrame.new(a: [1, 2, 3], b: ["a", "b", "c"])
      iex> right = Explorer.DataFrame.new(a: [1, 2, 4], c: ["d", "e", "f"])
      iex> grouped_left = Explorer.DataFrame.group_by(left, "b")
      iex> grouped_right = Explorer.DataFrame.group_by(right, "c")
      iex> Explorer.DataFrame.join(grouped_left, grouped_right, how: :cross)
      #Explorer.DataFrame<
        Polars[9 x 4]
        Groups: ["b"]
        a integer [1, 1, 1, 2, 2, ...]
        b string ["a", "a", "a", "b", "b", ...]
        a_right integer [1, 2, 4, 1, 2, ...]
        c string ["d", "e", "f", "d", "e", ...]
      >

  """
  @doc type: :multi
  @spec join(left :: DataFrame.t(), right :: DataFrame.t(), opts :: Keyword.t()) :: DataFrame.t()
  def join(%DataFrame{} = left, %DataFrame{} = right, opts \\ []) do
    left_columns = left.names
    right_columns = right.names

    opts =
      Keyword.validate!(opts,
        on: find_overlapping_columns(left_columns, right_columns),
        how: :inner
      )

    unless opts[:how] in @valid_join_types do
      raise ArgumentError,
            "join type is not valid: #{inspect(opts[:how])}. " <>
              "Valid options are: #{Enum.map_join(@valid_join_types, ", ", &inspect/1)}"
    end

    {on, how} =
      case {opts[:on], opts[:how]} do
        {[], :cross} ->
          {[], :cross}

        {[], _} ->
          raise(ArgumentError, "could not find any overlapping columns")

        {[_ | _] = on, how} ->
          normalized_on =
            Enum.map(on, fn
              {l_name, r_name} ->
                [l_column] = to_existing_columns(left, [l_name])
                [r_column] = to_existing_columns(right, [r_name])
                {l_column, r_column}

              name ->
                [l_column] = to_existing_columns(left, [name])
                [r_column] = to_existing_columns(right, [name])

                # This is an edge case for when an index is passed as column selection
                if l_column != r_column do
                  raise ArgumentError,
                        "the column given to option `:on` is not the same for both dataframes"
                end

                {l_column, r_column}
            end)

          {normalized_on, how}
      end

    out_df = out_df_for_join(how, left, right, on)

    Shared.apply_impl(left, :join, [right, out_df, on, how])
  end

  defp find_overlapping_columns(left_columns, right_columns) do
    left_columns = MapSet.new(left_columns)
    right_columns = MapSet.new(right_columns)
    left_columns |> MapSet.intersection(right_columns) |> MapSet.to_list()
  end

  defp out_df_for_join(:right, left, right, on) do
    {left_on, _right_on} = Enum.unzip(on)

    pairs = dtypes_pairs_for_common_join(right, left, left_on, "_left")

    {new_names, _} = Enum.unzip(pairs)
    %{right | names: new_names, dtypes: Map.new(pairs)}
  end

  defp out_df_for_join(how, left, right, on) do
    {_left_on, right_on} = Enum.unzip(on)

    right_on = if how == :cross, do: [], else: right_on

    pairs = dtypes_pairs_for_common_join(left, right, right_on)

    {new_names, _} = Enum.unzip(pairs)
    %{left | names: new_names, dtypes: Map.new(pairs)}
  end

  defp dtypes_pairs_for_common_join(left, right, right_on, suffix \\ "_right") do
    Enum.map(left.names, fn name -> {name, left.dtypes[name]} end) ++
      Enum.map(right.names -- right_on, fn right_name ->
        name =
          if right_name in left.names do
            right_name <> suffix
          else
            right_name
          end

        {name, right.dtypes[right_name]}
      end)
  end

  @doc """
  Combine two or more dataframes column-wise.

  This function expects the dataframes to have the same number of rows,
  otherwise rows may be silently discarded. Eager backends may check
  whenever this happens and raise instead of silently fail. But this may not
  be possible for lazy dataframes as the number of rows is not known upfront.

  When working with grouped dataframes, be aware that only groups from the first
  dataframe are kept in the resultant dataframe.

  ## Examples

      iex> df1 = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      iex> df2 = Explorer.DataFrame.new(z: [4, 5, 6], a: ["d", "e", "f"])
      iex> Explorer.DataFrame.concat_columns([df1, df2])
      #Explorer.DataFrame<
        Polars[3 x 4]
        x integer [1, 2, 3]
        y string ["a", "b", "c"]
        z integer [4, 5, 6]
        a string ["d", "e", "f"]
      >

  Conflicting names are suffixed with the index of the dataframe in the array:

      iex> df1 = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      iex> df2 = Explorer.DataFrame.new(x: [4, 5, 6], a: ["d", "e", "f"])
      iex> Explorer.DataFrame.concat_columns([df1, df2])
      #Explorer.DataFrame<
        Polars[3 x 4]
        x integer [1, 2, 3]
        y string ["a", "b", "c"]
        x_1 integer [4, 5, 6]
        a string ["d", "e", "f"]
      >

  """
  @doc type: :multi
  @spec concat_columns([DataFrame.t()]) :: DataFrame.t()
  def concat_columns([%DataFrame{} = head | tail] = dfs) do
    {names, dtypes} =
      Enum.reduce(Enum.with_index(tail, 1), {head.names, head.dtypes}, fn {df, idx},
                                                                          {names, dtypes} ->
        new_names_and_dtypes =
          for name <- df.names do
            if name in names do
              {name <> "_#{idx}", df.dtypes[name]}
            else
              {name, df.dtypes[name]}
            end
          end

        new_names = for {name, _} <- new_names_and_dtypes, do: name

        {names ++ new_names, Map.merge(dtypes, Map.new(new_names_and_dtypes))}
      end)

    out_df = %{head | names: names, dtypes: dtypes}

    Shared.apply_impl(dfs, :concat_columns, [out_df])
  end

  @doc """
  Combine two dataframes column-wise.

  When working with grouped dataframes, be aware that only groups from the left-hand side
  dataframe are kept in the resultant dataframe.

  `concat_columns(df1, df2)` is equivalent to `concat_columns([df1, df2])`.
  """
  @doc type: :multi
  @spec concat_columns(DataFrame.t(), DataFrame.t()) :: DataFrame.t()
  def concat_columns(%DataFrame{} = df1, %DataFrame{} = df2), do: concat_columns([df1, df2])
  def concat_columns(%DataFrame{} = df, [%DataFrame{} | _] = dfs), do: concat_columns([df | dfs])

  @doc """
  Combine two or more dataframes row-wise (stack).

  Column names and dtypes must match. The only exception is for numeric
  columns that can be mixed together, and casted automatically to float columns.

  When working with grouped dataframes, be aware that only groups from the first
  dataframe are kept in the resultant dataframe.

  ## Examples

      iex> df1 = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      iex> df2 = Explorer.DataFrame.new(x: [4, 5, 6], y: ["d", "e", "f"])
      iex> Explorer.DataFrame.concat_rows([df1, df2])
      #Explorer.DataFrame<
        Polars[6 x 2]
        x integer [1, 2, 3, 4, 5, ...]
        y string ["a", "b", "c", "d", "e", ...]
      >

      iex> df1 = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      iex> df2 = Explorer.DataFrame.new(x: [4.2, 5.3, 6.4], y: ["d", "e", "f"])
      iex> Explorer.DataFrame.concat_rows([df1, df2])
      #Explorer.DataFrame<
        Polars[6 x 2]
        x float [1.0, 2.0, 3.0, 4.2, 5.3, ...]
        y string ["a", "b", "c", "d", "e", ...]
      >
  """
  @doc type: :multi
  @spec concat_rows([DataFrame.t()]) :: DataFrame.t()
  def concat_rows([%DataFrame{} = head | _tail] = dfs) do
    changed_types = compute_changed_types_concat_rows(dfs)
    out_df = %{head | dtypes: Map.merge(head.dtypes, changed_types)}

    dfs =
      if Enum.empty?(changed_types) do
        dfs
      else
        cast_numeric_columns_to_float(dfs, changed_types)
      end

    Shared.apply_impl(dfs, :concat_rows, [out_df])
  end

  defp compute_changed_types_concat_rows([head | tail]) do
    types = head.dtypes

    Enum.reduce(tail, %{}, fn df, changed_types ->
      if n_columns(df) != map_size(types) do
        raise ArgumentError,
              "dataframes must have the same columns"
      end

      Enum.reduce(df.dtypes, changed_types, fn {name, type}, changed_types ->
        cond do
          not Map.has_key?(types, name) ->
            raise ArgumentError,
                  "dataframes must have the same columns"

          types[name] == type ->
            changed_types

          types_are_numeric_compatible?(types, name, type) ->
            Map.put(changed_types, name, :float)

          true ->
            raise ArgumentError,
                  "columns and dtypes must be identical for all dataframes"
        end
      end)
    end)
  end

  defp types_are_numeric_compatible?(types, name, type) do
    numeric_types = [:float, :integer]
    types[name] != type and types[name] in numeric_types and type in numeric_types
  end

  defp cast_numeric_columns_to_float(dfs, changed_types) do
    for df <- dfs do
      columns =
        for {name, :integer} <- df.dtypes,
            changed_types[name] == :float,
            do: name

      if Enum.empty?(columns) do
        df
      else
        mutate_with(ungroup(df), fn ldf ->
          for column <- columns, do: {column, Series.cast(ldf[column], :float)}
        end)
      end
    end
  end

  @doc """
  Combine two dataframes row-wise.

  `concat_rows(df1, df2)` is equivalent to `concat_rows([df1, df2])`.

  When working with grouped dataframes, be aware that only groups from the left-hand side
  dataframe are kept in the resultant dataframe.
  """
  @doc type: :multi
  @spec concat_rows(DataFrame.t(), DataFrame.t()) :: DataFrame.t()
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
        Polars[1094 x 10]
        Groups: ["country"]
        year integer [2010, 2010, 2010, 2010, 2010, ...]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
        total integer [2308, 1254, 32500, 141, 7924, ...]
        solid_fuel integer [627, 117, 332, 0, 0, ...]
        liquid_fuel integer [1601, 953, 12381, 141, 3649, ...]
        gas_fuel integer [74, 7, 14565, 0, 374, ...]
        cement integer [5, 177, 2598, 0, 204, ...]
        gas_flaring integer [0, 0, 2623, 0, 3697, ...]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, ...]
        bunker_fuels integer [9, 7, 663, 0, 321, ...]
      >

  Or you can group by multiple columns in a given list:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.group_by(df, ["country", "year"])
      #Explorer.DataFrame<
        Polars[1094 x 10]
        Groups: ["country", "year"]
        year integer [2010, 2010, 2010, 2010, 2010, ...]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
        total integer [2308, 1254, 32500, 141, 7924, ...]
        solid_fuel integer [627, 117, 332, 0, 0, ...]
        liquid_fuel integer [1601, 953, 12381, 141, 3649, ...]
        gas_fuel integer [74, 7, 14565, 0, 374, ...]
        cement integer [5, 177, 2598, 0, 204, ...]
        gas_flaring integer [0, 0, 2623, 0, 3697, ...]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, ...]
        bunker_fuels integer [9, 7, 663, 0, 321, ...]
      >

  Or by a range:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> Explorer.DataFrame.group_by(df, 0..1)
      #Explorer.DataFrame<
        Polars[1094 x 10]
        Groups: ["year", "country"]
        year integer [2010, 2010, 2010, 2010, 2010, ...]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
        total integer [2308, 1254, 32500, 141, 7924, ...]
        solid_fuel integer [627, 117, 332, 0, 0, ...]
        liquid_fuel integer [1601, 953, 12381, 141, 3649, ...]
        gas_fuel integer [74, 7, 14565, 0, 374, ...]
        cement integer [5, 177, 2598, 0, 204, ...]
        gas_flaring integer [0, 0, 2623, 0, 3697, ...]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, ...]
        bunker_fuels integer [9, 7, 663, 0, 321, ...]
      >

  Regexes and functions are also accepted in column names, as in `select/2`.
  """
  @doc type: :single
  @spec group_by(df :: DataFrame.t(), groups_or_group :: column_names() | column_name()) ::
          DataFrame.t()
  def group_by(df, group) when is_column(group), do: group_by(df, [group])

  def group_by(df, groups) do
    groups = to_existing_columns(df, groups)
    all_groups = Enum.uniq(df.groups ++ groups)
    %{df | groups: all_groups}
  end

  @doc """
  Removes grouping variables.

  Accepts a list of group names. If groups is not specified, then all groups are
  removed.

  ## Examples

  Ungroups all by default:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> df = Explorer.DataFrame.group_by(df, ["country", "year"])
      iex> Explorer.DataFrame.ungroup(df)
      #Explorer.DataFrame<
        Polars[1094 x 10]
        year integer [2010, 2010, 2010, 2010, 2010, ...]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
        total integer [2308, 1254, 32500, 141, 7924, ...]
        solid_fuel integer [627, 117, 332, 0, 0, ...]
        liquid_fuel integer [1601, 953, 12381, 141, 3649, ...]
        gas_fuel integer [74, 7, 14565, 0, 374, ...]
        cement integer [5, 177, 2598, 0, 204, ...]
        gas_flaring integer [0, 0, 2623, 0, 3697, ...]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, ...]
        bunker_fuels integer [9, 7, 663, 0, 321, ...]
      >

  Ungrouping a single column:

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> df = Explorer.DataFrame.group_by(df, ["country", "year"])
      iex> Explorer.DataFrame.ungroup(df, "country")
      #Explorer.DataFrame<
        Polars[1094 x 10]
        Groups: ["year"]
        year integer [2010, 2010, 2010, 2010, 2010, ...]
        country string ["AFGHANISTAN", "ALBANIA", "ALGERIA", "ANDORRA", "ANGOLA", ...]
        total integer [2308, 1254, 32500, 141, 7924, ...]
        solid_fuel integer [627, 117, 332, 0, 0, ...]
        liquid_fuel integer [1601, 953, 12381, 141, 3649, ...]
        gas_fuel integer [74, 7, 14565, 0, 374, ...]
        cement integer [5, 177, 2598, 0, 204, ...]
        gas_flaring integer [0, 0, 2623, 0, 3697, ...]
        per_capita float [0.08, 0.43, 0.9, 1.68, 0.37, ...]
        bunker_fuels integer [9, 7, 663, 0, 321, ...]
      >

  Lists, ranges, regexes, and functions are also accepted in column names, as in `select/2`.
  """
  @doc type: :single
  @spec ungroup(df :: DataFrame.t(), groups_or_group :: column_names() | column_name() | :all) ::
          DataFrame.t()
  def ungroup(df, groups \\ 0..-1//1)

  def ungroup(df, 0..-1//1), do: %{df | groups: []}

  def ungroup(df, group) when is_column(group), do: ungroup(df, [group])

  def ungroup(df, groups) do
    current_groups = groups(df)
    groups = to_existing_columns(df, groups)

    Enum.each(groups, fn group ->
      if group not in current_groups do
        raise ArgumentError,
              "could not find #{group} in current groups (#{current_groups})"
      end
    end)

    %{df | groups: current_groups -- groups}
  end

  @doc """
  Summarise each group to a single row using `Explorer.Query`.

  To summarise, you must perform aggregation, defined in `Explorer.Series`,
  on the desired columns. The query is compiled and runs efficiently
  against the dataframe. This function performs aggregations based on groups,
  and the query must contain at least one aggregation.
  It implicitly ungroups the resultant dataframe.

  > #### Notice {: .notice}
  >
  > This is a macro. You must `require  Explorer.DataFrame` before using it.

  See `summarise_with/2` for a callback version of this function without
  `Explorer.Query`.

  ## Examples

      iex> df = Explorer.Datasets.fossil_fuels()
      iex> grouped_df = Explorer.DataFrame.group_by(df, "year")
      iex> Explorer.DataFrame.summarise(grouped_df, total_max: max(total), total_min: min(total))
      #Explorer.DataFrame<
        Polars[5 x 3]
        year integer [2010, 2011, 2012, 2013, 2014]
        total_max integer [2393248, 2654360, 2734817, 2797384, 2806634]
        total_min integer [1, 2, 2, 2, 3]
      >

  Suppose you want to get the mean petal length of each Iris species. You could do something
  like this:

      iex> df = Explorer.Datasets.iris()
      iex> grouped_df = Explorer.DataFrame.group_by(df, "species")
      iex> Explorer.DataFrame.summarise(grouped_df, mean_petal_length: mean(petal_length))
      #Explorer.DataFrame<
        Polars[3 x 2]
        species string ["Iris-setosa", "Iris-versicolor", "Iris-virginica"]
        mean_petal_length float [1.464, 4.26, 5.552]
      >

  In case aggregations for all the dataframe is what you want, you can use ungrouped
  dataframes:

      iex> df = Explorer.Datasets.iris()
      iex> Explorer.DataFrame.summarise(df, mean_petal_length: mean(petal_length))
      #Explorer.DataFrame<
        Polars[1 x 1]
        mean_petal_length float [3.758666666666667]
      >

  """
  @doc type: :single
  defmacro summarise(df, query) do
    quote do
      require Explorer.Query
      Explorer.DataFrame.summarise_with(unquote(df), Explorer.Query.query(unquote(query)))
    end
  end

  @doc """
  Summarise each group to a single row using a callback function.

  In case no group is set, the entire dataframe will be considered.
  The callback receives a lazy dataframe. A lazy dataframe does not
  hold any values, instead it stores all operations in order to
  execute all summarizations performantly.

  This is a callback version of `summarise/2`.

  ## Examples

      iex> alias Explorer.{DataFrame, Series}
      iex> df = Explorer.Datasets.fossil_fuels() |> DataFrame.group_by("year")
      iex> DataFrame.summarise_with(df, &[total_max: Series.max(&1["total"]), countries: Series.n_distinct(&1["country"])])
      #Explorer.DataFrame<
        Polars[5 x 3]
        year integer [2010, 2011, 2012, 2013, 2014]
        total_max integer [2393248, 2654360, 2734817, 2797384, 2806634]
        countries integer [217, 217, 220, 220, 220]
      >

      iex> alias Explorer.{DataFrame, Series}
      iex> df = Explorer.Datasets.fossil_fuels()
      iex> DataFrame.summarise_with(df, &[total_max: Series.max(&1["total"]), countries: Series.n_distinct(&1["country"])])
      #Explorer.DataFrame<
        Polars[1 x 2]
        total_max integer [2806634]
        countries integer [222]
      >

  """
  @doc type: :single
  @spec summarise_with(
          df :: DataFrame.t(),
          callback :: (Explorer.Backend.LazyFrame.t() -> column_pairs(Series.lazy_t()))
        ) :: DataFrame.t()
  def summarise_with(%DataFrame{} = df, fun) when is_function(fun, 1) do
    ldf = Explorer.Backend.LazyFrame.new(df)

    result = fun.(ldf)

    column_pairs =
      to_column_pairs(df, result, fn value ->
        case value do
          %Series{data: %LazySeries{aggregation: true}} ->
            value

          %Series{data: %LazySeries{}} = series ->
            raise "expecting summarise with an aggregation operation, " <>
                    "but no aggregation was found in: #{inspect(series)}"

          other ->
            raise "expecting a lazy series, got: #{inspect(other)}"
        end
      end)

    new_dtypes = names_with_dtypes_for_column_pairs(df, column_pairs)
    new_names = for {name, _} <- new_dtypes, do: name
    df_out = %{df | names: new_names, dtypes: Map.new(new_dtypes), groups: []}

    column_pairs = for {name, %Series{data: lazy_series}} <- column_pairs, do: {name, lazy_series}

    Shared.apply_impl(df, :summarise_with, [df_out, column_pairs])
  end

  defp names_with_dtypes_for_column_pairs(df, column_pairs) do
    groups = for group <- df.groups, do: {group, df.dtypes[group]}

    names_with_dtypes =
      for {column_name, series} <- column_pairs do
        {column_name, series.dtype}
      end

    groups ++ names_with_dtypes
  end

  @doc """
  Display the DataFrame in a tabular fashion.

  ## Examples

     df = Explorer.Datasets.iris()
     Explorer.DataFrame.table(df)
     Explorer.DataFrame.table(df, limit: 1)
     Explorer.DataFrame.table(df, limit: :infinity)
  """
  @doc type: :introspection
  @spec table(df :: DataFrame.t(), opts :: Keyword.t()) :: :ok
  def table(df, opts \\ []) do
    {rows, columns} = shape(df)
    headers = df.names

    df =
      case opts[:limit] do
        :infinity -> df
        nrow when is_integer(nrow) and nrow >= 0 -> slice(df, 0, nrow)
        _ -> slice(df, 0, @default_sample_nrows)
      end

    types = Enum.map(df.names, &"\n<#{Atom.to_string(df.dtypes[&1])}>")

    values =
      headers
      |> Enum.map(&Series.to_list(df[&1]))
      |> Enum.zip_with(& &1)

    name_type = Enum.zip_with(headers, types, fn x, y -> x <> y end)

    TableRex.Table.new()
    |> TableRex.Table.put_title("Explorer DataFrame: [rows: #{rows}, columns: #{columns}]")
    |> TableRex.Table.put_header(name_type)
    |> TableRex.Table.put_header_meta(0..columns, align: :center)
    |> TableRex.Table.add_rows(values)
    |> TableRex.Table.render!(
      header_separator_symbol: "=",
      horizontal_style: :all
    )
    |> IO.puts()
  end

  @doc """
  Describe numeric columns of a DataFrame.

  Groups are ignored if the dataframe is using any.

  ## Options

   * `:percentiles` - Floating point list with the percentiles to be calculated. (default: `[0.25, 0.5, 0.75]`)

  ## Examples

      iex> df = Explorer.DataFrame.new(a: ["d", nil, "f"], b: [1, 2, 3], c: ["a", "b", "c"])
      iex> Explorer.DataFrame.describe(df)
      #Explorer.DataFrame<
        Polars[9 x 4]
        describe string ["count", "null_count", "mean", "std", "min", ...]
        a string ["3", "1", nil, nil, "d", ...]
        b float [3.0, 0.0, 2.0, 1.0, 1.0, ...]
        c string ["3", "0", nil, nil, "a", ...]
      >

      iex> df = Explorer.DataFrame.new(a: ["d", nil, "f"], b: [1, 2, 3], c: ["a", "b", "c"])
      iex> Explorer.DataFrame.describe(df, percentiles: [0.3, 0.5, 0.8])
      #Explorer.DataFrame<
        Polars[9 x 4]
        describe string ["count", "null_count", "mean", "std", "min", ...]
        a string ["3", "1", nil, nil, "d", ...]
        b float [3.0, 0.0, 2.0, 1.0, 1.0, ...]
        c string ["3", "0", nil, nil, "a", ...]
      >
  """
  @doc type: :single
  @spec describe(df :: DataFrame.t(), Keyword.t()) :: DataFrame.t()
  def describe(df, opts \\ []) do
    opts = Keyword.validate!(opts, percentiles: nil)

    Shared.apply_impl(df, :describe, [opts[:percentiles]])
  end

  @doc """
  Counts the number of null elements in each column.

  ## Examples

      iex> df = Explorer.DataFrame.new(a: ["d", nil, "f"], b: [nil, 2, nil], c: ["a", "b", "c"])
      iex> Explorer.DataFrame.nil_count(df)
      #Explorer.DataFrame<
        Polars[1 x 3]
        a integer [1]
        b integer [2]
        c integer [0]
      >
  """
  @doc type: :single
  @spec nil_count(df :: DataFrame.t()) :: DataFrame.t()
  def nil_count(df), do: Shared.apply_impl(df, :nil_count)

  @doc """
  Creates a new dataframe with unique rows and the frequencies of each.

  ## Examples

      iex> df = Explorer.DataFrame.new(a: ["a", "a", "b"], b: [1, 1, nil])
      iex> Explorer.DataFrame.frequencies(df, [:a, :b])
      #Explorer.DataFrame<
        Polars[2 x 3]
        a string ["a", "b"]
        b integer [1, nil]
        counts integer [2, 1]
      >
  """
  @doc type: :single
  @spec frequencies(df :: DataFrame.t(), columns :: column_names()) :: DataFrame.t()
  def frequencies(%DataFrame{} = df, [col | _] = columns) do
    df
    |> group_by(columns)
    |> summarise_with(&[counts: Series.count(&1[col])])
    |> arrange_with(&[desc: &1[:counts]])
  end

  def frequencies(_df, []), do: raise(ArgumentError, "columns cannot be empty")

  # Helpers

  defp backend_from_options!(opts) do
    backend = Explorer.Shared.backend_from_options!(opts) || Explorer.Backend.get()

    if opts[:lazy] do
      :"#{backend}.LazyFrame"
    else
      :"#{backend}.DataFrame"
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(df, opts) do
      force_unfit(
        concat([
          color("#Explorer.DataFrame<", :map, opts),
          nest(
            concat([line(), Shared.apply_impl(df, :inspect, [opts])]),
            2
          ),
          line(),
          color(">", :map, opts)
        ])
      )
    end
  end
end

defimpl Table.Reader, for: Explorer.DataFrame do
  def init(df) do
    columns = Explorer.DataFrame.names(df)

    data =
      Enum.map(columns, fn column ->
        df
        |> Explorer.DataFrame.pull(column)
        |> Explorer.Series.to_enum()
      end)

    {:columns, %{columns: columns, count: Explorer.DataFrame.n_rows(df)}, data}
  end
end
