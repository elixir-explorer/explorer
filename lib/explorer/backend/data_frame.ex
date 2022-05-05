defmodule Explorer.Backend.DataFrame do
  @moduledoc """
  The behaviour for DataFrame backends.
  """

  @type t :: %{__struct__: atom()}

  @type df :: Explorer.DataFrame.t()
  @type result(t) :: {:ok, t} | {:error, term()}
  @type series :: Explorer.Series.t()
  @type column_name :: String.t()

  # IO

  @callback from_csv(
              filename :: String.t(),
              dtypes :: list({String.t(), atom()}),
              delimiter :: String.t(),
              null_character :: String.t(),
              skip_rows :: integer(),
              header? :: boolean(),
              encoding :: String.t(),
              max_rows :: integer() | nil,
              columns :: list(String.t()) | list(Atom.t()) | list(integer()) | nil,
              infer_schema_length :: integer() | nil,
              parse_dates :: boolean()
            ) :: result(df)
  @callback to_csv(df, filename :: String.t(), header? :: boolean(), delimiter :: String.t()) ::
              result(String.t())

  @callback from_parquet(filename :: String.t()) :: result(df)
  @callback to_parquet(df, filename :: String.t()) :: result(String.t())

  @callback from_ipc(
              filename :: String.t(),
              columns :: list(String.t()) | list(Atom.t()) | list(integer()) | nil
            ) :: result(df)
  @callback to_ipc(df, filename :: String.t(), compression :: String.t()) ::
              result(String.t())

  @callback from_ndjson(
              filename :: String.t(),
              infer_schema_length :: integer(),
              batch_size :: integer()
            ) :: result(df)
  @callback to_ndjson(df, filename :: String.t()) :: result(String.t())

  # Conversion

  @callback from_tabular(Table.Reader.t()) :: df
  @callback from_series(map() | Keyword.t()) :: df
  @callback to_rows(df, atom_keys? :: boolean()) :: [map()]
  @callback dump_csv(df, header? :: boolean(), delimiter :: String.t()) :: String.t()

  # Introspection

  @callback names(df) :: [column_name]
  @callback dtypes(df) :: [String.t()]
  @callback shape(df) :: {integer(), integer()}
  @callback n_rows(df) :: integer()
  @callback n_columns(df) :: integer()

  # Single table verbs

  @callback head(df, rows :: integer()) :: df
  @callback tail(df, rows :: integer()) :: df
  @callback select(df, columns :: [column_name], :keep | :drop) :: df
  @callback filter(df, mask :: series) :: df
  @callback mutate(df, columns :: map()) :: df
  @callback arrange(df, columns :: [column_name | {:asc | :desc, column_name}]) :: df
  @callback distinct(df, columns :: [column_name], keep_all? :: boolean()) :: df
  @callback rename(df, [column_name]) :: df
  @callback dummies(df, columns :: [column_name]) :: df
  @callback sample(df, n :: integer(), replacement :: boolean(), seed :: integer()) :: df
  @callback pull(df, column :: String.t()) :: series
  @callback slice(df, offset :: integer(), length :: integer()) :: df
  @callback take(df, indices :: list(integer())) :: df
  @callback drop_nil(df, columns :: [column_name]) :: df
  @callback pivot_wider(
              df,
              id_columns :: [column_name],
              names_from :: [column_name],
              values_from ::
                [column_name],
              names_prefix :: String.t()
            ) :: df
  @callback pivot_longer(
              df,
              id_columns :: [column_name],
              value_columns :: [column_name],
              names_to :: column_name,
              values_to :: column_name
            ) :: df

  # Two or more table verbs

  @callback join(
              left :: df,
              right :: df,
              on :: list(String.t() | {String.t(), String.t()}),
              how :: :left | :inner | :outer | :right | :cross
            ) :: df

  @callback concat_rows([df]) :: df

  # Groups

  @callback group_by(df, columns :: [column_name]) :: df
  @callback ungroup(df, columns :: [column_name]) :: df
  @callback summarise(df, aggregations :: map()) :: df
end
