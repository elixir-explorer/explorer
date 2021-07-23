defmodule Explorer.Backend.DataFrame do
  @moduledoc """
  The behaviour for DataFrame backends.
  """

  @type t :: %{__struct__: atom()}

  @type df :: Explorer.DataFrame.t()
  @type result(t) :: {:ok, t} | {:error, term()}
  @type series :: Explorer.Series.t()
  @type colname :: String.t()

  # IO

  @callback read_csv(
              filename :: String.t(),
              names :: list(String.t()) | nil,
              dtypes :: list(atom()) | nil,
              delimiter :: String.t(),
              null_character :: String.t(),
              skip_rows :: Integer.t(),
              header? :: boolean(),
              encoding :: String.t(),
              max_rows :: Integer.t() | Inf,
              with_columns :: list(String.t()) | nil
            ) :: result(df)
  @callback write_csv(df, filename :: String.t(), header? :: boolean(), delimiter :: String.t()) ::
              result(String.t())

  # Conversion

  @callback from_map(map()) :: df
  @callback to_map(df, convert_series? :: boolean()) :: map()

  # Introspection

  @callback names(df) :: [colname]
  @callback dtypes(df) :: [String.t()]
  @callback shape(df) :: {integer(), integer()}
  @callback n_rows(df) :: integer()
  @callback n_cols(df) :: integer()

  # Single table verbs

  @callback head(df, rows :: integer()) :: df
  @callback tail(df, rows :: integer()) :: df
  @callback select(df, columns :: [colname], :keep | :drop) :: df
  @callback filter(df, mask :: series | list() | function()) :: df
  @callback mutate(df, with_columns :: Keyword.t()) :: df
  @callback arrange(df, columns :: [colname], direction :: :asc | :desc) :: df
  @callback distinct(df, columns :: [colname], keep_all? :: boolean()) :: df
  @callback rename(df, list(colname) | map() | function()) :: df
  @callback dummies(df, columns :: [colname]) :: df
  @callback sample(df, integer() | float(), with_replacement? :: boolean()) :: df
  @callback pull(df, column :: String.t()) :: series
  @callback slice(df, offset :: integer(), length :: integer()) :: df
  @callback take(df, indices :: list(integer())) :: df

  # Two table verbs

  @callback join(
              left :: df,
              right :: df,
              how :: :left | :inner | :outer | :right,
              on ::
                list(String.t())
            ) :: df
end
