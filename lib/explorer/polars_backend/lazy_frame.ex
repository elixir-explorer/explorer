defmodule Explorer.PolarsBackend.LazyFrame do
  @moduledoc false

  alias Explorer.DataFrame
  alias Explorer.PolarsBackend.Shared
  alias Explorer.PolarsBackend.DataFrame, as: PolarsDF

  @type t :: %__MODULE__{resource: binary(), reference: reference()}

  defstruct resource: nil, reference: nil

  @behaviour Explorer.Backend.DataFrame

  @impl true
  defdelegate read_csv(
                filename,
                names,
                dtypes,
                delimiter,
                null_character,
                skip_rows,
                header?,
                encoding,
                max_rows,
                with_columns,
                infer_schema_length,
                parse_dates
              ),
              to: PolarsDF

  @impl true
  def select(lf, columns, :keep) when is_list(columns),
    do: Shared.apply_native(lf, :lf_select, [columns])

  @impl true
  def names(lf),
    do: Shared.apply_native(lf, :lf_fetch, [1]) |> DataFrame.names()

  def collect(lf), do: Shared.apply_native(lf, :lf_collect)
end

defimpl Inspect, for: Explorer.PolarsBackend.LazyFrame do
  alias Explorer.PolarsBackend.Native

  def inspect(lf, _opts) do
    case Native.lf_describe_plan(lf) do
      {:ok, str} -> str
      {:error, error} -> raise "#{error}"
    end
  end
end
