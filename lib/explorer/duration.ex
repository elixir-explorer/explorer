defmodule Explorer.Duration do
  # Internal representation of a duration.
  @moduledoc false
  alias Explorer.Duration

  @enforce_keys [:value, :precision]
  defstruct [:value, :precision]

  # Nanosecond constants
  @us_ns 1_000
  @ms_ns 1_000 * @us_ns
  @sec_ns 1_000 * @ms_ns
  @min_ns 60 * @sec_ns
  @hour_ns 60 * @min_ns
  @day_ns 24 * @hour_ns

  def to_string(%Explorer.Duration{value: value, precision: precision}) do
    case precision do
      :millisecond -> format_nanoseconds(value * @ms_ns)
      :microsecond -> format_nanoseconds(value * @us_ns)
      :nanosecond -> format_nanoseconds(value)
    end
  end

  defp format_nanoseconds(nanoseconds) when is_integer(nanoseconds) do
    result = nanoseconds |> abs |> format_pos_nanoseconds()

    if nanoseconds < 0 do
      "-" <> result
    else
      result
    end
  end

  defp format_pos_nanoseconds(nanoseconds) when is_integer(nanoseconds) and nanoseconds >= 0 do
    [d: @day_ns, h: @hour_ns, m: @min_ns, s: @sec_ns, ms: @ms_ns, us: @us_ns, ns: 1]
    |> Enum.reduce({[], nanoseconds}, fn {unit, ns_per_unit}, {parts, ns} ->
      {num_units, remaining_ns} =
        if ns >= ns_per_unit do
          {div(ns, ns_per_unit), rem(ns, ns_per_unit)}
        else
          {0, ns}
        end

      {[{unit, num_units} | parts], remaining_ns}
    end)
    |> then(fn {parts_reversed, _} -> parts_reversed end)
    |> Enum.reverse()
    |> Enum.reject(fn {_unit, value} -> value == 0 end)
    |> Enum.map_join(" ", fn {unit, value} -> "#{value}#{unit}" end)
    |> case do
      "" -> "0"
      result -> result
    end
  end

  defimpl String.Chars do
    def to_string(%Duration{} = duration), do: Duration.to_string(duration)
  end

  defimpl Inspect do
    def inspect(%Duration{} = duration, _), do: "Duration[" <> Duration.to_string(duration) <> "]"
  end
end
