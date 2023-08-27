defmodule Explorer.Series.DurationTest do
  use ExUnit.Case, async: true

  alias Explorer.Duration
  alias Explorer.Series

  @aug_20 ~D[2023-08-20]
  @aug_21 ~D[2023-08-21]
  @one_hour_ms 3600 * 1_000
  @one_hour_us 3600 * 1_000_000
  @one_hour_duration_ms %Duration{value: @one_hour_ms, precision: :millisecond}
  @one_hour_duration_us %Duration{value: @one_hour_us, precision: :microsecond}
  @one_day_duration_ms %Duration{value: 24 * @one_hour_ms, precision: :millisecond}

  describe "list" do
    test "from a list of integers" do
      ms = Series.from_list([1], dtype: {:duration, :millisecond})
      us = Series.from_list([1_000], dtype: {:duration, :microsecond})
      ns = Series.from_list([1_000_000], dtype: {:duration, :nanosecond})

      # The series have the correct dtypes.
      assert ms.dtype == {:duration, :millisecond}
      assert us.dtype == {:duration, :microsecond}
      assert ns.dtype == {:duration, :nanosecond}

      # The orginal integer is preserved when converting back to a list.
      [%Duration{value: 1}] = Series.to_list(ms)
      [%Duration{value: 1_000}] = Series.to_list(us)
      [%Duration{value: 1_000_000}] = Series.to_list(ns)
    end

    test "from a list of durations" do
      ms = Series.from_list([%Duration{value: 1, precision: :millisecond}])
      us = Series.from_list([%Duration{value: 1_000, precision: :microsecond}])
      ns = Series.from_list([%Duration{value: 1_000_000, precision: :nanosecond}])

      # The series have the correct dtypes.
      assert ms.dtype == {:duration, :millisecond}
      assert us.dtype == {:duration, :microsecond}
      assert ns.dtype == {:duration, :nanosecond}

      # The orginal integer is preserved when converting back to a list.
      [%Duration{value: 1}] = Series.to_list(ms)
      [%Duration{value: 1_000}] = Series.to_list(us)
      [%Duration{value: 1_000_000}] = Series.to_list(ns)
    end

    test "can cast any precision to any other precision" do
      ms = Series.from_list([1], dtype: {:duration, :millisecond})
      us = Series.from_list([1_000], dtype: {:duration, :microsecond})
      ns = Series.from_list([1_000_000], dtype: {:duration, :nanosecond})

      assert ms |> Series.cast({:duration, :microsecond}) |> Series.all_equal(us)
      assert ms |> Series.cast({:duration, :nanosecond}) |> Series.all_equal(ns)
      assert us |> Series.cast({:duration, :millisecond}) |> Series.all_equal(ms)
      assert us |> Series.cast({:duration, :nanosecond}) |> Series.all_equal(ns)
      assert ns |> Series.cast({:duration, :millisecond}) |> Series.all_equal(ms)
      assert ns |> Series.cast({:duration, :microsecond}) |> Series.all_equal(us)
    end

    test "can convert to a list and back without needing the `dtype` option" do
      ms = Series.from_list([1], dtype: {:duration, :millisecond})
      us = Series.from_list([1_000], dtype: {:duration, :microsecond})
      ns = Series.from_list([1_000_000], dtype: {:duration, :nanosecond})

      assert ms |> Series.to_list() |> Series.from_list() |> Series.all_equal(ms)
      assert us |> Series.to_list() |> Series.from_list() |> Series.all_equal(us)
      assert ns |> Series.to_list() |> Series.from_list() |> Series.all_equal(ns)
    end
  end

  describe "io" do
    test "series to and from binary" do
      for precision <- [:millisecond, :microsecond, :nanosecond] do
        dtype = {:duration, precision}
        durations = Series.from_list([100, 101], dtype: dtype)

        [binary] = Series.to_iovec(durations)
        from_binary = Series.from_binary(binary, dtype)

        assert durations.dtype == from_binary.dtype
        assert Series.to_list(durations) == Series.to_list(from_binary)
      end
    end

    test "duration structs to_string similarly to polars" do
      strings = [
        "1ms",
        "10ms",
        "100ms",
        "1s",
        "10s",
        "1m 40s",
        "16m 40s",
        "2h 46m 40s",
        "1d 3h 46m 40s",
        "11d 13h 46m 40s",
        "115d 17h 46m 40s",
        # Like polars, the maximum unit is days so we don't show years.
        "1157d 9h 46m 40s"
      ]

      for {string, power} <- Enum.with_index(strings) do
        assert to_string(%Duration{value: 10 ** power, precision: :millisecond}) == string
      end
    end

    test "duration structs inspect as \"Duration[*]\"" do
      assert inspect(%Duration{value: 1, precision: :millisecond}) == "#Explorer.Duration[1ms]"
    end

    test "in a series, equal values are displayed the same regardless of precision" do
      ms = Series.from_list([1], dtype: {:duration, :millisecond})
      us = Series.from_list([1_000], dtype: {:duration, :microsecond})
      ns = Series.from_list([1_000_000], dtype: {:duration, :nanosecond})

      # Each series displays its values as "[1ms]" as well as the correct precision.
      assert inspect(ms) == """
             #Explorer.Series<
               Polars[1]
               duration[ms] [1ms]
             >\
             """

      assert inspect(us) == """
             #Explorer.Series<
               Polars[1]
               duration[μs] [1ms]
             >\
             """

      assert inspect(ns) == """
             #Explorer.Series<
               Polars[1]
               duration[ns] [1ms]
             >\
             """
    end
  end

  describe "add" do
    # Duration only

    test "duration[μs] + duration[μs]" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      two_hour_s = Series.from_list([2 * @one_hour_us], dtype: {:duration, :microsecond})
      sum_s = Series.add(one_hour_s, two_hour_s)

      three_hour_duration_us = %Duration{value: 3 * @one_hour_us, precision: :microsecond}
      assert sum_s.dtype == {:duration, :microsecond}
      assert Series.to_list(sum_s) == [three_hour_duration_us]
    end

    test "duration[ms] + duration[μs] (different precisions)" do
      one_hour_ms_s = Series.from_list([@one_hour_duration_ms])
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      sum_s = Series.add(one_hour_ms_s, one_hour_us_s)

      # Since we added a duration with :millisecond precision to a datetime with :microsecond
      # precision, the resulting difference has :microsecond precision since that was the highest
      # precision present in the operation.
      assert one_hour_ms_s.dtype == {:duration, :millisecond}
      assert one_hour_us_s.dtype == {:duration, :microsecond}
      assert sum_s.dtype == {:duration, :microsecond}

      two_hour_duration_us = %Duration{value: 2 * @one_hour_us, precision: :microsecond}
      assert Series.to_list(sum_s) == [two_hour_duration_us]
    end

    # Date

    test "date + duration[μs]" do
      aug_20_s = Series.from_list([@aug_20])

      # Adding a duration less than a day results in the same date.
      one_hour_s = Series.from_list([@one_hour_duration_us])
      sum_s = Series.add(aug_20_s, one_hour_s)

      assert sum_s.dtype == :date
      assert Series.to_list(sum_s) == [@aug_20]

      # Adding a duration at least a day results in the next date.
      one_day_s = Series.from_list([24 * @one_hour_us], dtype: {:duration, :microsecond})
      sum_s = Series.add(aug_20_s, one_day_s)

      assert sum_s.dtype == :date
      assert Series.to_list(sum_s) == [@aug_21]
    end

    test "duration[μs] + date" do
      aug_20_s = Series.from_list([@aug_20])

      # Adding a duration less than a day results in the same date.
      one_hour_s = Series.from_list([@one_hour_duration_us])
      sum_s = Series.add(one_hour_s, aug_20_s)

      assert sum_s.dtype == :date
      assert Series.to_list(sum_s) == [@aug_20]

      # Adding a duration at least a day results in the next date.
      one_day_s = Series.from_list([24 * @one_hour_us], dtype: {:duration, :microsecond})
      sum_s = Series.add(one_day_s, aug_20_s)

      assert sum_s.dtype == :date
      assert Series.to_list(sum_s) == [@aug_21]
    end

    test "Date + duration[μs]" do
      # Adding a duration less than a day results in the same date.
      one_hour_s = Series.from_list([@one_hour_duration_us])
      sum_s = Series.add(@aug_20, one_hour_s)

      assert sum_s.dtype == :date
      assert Series.to_list(sum_s) == [@aug_20]

      # Adding a duration at least a day results in the next date.
      one_day_s = Series.from_list([24 * @one_hour_us], dtype: {:duration, :microsecond})
      sum_s = Series.add(@aug_20, one_day_s)

      assert sum_s.dtype == :date
      assert Series.to_list(sum_s) == [@aug_21]
    end

    test "duration[μs] + Date" do
      # Adding a duration less than a day results in the same date.
      one_hour_s = Series.from_list([@one_hour_duration_us])
      sum_s = Series.add(one_hour_s, @aug_20)

      assert sum_s.dtype == :date
      assert Series.to_list(sum_s) == [@aug_20]

      # Adding a duration at least a day results in the next date.
      one_day_s = Series.from_list([24 * @one_hour_us], dtype: {:duration, :microsecond})
      sum_s = Series.add(one_day_s, @aug_20)

      assert sum_s.dtype == :date
      assert Series.to_list(sum_s) == [@aug_21]
    end

    test "Date + Date raises ArgumentError" do
      assert_raise ArgumentError,
                   "add/2 expects a series as one of its arguments, instead got two scalars: ~D[2023-08-20] and ~D[2023-08-21]",
                   fn -> Series.add(@aug_20, @aug_21) end
    end

    test "date + date raises ArgumentError" do
      aug_20_s = Series.from_list([@aug_20])
      aug_21_s = Series.from_list([@aug_21])

      assert_raise ArgumentError,
                   "cannot invoke Explorer.Series.add/2 with mismatched dtypes: :date and :date",
                   fn -> Series.add(aug_20_s, aug_21_s) end
    end

    # Datetime

    test "datetime[μs] + duration[μs]" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      eleven_s = Series.from_list([~N[2023-08-20 11:00:00.0000000]])
      sum_s = Series.add(eleven_s, one_hour_s)

      assert sum_s.dtype == {:datetime, :microsecond}
      twelve_ndt = ~N[2023-08-20 12:00:00.0000000]
      assert Series.to_list(sum_s) == [twelve_ndt]
    end

    test "duration[μs] + datetime[μs]" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      eleven_s = Series.from_list([~N[2023-08-20 11:00:00.0000000]])
      sum_s = Series.add(one_hour_s, eleven_s)

      assert sum_s.dtype == {:datetime, :microsecond}
      twelve_ndt = ~N[2023-08-20 12:00:00.0000000]
      assert Series.to_list(sum_s) == [twelve_ndt]
    end

    test "NaiveDateTime + duration[μs]" do
      eleven = ~N[2023-08-20 11:00:00.0000000]
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      sum_s = Series.add(eleven, one_hour_s)

      assert sum_s.dtype == {:datetime, :microsecond}
      assert Series.to_list(sum_s) == [~N[2023-08-20 12:00:00.0000000]]
    end

    test "duration[μs] + NaiveDateTime" do
      eleven = ~N[2023-08-20 11:00:00.0000000]
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      sum_s = Series.add(one_hour_s, eleven)

      assert sum_s.dtype == {:datetime, :microsecond}
      assert Series.to_list(sum_s) == [~N[2023-08-20 12:00:00.0000000]]
    end

    test "datetime[μs] + duration[ns] (different precisions)" do
      one_hour_ns = 3600 * 1_000_000_000
      one_hour_s = Series.from_list([one_hour_ns], dtype: {:duration, :nanosecond})
      eleven_s = Series.from_list([~N[2023-08-20 11:00:00.0000000]])
      sum_s = Series.add(eleven_s, one_hour_s)

      # Since we added a duration with :nanosecond precision from a datetime with :microsecond
      # precision, the resulting sum has :nanosecond precision since that was the highest
      # precision present in the operation.
      assert sum_s.dtype == {:datetime, :nanosecond}
      assert Series.to_list(sum_s) == [~N[2023-08-20 12:00:00.0000000]]
    end

    test "datetime[μs] + datetime[μs] raises ArgumentError" do
      eleven_s = Series.from_list([~N[2023-08-20 11:00:00]])
      twelve_s = Series.from_list([~N[2023-08-20 12:00:00]])

      assert_raise ArgumentError,
                   "cannot invoke Explorer.Series.add/2 with mismatched dtypes: {:datetime, :microsecond} and {:datetime, :microsecond}",
                   fn -> Series.add(eleven_s, twelve_s) end
    end
  end

  describe "subtract" do
    # Duration only

    test "duration[μs] - duration[μs]" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      two_hour_s = Series.from_list([2 * @one_hour_us], dtype: {:duration, :microsecond})
      diff_s = Series.subtract(two_hour_s, one_hour_s)

      assert diff_s.dtype == {:duration, :microsecond}
      assert Series.to_list(diff_s) == [@one_hour_duration_us]
    end

    test "duration[ms] - duration[μs] (different precisions)" do
      two_hour_us_s = Series.from_list([2 * @one_hour_us], dtype: {:duration, :microsecond})
      one_hour_ms_s = Series.from_list([@one_hour_duration_ms])
      diff_s = Series.subtract(two_hour_us_s, one_hour_ms_s)

      # Since we subtracted a duration with :millisecond precision from a duration with :microsecond
      # precision, the resulting difference has :microsecond precision since that was the highest
      # precision present in the operation.
      assert two_hour_us_s.dtype == {:duration, :microsecond}
      assert one_hour_ms_s.dtype == {:duration, :millisecond}
      assert diff_s.dtype == {:duration, :microsecond}
      assert Series.to_list(diff_s) == [@one_hour_duration_us]
    end

    # Date

    test "date - date" do
      aug_20_s = Series.from_list([@aug_20])
      aug_21_s = Series.from_list([@aug_21])
      diff_s = Series.subtract(aug_21_s, aug_20_s)

      assert diff_s.dtype == {:duration, :millisecond}
      assert Series.to_list(diff_s) == [@one_day_duration_ms]
    end

    test "Date - date" do
      aug_20_s = Series.from_list([@aug_20])
      diff_s = Series.subtract(@aug_21, aug_20_s)

      assert diff_s.dtype == {:duration, :millisecond}
      assert Series.to_list(diff_s) == [@one_day_duration_ms]
    end

    test "date - Date" do
      aug_21_s = Series.from_list([@aug_21])
      diff_s = Series.subtract(aug_21_s, @aug_20)

      assert diff_s.dtype == {:duration, :millisecond}
      assert Series.to_list(diff_s) == [@one_day_duration_ms]
    end

    test "Date - Date raises ArgumentError" do
      assert_raise ArgumentError,
                   "subtract/2 expects a series as one of its arguments, instead got two scalars: ~D[2023-08-21] and ~D[2023-08-20]",
                   fn -> Series.subtract(@aug_21, @aug_20) end
    end

    test "date - duration[ms]" do
      aug_21_s = Series.from_list([@aug_21])

      # Subtracting a duration less than a day results in the same date.
      one_hour_s = Series.from_list([@one_hour_duration_ms])
      diff_s = Series.subtract(aug_21_s, one_hour_s)

      assert diff_s.dtype == :date
      assert Series.to_list(diff_s) == [@aug_20]

      # Subtracting a duration at least a day results in the previous date.
      one_day_s = Series.from_list([@one_day_duration_ms])
      diff_s = Series.subtract(aug_21_s, one_day_s)

      assert diff_s.dtype == :date
      assert Series.to_list(diff_s) == [@aug_20]
    end

    # Datetime

    test "datetime[μs] - datetime[μs]" do
      eleven_s = Series.from_list([~N[2023-08-20 11:00:00.0000000]])
      twelve_s = Series.from_list([~N[2023-08-20 12:00:00.0000000]])
      diff_s = Series.subtract(twelve_s, eleven_s)

      assert diff_s.dtype == {:duration, :microsecond}
      assert Series.to_list(diff_s) == [@one_hour_duration_us]
    end

    test "datetime[μs] - duration[μs]" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      twelve_s = Series.from_list([~N[2023-08-20 12:00:00.0000000]])
      diff_s = Series.subtract(twelve_s, one_hour_s)

      assert diff_s.dtype == {:datetime, :microsecond}
      assert Series.to_list(diff_s) == [~N[2023-08-20 11:00:00.0000000]]
    end

    test "NaiveDateTime - datetime[μs]" do
      eleven_s = Series.from_list([~N[2023-08-20 11:00:00.0000000]])
      twelve = ~N[2023-08-20 12:00:00.0000000]
      diff_s = Series.subtract(twelve, eleven_s)

      assert diff_s.dtype == {:duration, :microsecond}
      assert Series.to_list(diff_s) == [@one_hour_duration_us]
    end

    test "datetime[μs] - NaiveDateTime" do
      eleven_s = Series.from_list([~N[2023-08-20 11:00:00.0000000]])
      twelve = ~N[2023-08-20 12:00:00.0000000]
      diff_s = Series.subtract(eleven_s, twelve)

      assert diff_s.dtype == {:duration, :microsecond}
      assert Series.to_list(diff_s) == [%Duration{value: -@one_hour_us, precision: :microsecond}]
    end

    test "NaiveDateTime - duration[μs]" do
      twelve = ~N[2023-08-20 12:00:00.0000000]
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      diff_s = Series.subtract(twelve, one_hour_s)

      assert diff_s.dtype == {:datetime, :microsecond}
      assert Series.to_list(diff_s) == [~N[2023-08-20 11:00:00.0000000]]
    end

    test "datetime[μs] - datetime[ns] (different precisions)" do
      one_hour_ns = 3600 * 1_000_000_000
      one_hour_s = Series.from_list([one_hour_ns], dtype: {:duration, :nanosecond})
      twelve_s = Series.from_list([~N[2023-08-20 12:00:00.0000000]])
      diff_s = Series.subtract(twelve_s, one_hour_s)

      # Since we subtracted a duration with :nanosecond precision from a datetime with :microsecond
      # precision, the resulting difference has :nanosecond precision since that was the highest
      # precision present in the operation.
      assert diff_s.dtype == {:datetime, :nanosecond}
      assert Series.to_list(diff_s) == [~N[2023-08-20 11:00:00.0000000]]
    end

    test "duration[μs] - datetime[μs] raises ArgumentError" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      twelve_s = Series.from_list([~N[2023-08-20 12:00:00]])

      assert_raise ArgumentError,
                   "cannot invoke Explorer.Series.subtract/2 with mismatched dtypes: {:duration, :microsecond} and {:datetime, :microsecond}",
                   fn -> Series.subtract(one_hour_s, twelve_s) end
    end
  end

  describe "multiply" do
    # Integer

    test "integer * duration[μs]" do
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      two_s = Series.from_list([2])
      product_s = Series.multiply(two_s, one_hour_us_s)

      assert product_s.dtype == {:duration, :microsecond}
      two_hour_duration_s = %Duration{value: 2 * @one_hour_us, precision: :microsecond}
      assert Series.to_list(product_s) == [two_hour_duration_s]
    end

    test "duration[μs] * integer" do
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      two_s = Series.from_list([2])
      product_s = Series.multiply(one_hour_us_s, two_s)

      assert product_s.dtype == {:duration, :microsecond}
      two_hour_duration_s = %Duration{value: 2 * @one_hour_us, precision: :microsecond}
      assert Series.to_list(product_s) == [two_hour_duration_s]
    end

    test "Integer * duration[μs]" do
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      product_s = Series.multiply(2, one_hour_us_s)

      assert product_s.dtype == {:duration, :microsecond}
      two_hour_duration_s = %Duration{value: 2 * @one_hour_us, precision: :microsecond}
      assert Series.to_list(product_s) == [two_hour_duration_s]
    end

    test "duration[μs] * Integer" do
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      product_s = Series.multiply(one_hour_us_s, 2)

      assert product_s.dtype == {:duration, :microsecond}
      two_hour_duration_s = %Duration{value: 2 * @one_hour_us, precision: :microsecond}
      assert Series.to_list(product_s) == [two_hour_duration_s]
    end

    # Float

    test "float * duration[μs]" do
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      two_s = Series.from_list([2.0])
      product_s = Series.multiply(two_s, one_hour_us_s)

      assert product_s.dtype == {:duration, :microsecond}
      two_hour_duration_s = %Duration{value: 2 * @one_hour_us, precision: :microsecond}
      assert Series.to_list(product_s) == [two_hour_duration_s]
    end

    test "duration[μs] * float" do
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      two_s = Series.from_list([2.0])
      product_s = Series.multiply(one_hour_us_s, two_s)

      assert product_s.dtype == {:duration, :microsecond}
      two_hour_duration_s = %Duration{value: 2 * @one_hour_us, precision: :microsecond}
      assert Series.to_list(product_s) == [two_hour_duration_s]
    end

    test "Float * duration[μs]" do
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      product_s = Series.multiply(2.0, one_hour_us_s)

      assert product_s.dtype == {:duration, :microsecond}
      two_hour_duration_s = %Duration{value: 2 * @one_hour_us, precision: :microsecond}
      assert Series.to_list(product_s) == [two_hour_duration_s]
    end

    test "duration[μs] * Float" do
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      product_s = Series.multiply(one_hour_us_s, 2.0)

      assert product_s.dtype == {:duration, :microsecond}
      two_hour_duration_s = %Duration{value: 2 * @one_hour_us, precision: :microsecond}
      assert Series.to_list(product_s) == [two_hour_duration_s]
    end

    test "fractional parts of floats work (roughly) as expected" do
      # This test is not exhaustive. Rather, its purpose is to give us a reasonable confidence that
      # multiplying durations by floats is fairly accurate.
      #
      # The exact answers we see here are subject to implementation details outside our control.
      # If we find that this test breaks unexpectedly (e.g. from a dependency update), then we may
      # wish to remove it.
      one_s = 1 / 3_600
      one_ms = 1 / 3_600_000
      one_us = 1 / 3_600_000_000
      one_ns = 1 / 3_600_000_000_000

      float_string_pairs = [
        {3 / 4, "45m"},
        {3 / 2, "1h 30m"},
        {1.0 + one_s, "1h 1s"},
        # Float rounding issue (but only off by one).
        {1.0 + one_ms, "1h 999us 999ns"},
        {1.0 + one_us, "1h 1us"},
        {1.0 + one_ns, "1h 1ns"}
      ]

      one_hour_ns_s = Series.from_list([1_000 * @one_hour_us], dtype: {:duration, :nanosecond})

      for {float, expected} <- float_string_pairs do
        [duration] = one_hour_ns_s |> Series.multiply(float) |> Series.to_list()
        assert to_string(duration) == expected
      end
    end

    test "duration[μs] * duration[μs] raises ArgumentError" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})

      assert_raise ArgumentError,
                   "cannot invoke Explorer.Series.multiply/2 with mismatched dtypes: {:duration, :microsecond} and {:duration, :microsecond}",
                   fn -> Series.multiply(one_hour_s, one_hour_s) end
    end
  end

  describe "divide" do
    # Integer

    test "duration[μs] / integer" do
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      two_s = Series.from_list([2])
      quotient_s = Series.divide(one_hour_us_s, two_s)

      assert quotient_s.dtype == {:duration, :microsecond}
      thirty_min_duration_s = %Duration{value: div(@one_hour_us, 2), precision: :microsecond}
      assert Series.to_list(quotient_s) == [thirty_min_duration_s]
    end

    test "duration[μs] / Integer" do
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      quotient_s = Series.divide(one_hour_us_s, 2)

      assert quotient_s.dtype == {:duration, :microsecond}
      thirty_min_duration_s = %Duration{value: div(@one_hour_us, 2), precision: :microsecond}
      assert Series.to_list(quotient_s) == [thirty_min_duration_s]
    end

    test "Integer / duration[μs] raises ArgumentError" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})

      assert_raise ArgumentError,
                   "cannot divide by duration",
                   fn -> Series.divide(2, one_hour_s) end
    end

    # Float

    test "duration[μs] / float" do
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      two_s = Series.from_list([2.0])
      quotient_s = Series.divide(one_hour_us_s, two_s)

      assert quotient_s.dtype == {:duration, :microsecond}
      thirty_min_duration_s = %Duration{value: div(@one_hour_us, 2), precision: :microsecond}
      assert Series.to_list(quotient_s) == [thirty_min_duration_s]
    end

    test "duration[μs] / Float" do
      one_hour_us_s = Series.from_list([@one_hour_duration_us])
      quotient_s = Series.divide(one_hour_us_s, 2.0)

      assert quotient_s.dtype == {:duration, :microsecond}
      thirty_min_duration_s = %Duration{value: div(@one_hour_us, 2), precision: :microsecond}
      assert Series.to_list(quotient_s) == [thirty_min_duration_s]
    end

    test "Float / duration[μs] raises ArgumentError" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})

      assert_raise ArgumentError,
                   "cannot divide by duration",
                   fn -> Series.divide(2.0, one_hour_s) end
    end

    test "duration[μs] / duration[μs] raises ArgumentError" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})

      assert_raise ArgumentError,
                   "cannot divide by duration",
                   fn -> Series.divide(one_hour_s, one_hour_s) end
    end
  end

  describe "DataFrame (this block belongs elsewhere, but let's keep the tests in one file for now)" do
    test "mutate/2" do
      require Explorer.DataFrame
      alias Explorer.DataFrame, as: DF

      eleven_s = Series.from_list([~N[2023-08-20 11:00:00.0000000]])
      twelve_s = Series.from_list([~N[2023-08-20 12:00:00.0000000]])
      df = DF.new(eleven: eleven_s, twelve: twelve_s)
      df_with_diff = DF.mutate(df, diff: twelve - eleven)

      assert inspect(df_with_diff) == """
             #Explorer.DataFrame<
               Polars[1 x 3]
               eleven datetime[μs] [2023-08-20 11:00:00.000000]
               twelve datetime[μs] [2023-08-20 12:00:00.000000]
               diff duration[μs] [1h]
             >\
             """
    end

    test "mutate/2 with duration + date" do
      require Explorer.DataFrame
      alias Explorer.DataFrame, as: DF

      df =
        DF.new(aug_20: [~D[2023-08-20]], aug_21: [~D[2023-08-21]])
        |> DF.mutate(sub: aug_21 - aug_20)

      # Arg-swapping works.
      df1 = DF.mutate(df, add1: sub + aug_20)
      assert df1["add1"].dtype == :date
      assert Series.to_list(df1["add1"]) == [~D[2023-08-21]]

      # Arg-swapping works.
      df2 = DF.mutate(df, add2: sub + ^df["aug_20"])
      assert df2["add2"].dtype == :date
      assert Series.to_list(df2["add2"]) == [~D[2023-08-21]]

      # Arg-swapping works.
      df2 = DF.mutate(df, add2: sub + aug_20 + sub)
      assert df2["add2"].dtype == :date
      assert Series.to_list(df2["add2"]) == [~D[2023-08-22]]

      # Arg-swapping does NOT work.
      # [src/dataframe.rs:507] &mutations = [
      #   [(col("sub")) + ([(col("aug_20")) + (col("sub"))])].alias("add2"),
      # ]
      df2 = DF.mutate(df, add2: sub + (aug_20 + sub))
      assert df2["add2"].dtype == :date
      assert Series.to_list(df2["add2"]) == [~D[2023-08-22]]
    end
  end
end
