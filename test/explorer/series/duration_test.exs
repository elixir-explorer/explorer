defmodule Explorer.Series.DurationTest do
  use ExUnit.Case, async: true

  alias Explorer.Series

  @one_hour_us 3600 * 1_000_000

  describe "add" do
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

    test "duration[μs] + duration[μs]" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      two_hour_s = Series.from_list([2 * @one_hour_us], dtype: {:duration, :microsecond})
      sum_s = Series.add(one_hour_s, two_hour_s)

      assert sum_s.dtype == {:duration, :microsecond}
      assert Series.to_list(sum_s) == [3 * @one_hour_us]
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
    test "datetime[μs] - datetime[μs]" do
      eleven_s = Series.from_list([~N[2023-08-20 11:00:00.0000000]])
      twelve_s = Series.from_list([~N[2023-08-20 12:00:00.0000000]])
      diff_s = Series.subtract(twelve_s, eleven_s)

      assert diff_s.dtype == {:duration, :microsecond}
      assert Series.to_list(diff_s) == [@one_hour_us]
    end

    test "datetime[μs] - duration[μs]" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      twelve_s = Series.from_list([~N[2023-08-20 12:00:00.0000000]])
      diff_s = Series.subtract(twelve_s, one_hour_s)

      assert diff_s.dtype == {:datetime, :microsecond}
      assert Series.to_list(diff_s) == [~N[2023-08-20 11:00:00.0000000]]
    end

    test "duration[μs] - duration[μs]" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      two_hour_s = Series.from_list([2 * @one_hour_us], dtype: {:duration, :microsecond})
      diff_s = Series.subtract(two_hour_s, one_hour_s)

      assert diff_s.dtype == {:duration, :microsecond}
      assert Series.to_list(diff_s) == [@one_hour_us]
    end

    test "NaiveDateTime - datetime[μs]" do
      eleven_s = Series.from_list([~N[2023-08-20 11:00:00.0000000]])
      twelve = ~N[2023-08-20 12:00:00.0000000]
      diff_s = Series.subtract(twelve, eleven_s)

      assert diff_s.dtype == {:duration, :microsecond}
      assert Series.to_list(diff_s) == [@one_hour_us]
    end

    test "datetime[μs] - NaiveDateTime" do
      eleven_s = Series.from_list([~N[2023-08-20 11:00:00.0000000]])
      twelve = ~N[2023-08-20 12:00:00.0000000]
      diff_s = Series.subtract(eleven_s, twelve)

      assert diff_s.dtype == {:duration, :microsecond}
      assert Series.to_list(diff_s) == [-@one_hour_us]
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
    test "duration[μs] * integer" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      ten_s = Series.from_list([10])
      prod_s = Series.multiply(one_hour_s, ten_s)

      assert prod_s.dtype == {:duration, :microsecond}
      assert Series.to_list(prod_s) == [10 * @one_hour_us]
    end

    test "integer * duration[μs]" do
      ten_s = Series.from_list([10])
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      prod_s = Series.multiply(ten_s, one_hour_s)

      assert prod_s.dtype == {:duration, :microsecond}
      assert Series.to_list(prod_s) == [10 * @one_hour_us]
    end

    test "duration[μs] * duration[μs] raises ArgumentError" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})

      assert_raise ArgumentError,
                   "cannot invoke Explorer.Series.multiply/2 with mismatched dtypes: {:duration, :microsecond} and {:duration, :microsecond}",
                   fn -> Series.multiply(one_hour_s, one_hour_s) end
    end
  end

  describe "divide" do
    test "duration[μs] / integer" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      ten_s = Series.from_list([10])
      quot_s = Series.divide(one_hour_s, ten_s)

      assert quot_s.dtype == {:duration, :microsecond}
      assert Series.to_list(quot_s) == [@one_hour_us / 10]
    end

    test "integer / duration[μs] raises ArgumentError" do
      ten_s = Series.from_list([10])
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})

      assert_raise ArgumentError,
                   "cannot divide by duration",
                   fn -> Series.divide(ten_s, one_hour_s) end
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
               diff duration[μs] [3600000000]
             >\
             """
    end
  end
end
