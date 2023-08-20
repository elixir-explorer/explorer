defmodule Explorer.Series.DurationTest do
  use ExUnit.Case, async: true

  alias Explorer.Series

  @one_hour_us 3600 * 1_000_000

  # BILLY-NOTE: This is a stand-in for a more exhaustive test suite.
  # BILLY-TODO: Add equivalent describe blocks for add, multiply, divide, etc.

  describe "subtract" do
    test "two {:datetime, :microsecond} series" do
      eleven_s = Series.from_list([~N[2023-08-20 11:00:00.0000000]])
      twelve_s = Series.from_list([~N[2023-08-20 12:00:00.0000000]])
      diff_s = Series.subtract(twelve_s, eleven_s)

      assert inspect(diff_s) == """
             #Explorer.Series<
               Polars[1]
               duration[μs] [3600000000]
             >\
             """

      assert diff_s.dtype == {:duration, :microsecond}
      assert Series.to_list(diff_s) == [@one_hour_us]
    end

    test "one {:datetime, :microsecond} series and a NaiveDateTime" do
      eleven_s = Series.from_list([~N[2023-08-20 11:00:00.0000000]])
      twelve = ~N[2023-08-20 12:00:00.0000000]

      # Positive
      diff_s = Series.subtract(twelve, eleven_s)

      assert inspect(diff_s) == """
             #Explorer.Series<
               Polars[1]
               duration[μs] [3600000000]
             >\
             """

      assert diff_s.dtype == {:duration, :microsecond}
      assert Series.to_list(diff_s) == [@one_hour_us]

      # Negative
      diff_s = Series.subtract(eleven_s, twelve)

      assert inspect(diff_s) == """
             #Explorer.Series<
               Polars[1]
               duration[μs] [-3600000000]
             >\
             """

      assert diff_s.dtype == {:duration, :microsecond}
      assert Series.to_list(diff_s) == [-@one_hour_us]
    end

    test "one {:datetime, :microsecond} series and one {:duration, :microsecond} series" do
      one_hour_s = Series.from_list([@one_hour_us], dtype: {:duration, :microsecond})
      twelve_s = Series.from_list([~N[2023-08-20 12:00:00.0000000]])
      diff_s = Series.subtract(twelve_s, one_hour_s)

      assert inspect(diff_s) == """
             #Explorer.Series<
               Polars[1]
               datetime[μs] [2023-08-20 11:00:00.000000]
             >\
             """

      assert diff_s.dtype == {:datetime, :microsecond}
      eleven_ndt = ~N[2023-08-20 11:00:00.0000000]
      assert Series.to_list(diff_s) == [eleven_ndt]
    end

    test "one {:datetime, :microsecond} series and one {:duration, :nanosecond} series" do
      one_hour_ns = 3600 * 1_000_000_000
      one_hour_s = Series.from_list([one_hour_ns], dtype: {:duration, :nanosecond})
      twelve_s = Series.from_list([~N[2023-08-20 12:00:00.0000000]])
      diff_s = Series.subtract(twelve_s, one_hour_s)

      assert inspect(diff_s) == """
             #Explorer.Series<
               Polars[1]
               datetime[ns] [2023-08-20 11:00:00.000000]
             >\
             """

      # Since we subtracted a duration with :nanosecond precision from a datetime with :microsecond
      # precision, the resulting difference has :nanosecond precision since that was the highest
      # precision present in the operation.
      assert diff_s.dtype == {:datetime, :nanosecond}
      eleven_ndt = ~N[2023-08-20 11:00:00.0000000]
      assert Series.to_list(diff_s) == [eleven_ndt]
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
