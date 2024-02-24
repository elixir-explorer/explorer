# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.8.1] - 2024-02-24

### Added

- Add `Explorer.Series.field/2` to extract a field from a struct series.
  It returns a new series with the field's dtype.

- Add `Explorer.Series.json_decode/2` that can decode a string series containing
  valid JSON objects according to `dtype`.

- Add eager `count/1` and lazy `size/1` to `Explorer.Series`.

- Add support for maps as expressions inside `Explorer.Query`. They are "converted"
  to structs.

- Add `json_path_match/2` to extract a string series from a string containing valid
  JSON objects. See the article [JSONPath - XPath for JSON](https://goessner.net/articles/JsonPath/)
  for details about JSON paths.

- Add `Explorer.Series.row_index/1` to retrieve the index of rows starting from 0.

- Add support for passing the `:on` column directly (instead of inside a list)
  in `Explorer.DataFrame.join/3`.

### Changed

- Remove some deprecated functions from documentation.

- Change internal representation of the `:struct` dtype to use list of tuples instead of a map
  to represent the dtypes of each field. This shouldn't break because we normalise maps to lists
  when a struct dtype is passed in `from_list/2` or `cast/2`.

- Update Rustler minimum version to `~> 0.31`. Since Rustler is optional, this shouldn't affect
  most of the users.

### Fixed

- Fix float overflow error to avoid crashing the VM, and instead it returns an argument error.

- Fix `Explorer.DataFrame.print/2` for when the DF contains structs.

## [v0.8.0] - 2024-01-20

### Added

- Add `explode/2` to `Explorer.DataFrame`. This function is useful to expand
  the contents of a `{:list, inner_dtype}` series into a "`inner_dtype`" series.

- Add the new series functions `all?/1` and `any?/1`, to work with boolean series.

- Add support for the "struct" dtype. This new dtype represents the struct
  dtype from Polars/Arrow.

- Add `map/2` and `map_with/2` to the `Explorer.Series` module.
  This change enables the usage of the `Explore.Query` features in a series.

- Add `sort_by/2` and `sort_with/2` to the `Explorer.Series` module.
  This change enables the usage of the lazy computations and the `Explorer.Query`
  module.

- Add `unnest/2` to `Explorer.DataFrame`. It works by taking the fields of a "struct" -
  the new dtype - and transform them into columns.

- Add pairwise correlation - `Explorer.DataFrame.correlation/2` - to calculate the
  correlation between numeric columns inside a data frame.

- Add pairwise covariance - `Explorer.DataFrame.covariance/2` - to calculate the
  covariance between numeric columns inside a data frame.

- Add support for more integer dtypes. This change introduces new signed and
  unsigned integer dtypes:
  - `{:s, 8}`, `{:s, 16}`, `{:s, 32}`
  - `{:u, 8}`, `{:u, 16}`, `{:u, 32}`, `{:u, 64}`.

  The existing `:integer` dtype is now represented as `{:s, 64}`, and it's still
  the default dtype for integers. But series and data frames can now work with the
  new dtypes. Short names for these new dtypes can be used in functions like
  `Explorer.Series.from_list/2`. For example, `{:u, 32}` can be represented with
  the atom `:u32`.

  This may bring more interoperability with Nx, and with Arrow related things, like
  ADBC and Parquet.

- Add `ewm_standard_deviation/2` and `ewm_variance/2` to `Explorer.Series`.
  They calculate the "exponentially weighted moving" variance and standard deviation.

- Add support for `:skip_rows_after_header` option for the CSV reader functions.

- Support `{:list, numeric_dtype}` for `Explorer.Series.frequencies/1`.

- Support pins in `cond`, inside the context of `Explorer.Query`.

- Introduce the `:null` dtype. This is a special dtype from Polars and Apache Arrow
  to represent "all null" series.

- Add `Explorer.DataFrame.transpose/2` to transpose a data frame.

### Changed

- Rename the functions related to sorting/arranging of the `Explorer.DataFrame`.
  Now `arrange_with` is named `sort_with`, and `arrange` is `sort_by`.

  The `sort_by/3` is a macro and it is going to work using the `Explorer.Query`
  module. On the other side, the `sort_with/2` uses a callback function.

- Remove unnecessary casts to `{:s, 64}` now that we support more integer dtypes.
  It affects some functions, like the following in the `Explorer.Series` module:

  - `argsort`
  - `count`
  - `rank`
  - `day_of_week`, `day_of_year`, `week_of_year`, `month`, `year`, `hour`, `minute`, `second`
  - `abs`
  - `clip`
  - `lengths`
  - `slice`
  - `n_distinct`
  - `frequencies`

  And also some functions from the `Explorer.DataFrame` module:

  - `mutate` - mostly because of series changes
  - `summarise` - mostly because of series changes
  - `slice`

### Fixed

- Fix inspection of series and data frames between nodes.

- Fix cast of `:string` series to `{:datetime, any()}`

- Fix mismatched types in `Explorer.Series.pow/2`, making it more consistent.

- Normalize sorting options.

- Fix functions with dtype mismatching the result from Polars.
  This fix is affecting the following functions:

  - `quantile/2` in the context of a lazy series
  - `mode/1` inside a summarisation
  - `strftime/2` in the context of a lazy series
  - `mutate_with/2` when creating a column from a `NaiveDateTime` or `Explorer.Duration`.

## [v0.7.2] - 2023-11-30

### Added

- Add the functions `day_of_year/1` and `week_of_year/1` to `Explorer.Series`.

- Add `filter/2` - a macro -, and `filter_with/2` to `Explorer.Series`.

  This change enables the usage of queries - using `Explorer.Query` - when
  filtering a series. The main difference is that series does not have a
  name when used outside a dataframe. So to refer to itself inside the
  query, we can use the special `_` variable.

        iex> s = Explorer.Series.from_list([1, 2, 3])
        iex> Explorer.Series.filter(s, _ > 2)
        #Explorer.Series<
          Polars[1]
          integer [3]
        >

- Add support for the `{:list, any()}` dtype, where `any()` can be any other
  valid dtype. This is a recursive dtype, that can represent nested lists.
  It's useful to group data together in the same series.

- Add `Explorer.Series.mode/2` to get the most common value(s) of the series.

- Add `split/2` and `join/2` to the `Explorer.Series` module.
  These functions are useful to split string series into `{:list, :string}`,
  or to join parts of a `{:list, :string}` and return a `:string` series.

- Expose `ddof` option for variance, covariance and standard deviation.

- Add a new `{:f, 32}` dtype to represent 32 bits float series.
  It's also possible to use the atom `:f32` to create this type of series.
  The atom `:f64` can be used as an alias for `{:f, 64}`, just like the
  `:float` atom.

- Add `lengths/1` and `member?/2` to `Explorer.Series`.
  These functions work with `{:list, any()}`, where `any()` is any valid dtype.
  The idea is to count the members of a "list" series, and check if a given
  value is member of a list series, respectively.

- Add support for streaming parquet files from a lazy dataframe to AWS S3
  compatible services.

### Changed

- Remove restriction on `pivot_wider` dtypes.
  In the early days, Polars only supported numeric dtypes for the "first"
  aggregation. This is not true anymore, and we can lift this restriction.

- Change `:float` dtype to be represented as `{:f, 64}`. It's still possible
  to use the atom `:float` to create float series, but now `Explorer.Series.dtype/1`
  returns `{:f, 64}` for float 64 bits series.

### Fixed

- Add missing implementation of `Explorer.Series.replace/3` for lazy series.

- Fix inspection of DFs and series when `limit: :infinity` is used.

### Removed

- Drop support for the `riscv64gc-unknown-linux-gnu` target.

  We decided to stop precompiling to this target because it's been hard to maintain it.
  Ideally we should support it again in the future.

## [v0.7.1] - 2023-09-25

### Added

- Add more temporal arithmetic operations. This change makes possible
  to mix some datatypes, like `date`, `duration` and scalar types like
  `integers` and `floats`.

  The following operations are possible now:

  * `date - date`
  * `date + duration`
  * `date - duration`
  * `duration + date`
  * `duration * integer`
  * `duration * float`
  * `duration / integer`
  * `duration / float`
  * `integer * duration`
  * `float * duration`

- Support lazy dataframes on `Explorer.DataFrame.print/2`.

- Add support for strings as the "indexes" of `Explorer.Series.categorise/2`.
  This makes possible to categorise a string series with a categories series.

- Introduce `cond/1` support in queries, which enables multi-clause conditions.
  Example of usage:

          iex> df = DF.new(a: [10, 4, 6])
          iex> DF.mutate(df,
          ...>   b:
          ...>     cond do
          ...>       a > 9 -> "Exceptional"
          ...>       a > 5 -> "Passed"
          ...>       true -> "Failed"
          ...>     end
          ...> )
          #Explorer.DataFrame<
            Polars[3 x 2]
            a integer [10, 4, 6]
            b string ["Exceptional", "Failed", "Passed"]
          >

- Similar to `cond/1`, this version also introduces support for the `if/2`
  and `unless/2` macros inside queries.

- Allow the usage of scalar booleans inside queries.

- Add `Explorer.Series.replace/3` for string series.
  This enables the replacement of patterns inside string series.

### Deprecated

- Deprecate `Explorer.DataFrame.to_lazy/1` in favor of just `lazy/1`.

### Fixed

- Fix the `Explorer.Series.in/2` function to work with series of the
  `:category` dtype.

  Now, if both series shares the same categories, we can compare them.
  To make sure that a categorical series shares the same categories from
  another series, you must create that series using the
  `Explorer.Series.categorise/2` function.

- Display the dtype of duration series correctly in `Explorer.DataFrame.print/2`.

## [v0.7.0] - 2023-08-28

### Added

- Enable reads and writes of dataframes from/to external file systems.

  It supports HTTP(s) URLs or AWS S3 locations.

  This feature introduces the [FSS](https://github.com/elixir-explorer/fss) abstraction,
  which is also going to be present in newer versions of Kino. This is going to make the integration
  of Livebook files with Explorer much easier.

  The implementation is done differently, depending on which file format is used, and if
  it's a read or write. All the writes to AWS S3 are done in the Rust side - using an abstraction
  called `CloudWriter` -, and most of the readers are implemented in Elixir, by doing a download
  of the files, and then loading the dataframe from it. The only exception is the reads of
  parquet files, which are done in Rust, using Polars' `scan_parquet` with streaming.

  We want to give a special thanks to [Qqwy / Marten](https://github.com/Qqwy) for the
  `CloudWriter` implementation!

- Add ADBC: Arrow Database Connectivity.

  Continuing with improvements in the IO area, we added support for reading dataframes from
  databases using ADBC, which is similar in idea to ODBC, but integrates much better with
  Apache Arrow, that is the backbone of Polars - our backend today.

  The function `Explorer.DataFrame.from_query/1` is the entrypoint for this feature, and it
  allows quering databases like PostgreSQL, SQLite and Snowflake.

  Check the [Elixir ADBC bindings docs](https://hexdocs.pm/adbc) for more information.

  For the this feature, we had a fundamental contribution from [Cocoa](https://github.com/cocoa-xu)
  in the ADBC bindings, so we want to say a special thanks to her!

  We want to thank the people that joined José in [his live streamings on Twitch](https://www.twitch.tv/josevalim),
  and helped to build this feature!

- Add the following functions to `Explorer.Series`:
  - [`window_median/3`](https://hexdocs.pm/explorer/Explorer.Series.html#window_median/3)
  - [`substring/3`](https://hexdocs.pm/explorer/Explorer.Series.html#substring/3)

- Add duration dtypes. This is adds the following dtypes:
  - `{:duration, :nanosecond}`
  - `{:duration, :microsecond}`
  - `{:duration, :millisecond}`

  This feature was a great contribution from [Billy Lanchantin](https://github.com/billylanchantin),
  and we want to thank him for this!

### Changed

- Return exception structs instead of strings for all IO operation errors, and for anything
  that returns an error from the NIF integration.

  This change makes easier to define which type of error we want to raise.

- Update Polars to v0.32.

  With that we made some minor API changes, like changing some options for `cut/qcut` operations
  in the `Explorer.Series` module.

- Use `nil_values` instead of `null_character` for IO operations.

- Never expect `nil` for CSV IO dtypes.

- Rename `Explorer.DataFrame.table/2` to `Explorer.DataFrame.print/2`.

- Change `:datetime` dtype to be `{:datetime, time_unit}`, where time unit can be
  the following:
  - `:millisecond`
  - `:microsecond`
  - `:nanosecond`

- Rename the following `Series` functions:
  - `trim/1` to `strip/2`
  - `trim_leading/1` to `lstrip/2`
  - `trim_trailing/1` to `rstrip/2`

  These functions now support a string argument.

### Fixed

- Fix warnings for the upcoming Elixir v1.16.

- Fix `Explorer.Series.abs/1` type specs.

- Allow comparison of strings with categories.

- Fix `Explorer.Series.is_nan/1` inside the context of `Explorer.Query`.
  The NIF function was not being exported.

## [v0.6.1] - 2023-07-06

### Fixed

- Fix summarise without groups for lazy frames.

## [v0.6.0] - 2023-07-05

### Added

- Add support for OTP 26 and Elixir 1.15.

- Allow `Explorer.DataFrame.summarise/2` to work without groups.
  The aggregations can work considering the entire dataframe.

- Add the following series functions: `product/1`, `cummulative_product/1`, `abs/1`,
  `skew/2`, `window_standard_deviation/3`, `rank/2`, `year/1`, `mounth/1`, `day/1`,
  `hour/1`, `minute/1`, `second/1`, `strptime/2`, `strftime/2`, `argmin/1`, `argmax/1`,
  `cut/3`, `qcut/3`, `correlation/3`, `covariance/2` and `clip/3`.

  They cover a lot in terms of functionality, so please check the `Explorer.Series`
  docs for further details.

- Add `Explorer.DataFrame.nil_count/1` that counts the number of nil elements in each
  column.

- Add `Explorer.DataFrame.frequencies/2` that creates a new dataframe with unique rows
  and the frequencies of each.

- Add `Explorer.DataFrame.relocate/3` that enables changing order of columns from a df.

- Add precompiled NIFs for FreeBSD.

- Support scalar values in the `on_true` and `on_false` arguments of `Explore.Series.select/3`.

### Fixed

- Fix `Series.day_of_week/1` and `Series.round/2` for operations using a lazy frame.

- Fix upcasted date to datetime for literal expressions. It allows to use scalar dates
  in expressions like this: `DF.mutate(a: ~D[2023-01-01])`. This also fixes the support
  for naive datetimes.

- Improve error messages returned from the NIF to be always strings. Now we add more
  context to the string returned, instead of having `{:context, error_message}`.

- Fix the `:infer_schema_length` option of `Explorer.DataFrame.from_csv/2` when passing `nil`.
  Now it's possible to take into account the entire file to infer the schema.


### Deprecated

- Deprecate `Explorer.Series.to_date/1` and `Explorer.Series.to_time/1` in favor of
  using `Explorer.Series.cast(s, :date)` and `Explorer.Series.cast(s, :time)` respectively.

## [v0.5.7] - 2023-05-10

### Added

- Allow `Explorer.Series.select/3` to receive series of size 1 in both sides.

- Add trigonometric functions `sin/1`, `cos/1`, `tan/1`, `asin/1`, `acos/1` and
  `atan/1` to `Explorer.Series`.

- Add `Explorer.DataFrame.to_rows_stream/2` function. This is useful to traverse
  dataframes with large series, but is not recommended since it can be an expensive
  operation.

- Add LazyFrame version of `Explorer.DataFrame.to_ipc/3`.

- Add options to control streaming when writing lazy dataframes. Now users can
  toggle streaming for the `to_ipc/3` and `to_parquet/3` functions.

- Add `Explorer.DataFrame.from_ipc_stream/2` lazy, but using the eager implementation
  underneath.

- Add option to control the end of line (EOF) char when reading CSV files.
  We call this new option `:eol_delimiter`, and it's available for the `from_csv/2`
  and `load_csv/2` functions in the `Explorer.DataFrame` module.

- Allow `Explorer.DataFrame.pivot_wider/4` to use category fields.

### Fixed

- Fix `nif_not_loaded` error when `Explorer.Series.ewm_mean/2` is called from query.

- Type check arguments for boolean series operations, only allowing series of
  the `boolean` dtype.

- Do not use `../0` in order to keep compatible with Elixir 1.13

### Removed

- Temporarely remove support for ARM 32 bits computers in the precompilation
  workflow.

## [v0.5.6] - 2023-03-24

### Added

- Add the following functions to the `Explorer.Series` module: `log/1`, `log/2`
  and `exp/1`. They compute the logarithm and exponential of a series.

### Fixed

- Allow `Explorer.Series.select/3` to receive series of size 1 for both the
  `on_true` and `on_false` arguments.

- Fix the encoding of special float values that may return from some series
  functions. This is going to encode the atoms for NaN and infinity values.

## [v0.5.5] - 2023-03-13

### Added

- Add support for multiple value columns in pivot wider.
  The resultant dataframe that is created from this type of pivoting is going
  to have columns with the names prefixed by the original value column, followed
  by an underscore and the name of the variable.

- Add `Explorer.Series.ewm_mean/2` for calculating exponentially weighted moving
  average.

### Changed

- Change the `Explorer.Backend.DataFrame`'s `pivot_wider` callback to work with
  multiple columns instead of only one.

- Change the `Explorer.Backend.DataFrame`'s `window_*` callbacks to work with
  variables instead of keyword args. This is needed to make explicit when a backend
  is not implementing an option.

- Change the `Explorer.Backend.DataFrame`'s `describe` callback and remove the
  need for an "out df", since we won't have a lazy version of that funcion.

- This shouldn't affect the API, but we had an update in Polars.
  It is now using `v0.27.2`. For further details, see:
  [Rust Polars 0.27.0](https://github.com/pola-rs/polars/releases/tag/rs-0.27.0).

### Fixed

- Provide hints when converting string/binary series to tensors.

- Add libatomic as a link to the generated NIF. This is needed to fix the load
  of the Explorer NIF when running on ARM 32 bits machines like the Pi 3.
  See the [original issue](https://github.com/philss/rustler_precompiled/issues/53)

## [v0.5.4] - 2023-03-09

### Fixed

- Fix missing "README.md" file in the list of package files.
  Our readme is now required in compilation, because it contains the moduledoc for
  the main `Explorer` module.

## [v0.5.3] - 2023-03-08

### Added

- Add the `Explorer.Series.format/1` function that concatenates multiple series together,
  always returning a string series.

- With the addition of `format/1`, we also have a new operator for string concatenation
  inside `Explorer.Query`. It is the `<>` operator, that is similar to what the `Kernel.<>/2`
  operator does, but instead of concatenating strings, it concatenates two series, returning
  a string series - it is using `format/1` underneath.

- Add support for slicing by series in dataframes and other series.

- Add support for 2D tensors in `Explorer.DataFrame.new/2`.

### Fixed

- Fix `Explorer.DataFrame.new/2` to respect the selected dtype when an entire series is nil.

- Improve error message for mismatched dtypes in series operations.

- Fix lazy series operations of binary series and binary values. This is going to wrap binary
  values in the correct dtype, in order to pass down to Polars.

- Fix two bugs in `Explorer.DataFrame.pivot_wider/3`: `nil` values in the series that is
  used for new column names is now correctly creating a `nil` column. We also fixed the problem
  of a duplicated column created after pivoting, and possibly conflicting with an existing
  ID column. We add a suffix for these columns.

## [v0.5.2] - 2023-02-28

### Added

- Add `across` and comprehensions to `Explorer.Query`. These features allow a
  more flexible and elegant way to work with multiple columns at once. Example:

  ```elixir
  iris = Explorer.Datasets.iris()
  Explorer.DataFrame.mutate(iris,
   for col <- across(["sepal_width", "sepal_length", "petal_length", "petal_width"]) do
     {col.name, (col - mean(col)) / variance(col)}
   end
  )
  ```

  See the `Explorer.Query` documentation for further details.

- Add support for regexes to select columns of a dataframe. Example:

  ```elixir
  df = Explorer.Datasets.wine()
  df[~r/(class|hue)/]
  ```

- Add the `:max_rows` and `:columns` options to `Explorer.DataFrame.from_parquet/2`. This mirrors
  the `from_csv/2` function.

- Allow `Explorer.Series` functions that accept floats to work with `:nan`, `:infinity`
  and `:neg_infinity` values.

- Add `Explorer.DataFrame.shuffle/2` and `Explorer.Series.shuffle/2`.

- Add support for a list of filters in `Explorer.DataFrame.filter/2`. These filters are
  joined as `and` expressions.

### Fixed

- Add `is_integer/1` guard to `Explorer.Series.shift/2`.
- Raise if series sizes do not match for binary operations.

### Changed

- Rename the option `:replacement` to `:replace` for `Explorer.DataFrame.sample/3` and
  `Explorer.Series.sample/3`.

- Change the default behaviour of sampling to not shuffle by default. A new option
  named `:shuffle` was added to control that.

## [v0.5.1] - 2023-02-17

### Added

- Add boolean dtype to `Series.in/2`.
- Add binary dtype to `Series.in/2`.
- Add `Series.day_of_week/1`.

- Allow `Series.fill_missing/2` to:
  - receive `:infinity` and `:neg_infinity` values.
  - receive date and datetime values.
  - receive binary values.

- Add support for `time` dtype.
- Add version of `Series.pow/2` that accepts series on both sides.
- Allow `Series.from_list/2` to receive `:nan`, `:infinity` and `:neg_infinity` atoms.
- Add `Series.to_date/1` and `Series.to_time/1` for datetime series.
- Allow casting of string series to category.
- Accept tensors when creating a new dataframe.
- Add compatibility with Nx v0.5.
- Add support for Nx's serialize and deserialize.

- Add the following function implementations for the Polars' Lazy dataframe backend:
  - `arrange_with`
  - `concat_columns`
  - `concat_rows`
  - `distinct`
  - `drop_nil`
  - `filter_with`
  - `join`
  - `mutate_with`
  - `pivot_longer`
  - `rename`
  - `summarise_with`
  - `to_parquet`

  Only `summarise_with` supports groups for this version.

### Changed

- Require version of Rustler to be `~> 0.27.0`, which mirrors the NIF requirement.

### Fixed

- Casting to an unknown dtype returns a better error message.

## [v0.5.0] - 2023-01-12

### Added

- Add `DataFrame.describe/2` to gather some statistics from a dataframe.
- Add `Series.nil_count/1` to count nil values.
- Add `Series.in/2` to check if a given value is inside a series.
- Add `Series` float predicates: `is_finite/1`, `is_infinite/1` and `is_nan/1`.
- Add `Series` string functions: `contains/2`, `trim/1`, `trim_leading/1`, `trim_trailing/1`,
  `upcase/1` and `downcase/1`.

- Enable slicing of lazy frames (`LazyFrame`).
- Add IO operations "from/load" to the lazy frame implementation.
- Add support for the `:lazy` option in the `DataFrame.new/2` function.
- Add `Series` float rounding methods: `round/2`, `floor/1` and `ceil/1`.
- Add support for precompiling to Linux running on RISCV CPUs.
- Add support for precompiling to Linux - with musl - running on AARCH64 computers.
- Allow `DataFrame.new/1` to receive the `:dtypes` option.
- Accept `:nan` as an option for `Series.fill_missing/2` with float series.
- Add basic support for the categorical dtype - the `:category` dtype.
- Add `Series.categories/1` to return categories from a categorical series.
- Add `Series.categorise/2` to categorise a series of integers using predefined categories.
- Add `Series.replace/2` to replace the contents of a series.
- Support selecting columns with unusual names (like with spaces) inside `Explorer.Query`
  with `col/1`.

  The usage is like this:

  ```elixir
  Explorer.DataFrame.filter(df, col("my col") > 42)
  ```

### Fixed

- Fix `DataFrame.mutate/2` using a boolean scalar value.
- Stop leaking `UInt32` series to Elixir.
- Cast numeric columns to our supported dtypes after IO read.
  This fix is only applied for the eager implementation for now.

### Changed

- Rename `Series.bintype/1` to `Series.iotype/1`.

## [v0.4.0] - 2022-11-29

### Added

- Add `Series.quotient/2` and `Series.remainder/2` to work with integer division.
- Add `Series.iotype/1` to return the underlying representation type.
- Allow series on both sides of binary operations, like: `add(series, 1)`
  and `add(1, series)`.

- Allow comparison, concat and coalesce operations on "(series, lazy series)".
- Add lazy version of `Series.sample/3` and `Series.size/1`.
- Add support for Arrow IPC Stream files.
- Add `Explorer.Query` and the macros that allow a simplified query API.
  This is a huge improvement to some of the main functions, and allow refering to
  columns as they were variables.

  Before this change we would need to write a filter like this:

  ```elixir
  Explorer.DataFrame.filter_with(df, &Explorer.Series.greater(&1["col1"], 42))
  ```

  But now it's also possible to write this operation like this:

  ```elixir
  Explorer.DataFrame.filter(df, col1 > 42)
  ```

  This operation is going to use `filter_with/2` underneath, which means that
  is going to use lazy series and compute the results at once.
  Notice that is mandatory to "require" the DataFrame module, since these operations
  are implemented as macros.

  The following new macros were added:
  - `filter/2`
  - `mutate/2`
  - `summarise/2`
  - `arrange/2`

  They substitute older versions that did not accept the new query syntax.

- Add `DataFrame.put/3` to enable adding or replacing columns in a eager manner.
  This works similar to the previous version of `mutate/2`.

- Add `Series.select/3` operation that enables selecting a value
  from two series based on a predicate.

- Add "dump" and "load" functions to IO operations. They are useful to load
  or dump dataframes from/to memory.

- Add `Series.to_iovec/2` and `Series.to_binary/1`. They return the underlying
  representation of series as binary. The first one returns a list of binaries,
  possibly with one element if the series is contiguous in memory. The second one
  returns a single binary representing the series.

- Add `Series.shift/2` that shifts the series by an offset with nil values.
- Rename `Series.fetch!/2` and `Series.take_every/2` to `Series.at/2`
  and `Series.at_every/2`.

- Add `DataFrame.discard/2` to drop columns. This is the opposite of `select/2`.

- Implement `Nx.LazyContainer` for `Explorer.DataFrame` and `Explorer.Series`
  so data can be passed into Nx.

- Add `Series.not/1` that negates values in a boolean series.
- Add the `:binary` dtype for Series. This enables the usage of arbitrary binaries.

### Changed

- Change DataFrame's `to_*` functions to return only `:ok`.
- Change series inspect to resamble the dataframe inspect with the backend name.
- Rename `Series.var/1` to `Series.variance/1`
- Rename `Series.std/1` to `Series.standard_deviation/1`
- Rename `Series.count/2` to `Series.frequencies/1` and add a new `Series.count/1`
  that returns the size of an "eager" series, or the count of members in a group
  for a lazy series.
  In case there is no groups, it calculates the size of the dataframe.
- Change the option to control direction in `Series.sort/2` and `Series.argsort/2`.
  Instead of a boolean, now we have a new option called `:direction` that accepts
  `:asc` or `:desc`.

### Fixed

- Fix the following DataFrame functions to work with groups:
  - `filter_with/2`
  - `head/2`
  - `tail/2`
  - `slice/2`
  - `slice/3`
  - `pivot_longer/3`
  - `pivot_wider/4`
  - `concat_rows/1`
  - `concat_columns/1`
- Improve the documentation of functions that behave differently with groups.
- Fix `arrange_with/2` to use "group by" stable, making results more predictable.
- Add `nil` as a possible return value of aggregations.
- Fix the behaviour of `Series.sort/2` and `Series.argsort/2` to add nils at the
  front when direction is descending, or at the back when the direction is ascending.
  This also adds an option to control this behaviour.

### Removed

- Remove support for `NDJSON` read and write for ARM 32 bits targets.
  This is due to a limitation of a dependency of Polars.

## [v0.3.1] - 2022-09-09

### Fixed

- Define `multiply` inside `*_with` operations.
- Fix column types in several operations, such as `n_distinct`.

## [v0.3.0] - 2022-09-01

### Added

- Add `DataFrame.concat_columns/1` and `DataFrame.concat_columns/2` for horizontally stacking
  dataframes.
- Add compression as an option to write parquet files.
- Add count metadata to `DataFrame` table reader.
- Add `DataFrame.filter_with/2`, `DataFrame.summarise_with/2`, `DataFrame.mutate_with/2` and
`DataFrame.arrange_with/2`. They all accept a `DataFrame` and a function, and they all work with
  a new concept called "lazy series".

  Lazy Series is an opaque representation of a series that can be
  used to perform complex operations without pulling data from the series. This is faster than
  using masks. There is no big difference from the API perspective compared to the functions that were
  accepting callbacks before (eg. `filter/2` and the new `filter_with/2`), with the exception being
  `DataFrame.summarise_with/2` that now accepts a lot more operations.

### Changed

- Bump version requirement of the `table` dependency to `~> 0.1.2`, and raise for non-tabular values.
- Normalize how columns are handled. This changes some functions to accept one column or
a list of columns, ranges, indexes and callbacks selecting columns.
- Rename `DataFrame.filter/2` to `DataFrame.mask/2`.
- Rename `Series.filter/2` to `Series.mask/2`.
- Rename `take/2` from both `Series` and `DataFrame` to `slice/2`. `slice/2` now they accept ranges as well.
- Raise an error if `DataFrame.pivot_wider/4` has float columns as IDs. This is because we can´t
properly compare floats.
- Change `DataFrame.distinct/2` to accept columns as argument instead of receiving it as option.

### Fixed

- Ensure that we can compare boolean series in functions like `Series.equal/2`.
- Fix rename of columns after summarise.
- Fix inspect of float series containing `NaN` or `Infinity` values. They are represented as atoms.

### Deprecated

- Deprecate `DataFrame.filter/2` with a callback in favor of `DataFrame.filter_with/2`.

## [v0.2.0] - 2022-06-22

### Added

- Consistently support ranges throughout the columns API
- Support negative indexes throughout the columns API
- Integrate with the `table` package
- Add `Series.to_enum/1` for lazily traversing the series
- Add `Series.coalesce/1` and `Series.coalesce/2` for finding the first non-null value in a list of series

### Changed

- `Series.length/1` is now `Series.size/1` in keeping with Elixir idioms
- `Nx` is now an optional dependency
- Minimum Elixir version is now 1.13
- `DataFrame.to_map/2` is now `DataFrame.to_columns/2` and `DataFrame.to_series/2`
- `Rustler` is now an optional dependency
- `read_` and `write_` IO functions are now `from_` and `to_`
- `to_binary` is now `dump_csv`
- Now uses `polars`'s "simd" feature
- Now uses `polars`'s "performant" feature
- `Explorer.default_backend/0` is now `Explorer.Backend.get/0`
- `Explorer.default_backend/1` is now `Explorer.Backend.put/1`
- `Series.cum_*` functions are now `Series.cumulative_*` to mirror `Nx`
- `Series.rolling_*` functions are now `Series.window_*` to mirror `Nx`
- `reverse?` is now an option instead of an argument in `Series.cumulative_*` functions
- `DataFrame.from_columns/2` and `DataFrame.from_rows/2` is now `DataFrame.new/2`
- Rename "col" to "column" throughout the API
- Remove "with\_" prefix in options throughout the API
- `DataFrame.table/2` accepts options with `:limit` instead of single integer
- `rename/2` no longer accepts a function, use `rename_with/2` instead
- `rename_with/3` now expects the function as the last argument

### Fixed

- Explorer now works on Linux with musl

## [v0.1.1] - 2022-04-27

### Security

- Updated Rust dependencies to address Dependabot security alerts: [1](https://github.com/elixir-explorer/explorer/security/dependabot/1), [2](https://github.com/elixir-explorer/explorer/security/dependabot/3), [3](https://github.com/elixir-explorer/explorer/security/dependabot/4)

## [v0.1.0] - 2022-04-26

First release.

[Unreleased]: https://github.com/elixir-explorer/explorer/compare/v0.8.1...HEAD
[v0.8.1]: https://github.com/elixir-explorer/explorer/compare/v0.8.0...v0.8.1
[v0.8.0]: https://github.com/elixir-explorer/explorer/compare/v0.7.2...v0.8.0
[v0.7.2]: https://github.com/elixir-explorer/explorer/compare/v0.7.1...v0.7.2
[v0.7.1]: https://github.com/elixir-explorer/explorer/compare/v0.7.0...v0.7.1
[v0.7.0]: https://github.com/elixir-explorer/explorer/compare/v0.6.1...v0.7.0
[v0.6.1]: https://github.com/elixir-explorer/explorer/compare/v0.6.0...v0.6.1
[v0.6.0]: https://github.com/elixir-explorer/explorer/compare/v0.5.7...v0.6.0
[v0.5.7]: https://github.com/elixir-explorer/explorer/compare/v0.5.6...v0.5.7
[v0.5.6]: https://github.com/elixir-explorer/explorer/compare/v0.5.5...v0.5.6
[v0.5.5]: https://github.com/elixir-explorer/explorer/compare/v0.5.4...v0.5.5
[v0.5.4]: https://github.com/elixir-explorer/explorer/compare/v0.5.3...v0.5.4
[v0.5.3]: https://github.com/elixir-explorer/explorer/compare/v0.5.2...v0.5.3
[v0.5.2]: https://github.com/elixir-explorer/explorer/compare/v0.5.1...v0.5.2
[v0.5.1]: https://github.com/elixir-explorer/explorer/compare/v0.5.0...v0.5.1
[v0.5.0]: https://github.com/elixir-explorer/explorer/compare/v0.4.0...v0.5.0
[v0.4.0]: https://github.com/elixir-explorer/explorer/compare/v0.3.1...v0.4.0
[v0.3.1]: https://github.com/elixir-explorer/explorer/compare/v0.3.0...v0.3.1
[v0.3.0]: https://github.com/elixir-explorer/explorer/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/elixir-explorer/explorer/compare/v0.1.1...v0.2.0
[v0.1.1]: https://github.com/elixir-explorer/explorer/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/elixir-explorer/explorer/releases/tag/v0.1.0
