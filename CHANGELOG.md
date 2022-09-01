# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

- Updated Rust dependencies to address Dependabot security alerts: [1](https://github.com/elixir-nx/explorer/security/dependabot/1), [2](https://github.com/elixir-nx/explorer/security/dependabot/3), [3](https://github.com/elixir-nx/explorer/security/dependabot/4)

## [v0.1.0] - 2022-04-26

First release.

[Unreleased]: https://github.com/elixir-nx/explorer/compare/v0.2.0...HEAD
[v0.2.0]: https://github.com/elixir-nx/explorer/compare/v0.1.1...v0.2.0
[v0.1.1]: https://github.com/elixir-nx/explorer/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/elixir-nx/explorer/releases/tag/v0.1.0
