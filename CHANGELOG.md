# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Consistently support ranges throughout the columns API
- Support negative indexes throughout the columns API
- Integrate with the `table` package
- Add `Series.to_enum/1` for lazily traversing the series

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
- Remove "with_" prefix in options throughout the API

## [v0.1.1] - 2022-04-27

### Security

- Updated Rust dependencies to address Dependabot security alerts: [1](https://github.com/elixir-nx/explorer/security/dependabot/1), [2](https://github.com/elixir-nx/explorer/security/dependabot/3), [3](https://github.com/elixir-nx/explorer/security/dependabot/4)

## [v0.1.0] - 2022-04-26

First release.
