# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `DataFrame.to_rows/2` converts a `DataFrame` to a list of maps

### Changed

- `Series.length/1` is now `Series.size/1` in keeping with Elixir idioms
- `Nx` is now an optional dependency
- Minimum Elixir version is now 1.13
- `DataFrame.to_map/2` is now `DataFrame.to_columns/2`
- `Rustler` is now an optional dependency
- `read_` and `write_` IO functions are now `from_` and `to_`
- `to_binary` is now `dump_csv`

## [v0.1.1] - 2022-04-27

### Security

- Updated Rust dependencies to address Dependabot security alerts: [1](https://github.com/elixir-nx/explorer/security/dependabot/1), [2](https://github.com/elixir-nx/explorer/security/dependabot/3), [3](https://github.com/elixir-nx/explorer/security/dependabot/4)

## [v0.1.0] - 2022-04-26

First release.
