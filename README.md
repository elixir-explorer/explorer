<h1><img src="explorer.png" alt="Explorer"></h1>

[![Package](https://img.shields.io/hexpm/v/explorer.svg)](https://hex.pm/packages/explorer) [![Documentation](http://img.shields.io/badge/hex.pm-docs-green.svg?style=flat)](https://hexdocs.pm/explorer) ![CI](https://github.com/elixir-nx/explorer/actions/workflows/ci.yml/badge.svg)

Explorer brings series (one-dimensional) and dataframes (two-dimensional) for fast data exploration to Elixir. Its high-level features are:

- Simply typed series: `:float`, `:integer`, `:boolean`, `:string`, `:date`, and `:datetime`.
- A powerful but constrained and opinionated API, so you spend less time looking for the right
  function and more time doing data manipulation.
- Pluggable backends, providing a uniform API whether you're working in-memory or (forthcoming) on
  remote databases or even Spark dataframes.
- The first (and default) backend is based on NIF bindings to the blazing-fast
  [polars](https://docs.rs/polars) library.

The API is heavily influenced by [Tidy Data](https://vita.had.co.nz/papers/tidy-data.pdf) and
borrows much of its design from [dplyr](https://dplyr.tidyverse.org). The philosophy is heavily
influenced by this passage from `dplyr`'s documentation:

> - By constraining your options, it helps you think about your data manipulation challenges.
> - It provides simple “verbs”, functions that correspond to the most common data manipulation
>   tasks, to help you translate your thoughts into code.
> - It uses efficient backends, so you spend less time waiting for the computer.

The aim here isn't to have the fastest dataframe library around (though it certainly helps that
[we're building on Polars, one of the fastest](https://h2oai.github.io/db-benchmark/)). Instead, we're
aiming to bridge the best of many worlds:

- the elegance of `dplyr`
- the speed of `polars`
- the joy of Elixir

That means you can expect the guiding principles to be 'Elixir-ish'. For example, you won't see
the underlying data mutated, even if that's the most efficient implementation. Explorer functions
will always return a new dataframe or series.

## Getting started

In order to use `Explorer`, you will need Elixir installed. Then create an
Elixir project via the `mix` build tool:

```
$ mix new my_app
```

Then you can add `Explorer` as dependency in your `mix.exs`.

```elixir
def deps do
  [
    {:explorer, "~> 0.1.1"}
  ]
end
```

Alternatively, inside a script or Livebook:

```elixir
Mix.install([
  {:explorer, "~> 0.1.1"}
])
```

## Contributing

Explorer uses Rust for its default backend implementation. While Rust is not necessary to
use Explorer as a package, you need Rust tooling installed on your machine if you want to
compile from source, which is the case when contributing to Explorer. In particular, you
will need Rust Nigthly, which can be installed with [Rustup](https://rust-lang.github.io/rustup/installation/index.html).

## Sponsors

<a href="https://amplified.ai"><img src="sponsors/amplified.png" width=100 alt="Amplified"></a>
