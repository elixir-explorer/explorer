<h1><img src="explorer.png" alt="Explorer"></h1>

![CI](https://github.com/elixir-nx/explorer/actions/workflows/ci.yml/badge.svg)
[![Documentation](http://img.shields.io/badge/hex.pm-docs-green.svg?style=flat)](https://hexdocs.pm/explorer)
[![Package](https://img.shields.io/hexpm/v/explorer.svg)](https://hex.pm/packages/explorer)

<!-- MDOC -->

Explorer brings series (one-dimensional) and dataframes (two-dimensional) for fast
data exploration to Elixir.

## Getting started

Inside an Elixir script or [Livebook](https://livebook.dev):

```elixir
Mix.install([
  {:explorer, "~> 0.5.0"}
])
```

Or in the `mix.exs` file of your application:

```elixir
def deps do
  [
    {:explorer, "~> 0.5.0"}
  ]
end
```

## A glimpse of the API

We have two ways to represent data with Explorer:

- using a series, that is similar to a list, but is guaranteed to contain items
  of one data type only - or one *dtype* for short. Notice that nil values are
  permitted in series of any dtype.

- using a dataframe, that is just a way to represent one or more series together,
  and work with them as a whole. The only restriction is that all the series shares
  the same size.

A series can be created from a list:

```elixir
fruits = Explorer.Series.from_list(["apple", "mango", "banana", "orange"])
```

Your newly created series is going to look like:

```
#Explorer.Series<
  Polars[4]
  string ["apple", "mango", "banana", "orange"]
>
```

And you can, for example, sort that series:

```elixir
Explorer.Series.sort(fruits)
```

Resulting in the following:

```
#Explorer.Series<
  Polars[4]
  string ["apple", "banana", "mango", "orange"]
>
```

### Dataframes

Dataframes can be created in two ways:

- by reading from files or memory using the
  [IO functions](https://hexdocs.pm/explorer/Explorer.DataFrame.html#module-io-operations).
  This is by far the most common way to load dataframes in Explorer.
  We accept Parquet, IPC, CSV, and NDJSON files.

- by using the `Explorer.DataFrame.new/2` function, that is neat for small experiments.
  We are going to use this function here.

You can pass either series or lists to it:

```elixir
mountains = Explorer.DataFrame.new(name: ["Everest", "K2", "Aconcagua"], elevation: [8848, 8611, 6962])
```

Your dataframe is going to look like this:

```
#Explorer.DataFrame<
  Polars[3 x 2]
  name string ["Everest", "K2", "Aconcagua"]
  elevation integer [8848, 8611, 6962]
>
```

It's also possible to see a dataframe like a table, using the `Explorer.DataFrame.table/2`
function:

```elixir
Explorer.DataFrame.table(mountains)
```

Prints:

```
+-------------------------------------------+
| Explorer DataFrame: [rows: 3, columns: 2] |
+---------------------+---------------------+
|        name         |      elevation      |
|      <string>       |      <integer>      |
+=====================+=====================+
| Everest             | 8848                |
+---------------------+---------------------+
| K2                  | 8611                |
+---------------------+---------------------+
| Aconcagua           | 6962                |
+---------------------+---------------------+
```

And now I want to show you how to filter our dataframe. But first, let's require
the `Explorer.DataFrame` module and give a short name to it:

```elixir
require Explorer.DataFrame, as: DF
```

The "require" is needed to load the macro features of that module.
We give it a shorter name to simplify our examples.

Now let's go to the filter. I want to filter the mountains that are above
the mean elevation in our dataframe:

```elixir
DF.filter(mountains, elevation > mean(elevation))
```

You can see that we can refer to the columns using their names, and use functions
without define them. This is possible due the powerful `Explorer.Query` features,
and it's the main reason we need to "require" the `Explorer.DataFrame` module.

The result is going to look like this:

```
#Explorer.DataFrame<
  Polars[2 x 2]
  name string ["Everest", "K2"]
  elevation integer [8848, 8611]
>
```

There is an extensive guide that you can play with Livebook:
[Ten Minutes to Explorer](https://hexdocs.pm/explorer/exploring_explorer.html)

You can also check the `Explorer.DataFrame` and `Explorer.Series` docs for further
details.

## Features and design

Explorer high-level features are:

- Simply typed series: `:binary`, `:boolean`, `:category`, `:date`, `:datetime`, `:float`, `:integer`, `:string`, and `:time`.

- A powerful but constrained and opinionated API, so you spend less time looking
  for the right function and more time doing data manipulation.

- Pluggable backends, providing a uniform API whether you're working in-memory
  or (forthcoming) on remote databases or even Spark dataframes.

- The first (and default) backend is based on NIF bindings to the blazing-fast
  [polars](https://docs.rs/polars) library.

The API is heavily influenced by [Tidy Data](https://vita.had.co.nz/papers/tidy-data.pdf)
and borrows much of its design from [dplyr](https://dplyr.tidyverse.org). The philosophy
is heavily influenced by this passage from `dplyr`'s documentation:

> - By constraining your options, it helps you think about your data manipulation
>   challenges.
>
> - It provides simple “verbs”, functions that correspond to the most common data
>   manipulation tasks, to help you translate your thoughts into code.
>
> - It uses efficient backends, so you spend less time waiting for the computer.

The aim here isn't to have the fastest dataframe library around (though it certainly
helps that [we're building on Polars, one of the fastest](https://h2oai.github.io/db-benchmark/)).
Instead, we're aiming to bridge the best of many worlds:

- the elegance of `dplyr`
- the speed of `polars`
- the joy of Elixir

That means you can expect the guiding principles to be 'Elixir-ish'. For example,
you won't see the underlying data mutated, even if that's the most efficient implementation.
Explorer functions will always return a new dataframe or series.

<!-- MDOC -->

## Contributing

Explorer uses Rust for its default backend implementation, and while Rust is not
necessary to use Explorer as a package, you need Rust tooling installed on your
machine if you want to compile from source, which is the case when contributing
to Explorer.

We require Rust Nightly, which can be installed with [Rustup](https://rust-lang.github.io/rustup/installation/index.html).

Once you have made your changes, run `mix ci`, to lint and format both Elixir
and Rust code.

## Sponsors

<a href="https://amplified.ai"><img src="sponsors/amplified.png" width=100 alt="Amplified"></a>
