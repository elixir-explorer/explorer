<h1><img src="explorer.png" alt="Explorer"></h1>

> :warning: Explorer is still very much a work in progress. It is incomplete and poorly documented. Use
> at your own risk.

Explorer is a dataframe library for Elixir. First and foremost, Explorer is an API for data
manipulation. Its high-level features are:

- Simply typed series: `:float`, `:integer`, `:boolean`, `:string`, `:date`, and `:datetime`.
- A powerful but constrained and opinionated API, so you spend less time looking for the right 
  function and more time doing data manipulation.
- Pluggable backends, providing a uniform API whether you're working in-memory or (forthcoming) on
  remote databases or even Spark dataframes.
- The first (and default) backend is based on NIF bindings to the blazing-fast
  [polars](https://docs.rs/polars) library.

The API is influenced heavily by [Tidy Data](https://vita.had.co.nz/papers/tidy-data.pdf) and 
borrows much of its design from [dplyr](https://dplyr.tidyverse.org). The philosophy is heavily 
influenced by this passage from `dplyr`'s documentation:

> - By constraining your options, it helps you think about your data manipulation challenges.
> - It provides simple “verbs”, functions that correspond to the most common data manipulation 
>   tasks, to help you translate your thoughts into code.
> - It uses efficient backends, so you spend less time waiting for the computer.

The aim here isn't to have the fastest dataframe library around (though it certainly helps that 
[we're building on one of the fastest](https://h2oai.github.io/db-benchmark/)). Instead, we're 
aiming to bridge the best of many worlds:

- the elegance of `dplyr`
- the speed of `polars`
- the joy of Elixir

That means you can expect the guiding principles to be 'Elixir-ish'. For example, you won't see 
the underlying data mutated, even if that's the most efficient implementation. Explorer functions 
will always return a new dataframe or series.

## Getting started

In order to use `Explorer`, you will need Elixir and Rust (stable) installed. Then create an 
Elixir project via the `mix` build tool:

```
$ mix new my_app
```

Then you can add `Explorer` as dependency in your `mix.exs`. At the moment you will have to use a 
Git dependency while we work on our first release:

```elixir
def deps do
  [
    {:explorer, "~> 0.1.0-dev", github: "elixir-nx/explorer", branch: "main"}
  ]
end
```

Alternatively, inside a script or Livebook:

```elixir
Mix.install([{:explorer, "~> 0.1.0-dev", github: "elixir-nx/explorer", branch: "main"}])
```

The [notebooks](./notebooks) directory has [an introductory
Livebook](./notebooks/exploring_explorer.livemd) to give you a feel for the API.

## Acknowledgements

While it has mostly been rewritten and expanded, key parts of the code are taken directly from 
[ex_polars](https://github.com/tyrchen/ex_polars) and 
[treebee's fork of ex_polars](https://github.com/treebee/ex_polars). Many thanks to @tyrchen and 
@treebee!

@jsonbecker has been a source of constant encouragement and a tireless duck. Thanks for quacking.

And of course, thanks to @ritchie46 for the wonderful `polars` library.

Finally, the initial development of this library took place on the lands of the Wurundjeri people 
and we wish to acknowledge them as Traditional Owners and thank them as Traditional Custodians of 
Country. We would also like to pay our respects to their Elders, past and present. Their 
sovereignty has never been ceded.

## Sponsors
<a href="https://amplified.ai"><img src="sponsors/amplified.png" width=100 alt="Amplified"></a>
