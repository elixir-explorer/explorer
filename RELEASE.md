# How to release

Because we use
[`RustlerPrecompiled`](https://hexdocs.pm/rustler_precompiled/RustlerPrecompiled.html), releasing
is a bit more involved than it would be otherwise.

1. Open a PR with any changes needed for the release.

- This must include at least updating the `version` in `mix.exs` and any other files that
  reference it, like `README.md`. It must also include updating `CHANGELOG.md` to reflect the
  release.

2. Once the PR is merged, cut a GitHub release with information from the changelog and tag the
   commit with the version number.
3. This will kick off the "Build precompiled NIFs" GitHub Action. Wait for this to complete. It
   usually takes around 40-60 minutes.
4. While the NIFs are compiling, ensure you have the latest version of `main` and don't have any
   intermediate builds by running `rm -rf native/explorer/target`.
5. Once the NIFs are built, use:

        EXPLORER_BUILD=true mix rustler_precompiled.download Explorer.PolarsBackend.Native --all --print

   to download all the artifacts and generate the checksum file.
6. Paste the SHA 256 contents into the release description on GitHub.
6. Run `mix hex.publish` - please double check the dependencies and files, and confirm.
7. Bump the version in the `mix.exs` and add the `-dev` flag to it.
