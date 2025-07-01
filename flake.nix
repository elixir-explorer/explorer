{
  description = "Explorer";

  inputs = {
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    nixpkgs.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    nixpkgs,
    flake-utils,
    fenix,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      overlays = [fenix.overlays.default];
      pkgs = import nixpkgs {inherit overlays system;};
      rustToolchain = pkgs.fenix.fromToolchainName {
        name = (nixpkgs.lib.importTOML ./rust-toolchain.toml).toolchain.channel;
        sha256 = "sha256-UAoZcxg3iWtS+2n8TFNfANFt/GmkuOMDf7QAE0fRxeA=";
      };

      rustPkg = rustToolchain.withComponents [
        "cargo"
        "clippy"
        "rust-src"
        "rustc"
        "rustfmt"
      ];
    in {
      devShell = pkgs.mkShell {
        buildInputs = with pkgs;
          [
            act
            clang
            elixir
            erlang_27
            libiconv
            openssl
            pkg-config
            rustPkg
            rust-analyzer-nightly
            cmake
            awscli2
          ]
          ++ lib.optionals stdenv.isDarwin [
            darwin.apple_sdk.frameworks.Security
            darwin.apple_sdk.frameworks.SystemConfiguration
          ];
        shellHook = ''
          mkdir -p .nix-mix
          mkdir -p .nix-hex
          export MIX_HOME=$PWD/.nix-mix
          export HEX_HOME=$PWD/.nix-hex
          export PATH=$MIX_HOME/bin:$PATH
          export PATH=$HEX_HOME/bin:$PATH
          export PATH=$MIX_HOME/escripts:$PATH
        '';
      };
    });
}
