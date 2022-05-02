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

  outputs = { self, nixpkgs, flake-utils, fenix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ fenix.overlay ];
        pkgs = import nixpkgs { inherit overlays system; };
        toolchain = "latest";
        rustPkg = fenix.packages.${system}.${toolchain}.withComponents [
          "cargo"
          "clippy"
          "rust-src"
          "rustc"
          "rustfmt"
        ];
      in
      {
        devShell = pkgs.mkShell {
          buildInputs = with pkgs; [
            act
            elixir
            erlang
            gdb
            libiconv
            openssl
            pkg-config
            rustPkg
            rust-analyzer-nightly
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
