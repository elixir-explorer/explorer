with import <nixpkgs> {};
let
  basePackages = [
    elixir
    cargo
    rustc
  ];

  hooks = ''
    # this allows mix to work on the local directory
    mkdir -p .nix-mix
    mkdir -p .nix-hex
    export MIX_HOME=$PWD/.nix-mix
    export HEX_HOME=$PWD/.nix-hex
    export PATH=$MIX_HOME/bin:$PATH
    export PATH=$HEX_HOME/bin:$PATH
  '';

in mkShell {
  buildInputs = basePackages;
  shellHook = hooks;
}
