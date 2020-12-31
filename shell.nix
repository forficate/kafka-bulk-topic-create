with import <nixpkgs> {};

stdenv.mkDerivation {
  name = "env";

  buildInputs = [
    rustup
    darwin.apple_sdk.frameworks.Security
    rdkafka
    cmake
  ];
}
