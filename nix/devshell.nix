{ pkgs, inputs, ... }:
pkgs.mkShell {
  packages = [
    inputs.zig-overlay.packages.${pkgs.stdenv.hostPlatform.system}.master
  ];
}
