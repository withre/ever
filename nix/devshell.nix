{ pkgs, inputs, ... }:

let
  system = pkgs.stdenv.hostPlatform.system;
  # Pinned explicitly: zig.nix now defaults to `master`, so relying on the
  # default here would silently make this the 0.17 dev build.
  # 0.16.0 is retired and cannot compile the source -- kept on PATH as
  # `zig-0.16` only to compare behaviour against a tagged release.
  zig_0_16 = import ./zig.nix { inherit inputs system; channel = "0.16.0"; };
  zig_0_17 = import ./zig.nix { inherit inputs system; channel = "master"; };
  zig_0_16_bin = pkgs.writeShellScriptBin "zig-0.16" ''
    exec ${zig_0_16}/bin/zig "$@"
  '';
in
pkgs.mkShell {
  # Order matters: zig_0_17 first so its `bin/zig` wins on PATH.
  packages = [
    zig_0_17
    zig_0_16_bin
  ];
}
