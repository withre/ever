{ inputs, system, channel ? "master" }:

# Zig toolchain used by this project.
#
# The default is the pinned `master` snapshot below -- the toolchain this
# project is verified to build and test against. Zig 0.16.0 is retired: the
# source uses std APIs it does not have, so it cannot compile this code, and
# defaulting to it would make `nix build` fail.
#
# The 0.16.0 path is deliberately kept reachable (`channel = "0.16.0"`) rather
# than deleted. The development line can change behaviour underneath us, and
# being able to drop back to a tagged release to compare is worth more than
# the few lines it costs. It is not a supported build.
#
# For tagged versions (`0.16.0`, future `0.17.0`) we go through
# mitchellh/zig-overlay. For the unreleased
# `master` development line we fetch the tarball directly from ziglang.org,
# because mitchellh/zig-overlay's `master.latest` lags upstream (its update
# bot can be several builds behind, sometimes missing the API surface we
# need). The version pinned here is overridable per-system via `master_*`
# attrs below; bump deliberately, alongside running the build.
let
  pkgs = inputs.nixpkgs.legacyPackages.${system};

  # Pinned master snapshot. Update both `version` and `sha256` together when
  # bumping; sha256 values are listed at https://ziglang.org/download/index.json
  # under the `master` key.
  master = {
    version = "0.17.0-dev.607+456b2ec07";
    x86_64-linux = {
      url = "https://ziglang.org/builds/zig-x86_64-linux-0.17.0-dev.607+456b2ec07.tar.xz";
      sha256 = "19275107de7b89ec33d29b50f00997c1381c524d1e33b728472dcbd551da2e33";
    };
    aarch64-linux = {
      url = "https://ziglang.org/builds/zig-aarch64-linux-0.17.0-dev.607+456b2ec07.tar.xz";
      sha256 = "96a1465b932e23eebcd9598c82d319d316b41529b6e0ef1bcff48eaf5e3cb15a";
    };
    x86_64-darwin = {
      url = "https://ziglang.org/builds/zig-x86_64-macos-0.17.0-dev.607+456b2ec07.tar.xz";
      sha256 = "3315ff00c1d90d2472c1bef7b583e3a1adb4b9160b3452aad828b077ad7dd5fa";
    };
    aarch64-darwin = {
      url = "https://ziglang.org/builds/zig-aarch64-macos-0.17.0-dev.607+456b2ec07.tar.xz";
      sha256 = "4f3143fa5a9723754b9516be6f9bc23fda2743abf1144570ae67ac875f5d2a09";
    };
  };

  masterFromUpstream = pkgs.stdenvNoCC.mkDerivation {
    pname = "zig";
    version = master.version;
    src = pkgs.fetchurl {
      url = master.${system}.url;
      sha256 = master.${system}.sha256;
    };
    dontConfigure = true;
    dontBuild = true;
    dontFixup = true;
    installPhase = ''
      mkdir -p $out/{bin,lib,doc}
      cp -r ./lib/* $out/lib/
      [ -d ./doc ] && cp -r ./doc/* $out/doc/ || true
      install -m755 ./zig $out/bin/zig
      # Match mitchellh/zig-overlay: redirect /usr/bin/env to coreutils env
      # so Zig's system-info path works under pure Nix builders.
      substituteInPlace $out/lib/std/zig/system.zig \
        --replace "/usr/bin/env" "${pkgs.lib.getExe' pkgs.coreutils "env"}"
    '';
    meta = {
      description = "Zig compiler (master, pinned from ziglang.org)";
      homepage = "https://ziglang.org";
      mainProgram = "zig";
    };
  };
in
if channel == "master" then masterFromUpstream
else inputs.zig-overlay.packages.${system}.${channel}
