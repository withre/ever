{ pkgs, inputs, ... }:

let
  system = pkgs.stdenv.hostPlatform.system;
  zig = import ./zig.nix { inherit inputs system; };
  zigCliKit = import ./zig-cli-kit-vendor.nix { inherit pkgs; };
in
pkgs.stdenv.mkDerivation {
  pname = "ever";
  version = "0.1.0";

  src = ./..;

  nativeBuildInputs = [ zig ];

  dontConfigure = true;

  buildPhase = ''
    runHook preBuild

    export XDG_CACHE_HOME="$TMPDIR/cache"
    mkdir -p "$XDG_CACHE_HOME"
    mkdir -p zig-pkg
    cp -R ${zigCliKit} zig-pkg/zig_cli_kit-0.1.0-oM7g3lgnAgAXGQvWuOvyLUdQG8PfMmQ5_3Apb5AY6f6l
    chmod -R u+w zig-pkg

    zig build \
      --cache-dir "$TMPDIR/zig-cache" \
      --global-cache-dir "$TMPDIR/zig-global-cache" \
      -Doptimize=ReleaseSafe

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall

    mkdir -p $out/bin
    cp zig-out/bin/ever $out/bin/

    runHook postInstall
  '';

  meta = {
    description = "Ever — lightweight, high-performance event storage";
    license = pkgs.lib.licenses.mit;
    mainProgram = "ever";
  };
}
