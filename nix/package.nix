{ pkgs, inputs, ... }:

let
  zig = inputs.zig-overlay.packages.${pkgs.stdenv.hostPlatform.system}.master;
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
