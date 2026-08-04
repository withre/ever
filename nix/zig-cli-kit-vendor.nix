{ pkgs }:

pkgs.fetchFromGitHub {
  owner = "withre";
  repo = "zig-cli-kit";
  rev = "f9def36ebaf7742a6bafe4138a811322099326b6";
  hash = "sha256-h3pfr4P6UtTaQQtcQIdZwEJzTfLoKO7eUP2ZU+wXyPc=";
}
