{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    pre-commit-hooks = {
      url = "github:cachix/git-hooks.nix";
    };
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      pre-commit-hooks,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };

        preCommitCheck = pre-commit-hooks.lib.${system}.run {
          src = ./.;
          configPath = ".pre-commit-config-nix.yaml";
          default_stages = [ "pre-commit" ];
          hooks = {
            check-added-large-files = {
              enable = true;
              stages = [ "pre-commit" ];
            };
            check-case-conflicts = {
              enable = true;
              stages = [ "pre-commit" ];
            };
            check-json = {
              enable = true;
              stages = [ "pre-commit" ];
            };
            check-merge-conflicts = {
              enable = true;
              stages = [ "pre-commit" ];
            };
            check-yaml = {
              enable = true;
              stages = [ "pre-commit" ];
            };
            deadnix = {
              enable = true;
              stages = [ "pre-commit" ];
            };
            detect-private-keys = {
              enable = true;
              stages = [ "pre-commit" ];
            };
            end-of-file-fixer = {
              enable = true;
              stages = [ "pre-commit" ];
            };
            eslint = {
              enable = true;
              settings.binPath = "./node_modules/.bin/eslint";
              stages = [ "pre-commit" ];
            };
            markdownlint = {
              enable = true;
              settings.configuration.MD013 = false;
              stages = [ "pre-commit" ];
            };
            nixfmt-rfc-style = {
              enable = true;
              stages = [ "pre-commit" ];
            };
            prettier = {
              enable = true;
              settings.list-different = false;
              stages = [ "pre-commit" ];
            };
            pretty-format-json = {
              enable = true;
              settings.autofix = true;
              stages = [ "pre-commit" ];
            };
            statix = {
              enable = true;
              settings.ignore = [ ".direnv" ];
              stages = [ "pre-commit" ];
            };
            trim-trailing-whitespace = {
              enable = true;
              stages = [ "pre-commit" ];
            };
            trufflehog = {
              enable = true;
              stages = [ "pre-commit" ];
            };
          };
        };
      in
      {
        devShells.default = pkgs.mkShell {
          shellHook = ''
            ${preCommitCheck.shellHook}
          '';

          buildInputs =
            preCommitCheck.enabledPackages
            ++ (with pkgs; [
              nodejs_latest
              yarn
              serverless
            ]);
        };
      }
    );
}
