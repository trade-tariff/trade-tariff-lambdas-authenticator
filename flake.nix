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
      self,
      nixpkgs,
      flake-utils,
      pre-commit-hooks,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };

        reviewEnvironment = ''
          if [ "''${REVIEW_SANDBOX:-}" != 1 ] || [ "$PWD" != /workspace ]; then exit 64; fi
          export TMPDIR=/workspace/.review/tmp PRE_COMMIT_HOME=/workspace/.review/pre-commit
          export npm_config_ignore_scripts=true YARN_IGNORE_SCRIPTS=true YARN_OFFLINE=true YARN_CACHE_FOLDER="$TMPDIR/yarn-cache"
          export CI=true AWS_EC2_METADATA_DISABLED=true
          mkdir -p "$TMPDIR"
        '';
        reviewTools = import (builtins.fetchTree {
          type = "github";
          owner = "NixOS";
          repo = "nixpkgs";
          rev = "4a29d733e8a7d5b824c3d8c958a946a9867b3eb2";
          narHash = "sha256-f+JACbTqzZ+G92DSnXOUGRhGANb8Blh7CoeYOeBF8/U=";
        }) { inherit system; };
        reviewMarkdownTools = import (builtins.fetchTree {
          type = "github";
          owner = "NixOS";
          repo = "nixpkgs";
          rev = "2bc1f07311cbe84e9dad955a32e275d43addd243";
          narHash = "sha256-aJfOPitYdBeun/Ml75utyN8dzM8atq+MZwHrqZ0mOx8=";
        }) { inherit system; };
        reviewPrettierTools = import (builtins.fetchTree {
          type = "github";
          owner = "NixOS";
          repo = "nixpkgs";
          rev = "c6e5ca3c836a5f4dd9af9f2c1fc1c38f0fac988a";
          narHash = "sha256-cY07EsdhBJ8tFXPzDYevgqxRev9ZLxFonuq9wmq5kwg=";
        }) { inherit system; };
        reviewLatestTools = import (builtins.fetchTree {
          type = "github";
          owner = "NixOS";
          repo = "nixpkgs";
          rev = "55bfb4d7437bd4e09ab7cba58a43bd68eb310044";
          narHash = "sha256-jBYGPi4udQ9uo4KQp9m2J0546nJVQitG15gvZSaVVAs=";
        }) { inherit system; };
        reviewPrettierSource = builtins.fetchTree {
          type = "github";
          owner = "JoC0de";
          repo = "pre-commit-prettier";
          rev = "b3e25fa39aa676c36bc18eb9eae6f26d9bb63f39";
          narHash = "sha256-PXLG4eyw1spZ5ZLNlhzm/86EVPKw73+G2nYcrU90EBE=";
        };
        reviewPrettier = pkgs.writeShellScriptBin "pre-commit-prettier" ''
          export NODE_PATH="${reviewPrettierTools.prettier}/lib/node_modules"
          exec ${reviewTools.nodejs}/bin/node ${reviewPrettierSource}/run-prettier.js "$@"
        '';
        reviewEslintModules =
          reviewTools.runCommand "eslint-9.38.0-modules"
            {
              nativeBuildInputs = [
                reviewTools.nodejs
                reviewTools.cacert
              ];
              outputHashMode = "recursive";
              outputHashAlgo = "sha256";
              outputHash = "sha256-6UYYMlzX7yVpdV072/Ja8QExEsWDYsAbJlpzXbN0YBw=";
            }
            ''
              export HOME="$TMPDIR/home" npm_config_cache="$TMPDIR/npm-cache"
              export npm_config_userconfig=/dev/null npm_config_registry=https://registry.npmjs.org
              mkdir -p "$HOME"
              npm install --prefix "$TMPDIR/deps" --ignore-scripts --no-audit --no-fund --omit=dev --before=2026-10-26T00:00:00Z eslint@9.38.0
              mkdir -p "$out"
              cp -a "$TMPDIR/deps/node_modules" "$out/"
            '';
        reviewEslint = pkgs.writeShellScriptBin "eslint" ''
          exec ${reviewTools.nodejs}/bin/node ${reviewEslintModules}/node_modules/eslint/bin/eslint.js "$@"
        '';
        reviewTrufflehogScan = pkgs.writeShellScript "trufflehog-pre-commit" ''
          set -euo pipefail
          exclude_file="$(mktemp)"
          trap 'rm -f "$exclude_file"' EXIT
          printf '%s\n' '(^|/)\.git(/|$)' '(^|/)\.terraform(/|$)' '(^|/)\.terragrunt-cache(/|$)' > "$exclude_file"
          while IFS= read -r ignored; do
            [ -n "$ignored" ] || continue
            escaped="$(printf '%s' "''${ignored%/}" | sed 's/[][(){}.^$*+?|\\]/\\&/g')"
            case "$ignored" in
              */) printf '(^|/)%s(/|$)\n' "$escaped" >> "$exclude_file" ;;
              *) printf '(^|/)%s$\n' "$escaped" >> "$exclude_file" ;;
            esac
          done < <(git ls-files --others --ignored --exclude-standard --directory)
          exec ${reviewLatestTools.trufflehog}/bin/trufflehog filesystem "$(pwd -P)" --exclude-paths="$exclude_file" --results=verified,unknown --fail
        '';
        reviewHook =
          id: entry: attrs:
          {
            inherit id entry;
            name = id;
            language = "system";
          }
          // attrs;
        reviewPreCommitConfig = pkgs.writeText "review-pre-commit.json" (
          builtins.toJSON {
            repos = [
              {
                repo = "local";
                hooks = [
                  (reviewHook "check-json" "${reviewTools.python3Packages.pre-commit-hooks}/bin/check-json" {
                    types = [ "json" ];
                  })
                  (reviewHook "check-merge-conflict"
                    "${reviewTools.python3Packages.pre-commit-hooks}/bin/check-merge-conflict"
                    { types = [ "text" ]; }
                  )
                  (reviewHook "check-yaml" "${reviewTools.python3Packages.pre-commit-hooks}/bin/check-yaml" {
                    types = [ "yaml" ];
                  })
                  (reviewHook "end-of-file-fixer"
                    "${reviewTools.python3Packages.pre-commit-hooks}/bin/end-of-file-fixer"
                    {
                      types = [ "text" ];
                      stages = [
                        "pre-commit"
                        "pre-push"
                        "manual"
                      ];
                    }
                  )
                  (reviewHook "pretty-format-json"
                    "${reviewTools.python3Packages.pre-commit-hooks}/bin/pretty-format-json"
                    {
                      types = [ "json" ];
                      args = [ "--autofix" ];
                    }
                  )
                  (reviewHook "trailing-whitespace"
                    "${reviewTools.python3Packages.pre-commit-hooks}/bin/trailing-whitespace-fixer"
                    {
                      types = [ "text" ];
                      stages = [
                        "pre-commit"
                        "pre-push"
                        "manual"
                      ];
                    }
                  )
                  (reviewHook "markdownlint-docker" "${reviewMarkdownTools.markdownlint-cli}/bin/markdownlint" {
                    types = [ "markdown" ];
                    args = [
                      "--disable"
                      "MD013"
                      "--fix"
                    ];
                  })
                  (reviewHook "eslint" "${reviewEslint}/bin/eslint" {
                    types = [ "javascript" ];
                    args = [ "--fix" ];
                    require_serial = false;
                  })
                  (reviewHook "prettier"
                    "${reviewPrettier}/bin/pre-commit-prettier --write --ignore-unknown --experimental-cli --no-cache"
                    {
                      types = [ "text" ];
                      exclude_types = [ "json" ];
                      args = [ "--write" ];
                      require_serial = false;
                    }
                  )
                  (reviewHook "trufflehog" "${reviewTrufflehogScan}" {
                    stages = [
                      "pre-commit"
                      "pre-push"
                    ];
                  })
                ];
              }
            ];
          }
        );

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
              package = pre-commit-hooks.inputs.nixpkgs.legacyPackages.${system}.nixfmt;
              enable = true;
              stages = [ "pre-commit" ];
            };
            prettier = {
              enable = true;
              exclude_types = [ "json" ];
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
        devShells.review = self.devShells.${system}.default.overrideAttrs (old: {
          shellHook = "";
          PRE_COMMIT_NO_CONCURRENCY = "1";
          GOMAXPROCS = "2";
          nativeBuildInputs = [
            pkgs.gitMinimal
            pkgs.jq
            pkgs.pre-commit
            (pkgs.writeShellApplication {
              name = "review-bootstrap";
              text = reviewEnvironment + ''
                printf 'yarn-offline-mirror "/deps/yarn-archives"\n' > "$TMPDIR/yarnrc"
                yarn --use-yarnrc "$TMPDIR/yarnrc" install --offline --frozen-lockfile --ignore-scripts
              '';
            })
            (pkgs.writeShellApplication {
              name = "review-check";
              text = reviewEnvironment + ''
                status=0
                pre-commit run --all-files --config ${reviewPreCommitConfig} || status=$?
                yarn test --runInBand --json --outputFile="$TMPDIR/unit.json" || status=$?
                jq -e '.numTotalTests > 0 and .numFailedTests == 0 and .numPendingTests == 0 and .wasInterrupted == false' "$TMPDIR/unit.json" || status=$?
                exit "$status"
              '';
            })
          ]
          ++ (old.nativeBuildInputs or [ ]);
          TZDIR = "${pkgs.tzdata.outPath}/share/zoneinfo";
          SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
        });

        devShells.default = pkgs.mkShell {
          shellHook = ''
            ${preCommitCheck.shellHook}
            export PATH=${pkgs.writeShellScriptBin "pre-commit" ''
              set -euo pipefail

              has_config=false
              for arg in "$@"; do
                case "$arg" in
                  -c|--config|--config=*)
                    has_config=true
                    ;;
                esac
              done

              if [ "$has_config" = true ]; then
                exec ${preCommitCheck.config.package}/bin/pre-commit "$@"
              fi

              if [ "''${1:-}" = "run" ]; then
                shift
                exec ${preCommitCheck.config.package}/bin/pre-commit run --config .pre-commit-config-nix.yaml "$@"
              fi

              exec ${preCommitCheck.config.package}/bin/pre-commit "$@"
            ''}/bin:$PATH
            export PATH="$PWD/node_modules/.bin:$PATH"
          '';

          buildInputs =
            preCommitCheck.enabledPackages
            ++ (with pkgs; [
              nodejs_latest
              yarn
            ]);
        };
      }
    );
}
