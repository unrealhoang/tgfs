{
  description = "tgfs — use Telegram as a file storage / backup backend";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages = rec {
          tgfs = pkgs.rustPlatform.buildRustPackage {
            pname = "tgfs";
            version = "0.1.0";

            src = pkgs.lib.cleanSource self;
            cargoLock.lockFile = ./Cargo.lock;

            # libsql-ffi (grammers session storage) compiles its bundled
            # sqlite via cmake and can regenerate bindings with bindgen;
            # zstd-sys / rusqlite / blake3 only need a C compiler.
            nativeBuildInputs = [
              pkgs.cmake
              pkgs.rustPlatform.bindgenHook
            ];

            # Tests are hermetic (no network); keep them on.
            doCheck = true;

            meta = {
              description = "Use Telegram as a file storage / backup backend";
              homepage = "https://github.com/unrealhoang/tgfs";
              license = pkgs.lib.licenses.mit;
              mainProgram = "tgfs";
            };
          };
          default = tgfs;
        };

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            cargo
            rustc
            clippy
            rustfmt
            rust-analyzer
            cmake
          ];
          inputsFrom = [ self.packages.${system}.tgfs ];

          env.RUST_BACKTRACE = "1";
        };

        apps = rec {
          tgfs = {
            type = "app";
            program = pkgs.lib.getExe self.packages.${system}.tgfs;
          };
          default = tgfs;
        };
      });
}
