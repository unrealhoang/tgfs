## tgfs

Telegram as filesystem — a Rust CLI that uses Telegram as a free, durable
storage backend, primarily for backups.

Folder-based, like git: `tgfs init` inside a folder creates a `.tgfs/`
directory and a private Telegram channel for it; every other command works
from anywhere inside that folder.

```
tgfs login               # authenticate the Telegram account (once per machine)
tgfs genkey <path>       # create a new 0600 key file without overwriting
tgfs init [--encrypt] [--key <k> | --keyfile <path>]
tgfs status [-v]         # local changes vs index, and local vs remote version
tgfs push [-v] [--key <k> | --keyfile <path>]
tgfs pull [--key <k> | --keyfile <path>]
tgfs ls [prefix]         # list indexed files
tgfs get <path> [dest] [--key <k> | --keyfile <path>]
tgfs log                 # list index snapshots
tgfs channels            # list this account's tgfs channels (repos to clone)
tgfs clone <name> [dir] [--key <k> | --keyfile <path>]
tgfs share <@user> [--write] | --link  # share this repo with another user
tgfs members             # list who has access
tgfs unshare <@user>     # remove access
```

Every push pins a versioned index snapshot in the channel; `status`, `push`
and `pull` compare the local index version against the pinned one, so two
machines backing up the same folder detect each other's pushes instead of
silently clobbering them.

Working-tree scans are streamed and use the `size + mtime` fast path. By
default, `status` lists at most 20 paths in each change category; use
`tgfs status -v` to stream every changed path. `push -v` similarly enables
one completion line per uploaded file. On a terminal, both commands show a
throttled progress line on stderr, leaving stdout pipeable.

Scans never apply `.gitignore` implicitly. Instead, tgfs reads
`.tgfsignore` files (gitignore syntax) at any directory level and the
`exclude` list in `.tgfs/config.toml`. Newly initialized repositories start
with these visible defaults:

```toml
exclude = [".git/", ".DS_Store", "Thumbs.db"]
```

Exclusions apply before tgfs stats or descends into a path. If a file is
already indexed and later becomes excluded, `status` reports it as deleted
and the next `push` tombstones it in the remote snapshot. Remove the rule
before pushing if that is not intended.

The advertised way to provide a key is through the environment, preferably
with a keyfile:

```sh
tgfs genkey key.txt
TGFS_KEYFILE=key.txt tgfs init --encrypt
TGFS_KEYFILE=key.txt tgfs push
TGFS_KEYFILE=key.txt tgfs pull
TGFS_KEYFILE=key.txt tgfs get path/to/file
```

`TGFS_KEY` supplies the base64 key directly; `TGFS_KEYFILE` supplies a path.
They are mutually exclusive and correspond to the `--key` and `--keyfile`
flags, which remain available as alternatives. A key file contains the base64
key printed by `init --encrypt` (surrounding whitespace is ignored). The secret
is never saved in `.tgfs/config.toml`; the config stores only a
domain-separated BLAKE3 verifier so a wrong key can be rejected locally.

On `init`, any supplied key requires `--encrypt`; without one, `--encrypt`
securely generates and prints a new key. On Unix, keyfiles with any group or
other permissions are rejected; use `chmod 600 <path>` or create one with
`genkey`.

### Building

With cargo: `cargo build --release`. With nix flakes:

```
nix build          # build the tgfs package
nix run . -- help  # run it
nix develop        # devshell with cargo, clippy, rustfmt, rust-analyzer
```

Status: early development. See [docs/SPEC.md](docs/SPEC.md) for the design
(Telegram API limits, chunked uploads, metadata index, versioning, optional
FUSE mount).
