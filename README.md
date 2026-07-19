## tgfs

Telegram as filesystem — a Rust CLI that uses Telegram as a free, durable
storage backend, primarily for backups.

Folder-based, like git: `tgfs init` inside a folder creates a `.tgfs/`
directory and a private Telegram channel for it; every other command works
from anywhere inside that folder.

```
tgfs login               # authenticate the Telegram account (once per machine)
tgfs init                # in the folder to back up: create .tgfs/ + the channel
tgfs status              # local changes vs index, and local vs remote version
tgfs sync                # push new/changed files, tombstone deleted, pin snapshot
tgfs pull                # import a newer remote index snapshot
tgfs ls [prefix]         # list indexed files
tgfs get <path> [dest]   # restore a file or folder
tgfs log                 # list index snapshots
tgfs channels            # list this account's tgfs channels (repos to clone)
tgfs clone <name> [dir]  # pull an existing channel's index into a new folder
```

Every sync pins a versioned index snapshot in the channel; `status`, `sync`
and `pull` compare the local index version against the pinned one, so two
machines backing up the same folder detect each other's pushes instead of
silently clobbering them.

Status: early development. See [docs/SPEC.md](docs/SPEC.md) for the design
(Telegram API limits, chunked uploads, metadata index, versioning, optional
FUSE mount).
