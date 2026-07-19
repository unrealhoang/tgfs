# tgfs — Telegram as File Storage

Initial specification. Goal: a Rust CLI that uses Telegram as a free, durable
storage backend, primarily for **backup** ("push a folder up, get it back
later"), with an optional real-filesystem mount later.

## 1. Telegram file upload API & limitations

There are two ways to talk to Telegram; the choice drives the whole design.

### 1.1 Bot API (`api.telegram.org`)

- Upload limit: **50 MB** per file via `sendDocument`.
- Download limit: **20 MB** via `getFile`.
- Both limits can be raised to **2 GB / unlimited download** by self-hosting
  the [local Bot API server](https://github.com/tdlib/telegram-bot-api), but
  that requires running an extra C++ daemon and your own `api_id`/`api_hash`
  anyway — at which point using MTProto directly is simpler.

### 1.2 MTProto (user account) — **chosen**

- Upload limit: **2 GiB** per file, **4 GiB** with Telegram Premium.
- Files are uploaded in parts via `upload.saveFilePart` (small files) or
  `upload.saveBigFilePart` (files > 10 MB). Part size must be a power of two,
  1 KiB ≤ part ≤ **512 KiB**; every part except the last must be full.
  Parts can be uploaded **in parallel** over multiple connections.
- Download via `upload.getFile`, also chunked, offset-addressable — this
  gives us random-access reads, which the optional FUSE mount needs.
- Rust client: [`grammers`](https://github.com/Lonami/grammers)
  (`grammers-client`), a maintained pure-Rust MTProto implementation.

### 1.3 Limitations to design around

| Constraint | Value | Consequence |
|---|---|---|
| Max file size | 2 GiB (4 GiB premium) | files larger than the cap are split into chunks |
| Part size | ≤ 512 KiB, power of two | streaming upload, no need to hold file in RAM |
| Rate limits | `FLOOD_WAIT_X` errors | retry with the server-given backoff; throttle parallelism |
| Caption length | 1024 chars (up to 4096 premium) | don't store metadata in captions; use a separate index |
| No server-side rename/edit of files | messages are immutable | metadata must live outside the file messages |
| Message deletion | anyone with admin rights can delete | store in a **private channel** owned by the user |
| Retention | indefinite, free | the reason this project exists |

Storage location: a dedicated **private channel** ("tgfs-storage"), created by
`tgfs init`. Channels give us: unlimited history, no 1-to-1 chat clutter,
stable message IDs, and access control.

## 2. How to structure uploads

- Files are content-addressed: split every file into **chunks** (default
  chunk size 1 GiB, configurable ≤ the account's cap), hash each chunk
  (BLAKE3) and the whole file.
- Each chunk is uploaded as one Telegram document named `<blake3-prefix>.bin`.
  The resulting `(message_id, file_reference)` pair is recorded in the index.
- Chunk-level **deduplication**: a chunk whose hash is already in the index is
  not re-uploaded (renames and duplicate files cost nothing).
- Uploads are resumable at the part level: chunks above 10 MiB go through
  Telegram's big-file path with 4 parallel 512 KiB part uploads; the
  contiguous prefix of confirmed parts is journaled in the local index
  (`upload_journal`), so an interrupted 2 GiB upload continues from where it
  stopped instead of restarting.
- Flood waits are handled by a client-wide retry policy: `FLOOD_WAIT_X` is
  slept out (up to 30 minutes, as instructed by the server) and transient
  I/O errors retry with exponential backoff.
- Optional client-side encryption (`tgfs init --encrypt`), since Telegram
  cloud chats are not E2E-encrypted: XChaCha20-Poly1305 over independent
  1 MiB segments, with nonces derived (keyed BLAKE3) from the chunk's
  plaintext hash and segment number. Deterministic ciphertext keeps dedup
  and upload resume working; the per-repo key lives in `.tgfs/config.toml`
  (`tgfs key` prints it; `tgfs clone`/`tgfs init` on a new machine take it via `--key` or prompt for it, validating it against the pinned snapshot).
  Index snapshots are sealed with the same key. Sizes, chunk counts and
  chunk equality remain visible — contents do not.

## 3. Where to store metadata

Two layers, so the local machine is a cache and Telegram remains the source of
truth:

1. **Local index** — SQLite (`.tgfs/index.db` inside the synced folder):
   tables for `files` (path, size, mtime, file hash), `chunks` (hash, size,
   message_id), `snapshots` (sync runs), and `meta` (index version, base
   remote version). Used for fast diffing during `tgfs sync`.
2. **Remote index** — after each sync, the index is serialized (JSON,
   zstd-compressed), uploaded to the same channel as a document, and the
   message is **pinned**. Recovery on a new machine = find the pinned message,
   download, rebuild SQLite. Old index snapshots are kept, giving free
   point-in-time restore.

File `file_reference`s expire; the index stores enough (channel id +
message id) to re-fetch fresh references on demand.

### Index versioning

Every uploaded snapshot carries a monotonically increasing **version**. The
version is embedded in the pinned message's caption
(`tgfs-index v<N> files=<n> chunks=<n> created=<unix>`), so comparing local
and remote state only needs `messages.getPinnedMessage` — no snapshot
download. The local index remembers the last version it pushed or pulled:

- local == remote — **up to date**; `sync` pushes version N+1.
- local < remote — **behind** (another machine pushed); `sync` refuses until
  `tgfs pull` imports the newer remote snapshot (or `--force` overrides).
- local > remote — **ahead** (a push half-failed or the pin was changed);
  `sync --force` re-publishes, `pull --force` rolls back.

This is git-flavored optimistic locking, not merging: concurrent writers are
detected, and the loser is told to pull first.

## 4. Interface

Folder-based CLI, git-flavored: `tgfs init` runs **inside the folder to back
up** and creates a `.tgfs/` directory (repo config + local index). Every
other command discovers the repo by walking up from the current directory,
and remote paths are relative to the repo root. Each repo gets its own
private channel, named `tgfs-<folder-name>`.

```
tgfs login               # authenticate the Telegram account (once per machine)
tgfs init [--encrypt]    # in the folder to back up: create .tgfs/ + the channel
tgfs status              # local changes vs index, and local vs remote version
tgfs sync                # push new/changed files, tombstone deleted, pin snapshot
tgfs pull                # import a newer remote index snapshot
tgfs ls [prefix]         # list indexed files
tgfs get <path> [dest]   # restore a file or folder from the channel
tgfs log                 # list index snapshots
tgfs channels            # list this account's tgfs channels (repos to clone)
tgfs clone <name> [dir] [--key <k>]  # pull an existing channel into a new folder
tgfs key                 # print this repo's encryption key
```

- `tgfs sync` is idempotent and incremental (mtime+size fast path, hash to
  confirm), safe to run from cron/systemd-timer.
- Account credentials (api_id/api_hash) in `~/.config/tgfs/config.toml` and
  the MTProto session in `~/.local/share/tgfs/session.db` are per-machine;
  `.tgfs/config.toml` (channel, chunk size) is per-repo. Secrets are stored
  with 0600 perms.
- `tgfs init` in a folder whose channel already exists adopts it and pulls
  the pinned index — that is the recovery path on a new machine.
- Nice-to-haves after v1: `--watch` mode, include/exclude globs, `tgfs verify`
  (re-hash remote chunks), `tgfs prune` (drop tombstoned data).

## 5. Optional: mount as a real filesystem

Out of scope for v1, but the design keeps it possible:

- FUSE via the [`fuser`](https://crates.io/crates/fuser) crate behind a
  `mount` cargo feature: `tgfs mount <mountpoint>`.
- Reads map directly onto `upload.getFile` offset/limit — random access works
  without downloading whole chunks; add an LRU block cache on disk.
- Read-only first; write support would reuse the sync pipeline (write-back on
  close/fsync).

## 6. Milestones

- [x] **M1** — auth & channel bootstrap (`tgfs init`) with grammers.
- [x] **M2** — chunked upload/download of a single file (chunk-level resume:
      already-uploaded chunks are skipped on retry; part-level resume within a
      chunk is left to M4).
- [x] **M3** — SQLite index, `sync`/`ls`/`get`, remote index snapshots.
- [x] **M4** — hardening: FLOOD_WAIT handling (client-wide retry policy),
      parallel part uploads, part-level resume (journaled), client-side
      encryption, index restore from a pinned snapshot (`pull`/`clone`).
- [ ] **M5 (optional)** — read-only FUSE mount.
