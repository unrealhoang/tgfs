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
- Uploads are resumable: part-level progress is journaled locally, so an
  interrupted 2 GiB upload continues instead of restarting.
- Optional (later): client-side encryption (age or XChaCha20-Poly1305) before
  upload, since Telegram cloud chats are not E2E-encrypted.

## 3. Where to store metadata

Two layers, so the local machine is a cache and Telegram remains the source of
truth:

1. **Local index** — SQLite (`~/.local/share/tgfs/index.db`): tables for
   `files` (path, size, mtime, file hash), `chunks` (hash, size, message_id,
   file_reference), `snapshots` (sync runs). Used for fast diffing during
   `tgfs sync`.
2. **Remote index** — after each sync, the index is serialized (JSON,
   zstd-compressed), uploaded to the same channel as a document, and the
   message is **pinned**. Recovery on a new machine = find the pinned message,
   download, rebuild SQLite. Old index snapshots are kept, giving free
   point-in-time restore.

File `file_reference`s expire; the index stores enough (channel id +
message id) to re-fetch fresh references on demand.

## 4. Interface

Easy-to-use CLI, rclone-flavored:

```
tgfs init                 # log in (QR/code), create the private storage channel
tgfs sync <folder>        # one-way backup: upload new/changed files, tombstone deleted
tgfs ls [prefix]          # list remote files
tgfs get <remote> [dest]  # restore a file or folder
```

- `tgfs sync` is idempotent and incremental (mtime+size fast path, hash to
  confirm), safe to run from cron/systemd-timer.
- Config in `~/.config/tgfs/config.toml` (api_id/api_hash, channel, chunk
  size, parallelism); session key stored with 0600 perms.
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

- **M1** — auth & channel bootstrap (`tgfs init`) with grammers.
- **M2** — chunked, resumable upload/download of a single file.
- **M3** — SQLite index, `sync`/`ls`/`get`, remote index snapshots.
- **M4** — hardening: FLOOD_WAIT handling, parallel parts, dedup, encryption.
- **M5 (optional)** — read-only FUSE mount.
