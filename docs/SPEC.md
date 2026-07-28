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
- One uploader actor owns retries and parallelism. On `FLOOD_WAIT_X` it pauses
  for the server-requested delay (up to 30 minutes) and reduces concurrency
  from four to one. Successful uploads then restore one slot per
  throttle-free minute; another flood resets the limit to one. Transient I/O
  errors retry with exponential backoff. Finalization repairs one
  `FILE_PART_X_MISSING`; a missing journaled-prefix part or a second missing
  part restarts the upload once under a fresh file id.
- Optional client-side encryption (`tgfs init --encrypt`), since Telegram
  cloud chats are not E2E-encrypted: XChaCha20-Poly1305 over independent
  1 MiB segments, with nonces derived (keyed BLAKE3) from the chunk's
  plaintext hash and segment number. Deterministic ciphertext keeps dedup
  and upload resume working. `tgfs init`, `push`, `pull`, `get`, and `clone`
  accept the key via `--key` or `--keyfile`, validating it before use.
  Index snapshots are sealed with the same key. Sizes, chunk counts and
  chunk equality remain visible — contents do not.
- `--key` and `--keyfile` are mutually exclusive. Key files contain the same
  base64 text as `--key`, with surrounding whitespace ignored. The secret is
  never persisted: `.tgfs/config.toml` stores only `encrypted = true` and a
  domain-separated BLAKE3 verifier used to reject a wrong key locally.
  `TGFS_KEY` and `TGFS_KEYFILE` populate the corresponding options and are the
  preferred interface, with `TGFS_KEYFILE` recommended for automation. On
  Unix, a keyfile must have no group or other permission bits (`0600` and
  `0400` are accepted).

## 3. Where to store metadata

Two layers, so the local machine is a cache and Telegram remains the source of
truth:

1. **Local index** — SQLite (`.tgfs/index.db` inside the synced folder):
   tables for `files` (path, size, mtime, file hash), `chunks` (hash, size,
   message_id), `snapshots` (push runs), and `meta` (index version, base
   remote version). Used for fast diffing during `tgfs push`.
2. **Remote index** — after each push, the index is serialized (JSON,
   zstd-compressed), uploaded to the same channel as a document, and the
   message is **pinned**, and the previously pinned snapshots are unpinned so
   the channel holds exactly one pinned index. Recovery on a new machine =
   find the pinned message, download, rebuild SQLite. The old snapshot
   messages themselves are kept, giving free point-in-time restore.

File `file_reference`s expire; the index stores enough (channel id +
message id) to re-fetch fresh references on demand.

### Index versioning

Every uploaded snapshot carries a monotonically increasing **version**. The
version is embedded in the pinned message's caption
(`tgfs-index v<N> files=<n> chunks=<n> created=<unix>`), so comparing local
and remote state only needs `messages.getPinnedMessage` — no snapshot
download. The local index remembers the last version it pushed or pulled:

- local == remote — **up to date**; `push` uploads version N+1.
- local < remote — **behind** (another machine pushed); `push` refuses until
  `tgfs pull` imports the newer remote snapshot (or `--force` overrides).
- local > remote — **ahead** (a push half-failed or the pin was changed);
  `push --force` re-publishes, `pull --force` rolls back.

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
tgfs genkey <path>       # create a new 0600 key file without overwriting
tgfs init [--encrypt] [--key <k> | --keyfile <path>]
tgfs status              # local changes vs index, and local vs remote version
tgfs push [--key <k> | --keyfile <path>]
tgfs pull [--key <k> | --keyfile <path>]
tgfs ls [prefix]         # list indexed files
tgfs get <path> [dest] [--key <k> | --keyfile <path>]
tgfs log                 # list index snapshots
tgfs channels            # list this account's tgfs channels (repos to clone)
tgfs clone <name> [dir] [--key <k> | --keyfile <path>]
tgfs share <@user> [--write] | --link  # share this repo (see §5)
tgfs members             # list who has access
tgfs unshare <@user>     # remove access
```

- `tgfs push` is idempotent and incremental (mtime+size fast path, hash to
  confirm), safe to run from cron/systemd-timer.
- Account credentials (api_id/api_hash) in `~/.config/tgfs/config.toml` and
  the MTProto session in `~/.local/share/tgfs/session.db` are per-machine;
  `.tgfs/config.toml` (channel, chunk size, encryption verifier) is per-repo.
  Encryption keys are never stored by tgfs.
- `tgfs init` in a folder whose channel already exists adopts it and pulls
  the pinned index — that is the recovery path on a new machine.
- Nice-to-haves after v1: `--watch` mode, include/exclude globs, `tgfs verify`
  (re-hash remote chunks), `tgfs prune` (drop tombstoned data).

## 5. Sharing with other users

Sharing rides entirely on Telegram's own access control — tgfs adds no
server, accounts, or ACLs of its own. A repo *is* a private broadcast
channel, so:

- **membership = read access** (any subscriber can download every chunk and
  the index snapshots);
- **admin with post+pin rights = write access** (`push` needs to post chunk
  documents and re-pin the index snapshot). In a broadcast channel
  non-admins cannot post, so Telegram itself enforces read-only — a
  reader's `tgfs push` fails server-side, while `status`/`pull`/`get` work.

```
tgfs share <@user> [--write]   # invite a user (writer = post+pin admin)
tgfs share --link              # create a read-only invite link instead
tgfs members                   # list members and their tgfs role
tgfs unshare <@user>           # demote and remove a user
```

Recipient flow: after joining, the channel shows up in `tgfs channels`, and
`tgfs clone <name>` adopts it — same path as a second machine of the owner.
Writers collaborate safely thanks to index versioning (§3): concurrent
pushes are detected and the loser pulls first.

Encryption interacts as expected:

- The key never touches Telegram or `.tgfs/config.toml`. For an encrypted repo
  the owner provides it out-of-band; commands accept it only through `--key`
  or `--keyfile` and validate it against the local verifier or encrypted
  snapshot.
- Channel membership without the key reveals only sizes, chunk counts and
  chunk equality — an encrypted repo can be "shared" with an untrusted
  relay (e.g. a bot that mirrors the channel) without exposing contents.

Caveats, stated honestly:

- Telegram privacy settings may forbid being invited directly
  (`USER_PRIVACY_RESTRICTED`); `share --link` is the fallback.
- Revoking (`unshare`) stops future access; it cannot claw back what was
  already downloaded, and the encryption key cannot be rotated yet — key
  rotation (re-encrypt + new snapshot lineage) is future work.
- Writers are admins: Telegram cannot stop a malicious writer from deleting
  messages history-wide. Share write access like you would push access to a
  git remote.

## 6. Optional: mount as a real filesystem

Out of scope for v1, but the design keeps it possible:

- FUSE via the [`fuser`](https://crates.io/crates/fuser) crate behind a
  `mount` cargo feature: `tgfs mount <mountpoint>`.
- Reads map directly onto `upload.getFile` offset/limit — random access works
  without downloading whole chunks; add an LRU block cache on disk.
- Read-only first; write support would reuse the push pipeline (write-back on
  close/fsync).

## 7. Milestones

- [x] **M1** — auth & channel bootstrap (`tgfs init`) with grammers.
- [x] **M2** — chunked upload/download of a single file (chunk-level resume:
      already-uploaded chunks are skipped on retry; part-level resume within a
      chunk is left to M4).
- [x] **M3** — SQLite index, `push`/`ls`/`get`, remote index snapshots.
- [x] **M4** — hardening: FLOOD_WAIT handling (coordinated uploader actor),
      parallel part uploads, part-level resume (journaled), client-side
      encryption, index restore from a pinned snapshot (`pull`/`clone`).
- [x] **M4.5** — sharing: `share`/`unshare`/`members` on top of channel
      membership and admin rights (§5).
- [ ] **M5 (optional)** — read-only FUSE mount.
